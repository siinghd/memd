"""Stripe billing for hosted memd: Checkout, Billing Portal, the signed
webhook, the meter-event push and the daily reconciliation.

`stripe` (the memd[billing] extra) is imported lazily, only when a Stripe
call or a webhook verification actually happens. Structure follows the
design reference the spec names (snaplab's billing routes): checkout /
portal / webhook, subscription sync from the billed PRICE rather than echoed
metadata, idempotency by Stripe event id, and a per-org cursor so re-ordered
deliveries cannot roll a plan back.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Mapping

from memd.hosted.plans import GAUGES, METERS, STRIPE_UNIT_SCALE, Plans
from memd.hosted.store import AdminStore
from memd.metrics import METRICS

_log = logging.getLogger("memd.billing")

# labelled ns="_billing" (not a valid namespace name): the tenancy filter on
# /metrics then shows these series to operator keys only
_OPS = {"ns": "_billing"}

DEAD_STATUSES = frozenset({"canceled", "incomplete_expired"})
GRACE_STATUSES = frozenset({"past_due", "unpaid"})
# "no_payment_required": a 100%-off coupon or a trial
PAID_SESSION_STATUSES = frozenset({"paid", "no_payment_required"})
INTERVALS = {"month": "MONTHLY", "year": "YEARLY"}


class LiveKeyRefused(RuntimeError):
    """A live Stripe key without MEMD_ALLOW_LIVE_BILLING=1."""


class BillingError(Exception):
    def __init__(self, status: int, code: str, detail: str):
        super().__init__(detail)
        self.status, self.code, self.detail = status, code, detail


def guard_live_key(key: str | None, env: Mapping[str, str] | None = None) -> None:
    """Refuse live keys (secret or restricted) unless explicitly allowed:
    a live key on a dev box bills real cards."""
    env = os.environ if env is None else env
    if key and key.startswith(("sk_live_", "rk_live_")) and env.get("MEMD_ALLOW_LIVE_BILLING") != "1":
        raise LiveKeyRefused(
            "refusing to start hosted billing with a LIVE Stripe key; use an sk_test_ key, "
            "or set MEMD_ALLOW_LIVE_BILLING=1 on the production deployment")


@dataclass
class BillingConfig:
    secret_key: str = ""
    webhook_secret: str = ""
    api_base: str = ""
    webhook_tolerance_s: int = 300
    grace_days: float = 7.0
    drift_tolerance: float = 0.01
    public_url: str = "http://localhost:8700"
    success_url: str = ""
    cancel_url: str = ""
    return_url: str = ""
    env: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "BillingConfig":
        env = dict(os.environ if env is None else env)
        cfg = cls(
            secret_key=env.get("MEMD_STRIPE_SECRET_KEY", ""),
            webhook_secret=env.get("MEMD_STRIPE_WEBHOOK_SECRET", ""),
            api_base=env.get("MEMD_STRIPE_API_BASE", ""),
            webhook_tolerance_s=int(env.get("MEMD_STRIPE_WEBHOOK_TOLERANCE_S", "300")),
            grace_days=float(env.get("MEMD_BILLING_GRACE_DAYS", "7")),
            drift_tolerance=float(env.get("MEMD_BILLING_DRIFT_TOLERANCE", "0.01")),
            public_url=env.get("MEMD_PUBLIC_URL", "http://localhost:8700").rstrip("/"),
            env={k: v for k, v in env.items() if k.startswith("MEMD_STRIPE_")},
        )
        cfg.success_url = env.get("MEMD_BILLING_SUCCESS_URL") or (
            f"{cfg.public_url}/billing/success?session_id={{CHECKOUT_SESSION_ID}}")
        cfg.cancel_url = env.get("MEMD_BILLING_CANCEL_URL") or f"{cfg.public_url}/billing/cancel"
        cfg.return_url = env.get("MEMD_BILLING_RETURN_URL") or f"{cfg.public_url}/billing"
        guard_live_key(cfg.secret_key, env)
        return cfg

    @property
    def configured(self) -> bool:
        return bool(self.secret_key)

    def flat_price(self, plan: str, interval: str = "month") -> str:
        return self.env.get(f"MEMD_STRIPE_PRICE_{plan.upper()}_{INTERVALS.get(interval, '')}", "")

    def metered_price(self, plan: str, meter: str) -> str:
        return self.env.get(f"MEMD_STRIPE_PRICE_{plan.upper()}_{meter.upper()}", "")

    def event_name(self, meter: str) -> str:
        """The Stripe meter's event_name for a memd meter."""
        return self.env.get(f"MEMD_STRIPE_METER_{meter.upper()}", f"memd_{meter}")

    def meter_id(self, meter: str) -> str:
        return self.env.get(f"MEMD_STRIPE_METER_ID_{meter.upper()}", "")

    def plan_for_prices(self, price_ids: list[str], plans: Plans) -> str | None:
        """The plan a subscription's prices bill for: a flat (monthly or
        yearly) price decides; failing that, the plan owning the most of the
        metered prices (scale has no flat fee)."""
        ids = {p for p in price_ids if p}
        if not ids:
            return None
        best, best_n = None, 0
        for name in plans.paid_names():
            if any(self.flat_price(name, i) in ids for i in INTERVALS if self.flat_price(name, i)):
                return name
            n = sum(1 for m in METERS if self.metered_price(name, m) and self.metered_price(name, m) in ids)
            if n > best_n:
                best, best_n = name, n
        return best


def _price_ids(sub: Mapping[str, Any] | None) -> list[str]:
    items = ((sub or {}).get("items") or {}).get("data") or []
    out = []
    for it in items:
        price = it.get("price") or it.get("plan") or {}
        if isinstance(price, str):
            out.append(price)
        elif price.get("id"):
            out.append(price["id"])
    return out


def _period_end(sub: Mapping[str, Any] | None) -> int | None:
    if not sub:
        return None
    v = sub.get("current_period_end")
    if not v:
        items = (sub.get("items") or {}).get("data") or []
        v = items[0].get("current_period_end") if items else None
    try:
        return int(v) if v else None
    except (TypeError, ValueError):
        return None


class _Rollback(Exception):
    def __init__(self, result: dict):
        super().__init__(result.get("reason", "rollback"))
        self.result = result


class Billing:
    def __init__(self, store: AdminStore, plans: Plans, cfg: BillingConfig, client: Any = None):
        self.store = store
        self.plans = plans
        self.cfg = cfg
        self._client = client
        self._meter_ids: dict[str, str] = {}

    # ---------------------------------------------------------------- client

    @property
    def svc(self) -> Any:
        """The Stripe v1 service surface (lazy: first use imports stripe)."""
        if self._client is None:
            if not self.cfg.configured:
                raise BillingError(503, "billing_not_configured", "Stripe is not configured on this instance")
            import stripe  # memd[billing]

            kw: dict[str, Any] = {"max_network_retries": 2}
            if self.cfg.api_base:
                kw["base_addresses"] = {"api": self.cfg.api_base}
            self._client = stripe.StripeClient(self.cfg.secret_key, **kw)
        return getattr(self._client, "v1", self._client)

    # ---------------------------------------------------- checkout / portal

    def checkout(self, org: dict, plan_name: str, interval: str = "month") -> dict:
        if not self.cfg.configured:
            raise BillingError(503, "billing_not_configured", "Stripe is not configured on this instance")
        if plan_name not in self.plans.paid_names():
            raise BillingError(400, "invalid_plan", f"unknown or unpaid plan {plan_name!r}")
        if interval not in INTERVALS:
            raise BillingError(400, "invalid_interval", f"unknown billing interval {interval!r}")
        if (self.plans.get(org["plan"]).paid and org.get("stripe_subscription_id")
                and org.get("status") not in DEAD_STATUSES):
            raise BillingError(409, "already_subscribed",
                               "the org already has a subscription; change plans in the billing portal")
        plan = self.plans.get(plan_name)
        flat = self.cfg.flat_price(plan_name, interval)
        if interval == "year" and not flat:
            raise BillingError(503, "billing_not_configured", f"no yearly Stripe price for {plan_name!r}")
        missing = sorted(m for m in plan.metered if not self.cfg.metered_price(plan_name, m))
        if missing:
            # a metered meter without a price would be usage nobody can bill
            raise BillingError(503, "billing_not_configured",
                               f"no metered Stripe price for {plan_name!r}: {missing}")
        items: list[dict] = [{"price": flat, "quantity": 1}] if flat else []
        items += [{"price": self.cfg.metered_price(plan_name, m)} for m in sorted(plan.metered)]
        if not items:
            raise BillingError(503, "billing_not_configured", f"no Stripe prices for {plan_name!r}")
        meta = {"memd_org_id": org["id"], "plan": plan_name, "interval": interval}
        try:
            customer = self._ensure_customer(org)
            session = self.svc.checkout.sessions.create(params={
                "mode": "subscription",
                "customer": customer,
                "client_reference_id": org["id"],
                "line_items": items,
                "success_url": self.cfg.success_url,
                "cancel_url": self.cfg.cancel_url,
                "allow_promotion_codes": True,
                "metadata": meta,
                "subscription_data": {"metadata": meta},
            })
        except BillingError:
            raise
        except Exception as ex:
            _log.warning("memd billing: checkout failed for %s: %s", org["id"], ex)
            METRICS.inc("memd_billing_stripe_errors_total", op="checkout", **_OPS)
            raise BillingError(502, "billing_error", "could not start checkout") from None
        self.store.append_log(org["id"], "checkout_started", {"plan": plan_name, "interval": interval,
                                                              "session": session.id})
        return {"url": session.url, "id": session.id}

    def _ensure_customer(self, org: dict) -> str:
        if org.get("stripe_customer_id"):
            return org["stripe_customer_id"]
        # the idempotency key makes two racing checkouts create ONE customer
        cust = self.svc.customers.create(
            params={"name": org.get("name") or org["id"], "metadata": {"memd_org_id": org["id"]}},
            options={"idempotency_key": f"memd-customer-{org['id']}"})
        with self.store.txn() as con:
            con.execute("UPDATE org SET stripe_customer_id = ? WHERE id = ? AND stripe_customer_id IS NULL",
                        (cust.id, org["id"]))
            row = con.execute("SELECT stripe_customer_id FROM org WHERE id = ?", (org["id"],)).fetchone()
        return row["stripe_customer_id"]

    def portal(self, org: dict) -> dict:
        if not self.cfg.configured:
            raise BillingError(503, "billing_not_configured", "Stripe is not configured on this instance")
        if not org.get("stripe_customer_id"):
            raise BillingError(400, "no_customer", "no billing account yet; start with /v1/billing/checkout")
        try:
            s = self.svc.billing_portal.sessions.create(params={
                "customer": org["stripe_customer_id"], "return_url": self.cfg.return_url})
        except Exception as ex:
            _log.warning("memd billing: portal failed for %s: %s", org["id"], ex)
            METRICS.inc("memd_billing_stripe_errors_total", op="portal", **_OPS)
            raise BillingError(502, "billing_error", "could not open the billing portal") from None
        return {"url": s.url}

    # --------------------------------------------------------------- webhook

    def verify_webhook(self, payload: bytes, signature: str | None) -> dict:
        """stripe.Webhook.construct_event: HMAC-SHA256 over "{t}.{payload}"
        with the endpoint secret, constant-time compared, and a timestamp
        within the tolerance (default 300 s) - so a captured delivery cannot
        be replayed later. Returns the event as a plain dict."""
        if not self.cfg.webhook_secret:
            raise BillingError(503, "billing_not_configured", "webhook secret is not configured")
        if not payload or not signature:
            raise BillingError(400, "invalid_signature", "missing body or Stripe-Signature header")
        import stripe  # memd[billing]

        try:
            event = stripe.Webhook.construct_event(payload, signature, self.cfg.webhook_secret,
                                                   tolerance=self.cfg.webhook_tolerance_s)
        except (stripe.SignatureVerificationError, ValueError) as ex:
            METRICS.inc("memd_billing_webhook_rejected_total", **_OPS)
            _log.warning("memd billing: webhook rejected: %s", ex)
            raise BillingError(400, "invalid_signature", "signature verification failed") from None
        return event.to_dict()

    def handle_event(self, event: Mapping[str, Any]) -> dict:
        """Apply one Stripe event. The processed_events claim and the event's
        effects commit in ONE transaction: a re-delivery is a no-op, and an
        event whose effects failed (or asked to be retried) stays unclaimed so
        Stripe's retry is processed for real."""
        eid = str(event.get("id") or "")
        etype = str(event.get("type") or "")
        if not eid:
            return {"handled": False, "type": etype, "reason": "no_event_id"}
        try:
            created = int(event.get("created") or 0)
        except (TypeError, ValueError):
            created = 0
        obj = dict((event.get("data") or {}).get("object") or {})
        if self.store.processed_event(eid) is not None:
            METRICS.inc("memd_billing_webhook_total", type=etype, result="duplicate", **_OPS)
            return {"handled": False, "type": etype, "duplicate": True}
        sub = None
        if etype == "checkout.session.completed":
            # the Stripe call happens OUTSIDE the admin write transaction
            sub = self._subscription_of(obj)
        try:
            with self.store.txn() as con:
                if not AdminStore.claim_event(con, eid, etype, created or None):
                    result = {"handled": False, "type": etype, "duplicate": True}
                else:
                    result = self._apply(con, etype, obj, created, sub)
                    if result.get("retry"):
                        raise _Rollback(result)
                    AdminStore.finish_event(con, eid, result.get("org_id"),
                                            "handled" if result.get("handled") else result.get("reason", ""))
        except _Rollback as rb:
            result = rb.result
        METRICS.inc("memd_billing_webhook_total", type=etype,
                    result="duplicate" if result.get("duplicate") else
                    ("retry" if result.get("retry") else ("handled" if result.get("handled") else "ignored")),
                    **_OPS)
        return result

    def _subscription_of(self, session: Mapping[str, Any]) -> dict | None:
        sub = session.get("subscription")
        if isinstance(sub, Mapping):
            return dict(sub)  # expanded in the payload
        if not sub or not self.cfg.configured:
            return None
        # raises on a Stripe outage: the webhook answers 500 and Stripe retries
        return self.svc.subscriptions.retrieve(str(sub)).to_dict()

    @staticmethod
    def _org(con, org_id: str | None = None, customer: str | None = None) -> dict | None:
        row = None
        if customer:
            row = con.execute("SELECT * FROM org WHERE stripe_customer_id = ?", (customer,)).fetchone()
        if row is None and org_id:
            row = con.execute("SELECT * FROM org WHERE id = ?", (org_id,)).fetchone()
        return dict(row) if row is not None else None

    def _apply(self, con, etype: str, obj: dict, created: int, sub: dict | None) -> dict:
        now = created or int(time.time())
        grace_s = int(self.cfg.grace_days * 86400)
        customer = obj.get("customer") if isinstance(obj.get("customer"), str) else (
            (obj.get("customer") or {}).get("id"))
        meta = obj.get("metadata") or {}

        if etype == "checkout.session.completed":
            ref = obj.get("client_reference_id") or meta.get("memd_org_id")
            org = self._org(con, org_id=ref) if ref else self._org(con, customer=customer)
            if org is None:
                return {"handled": False, "type": etype, "reason": "unknown_org"}
            if org.get("stripe_customer_id") and customer and org["stripe_customer_id"] != customer:
                # a session naming this org but paid by another customer
                # (e.g. a payment link with a forged client_reference_id)
                # must not re-point the org's billing account
                AdminStore.log(con, org["id"], "customer_mismatch", {"session": obj.get("id")})
                return {"handled": False, "type": etype, "org_id": org["id"], "reason": "customer_mismatch"}
            sub_id = obj.get("subscription") if isinstance(obj.get("subscription"), str) else (sub or {}).get("id")
            if str(obj.get("payment_status") or "") not in PAID_SESSION_STATUSES:
                self.store.update_org(org["id"], con, status="incomplete",
                                      stripe_customer_id=customer or org.get("stripe_customer_id"),
                                      stripe_subscription_id=sub_id or org.get("stripe_subscription_id"))
                AdminStore.log(con, org["id"], "checkout_unpaid", {"session": obj.get("id")})
                return {"handled": False, "type": etype, "org_id": org["id"], "reason": "payment_incomplete"}
            # the plan is the one Stripe BILLS (the subscription's prices);
            # the metadata we wrote into the session is the fallback
            plan = self.cfg.plan_for_prices(_price_ids(sub), self.plans)
            if plan is None and meta.get("plan") in self.plans.paid_names():
                plan = meta["plan"]
            plan = plan or org["plan"]
            self.store.update_org(org["id"], con, plan=plan, status="active", grace_until=None,
                                  stripe_customer_id=customer or org.get("stripe_customer_id"),
                                  stripe_subscription_id=sub_id or org.get("stripe_subscription_id"),
                                  current_period_end=_period_end(sub) or org.get("current_period_end"))
            AdminStore.log(con, org["id"], "subscription_started", {"plan": plan, "session": obj.get("id")})
            return {"handled": True, "type": etype, "org_id": org["id"], "plan": plan}

        if etype in ("customer.subscription.created", "customer.subscription.updated",
                     "customer.subscription.deleted"):
            org = self._org(con, customer=customer)
            if org is None and meta.get("memd_org_id"):
                cand = self._org(con, org_id=meta["memd_org_id"])
                if cand and (not cand.get("stripe_customer_id") or cand["stripe_customer_id"] == customer):
                    org = cand
            if org is None:
                # the checkout webhook may still be in flight: 500 -> Stripe retries
                return {"handled": False, "type": etype, "reason": "unknown_org", "retry": True}
            if created and created < int(org.get("last_event_created") or 0):
                return {"handled": False, "type": etype, "org_id": org["id"], "reason": "stale_event"}
            status = "canceled" if etype.endswith(".deleted") else str(obj.get("status") or "active")
            dead = status in DEAD_STATUSES
            if dead:
                plan = "free"
            else:
                plan = self.cfg.plan_for_prices(_price_ids(obj), self.plans)
                if plan is None and meta.get("plan") in self.plans.paid_names():
                    plan = meta["plan"]
                plan = plan or org["plan"]
            if status in GRACE_STATUSES:
                grace = org.get("grace_until") or now + grace_s
            else:
                grace = None
            self.store.update_org(
                org["id"], con, plan=plan, status=status, grace_until=grace,
                stripe_customer_id=customer or org.get("stripe_customer_id"),
                stripe_subscription_id=None if dead else (obj.get("id") or org.get("stripe_subscription_id")),
                current_period_end=None if dead else _period_end(obj),
                last_event_created=max(created, int(org.get("last_event_created") or 0)))
            AdminStore.log(con, org["id"], etype.rsplit(".", 1)[1] + "_subscription",
                           {"plan": plan, "status": status, "subscription": obj.get("id")})
            return {"handled": True, "type": etype, "org_id": org["id"], "plan": plan, "status": status}

        if etype == "invoice.payment_failed":
            org = self._org(con, customer=customer)
            if org is None:
                return {"handled": False, "type": etype, "reason": "unknown_org"}
            if not self.plans.get(org["plan"]).paid:
                return {"handled": False, "type": etype, "org_id": org["id"], "reason": "not_on_paid_plan"}
            # the grace clock starts at the FIRST failure; Stripe's retries
            # of the same invoice must not extend it
            grace = org.get("grace_until") or now + grace_s
            self.store.update_org(org["id"], con, status="past_due", grace_until=grace)
            AdminStore.log(con, org["id"], "payment_failed", {"invoice": obj.get("id"), "grace_until": grace})
            return {"handled": True, "type": etype, "org_id": org["id"], "grace_until": grace}

        if etype in ("invoice.payment_succeeded", "invoice.paid"):
            org = self._org(con, customer=customer)
            if org is None:
                return {"handled": False, "type": etype, "reason": "unknown_org"}
            if org["status"] in GRACE_STATUSES or org["status"] == "incomplete" or org.get("grace_until"):
                self.store.update_org(org["id"], con, status="active", grace_until=None)
            AdminStore.log(con, org["id"], "payment_succeeded", {"invoice": obj.get("id")})
            return {"handled": True, "type": etype, "org_id": org["id"]}

        return {"handled": False, "type": etype, "reason": "ignored"}

    # ------------------------------------------------------------ meter push

    def push_usage(self, now: float | None = None) -> dict:
        """Claim unpushed ledger rows into batches, then send every pending
        batch as a Stripe meter event (identifier = idempotency key = batch
        id). At-least-once: a crash anywhere re-sends the same key, which
        Stripe deduplicates. Batches that fail stay pending for the next
        run."""
        claimed: dict[str, int] = {}
        for _ in range(100):  # bounded: at most 100 x 50K rows per run
            got = self.store.claim_push_batches(self.cfg.event_name, now=now, unit_scale=STRIPE_UNIT_SCALE)
            for k, v in got.items():
                claimed[k] = claimed.get(k, 0) + v
            if not any(got.values()):
                break
        sent = failed = 0
        for b in self.store.pending_pushes():
            try:
                self.svc.billing.meter_events.create(
                    params={"event_name": self.cfg.event_name(b["meter"]),
                            "payload": {"stripe_customer_id": b["customer"], "value": str(b["value"])},
                            "identifier": b["id"], "timestamp": int(b["ts"])},
                    options={"idempotency_key": b["id"]})
            except BillingError:
                raise
            except Exception as ex:
                failed += 1
                self.store.mark_push_failed(b["id"], f"{type(ex).__name__}: {ex}")
                METRICS.inc("memd_billing_push_failures_total", meter=b["meter"], **_OPS)
                _log.warning("memd billing: meter push %s failed: %s", b["id"], ex)
                continue
            self.store.mark_pushed(b["id"], now=now)
            sent += 1
            METRICS.inc("memd_billing_meter_events_pushed_total", meter=b["meter"], **_OPS)
        for reason in ("no_customer", "expired", "unmapped"):
            if claimed.get(reason):
                METRICS.inc("memd_billing_unbillable_events_total", claimed[reason], reason=reason, **_OPS)
        METRICS.set_gauge("memd_billing_unpushed_events", self.store.unpushed_count(), **_OPS)
        return {**claimed, "sent": sent, "failed": failed}

    # -------------------------------------------------------- reconciliation

    def _meter_id_for(self, meter: str) -> str:
        mid = self.cfg.meter_id(meter) or self._meter_ids.get(meter)
        if mid:
            return mid
        want = self.cfg.event_name(meter)
        for m in self.svc.billing.meters.list(params={"limit": 100}).data:
            if getattr(m, "event_name", None) == want:
                self._meter_ids[meter] = m.id
                return m.id
        return ""

    def reconcile(self, start: int, end: int) -> dict:
        """Compare what the ledger pushed for [start, end) with Stripe's
        meter event summaries, per (org, meter). |drift| above the tolerance
        raises memd_billing_drift_alerts_total and a billing_log entry.
        Counters compare sums; gauges (Stripe aggregation 'last') compare
        the last value."""
        rows, alerts = [], 0
        for pair in self.store.pushed_pairs(start, end):
            org_id, meter, customer = pair["org_id"], pair["meter"], pair["customer"]
            try:
                mid = self._meter_id_for(meter)
                if not mid:
                    rows.append({"org_id": org_id, "meter": meter, "error": "no_meter_id"})
                    continue
                summ = self.svc.billing.meters.event_summaries.list(
                    mid, params={"customer": customer, "start_time": start, "end_time": end})
                stripe_val = float(sum(float(s.aggregated_value) for s in summ.data))
            except Exception as ex:
                METRICS.inc("memd_billing_stripe_errors_total", op="reconcile", **_OPS)
                rows.append({"org_id": org_id, "meter": meter, "error": f"{type(ex).__name__}: {ex}"})
                continue
            if meter in GAUGES:
                ledger_val = float(self.store.last_pushed_gauge(org_id, meter, start, end) or 0)
            else:
                ledger_val = float(self.store.pushed_totals(org_id, meter, start, end)[1])
            drift = stripe_val - ledger_val
            rel = abs(drift) / max(ledger_val, 1.0)
            METRICS.set_gauge("memd_billing_drift_ratio", rel, meter=meter, **_OPS)
            row = {"org_id": org_id, "meter": meter, "ledger": ledger_val, "stripe": stripe_val,
                   "drift": drift, "drift_ratio": rel, "alert": rel > self.cfg.drift_tolerance}
            if row["alert"]:
                alerts += 1
                METRICS.inc("memd_billing_drift_alerts_total", meter=meter, **_OPS)
                _log.warning("memd billing: DRIFT org=%s meter=%s ledger=%s stripe=%s", org_id, meter,
                             ledger_val, stripe_val)
                self.store.append_log(org_id, "drift_alert", {k: row[k] for k in
                                                              ("meter", "ledger", "stripe", "drift")})
            rows.append(row)
        return {"start": start, "end": end, "pairs": rows, "alerts": alerts}
