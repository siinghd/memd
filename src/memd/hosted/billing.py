"""Stripe billing for hosted memd: Checkout, Billing Portal, the signed
webhook, the meter-event push and the daily reconciliation.

`stripe` (the memd[billing] extra) is imported lazily, only when a Stripe
call or a webhook verification actually happens. Structure: checkout /
portal / webhook, subscription sync from the billed PRICE rather than echoed
metadata, idempotency by Stripe event id, and a per-org cursor so re-ordered
deliveries cannot roll a plan back.
"""
from __future__ import annotations

import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Mapping

from memd.hosted.plans import GAUGES, METERS, STRIPE_UNIT_SCALE, Plans
from memd.hosted.store import (MAX_AUTO_PUSH_AGE_S, STRIPE_MAX_EVENT_AGE_S, AdminStore, period_bounds,
                               stripe_units)
from memd.metrics import METRICS

_log = logging.getLogger("memd.billing")

# labelled ns="_billing" (not a valid namespace name): the tenancy filter on
# /metrics then shows these series to operator keys only
_OPS = {"ns": "_billing"}

# subscription status -> entitlement. Only these grant the paid plan:
PAID_SUB_STATUSES = frozenset({"active", "trialing"})
# the plan is kept, the grace clock runs (then read-only)
GRACE_SUB_STATUSES = frozenset({"past_due"})
# no longer a subscription at all
DEAD_STATUSES = frozenset({"canceled", "incomplete_expired"})
# still the org's subscription (so a second one is a duplicate), but not
# entitling anything unless in PAID/GRACE: incomplete, unpaid, paused
LIVE_SUB_STATUSES = frozenset({"active", "trialing", "past_due", "unpaid", "incomplete", "paused"})
GRACE_STATUSES = GRACE_SUB_STATUSES  # org.status values the grace/read-only rules watch
# "no_payment_required": a 100%-off coupon or a trial
PAID_SESSION_STATUSES = frozenset({"paid", "no_payment_required"})
INTERVALS = {"month": "MONTHLY", "year": "YEARLY"}
# a Checkout Session memd creates expires after this (Stripe: 30 min - 24 h);
# while one is open, a second checkout is refused
CHECKOUT_TTL_S = 3600
_SECRET_ENV = ("MEMD_STRIPE_SECRET_KEY", "MEMD_STRIPE_WEBHOOK_SECRET")

_KEYLIKE = [
    (re.compile(r"\b((?:sk|rk|pk)_(?:live|test))_[A-Za-z0-9_]+"), r"\1_[REDACTED]"),
    (re.compile(r"\bwhsec_[A-Za-z0-9_]+"), "whsec_[REDACTED]"),
    (re.compile(r"\bmemd_[A-Za-z0-9_.-]+_[0-9a-f]{8}_[0-9a-f]{16,}"), "memd_[REDACTED]"),
    (re.compile(r"(?i)\b(bearer|basic)\s+[^\s'\",;]+"), r"\1 [REDACTED]"),
]


def redact(text: object) -> str:
    """Stripe error text can echo the API key ("Authorization was 'Bearer
    sk_test_...'"): nothing key-like is ever logged or stored."""
    out = str(text)
    for rx, sub in _KEYLIKE:
        out = rx.sub(sub, out)
    return out


class _RedactingFilter(logging.Filter):
    """The Stripe SDK logs response bodies at INFO/DEBUG, and an auth error's
    body echoes the key ("Authorization was 'Bearer sk_...'")."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
        except Exception:
            return True
        clean = redact(msg)
        if clean != msg:
            record.msg, record.args = clean, ()
        if record.exc_info and not record.exc_text:
            # formatters reuse a cached exc_text: cache the redacted one
            record.exc_text = redact(logging.Formatter().formatException(record.exc_info))
        return True


def install_log_redaction() -> None:
    for name in ("stripe", "memd.billing", "memd.hosted"):
        lg = logging.getLogger(name)
        if not any(isinstance(f, _RedactingFilter) for f in lg.filters):
            lg.addFilter(_RedactingFilter())


class LiveKeyRefused(RuntimeError):
    """A live Stripe key without MEMD_ALLOW_LIVE_BILLING=1."""


class BillingError(Exception):
    def __init__(self, status: int, code: str, detail: str, **extra: Any):
        super().__init__(detail)
        self.status, self.code, self.detail, self.extra = status, code, detail, extra


def guard_live_key(key: str | None, env: Mapping[str, str] | None = None) -> None:
    """Refuse live keys (secret or restricted) unless explicitly allowed:
    a live key on a dev box bills real cards. Whitespace around the key
    (a pasted newline) does not hide it."""
    env = os.environ if env is None else env
    if key and key.strip().startswith(("sk_live_", "rk_live_")) and env.get("MEMD_ALLOW_LIVE_BILLING") != "1":
        raise LiveKeyRefused(
            "refusing to start hosted billing with a LIVE Stripe key; use an sk_test_ key, "
            "or set MEMD_ALLOW_LIVE_BILLING=1 on the production deployment")


@dataclass
class BillingConfig:
    secret_key: str = field(default="", repr=False)
    webhook_secret: str = field(default="", repr=False)
    api_base: str = ""
    webhook_tolerance_s: int = 300
    max_push_age_s: int = MAX_AUTO_PUSH_AGE_S
    settle_s: int = 3600
    max_network_retries: int = 2
    grace_days: float = 7.0
    drift_tolerance: float = 0.01
    public_url: str = "http://localhost:8700"
    success_url: str = ""
    cancel_url: str = ""
    return_url: str = ""
    env: dict[str, str] = field(default_factory=dict, repr=False)  # prices and meters; no secrets

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "BillingConfig":
        env = dict(os.environ if env is None else env)
        guard_live_key(env.get("MEMD_STRIPE_SECRET_KEY", ""), env)
        for name in _SECRET_ENV:
            v = env.get(name, "")
            if any(c.isspace() for c in v):
                raise ValueError(f"{name} contains whitespace (a pasted newline?); refusing to start")
        tolerance = int(env.get("MEMD_STRIPE_WEBHOOK_TOLERANCE_S", "300"))
        if tolerance <= 0:
            # stripe.Webhook treats 0 as "no timestamp check": replays forever
            raise ValueError("MEMD_STRIPE_WEBHOOK_TOLERANCE_S must be > 0")
        cfg = cls(
            secret_key=env.get("MEMD_STRIPE_SECRET_KEY", ""),
            webhook_secret=env.get("MEMD_STRIPE_WEBHOOK_SECRET", ""),
            api_base=env.get("MEMD_STRIPE_API_BASE", ""),
            webhook_tolerance_s=tolerance,
            # capped below Stripe's ~24 h idempotency-key memory
            max_push_age_s=min(int(env.get("MEMD_BILLING_MAX_PUSH_AGE_S", str(MAX_AUTO_PUSH_AGE_S))),
                               MAX_AUTO_PUSH_AGE_S),
            settle_s=int(env.get("MEMD_BILLING_SETTLE_S", "3600")),
            max_network_retries=int(env.get("MEMD_STRIPE_MAX_NETWORK_RETRIES", "2")),
            grace_days=float(env.get("MEMD_BILLING_GRACE_DAYS", "7")),
            drift_tolerance=float(env.get("MEMD_BILLING_DRIFT_TOLERANCE", "0.01")),
            public_url=env.get("MEMD_PUBLIC_URL", "http://localhost:8700").rstrip("/"),
            env={k: v for k, v in env.items() if k.startswith("MEMD_STRIPE_") and k not in _SECRET_ENV},
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


def _object_id(v: Any) -> str | None:
    if isinstance(v, str):
        return v or None
    if isinstance(v, Mapping):
        return v.get("id")
    return None


def _customer_id(obj: Mapping[str, Any] | None) -> str | None:
    return _object_id((obj or {}).get("customer"))


def _invoice_subscription(inv: Mapping[str, Any]) -> str | None:
    """The subscription an invoice bills: `subscription` (older API
    versions) or parent.subscription_details.subscription (newer)."""
    sid = _object_id(inv.get("subscription"))
    if sid:
        return sid
    details = ((inv.get("parent") or {}).get("subscription_details") or {})
    return _object_id(details.get("subscription"))


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
        install_log_redaction()
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

            kw: dict[str, Any] = {"max_network_retries": self.cfg.max_network_retries}
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
        now = int(time.time())
        self._claim_checkout_slot(org["id"], now)
        meta = {"memd_org_id": org["id"], "plan": plan_name, "interval": interval}
        expires = now + CHECKOUT_TTL_S
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
                "expires_at": expires,
                "metadata": meta,
                "subscription_data": {"metadata": meta},
            })
        except Exception as ex:
            self.store.update_org(org["id"], checkout_session_id=None, checkout_url=None, checkout_expires_at=None)
            if isinstance(ex, BillingError):
                raise
            _log.warning("memd billing: checkout failed for %s: %s", org["id"], redact(ex))
            METRICS.inc("memd_billing_stripe_errors_total", op="checkout", **_OPS)
            raise BillingError(502, "billing_error", "could not start checkout") from None
        self.store.update_org(org["id"], checkout_session_id=session.id, checkout_url=session.url,
                              checkout_expires_at=expires)
        self.store.append_log(org["id"], "checkout_started", {"plan": plan_name, "interval": interval,
                                                              "session": session.id})
        return {"url": session.url, "id": session.id}

    def _claim_checkout_slot(self, org_id: str, now: int) -> None:
        """One subscription per org: refuse while one is live, and let only
        ONE Checkout Session be open at a time - claimed atomically, so two
        racing requests cannot both reach Stripe."""
        with self.store.txn() as con:
            o = dict(con.execute("SELECT * FROM org WHERE id = ?", (org_id,)).fetchone())
            if o.get("stripe_subscription_id") and o.get("status") in LIVE_SUB_STATUSES:
                raise BillingError(409, "already_subscribed",
                                   "the org already has a subscription; change plans in the billing portal")
            if o.get("checkout_expires_at") and int(o["checkout_expires_at"]) > now:
                raise BillingError(409, "checkout_pending",
                                   "a checkout for this org is already open; finish it or let it expire",
                                   url=o.get("checkout_url"), expires_at=o.get("checkout_expires_at"))
            # the slot is held (for the Stripe round trip) before any call
            con.execute("UPDATE org SET checkout_session_id = NULL, checkout_url = NULL, checkout_expires_at = ?"
                        " WHERE id = ?", (now + 120, org_id))

    def _ensure_customer(self, org: dict) -> str:
        if org.get("stripe_customer_id"):
            return org["stripe_customer_id"]
        # the metadata marks the customer as created by OUR checkout for THIS
        # org (webhooks bind a customer to an org only on that evidence); the
        # idempotency key makes two racing checkouts create ONE customer
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
            _log.warning("memd billing: portal failed for %s: %s", org["id"], redact(ex))
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
            _log.warning("memd billing: webhook rejected: %s", redact(ex))
            raise BillingError(400, "invalid_signature", "signature verification failed") from None
        return event.to_dict()

    def handle_event(self, event: Mapping[str, Any]) -> dict:
        """Apply one Stripe event. Stripe reads it needs (the subscription a
        session completed, a customer's metadata) happen first, outside the
        admin write transaction; a failed read raises (500: Stripe retries).
        The processed_events claim and the event's effects then commit in
        ONE transaction: a re-delivery is a no-op, and an event whose effects
        failed (or asked to be retried) stays unclaimed."""
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
        pre = self._prefetch(etype, obj)
        try:
            with self.store.txn() as con:
                if not AdminStore.claim_event(con, eid, etype, created or None):
                    result = {"handled": False, "type": etype, "duplicate": True}
                else:
                    result = self._apply(con, etype, obj, created, pre)
                    if result.get("retry"):
                        raise _Rollback(result)
                    AdminStore.finish_event(con, eid, result.get("org_id"),
                                            "handled" if result.get("handled") else result.get("reason", ""))
        except _Rollback as rb:
            result = rb.result
        if result.get("reason") == "unknown_customer":
            METRICS.inc("memd_billing_webhook_unmapped_total", type=etype, **_OPS)
        METRICS.inc("memd_billing_webhook_total", type=etype,
                    result="duplicate" if result.get("duplicate") else
                    ("retry" if result.get("retry") else ("handled" if result.get("handled") else "ignored")),
                    **_OPS)
        return result

    def _session_org(self, obj: Mapping[str, Any], con=None) -> dict | None:
        ref = obj.get("client_reference_id") or (obj.get("metadata") or {}).get("memd_org_id")
        if ref:
            if con is not None:
                row = con.execute("SELECT * FROM org WHERE id = ?", (str(ref),)).fetchone()
                return dict(row) if row is not None else None
            return self.store.get_org(str(ref))
        cust = _customer_id(obj)
        if not cust:
            return None
        if con is not None:
            row = con.execute("SELECT * FROM org WHERE stripe_customer_id = ?", (cust,)).fetchone()
            return dict(row) if row is not None else None
        return self.store.org_by_customer(cust)

    def _customer_org_id(self, customer: str) -> str | None:
        """The memd org OUR checkout created this Stripe customer for (the
        metadata _ensure_customer writes), or None."""
        c = self.svc.customers.retrieve(customer).to_dict()
        if c.get("deleted"):
            return None
        return (c.get("metadata") or {}).get("memd_org_id")

    def _prefetch(self, etype: str, obj: Mapping[str, Any]) -> dict:
        pre: dict[str, Any] = {}
        customer = _customer_id(obj)
        if etype == "checkout.session.completed":
            sid = _object_id(obj.get("subscription"))
            if obj.get("mode") == "subscription" and sid:
                # ALWAYS Stripe's current copy, never the payload's: a late
                # delivery must not apply a state the subscription has left
                pre["sub"] = self.svc.subscriptions.retrieve(sid).to_dict()
            org = self._session_org(obj)
            if org is not None and not org.get("stripe_customer_id") and customer:
                pre["customer_org"] = self._customer_org_id(customer)
        elif etype.startswith("customer.subscription.") and customer:
            org = self.store.org_by_customer(customer)
            if org is None:
                oid = (obj.get("metadata") or {}).get("memd_org_id")
                cand = self.store.get_org(str(oid)) if oid else None
                if cand is not None and not cand.get("stripe_customer_id"):
                    pre["customer_org"] = self._customer_org_id(customer)
            else:
                status = "canceled" if etype.endswith(".deleted") else str(obj.get("status") or "")
                if (status in DEAD_STATUSES and obj.get("id") == org.get("stripe_subscription_id")
                        and self.store.duplicates(org["id"])):
                    # the current subscription ends while duplicates are
                    # listed: Stripe's CURRENT copy of each decides which,
                    # if any, survives to become the org's subscription
                    pre["survivors"] = {}
                    for d in self.store.duplicates(org["id"]):
                        found = self._read_duplicate(d)
                        if found is not None:
                            pre["survivors"][d] = found
        return pre

    def _read_duplicate(self, sid: str) -> "dict | str | None":
        """Stripe's copy of one listed duplicate, read independently of the
        others: "missing" when Stripe no longer has it (404: treat as ended),
        None when it could not be read (it stays listed - and held - until
        its own next event). One bad duplicate never fails the webhook."""
        try:
            return self.svc.subscriptions.retrieve(sid).to_dict()
        except BillingError:
            raise
        except Exception as ex:
            if getattr(ex, "http_status", None) == 404 or getattr(ex, "code", None) == "resource_missing":
                return "missing"
            METRICS.inc("memd_billing_stripe_errors_total", op="read_duplicate", **_OPS)
            _log.warning("memd billing: could not re-read duplicate subscription %s: %s", sid, redact(ex))
            return None

    @staticmethod
    def _org(con, org_id: str | None = None, customer: str | None = None) -> dict | None:
        row = None
        if customer:
            row = con.execute("SELECT * FROM org WHERE stripe_customer_id = ?", (customer,)).fetchone()
        if row is None and org_id:
            row = con.execute("SELECT * FROM org WHERE id = ?", (org_id,)).fetchone()
        return dict(row) if row is not None else None

    def _may_bind(self, con, org: dict, customer: str | None, pre: dict, etype: str, ref: Any) -> bool:
        """An org is bound to a Stripe customer only if it has none yet AND
        the customer was created by our checkout for this org."""
        if org.get("stripe_customer_id"):
            if customer == org["stripe_customer_id"]:
                return True
            # e.g. a payment link with a forged client_reference_id
            AdminStore.log(con, org["id"], "customer_mismatch", {"event": etype, "object": ref})
            return False
        if customer and pre.get("customer_org") == org["id"]:
            return True
        AdminStore.log(con, org["id"], "customer_unverified", {"event": etype, "object": ref})
        METRICS.inc("memd_billing_customer_unverified_total", **_OPS)
        return False

    def _apply(self, con, etype: str, obj: dict, created: int, pre: dict) -> dict:
        now = created or int(time.time())
        grace_s = int(self.cfg.grace_days * 86400)
        customer = _customer_id(obj)
        meta = obj.get("metadata") or {}
        unmapped = {"handled": False, "type": etype, "reason": "unknown_customer"}

        if etype in ("checkout.session.completed", "checkout.session.expired"):
            org = self._session_org(obj, con)
            if org is None:
                return unmapped
            ours = org.get("checkout_session_id") == obj.get("id")
            if etype == "checkout.session.expired":
                if ours:
                    self.store.update_org(org["id"], con, checkout_session_id=None, checkout_url=None,
                                          checkout_expires_at=None)
                return {"handled": ours, "type": etype, "org_id": org["id"],
                        **({} if ours else {"reason": "not_the_open_checkout"})}
            if not self._may_bind(con, org, customer, pre, etype, obj.get("id")):
                return {"handled": False, "type": etype, "org_id": org["id"], "reason": "customer_unverified"}
            if ours:
                self.store.update_org(org["id"], con, checkout_session_id=None, checkout_url=None,
                                      checkout_expires_at=None)
            sub = pre.get("sub")
            if obj.get("mode") != "subscription" or not sub:
                # a payment-mode session (or one without a subscription) never
                # grants a plan nor clears past_due / grace
                return {"handled": False, "type": etype, "org_id": org["id"], "reason": "not_a_subscription"}
            if _customer_id(sub) != customer:
                AdminStore.log(con, org["id"], "subscription_customer_mismatch", {"subscription": sub.get("id")})
                return {"handled": False, "type": etype, "org_id": org["id"], "reason": "customer_mismatch"}
            return self._sync_subscription(con, org, sub, created, etype, customer, grace_s, now)

        if etype in ("customer.subscription.created", "customer.subscription.updated",
                     "customer.subscription.deleted"):
            org = self._org(con, customer=customer)
            if org is None:
                cand = self._org(con, org_id=str(meta["memd_org_id"])) if meta.get("memd_org_id") else None
                if cand is None:
                    return unmapped  # not a memd customer: ack, do not retry
                if not self._may_bind(con, cand, customer, pre, etype, obj.get("id")):
                    return {"handled": False, "type": etype, "org_id": cand["id"], "reason": "customer_unverified"}
                org = cand
            sub = dict(obj)
            if etype.endswith(".deleted"):
                sub["status"] = "canceled"
            return self._sync_subscription(con, org, sub, created, etype, customer, grace_s, now,
                                           survivors=pre.get("survivors"))

        if etype in ("invoice.payment_failed", "invoice.payment_succeeded", "invoice.paid"):
            org = self._org(con, customer=customer)
            if org is None:
                return unmapped
            inv_sub = _invoice_subscription(obj)
            if not inv_sub or inv_sub != org.get("stripe_subscription_id"):
                # a one-off invoice or another subscription's: it neither
                # starts nor ends the current subscription's grace
                return {"handled": False, "type": etype, "org_id": org["id"], "reason": "noncurrent_subscription"}
            if etype == "invoice.payment_failed":
                if not self.plans.get(org["plan"]).paid:
                    return {"handled": False, "type": etype, "org_id": org["id"], "reason": "not_on_paid_plan"}
                # the grace clock starts at the FIRST failure; Stripe's retries
                # of the same invoice must not extend it
                grace = org.get("grace_until") or now + grace_s
                self.store.update_org(org["id"], con, status="past_due", grace_until=grace)
                AdminStore.log(con, org["id"], "payment_failed", {"invoice": obj.get("id"), "grace_until": grace})
                return {"handled": True, "type": etype, "org_id": org["id"], "grace_until": grace}
            if org["status"] in GRACE_STATUSES or org.get("grace_until"):
                self.store.update_org(org["id"], con, status="active", grace_until=None)
            AdminStore.log(con, org["id"], "payment_succeeded", {"invoice": obj.get("id")})
            return {"handled": True, "type": etype, "org_id": org["id"]}

        return {"handled": False, "type": etype, "reason": "ignored"}

    def _sync_subscription(self, con, org: dict, sub: Mapping[str, Any], created: int, etype: str,
                           customer: str | None, grace_s: int, now: int,
                           survivors: Mapping[str, Mapping[str, Any]] | None = None) -> dict:
        """Apply a subscription's state to its org - one subscription per org.

        Only active/trialing grant the plan its prices bill; past_due keeps
        it while the grace clock runs; incomplete, unpaid, paused and the dead
        statuses grant nothing. Every other live subscription is listed as a
        duplicate (its metered usage would be billed twice, so pushes are
        held while ANY is listed) instead of replacing the current one; a
        non-current subscription ending never downgrades the org; when the
        current one ends, a duplicate Stripe reports live is promoted to
        current. Re-ordered older events are ignored."""
        sid = str(sub.get("id") or "")
        status = str(sub.get("status") or "")
        if created and created < int(org.get("last_event_created") or 0):
            return {"handled": False, "type": etype, "org_id": org["id"], "reason": "stale_event"}
        # the subscription's own history: an ended subscription stays ended
        # (canceled / incomplete_expired are final in Stripe), and an older
        # event of it cannot undo a newer one - for duplicates too
        st = AdminStore.subscription_state(con, sid)
        if st is not None:
            if st["ended_at"] is not None and status not in DEAD_STATUSES:
                return {"handled": False, "type": etype, "org_id": org["id"], "reason": "subscription_ended"}
            if created and created < int(st["last_event_created"] or 0):
                return {"handled": False, "type": etype, "org_id": org["id"], "reason": "stale_event"}
        AdminStore.note_subscription(con, org["id"], sid, created, ended=status in DEAD_STATUSES)
        cur = org.get("stripe_subscription_id")
        if cur != sid and (status not in LIVE_SUB_STATUSES or (cur and org.get("status") in LIVE_SUB_STATUSES)):
            if status in LIVE_SUB_STATUSES:
                if AdminStore.set_duplicate(con, org["id"], sid, True):
                    AdminStore.log(con, org["id"], "duplicate_subscription", {"current": cur, "duplicate": sid})
                    METRICS.inc("memd_billing_duplicate_subscriptions_total", **_OPS)
                    _log.warning("memd billing: org %s has another live subscription %s (current %s): metered "
                                 "pushes held until it is canceled", org["id"], sid, cur)
                return {"handled": False, "type": etype, "org_id": org["id"], "reason": "duplicate_subscription"}
            if AdminStore.set_duplicate(con, org["id"], sid, False):
                left = AdminStore.duplicates_in(con, org["id"])
                AdminStore.log(con, org["id"], "duplicate_resolved", {"subscription": sid, "still_listed": left})
                return {"handled": True, "type": etype, "org_id": org["id"], "reason": "duplicate_resolved"}
            AdminStore.log(con, org["id"], "noncurrent_subscription", {"subscription": sid, "status": status})
            return {"handled": False, "type": etype, "org_id": org["id"], "reason": "noncurrent_subscription"}

        dead = status in DEAD_STATUSES
        if dead and sid == cur and AdminStore.duplicates_in(con, org["id"]):
            promoted = self._promote_survivor(con, org, customer, survivors or {}, created)
            if promoted is not None:
                sub, sid, status, dead = promoted, str(promoted["id"]), str(promoted["status"]), False
        if not dead:
            AdminStore.set_duplicate(con, org["id"], sid, False)  # current is never also a duplicate
        if status in PAID_SUB_STATUSES or status in GRACE_SUB_STATUSES:
            plan = self.cfg.plan_for_prices(_price_ids(sub), self.plans)
            if plan is None and (sub.get("metadata") or {}).get("plan") in self.plans.paid_names():
                plan = sub["metadata"]["plan"]
            plan = plan or (org["plan"] if self.plans.get(org["plan"]).paid else "free")
        else:
            plan = "free"  # incomplete, unpaid, paused, canceled, incomplete_expired
        grace = (org.get("grace_until") or now + grace_s) if status in GRACE_SUB_STATUSES else None
        fields: dict[str, Any] = dict(
            plan=plan, status=status, grace_until=grace,
            stripe_customer_id=org.get("stripe_customer_id") or customer,
            stripe_subscription_id=None if dead else sid,
            current_period_end=None if dead else _period_end(sub),
            last_event_created=max(created, int(org.get("last_event_created") or 0)))
        self.store.update_org(org["id"], con, **fields)
        AdminStore.log(con, org["id"], etype.rsplit(".", 1)[1] + "_subscription",
                       {"plan": plan, "status": status, "subscription": sid})
        return {"handled": True, "type": etype, "org_id": org["id"], "plan": plan, "status": status}

    @staticmethod
    def _promote_survivor(con, org: dict, customer: str | None,
                          survivors: Mapping[str, Any], created: int = 0) -> dict | None:
        """The org's current subscription ended: the first listed duplicate
        that Stripe (re-read just before) reports live becomes current.
        Duplicates Stripe reports dead - or no longer has (404) - leave the
        list with a tombstone; one that could not be re-read stays listed
        (and held) until its own next event."""
        owner = org.get("stripe_customer_id") or customer
        chosen = None
        for d in AdminStore.duplicates_in(con, org["id"]):
            s = survivors.get(d)
            if s is None:
                continue
            if (s == "missing" or _customer_id(s) != owner
                    or str(s.get("status") or "") not in LIVE_SUB_STATUSES):
                AdminStore.set_duplicate(con, org["id"], d, False)
                if s == "missing" or str(s.get("status") or "") in DEAD_STATUSES:
                    AdminStore.note_subscription(con, org["id"], d, created, ended=True)
                continue
            if chosen is None:
                chosen = dict(s)
        if chosen is not None:
            AdminStore.set_duplicate(con, org["id"], str(chosen["id"]), False)
            AdminStore.log(con, org["id"], "duplicate_promoted", {"subscription": chosen["id"],
                                                                  "status": chosen.get("status")})
        return chosen

    # ------------------------------------------------------------ meter push

    def _send(self, b: Mapping[str, Any]) -> bool:
        try:
            self.svc.billing.meter_events.create(
                params={"event_name": self.cfg.event_name(b["meter"]),
                        "payload": {"stripe_customer_id": b["customer"], "value": str(b["value"])},
                        "identifier": b["id"], "timestamp": int(b["ts"])},
                options={"idempotency_key": b["id"]})
        except BillingError:
            raise
        except Exception as ex:
            self.store.mark_push_failed(b["id"], redact(f"{type(ex).__name__}: {ex}"))
            METRICS.inc("memd_billing_push_failures_total", meter=b["meter"], **_OPS)
            _log.warning("memd billing: meter push %s failed: %s", b["id"], redact(ex))
            return False
        METRICS.inc("memd_billing_meter_events_pushed_total", meter=b["meter"], **_OPS)
        return True

    def _retry_safe(self, b: Mapping[str, Any], now: int) -> bool:
        """Re-sending a batch is safe only while Stripe still remembers its
        key: its first send (created) and, for a regular batch, its oldest
        usage event are younger than max_push_age_s (20 h, inside Stripe's
        ~24 h window with margin)."""
        age = self.cfg.max_push_age_s
        if now - int(b["created"]) > age:
            return False
        return b.get("kind") == "reconcile" or now - int(b["first_ts"]) <= age

    def push_usage(self, now: float | None = None) -> dict:
        """Claim unpushed ledger rows into batches, then send every pending
        batch as a Stripe meter event (identifier = idempotency key = batch
        id). At-least-once: a crash anywhere re-sends the same key, which
        Stripe deduplicates. Batches that fail stay pending for the next run
        - but never past max_push_age_s: an older usage event is never
        pushed automatically (Stripe may have forgotten the key, so a retry
        could bill twice); it goes to reconcile_pending() instead."""
        now_i = int(time.time() if now is None else now)
        claimed: dict[str, int] = {}
        for _ in range(100):  # bounded: at most 100 x 50K rows per run
            got = self.store.claim_push_batches(self.cfg.event_name, now=now_i, unit_scale=STRIPE_UNIT_SCALE,
                                                max_age_s=self.cfg.max_push_age_s)
            for k, v in got.items():
                claimed[k] = claimed.get(k, 0) + v
            if not any(got.values()):
                break
        sent = failed = abandoned = 0
        for b in self.store.pending_pushes():
            if not self._retry_safe(b, now_i):
                self.store.abandon_batch(b["id"], now=now_i)
                abandoned += 1
                METRICS.inc("memd_billing_batches_abandoned_total", meter=b["meter"], **_OPS)
                continue
            if self._send(b):
                self.store.mark_pushed(b["id"], now=now_i,
                                       status="reconciled" if b.get("kind") == "reconcile" else "sent")
                sent += 1
            else:
                failed += 1
        for reason in ("no_customer", "unmapped"):
            if claimed.get(reason):
                METRICS.inc("memd_billing_unbillable_events_total", claimed[reason], reason=reason, **_OPS)
        METRICS.set_gauge("memd_billing_unpushed_events", self.store.unpushed_count(), **_OPS)
        METRICS.set_gauge("memd_billing_needs_reconcile_events", self.store.needs_reconcile_count(), **_OPS)
        return {**claimed, "sent": sent, "failed": failed, "abandoned": abandoned}

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

    def _stripe_total(self, meter: str, customer: str, start: int, end: int) -> float:
        mid = self._meter_id_for(meter)
        if not mid:
            raise LookupError(f"no Stripe meter found for {meter!r} (set MEMD_STRIPE_METER_ID_{meter.upper()})")
        summ = self.svc.billing.meters.event_summaries.list(
            mid, params={"customer": customer, "start_time": int(start), "end_time": int(end)})
        return float(sum(float(s.aggregated_value) for s in summ.data))

    def _undecidable(self, g: Mapping[str, Any], reason: str, **detail: Any) -> None:
        METRICS.inc("memd_billing_drift_alerts_total", meter=g["meter"], **_OPS)
        _log.warning("memd billing: DRIFT (reconcile undecidable, %s) org=%s meter=%s %s", reason,
                     g["org_id"], g["meter"], detail)
        self.store.append_log(g["org_id"], "drift_alert", {"meter": g["meter"], "reason": reason,
                                                           "events": len(g["ids"]), **detail})

    def reconcile_pending(self, now: float | None = None) -> dict:
        """Settle needs_reconcile usage (events too old to push on their own,
        and batches abandoned for the same reason) against what Stripe
        actually holds.

        Per (org, meter, customer, quota period): Stripe's meter summary for
        [period start, the group's last hour) is compared with what it should
        hold - every settled batch in that window plus these events. The
        difference is the verified missing quantity: 0 means the earlier
        attempts landed (nothing is sent), up to the group's own units is
        pushed under a FRESH idempotency key recorded before the send.
        Anything else (Stripe holds more than the ledger, or lacks more than
        these events) cannot be decided safely: drift alert, nothing sent,
        the events stay needs_reconcile. A window with a batch still pending
        or sent within settle_s (summaries lag) is deferred to the next run.
        Gauges are settled per event over its hour (Stripe aggregates them
        as 'last')."""
        now_i = int(time.time() if now is None else now)
        out = {"groups": 0, "reconciled": 0, "pushed_units": 0, "credited_units": 0, "deferred": 0,
               "undecidable": 0, "expired": 0, "failed": 0}
        for g in self.store.needs_reconcile_groups():
            out["groups"] += 1
            meter, customer = g["meter"], g["customer"]
            units = stripe_units(g["billable"], STRIPE_UNIT_SCALE.get(meter, 1))
            if not customer:
                self.store.close_events(g["ids"], "no_customer", now=now_i)
                continue
            if now_i - g["max_ts"] > STRIPE_MAX_EVENT_AGE_S:
                # Stripe refuses events this old: nothing can bill them now
                self.store.close_events(g["ids"], "expired", now=now_i)
                self._undecidable(g, "expired", units=units)
                out["expired"] += 1
                continue
            if meter in GAUGES:
                start = g["max_ts"] // 3600 * 3600
                end = start + 3600
            else:
                start = period_bounds(g["period"])[0]
                end = min(period_bounds(g["period"])[1], g["max_ts"] // 3600 * 3600 + 3600)
            if self.store.unsettled(g["org_id"], meter, customer, start, end, now_i - self.cfg.settle_s):
                out["deferred"] += 1
                continue
            try:
                in_stripe = self._stripe_total(meter, customer, start, end)
            except Exception as ex:
                METRICS.inc("memd_billing_stripe_errors_total", op="reconcile", **_OPS)
                _log.warning("memd billing: reconcile deferred for %s/%s: %s", g["org_id"], meter, redact(ex))
                out["deferred"] += 1
                continue
            settled = 0 if meter in GAUGES else self.store.settled_units(g["org_id"], meter, customer, start, end)
            missing = settled + units - in_stripe
            whole = round(missing)
            ok = abs(missing - whole) < 1e-6 and 0 <= whole <= units
            if meter in GAUGES:
                ok = ok and whole in (0, units)  # 'last': all there or not there at all
            if not ok:
                self._undecidable(g, "stripe_differs", expected=settled + units, stripe=in_stripe,
                                  settled=settled, units=units)
                out["undecidable"] += 1
                continue
            bid = self.store.create_reconcile_batch(g["org_id"], meter, customer, value=int(whole),
                                                    credited=units - int(whole), ts=g["max_ts"],
                                                    first_ts=g["min_ts"], ids=g["ids"], now=now_i)
            if whole == 0:
                self.store.mark_pushed(bid, now=now_i, status="reconciled")  # nothing to send
            else:
                batch = {"id": bid, "meter": meter, "customer": customer, "value": int(whole), "ts": g["max_ts"]}
                if not self._send(batch):
                    out["failed"] += 1  # stays pending: retried under the SAME fresh key
                    continue
                self.store.mark_pushed(bid, now=now_i, status="reconciled")
            out["reconciled"] += 1
            out["pushed_units"] += int(whole)
            out["credited_units"] += units - int(whole)
            self.store.append_log(g["org_id"], "reconciled", {"meter": meter, "units": units,
                                                              "pushed": int(whole), "batch": bid})
        METRICS.set_gauge("memd_billing_needs_reconcile_events", self.store.needs_reconcile_count(), **_OPS)
        return out

    def reconcile(self, start: int, end: int) -> dict:
        """The drift report: compare what the ledger settled for [start, end)
        with Stripe's meter event summaries, per (org, meter). |drift| above
        the tolerance raises memd_billing_drift_alerts_total and a
        billing_log entry. Counters compare sums; gauges (Stripe aggregation
        'last') compare the last value. The daily job reports the quota
        period to date, so a reconciliation push timestamped later in the
        period than the attempt it completes cannot show up as drift."""
        rows, alerts = [], 0
        for pair in self.store.settled_pairs(start, end):
            org_id, meter, customer = pair["org_id"], pair["meter"], pair["customer"]
            try:
                stripe_val = self._stripe_total(meter, customer, start, end)
            except Exception as ex:
                METRICS.inc("memd_billing_stripe_errors_total", op="reconcile", **_OPS)
                rows.append({"org_id": org_id, "meter": meter, "error": redact(f"{type(ex).__name__}: {ex}")})
                continue
            if meter in GAUGES:
                ledger_val = float(self.store.last_settled_gauge(org_id, meter, customer, start, end) or 0)
            else:
                ledger_val = float(self.store.settled_units(org_id, meter, customer, start, end))
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
