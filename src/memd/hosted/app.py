"""Hosted-mode wiring for the REST server: the org-aware key store, the
metering hooks, the /v1/billing routes and the background billing jobs.

memd.server.http imports this module only when hosted mode is on."""
from __future__ import annotations

import hmac
import importlib.util
import logging
import os
import threading
import time
from collections import OrderedDict
from typing import Any

from fastapi import Depends, Request
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from memd.hosted.billing import Billing, BillingConfig, BillingError
from memd.hosted.metering import Forbidden, Metering, QuotaDenied
from memd.hosted.plans import Plans
from memd.hosted.store import AdminStore, parse_scopes, period_bounds, period_of
from memd.metrics import METRICS
from memd.server.auth import Principal, hash_secret

_log = logging.getLogger("memd.hosted")
_OPS = {"ns": "_billing"}
# Stripe's events are a few KiB; nothing legitimate comes near this
WEBHOOK_MAX_BYTES = 1 << 20


class CheckoutIn(BaseModel):
    plan: str = Field(min_length=1, max_length=64)
    interval: str = "month"


class HostedKeyStore:
    """KeyStore-compatible adapter over the admin store: keys belong to an
    org and are bound to one of its namespaces; only SHA-256 hashes of the
    secrets are stored. The operator key (MEMD_ADMIN_KEY) stays org-less:
    it is never metered or billed. Legacy keys.toml.json keys are NOT
    honoured in hosted mode (they carry no org, so they would bypass
    metering) - adopt them with `memd key migrate --org ...`."""

    CACHE_TTL_S = 5.0  # bounded staleness for a revocation made by another process
    CACHE_MAX = 10_000

    def __init__(self, store: AdminStore, admin_key: str | None = None):
        self.store = store
        self._admin_hash = hash_secret(admin_key) if admin_key else None
        self._lock = threading.Lock()
        self._cache: "OrderedDict[str, tuple[Principal, float]]" = OrderedDict()

    def authenticate(self, bearer: str | None) -> Principal | None:
        if not bearer:
            return None
        h = hash_secret(bearer)
        if self._admin_hash and hmac.compare_digest(h, self._admin_hash):
            return Principal(key_id="admin", namespace="*", scope_override=True)
        now = time.monotonic()
        with self._lock:
            hit = self._cache.get(h)
            if hit is not None and now - hit[1] < self.CACHE_TTL_S:
                return hit[0]
        rec = self.store.key_for_bearer(bearer)
        if rec is None:
            with self._lock:
                self._cache.pop(h, None)
            return None
        scopes = frozenset(rec["scopes"].split())
        p = Principal(key_id=rec["key_id"], namespace=rec["ns"], pinned_user=rec.get("pinned_user"),
                      scope_override="override" in scopes, org_id=rec["org_id"], scopes=scopes)
        with self._lock:
            self._cache[h] = (p, now)
            self._cache.move_to_end(h)
            while len(self._cache) > self.CACHE_MAX:
                self._cache.popitem(last=False)
        return p

    def create(self, namespace: str, name: str = "", pinned_user: str | None = None,
               scope_override: bool = False, *, org_id: str, scopes: str | list[str] | None = None
               ) -> tuple[str, str]:
        sc = parse_scopes(scopes)
        if scope_override and "override" not in sc:
            sc.append("override")
        return self.store.create_key(org_id, namespace, name=name, scopes=sc, pinned_user=pinned_user)

    def revoke(self, kid: str) -> bool:
        ok = self.store.revoke_key(kid)
        with self._lock:
            for h, (p, _) in list(self._cache.items()):
                if p.key_id == kid:
                    self._cache.pop(h, None)
        return ok

    def list_keys(self) -> list[dict]:
        return self.store.list_keys()


class BillingJobs:
    """Background loop: the daily gauge snapshot, the hourly meter push, the
    settling of usage too old to push on its own, and the daily drift
    report. Every step is idempotent, so a restart mid-step is safe."""

    def __init__(self, hosted: "Hosted"):
        self.hosted = hosted
        self.push_interval_s = float(os.environ.get("MEMD_BILLING_PUSH_INTERVAL_S", "3600"))
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._snapshot_day: str | None = None
        self._reconciled_day: int | None = None

    def run_once(self, now: float | None = None) -> dict:
        h = self.hosted
        now = time.time() if now is None else now
        out: dict[str, Any] = {}
        day = time.strftime("%Y-%m-%d", time.gmtime(now))
        if day != self._snapshot_day:
            out["snapshot"] = h.metering.snapshot_gauges(now=now)
            self._snapshot_day = day
        if h.billing.cfg.configured:
            out["push"] = h.billing.push_usage(now=now)
            # every tick: usage too old to push on its own is settled against
            # what Stripe verifiably holds (a no-op when there is none)
            out["reconcile_pending"] = h.billing.reconcile_pending(now=now)
            today = int(now // 86400) * 86400
            if self._reconciled_day != today:
                # the drift report covers yesterday's quota period to date
                out["reconcile"] = h.billing.reconcile(period_bounds(period_of(today - 1))[0], today)
                self._reconciled_day = today
        return out

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception:
                METRICS.inc("memd_billing_job_failures_total", **_OPS)
                _log.exception("memd billing: background job failed")
            self._stop.wait(self.push_interval_s)

    def start(self) -> None:
        if self._thread is None and os.environ.get("MEMD_BILLING_JOBS", "1") != "0":
            self._thread = threading.Thread(target=self._loop, daemon=True, name="memd-billing-jobs")
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=10)


class Hosted:
    def __init__(self, data_dir: str, engine: Any, *, admin_key: str | None = None,
                 plans: Plans | None = None, billing_config: BillingConfig | None = None,
                 stripe_client: Any = None):
        # from_env runs the sk_live_ guard: a refused key stops startup here
        self.cfg = billing_config or BillingConfig.from_env()
        if (self.cfg.configured or self.cfg.webhook_secret) and importlib.util.find_spec("stripe") is None:
            raise RuntimeError("hosted billing is configured but the Stripe SDK is missing: "
                               "pip install 'memd[billing]'")
        self.plans = plans or Plans.from_env()
        self.store = AdminStore.for_data_root(data_dir)
        # one process per data root: reservations on disk are a dead process's
        self.store.clear_reservations()
        self.keystore = HostedKeyStore(self.store, admin_key=admin_key)
        self.metering = Metering(self.store, self.plans, engine)
        self.billing = Billing(self.store, self.plans, self.cfg, client=stripe_client)
        self.jobs = BillingJobs(self)
        if not self.cfg.configured:
            _log.warning("memd hosted: no MEMD_STRIPE_SECRET_KEY - tenancy and quotas are on, "
                         "billing endpoints answer 503 billing_not_configured")

    def close(self) -> None:
        self.jobs.stop()
        self.store.close()

    def install(self, app: Any, auth: Any) -> None:
        """Register the /v1/billing routes and the 402/403 handlers."""
        from memd.server.http import ApiError, _error

        hosted = self

        @app.exception_handler(QuotaDenied)
        async def quota_denied(_request: Request, exc: QuotaDenied):
            return _error(402, exc.detail, exc.code, extra=exc.extra)

        @app.exception_handler(Forbidden)
        async def forbidden(_request: Request, exc: Forbidden):
            return _error(403, exc.detail)

        @app.exception_handler(BillingError)
        async def billing_error(_request: Request, exc: BillingError):
            return _error(exc.status, exc.detail, exc.code, extra=exc.extra)

        def billing_org(p: Principal = Depends(auth)) -> dict:
            if p.org_id is None:
                raise ApiError(400, "billing routes act on the calling key's org; this key has none",
                               code="no_org")
            if "billing" not in p.scopes:  # exact: `override` does not imply it
                raise ApiError(403, "key lacks the 'billing' scope")
            org = hosted.store.get_org(p.org_id)
            if org is None:
                raise ApiError(404, "org not found")
            return org

        @app.post("/v1/billing/checkout")
        def checkout(body: CheckoutIn, org: dict = Depends(billing_org)):
            return hosted.billing.checkout(org, body.plan, body.interval)

        @app.post("/v1/billing/portal")
        def portal(org: dict = Depends(billing_org)):
            return hosted.billing.portal(org)

        @app.get("/v1/billing/usage")
        def usage(org: dict = Depends(billing_org)):
            return hosted.metering.usage_report(org["id"])

        @app.post("/v1/billing/webhook")
        async def webhook(request: Request):
            # unauthenticated by design: the Stripe-Signature HMAC over the
            # RAW body is the authentication (parsed JSON would not verify).
            # Capped by bytes actually READ: a chunked body has no
            # Content-Length for the global 8 MiB guard to see.
            buf = bytearray()
            async for chunk in request.stream():
                buf += chunk
                if len(buf) > WEBHOOK_MAX_BYTES:
                    METRICS.inc("memd_billing_webhook_rejected_total", reason="too_large", **_OPS)
                    return _error(413, "webhook body too large")
            event = hosted.billing.verify_webhook(bytes(buf), request.headers.get("stripe-signature"))
            try:
                result = await run_in_threadpool(hosted.billing.handle_event, event)
            except BillingError:
                raise
            except Exception:
                _log.exception("memd billing: webhook handler failed for %s", event.get("id"))
                METRICS.inc("memd_billing_webhook_total", type=str(event.get("type")), result="error", **_OPS)
                return _error(500, "webhook handler failed")  # Stripe re-delivers
            if result.get("retry"):
                return _error(500, "event not applicable yet; retry", "retry", extra={"result": result})
            return {"received": True, **result}
