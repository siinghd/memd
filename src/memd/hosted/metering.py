"""Entitlement enforcement and usage recording on the request path.

One admission per metered request, a no-op for principals without an org
(the operator key) and absent entirely when hosted mode is off:

    with metering.admit(p, ns, searches=1) as meter:   # check + reserve
        res = engine.search(...)                        # the operation
        meter.record(searches=1, reranked=res.reranked) # commit usage

  admit()   BEFORE the operation: read-only (grace expired) and hard caps
            -> QuotaDenied (402). The check and a RESERVATION of the
            requested quantity happen in one BEGIN IMMEDIATE transaction
            against the rollup plus every in-flight reservation, so
            concurrent requests at the cap boundary cannot all pass: exactly
            the remaining quantity is admitted. Deletes never call it: they
            are always allowed, whatever the plan or payment state.
  record()  AFTER the operation succeeded, BEFORE the response: the usage
            event, the rollup and the release of the reservation commit
            (fsynced) in one transaction, so every acknowledged operation is
            billed. A failed operation releases its reservation instead. A
            crash between the operation and this commit loses the event of an
            operation the client never saw acknowledged (its retry is the one
            billed); a crash after it bills an operation whose ack was lost -
            the at-least-once side, which the idempotent push turns into
            exactly one Stripe event per ledger row. A crashed request's
            reservation stops counting after RESERVATION_TTL_S.
"""
from __future__ import annotations

import contextlib
import threading
import time
from typing import Any, Callable

from memd.hosted.plans import (EXTRACTIONS_OUR_KEY, GAUGES, MEMORIES_STORED, METERS, RERANKED_SEARCHES,
                               SEARCHES, STORED_GB, WRITES, Plans)
from memd.hosted.store import AdminStore, period_bounds, period_of
from memd.metrics import METRICS

_OPS = {"ns": "_billing"}  # operator-only series (see billing._OPS)
_NULL_LOCK = contextlib.nullcontext()


class QuotaDenied(Exception):
    """402: over a hard cap (quota_exceeded) or read-only after the grace
    period (payment_required)."""

    def __init__(self, code: str, detail: str, **extra: Any):
        super().__init__(detail)
        self.status = 402
        self.code = code
        self.detail = detail
        self.extra = extra


class Forbidden(Exception):
    def __init__(self, detail: str):
        super().__init__(detail)
        self.detail = detail


class _NoAdmission:
    """An org-less principal (the operator key): nothing reserved or billed."""

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        return None

    def record(self, **usage) -> list[str]:
        return []


NO_ADMISSION = _NoAdmission()


class Admission:
    def __init__(self, metering: "Metering", p: Any, ns: str, reservation: str | None):
        self.metering, self.p, self.ns, self.reservation = metering, p, ns, reservation
        self.recorded = False

    def __enter__(self) -> "Admission":
        return self

    def record(self, *, writes: int = 0, searches: int = 0, reranked: bool | int = 0,
               extraction: dict | None = None) -> list[str]:
        ids = self.metering._record(self.p, self.ns, reservation=self.reservation, writes=writes,
                                    searches=searches, reranked=reranked, extraction=extraction)
        self.recorded = True
        return ids

    def __exit__(self, *exc) -> None:
        # the operation failed (or never recorded): give the quota back
        if not self.recorded and self.reservation is not None:
            self.metering.store.release(self.reservation)
        return None


class Metering:
    # a namespace's live record count is re-read from the engine at most
    # this often; between reads it moves by the writes/deletes seen here
    ANCHOR_TTL_S = 300.0
    # a reservation outliving this belongs to a crashed request
    RESERVATION_TTL_S = 600.0

    def __init__(self, store: AdminStore, plans: Plans, engine: Any,
                 clock: Callable[[], float] = time.time):
        self.store = store
        self.plans = plans
        self.engine = engine
        self.clock = clock
        self._lock = threading.Lock()
        # orders "read the live memories count + reserve" against "move the
        # count + release the reservation", so the two never interleave
        self._stored_lock = threading.Lock()
        self._counts: dict[str, list[float]] = {}  # ns -> [records, anchored_at (monotonic)]
        self._owners: dict[str, str] = {}  # ns -> org (ownership never moves)

    # -------------------------------------------------------------- authz

    def authorize(self, p: Any, ns: str | None) -> None:
        """Tenant isolation, on top of the key's namespace binding: the
        namespace must belong to the key's org, and data routes need the
        `memory` scope (a billing-only key cannot read memories)."""
        org_id = getattr(p, "org_id", None)
        if org_id is None or ns is None:
            return
        if "memory" not in p.scopes and not p.scope_override:
            raise Forbidden("key lacks the 'memory' scope")
        owner = self._owners.get(ns)
        if owner is None:
            owner = self.store.ns_owner(ns)
            if owner is not None:
                self._owners[ns] = owner
        if owner != org_id:
            METRICS.inc("memd_authz_denials_total", help="namespace authorization denials", reason="org")
            raise Forbidden(f"key not valid for namespace {ns!r}")

    # ---------------------------------------------------------- entitlements

    def read_only(self, org: dict, now: float | None = None) -> bool:
        """Past the grace period after a payment failure: searches and reads
        keep working, writes answer 402. Data is never dropped."""
        now = self.clock() if now is None else now
        return (org.get("status") in ("past_due", "unpaid")
                and org.get("grace_until") is not None and now >= org["grace_until"])

    def our_key_extraction(self) -> bool:
        """Extraction is metered only when it runs on the operator's LLM
        key (the dominant COGS line); the local heuristic costs nothing."""
        return bool(getattr(getattr(self.engine, "extractor", None), "api_key", None))

    def admit(self, p: Any, ns: str, *, writes: int = 0, searches: int = 0,
              extract: bool = False) -> "Admission | _NoAdmission":
        org_id = getattr(p, "org_id", None)
        if org_id is None:
            return NO_ADMISSION
        org = self.store.get_org(org_id)
        if org is None:
            raise Forbidden("the key's org no longer exists")
        now = self.clock()
        if (writes or extract) and self.read_only(org, now):
            METRICS.inc("memd_quota_denials_total", meter="read_only", **_OPS)
            raise QuotaDenied("payment_required",
                              "payment failed and the grace period is over: the org is read-only "
                              "(searches and deletes still work); update the payment method in the "
                              "billing portal", grace_until=org["grace_until"])
        plan = self.plans.get(org["plan"])
        checks: list[tuple[str, int]] = []
        if writes:
            checks += [(MEMORIES_STORED, writes), (WRITES, writes)]
        if searches:
            checks.append((SEARCHES, searches))
            if getattr(self.engine, "rerank", None) is not None:
                checks.append((RERANKED_SEARCHES, searches))
        if extract and self.our_key_extraction():
            checks.append((EXTRACTIONS_OUR_KEY, 1))
        period = period_of(now)
        hard = [(m, q, plan.entitlement(m).limit) for m, q in checks
                if plan.entitlement(m).hard and plan.entitlement(m).limit is not None]
        if not hard:
            return Admission(self, p, ns, None)
        live = any(m == MEMORIES_STORED for m, _, _ in hard)
        if live:
            self.stored_records(org_id, ns)  # refresh a stale anchor outside the lock
        with self._stored_lock if live else _NULL_LOCK:
            base = self.stored_records(org_id, ns) if live else None
            rid, denied = self.store.try_reserve(
                org_id, period, [(m, q, lim, base if m == MEMORIES_STORED else None) for m, q, lim in hard],
                now=now, ttl_s=self.RESERVATION_TTL_S)
        if denied is not None:
            METRICS.inc("memd_quota_denials_total", meter=denied["meter"], **_OPS)
            raise QuotaDenied("quota_exceeded", f"{plan.name} plan limit reached for {denied['meter']}",
                              meter=denied["meter"], limit=denied["limit"], used=denied["used"],
                              plan=plan.name)
        return Admission(self, p, ns, rid)

    # -------------------------------------------------------------- usage

    def _record(self, p: Any, ns: str, *, reservation: str | None, writes: int = 0, searches: int = 0,
                reranked: bool | int = 0, extraction: dict | None = None) -> list[str]:
        org_id = getattr(p, "org_id", None)
        org = self.store.get_org(org_id) if org_id is not None else None
        if org is None:
            if reservation is not None:
                self.store.release(reservation)
            return []
        usage = {WRITES: writes, SEARCHES: searches, RERANKED_SEARCHES: int(reranked)}
        stored = writes
        if extraction:
            if self.our_key_extraction():
                usage[EXTRACTIONS_OUR_KEY] = int(extraction.get("raw_considered") or 0)
            stored += int(extraction.get("facts_written") or 0)
        with self._stored_lock if stored else _NULL_LOCK:
            # the live count moves BEFORE the reservation is released: an
            # admission in between sees one or the other, never neither
            if stored:
                self.stored_delta(ns, stored)
            ids = self.store.record_usage(org_id, ns, usage, self.plans.get(org["plan"]), ts=self.clock(),
                                          reservation=reservation)
        for m, q in usage.items():
            if q:
                METRICS.inc("memd_billing_usage_total", q, meter=m, **_OPS)
        return ids

    # --------------------------------------------------- memories_stored gauge

    def _anchor(self, ns: str) -> int:
        n = 0
        if self.engine.has_namespace(ns):
            n = int(self.engine.stats(namespace=ns).get("records", 0))
        with self._lock:
            self._counts[ns] = [n, time.monotonic()]
        return n

    def stored_records(self, org_id: str, ns: str | None = None) -> int:
        """Live memories stored across the org's namespaces. The namespace
        being written is kept exact (anchored to the engine's count, moved by
        every write/delete seen here); the others use their last anchor or
        the daily snapshot."""
        total = 0
        for row in self.store.org_namespaces(org_id):
            name = row["ns"]
            with self._lock:
                c = self._counts.get(name)
            if name == ns:
                fresh = c is not None and time.monotonic() - c[1] < self.ANCHOR_TTL_S
                total += int(c[0]) if fresh else self._anchor(name)
            else:  # never open other namespaces on the request path
                total += int(c[0]) if c is not None else int(row.get("records") or 0)
        return total

    def stored_delta(self, ns: str, delta: int) -> None:
        with self._lock:
            c = self._counts.get(ns)
            if c is not None:
                c[0] = max(0, c[0] + delta)

    def stored_reset(self, ns: str) -> None:
        """The namespace was destroyed (crypto-shred): it stores nothing."""
        with self._lock:
            self._counts[ns] = [0, time.monotonic()]
        if self.store.ns_owner(ns) is not None:
            self.store.set_ns_measure(ns, 0, 0)

    def snapshot_gauges(self, now: float | None = None) -> dict:
        """The daily gauge snapshot: memories_stored and stored_gb per org.
        Idempotent per UTC day (see AdminStore.record_gauge)."""
        now = self.clock() if now is None else now
        day = time.strftime("%Y-%m-%d", time.gmtime(now))
        out = {}
        for org in self.store.list_orgs():
            if (self.store.gauge_recorded(org["id"], MEMORIES_STORED, day)
                    and self.store.gauge_recorded(org["id"], STORED_GB, day)):
                continue  # already taken today (a restart, another worker): skip the stats walk
            plan = self.plans.get(org["plan"])
            recs = nbytes = 0
            for row in self.store.org_namespaces(org["id"]):
                if not self.engine.has_namespace(row["ns"]):
                    continue
                st = self.engine.stats(namespace=row["ns"])
                r = int(st.get("records", 0))
                b = int(st.get("segment_bytes", 0) or 0) + int(st.get("wal_bytes", 0) or 0)
                self.store.set_ns_measure(row["ns"], r, b, at=int(now))
                with self._lock:
                    self._counts[row["ns"]] = [r, time.monotonic()]
                recs += r
                nbytes += b
            self.store.record_gauge(org["id"], MEMORIES_STORED, recs, plan, day, ts=now)
            self.store.record_gauge(org["id"], STORED_GB, round(nbytes / 1e9, 6), plan, day, ts=now)
            out[org["id"]] = {"memories_stored": recs, "stored_bytes": nbytes}
        return out

    # ------------------------------------------------------------- reporting

    def usage_report(self, org_id: str) -> dict:
        """GET /v1/billing/usage: the org's current-period usage per meter."""
        org = self.store.get_org(org_id) or {}
        plan = self.plans.get(org.get("plan"))
        now = self.clock()
        period = period_of(now)
        start, end = period_bounds(period)
        roll = self.store.rollups(org_id, period)
        meters = {}
        for m in METERS:
            ent = plan.entitlement(m)
            if m == MEMORIES_STORED:
                used = float(self.stored_records(org_id))
            else:
                used = float((roll.get(m) or {}).get("quantity", 0.0))
            meters[m] = {
                "used": used,
                "limit": ent.limit,
                "hard": ent.hard,
                "metered": m in plan.metered,
                "billable": float((roll.get(m) or {}).get("billable", 0.0)),
                "kind": "gauge" if m in GAUGES else "counter",
            }
            if m == STORED_GB:
                meters[m]["note"] = "daily snapshot"
        return {
            "org_id": org_id,
            "plan": plan.name,
            "status": org.get("status"),
            "period": {"key": period, "start": start, "end": end},
            "current_period_end": org.get("current_period_end"),
            "grace_until": org.get("grace_until"),
            "read_only": self.read_only(org, now) if org else False,
            "meters": meters,
        }
