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
from memd.pipeline.extractor import REPLY_FAILURES
from memd.storage.engine import NamespaceBusyError

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


def metered_extraction_turns(extraction: dict) -> int:
    """The turns of a session close that `extractions_our_key` counts: the
    turns the LLM extracted, plus the turns of a failed call that got a
    reply from the provider (a malformed, empty, truncated or oversize
    reply). The provider can bill such a call. A failed call with no reply
    (an HTTP error status, a transport error, a timeout) is not counted.
    A close result without `raw_failed_by_reason` (an older engine) counts
    no failed turn."""
    failed = int(extraction.get("raw_failed") or 0)
    by_reason = extraction.get("raw_failed_by_reason") or {}
    replied = sum(int(n or 0) for r, n in by_reason.items() if r in REPLY_FAILURES)
    return max(0, int(extraction.get("raw_considered") or 0) - failed + min(replied, failed))


class _NoAdmission:
    """An org-less principal (the operator key): nothing reserved or billed."""

    allow_rerank = True
    extract_limit: int | None = None
    max_facts: int | None = None

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        return None

    def record(self, **usage) -> list[str]:
        return []


NO_ADMISSION = _NoAdmission()


class Admission:
    """What the operation may do, and at most what it may record:
    `allow_rerank` (the reranked-search quota), `extract_limit` (raw records
    the extractor may see) and `max_facts` (facts a session close may
    write: None, or a callable the engine calls with the number of facts
    extracted, which reserves and returns how many may be written). None:
    unlimited. Every recorded quantity stays within what was reserved."""

    def __init__(self, metering: "Metering", p: Any, ns: str, reservation: str | None, *,
                 allow_rerank: bool = True, extract_limit: int | None = None,
                 max_facts: "int | Callable[[int], int] | None" = None):
        self.metering, self.p, self.ns, self.reservation = metering, p, ns, reservation
        self.allow_rerank, self.extract_limit, self.max_facts = allow_rerank, extract_limit, max_facts
        self.recorded = False
        if reservation is not None:
            metering._live_add(reservation)

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
        if self.reservation is not None:
            try:
                if not self.recorded:
                    self.metering.store.release(self.reservation)
            finally:
                self.metering._live_discard(self.reservation)
        return None


class Metering:
    # a namespace's live record count is re-read from the engine at most
    # this often; between reads it moves by the writes/deletes seen here
    ANCHOR_TTL_S = 300.0
    # a reservation outliving this (and no longer held by a running request
    # of this process) belongs to a crashed request
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
        # reservations of requests still running here: never expired, however
        # long the request takes (the TTL only reclaims a dead request's)
        self._live: set[str] = set()

    def _live_add(self, rid: str) -> None:
        with self._lock:
            self._live.add(rid)

    def _live_discard(self, rid: str) -> None:
        with self._lock:
            self._live.discard(rid)

    def _live_ids(self) -> frozenset[str]:
        with self._lock:
            return frozenset(self._live)

    # -------------------------------------------------------------- authz

    def authorize(self, p: Any, ns: str | None) -> None:
        """Tenant isolation, on top of the key's namespace binding: the
        namespace must belong to the key's org, and data routes need the
        `memory` scope (a billing-only key cannot read memories)."""
        org_id = getattr(p, "org_id", None)
        if org_id is None or ns is None:
            return
        if "memory" not in p.scopes:  # exact: `override` does not imply it
            raise Forbidden("key lacks the 'memory' scope")
        owner = self._owners.get(ns)
        if owner is None:
            owner = self.store.ns_owner(ns)
            if owner is not None:
                self._owners[ns] = owner
        if owner != org_id:
            METRICS.inc("memd_authz_denials_total", help="namespace authorization denials", reason="org")
            raise Forbidden(f"key not valid for namespace {ns!r}")

    def require_memory(self, p: Any) -> None:
        """/v1/status, /metrics, /v1/metrics/json describe a namespace's
        data and traffic: `memory` scope (or the operator key). A `billing`
        key reads /v1/billing/* only."""
        if getattr(p, "org_id", None) is not None and "memory" not in p.scopes:
            raise Forbidden("key lacks the 'memory' scope")

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

    def admit(self, p: Any, ns: str, *, writes: int = 0, searches: int = 0, extract: bool = False,
              session_id: str | None = None, user_id: str | None = None) -> "Admission | _NoAdmission":
        """Check and reserve. `writes`: records to add. `searches`: a search
        (a spent reranked quota only turns reranking off - allow_rerank -
        never refuses it). `extract`: a session close - the extraction
        allowance (at most the session's raw records) and the facts
        allowance (the memories cap) are reserved up front and handed to the
        engine as extract_limit / max_facts, so what is recorded can never
        exceed what was reserved."""
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

        def hard(meter: str) -> float | None:
            ent = plan.entitlement(meter)
            return ent.limit if ent.hard and ent.limit is not None else None

        # (meter, want, minimum, limit)
        checks: list[tuple[str, float, float, float]] = []
        extract_limit = None
        if writes:
            checks += [(m, writes, writes, hard(m)) for m in (MEMORIES_STORED, WRITES) if hard(m) is not None]
        if searches and hard(SEARCHES) is not None:
            checks.append((SEARCHES, searches, searches, hard(SEARCHES)))
        if extract:
            if self.our_key_extraction() and hard(EXTRACTIONS_OUR_KEY) is not None:
                size = self.engine.session_raw_count(session_id, user_id=user_id, namespace=ns) if session_id else 0
                # at least 1 of a non-empty session, or nothing to reserve
                checks.append((EXTRACTIONS_OUR_KEY, size, min(size, 1), hard(EXTRACTIONS_OUR_KEY)))
            if hard(MEMORIES_STORED) is not None:
                # a close writes facts: like any write it needs room for one.
                # Only that one is held while the extractor runs; the facts
                # are reserved once their number is known (_reserve_facts)
                checks.append((MEMORIES_STORED, 1, 1, hard(MEMORIES_STORED)))
        period = period_of(now)
        rid: str | None = None
        granted: dict[str, float] = {}
        if checks:
            live = any(m == MEMORIES_STORED for m, *_ in checks)
            if live:
                self.stored_records(org_id, ns)  # refresh a stale anchor outside the lock
            with self._stored_lock if live else _NULL_LOCK:
                base = self.stored_records(org_id, ns) if live else None
                rid, denied, granted = self.store.reserve(
                    org_id, period,
                    [(m, w, mn, lim, base if m == MEMORIES_STORED else None) for m, w, mn, lim in checks],
                    now=now, ttl_s=self.RESERVATION_TTL_S, live=self._live_ids())
            if denied is not None:
                METRICS.inc("memd_quota_denials_total", meter=denied["meter"], **_OPS)
                raise QuotaDenied("quota_exceeded", f"{plan.name} plan limit reached for {denied['meter']}",
                                  meter=denied["meter"], limit=denied["limit"], used=denied["used"],
                                  plan=plan.name)
            if extract and EXTRACTIONS_OUR_KEY in granted:
                extract_limit = int(granted[EXTRACTIONS_OUR_KEY])
        allow_rerank = True
        if searches and getattr(self.engine, "rerank", None) is not None and hard(RERANKED_SEARCHES) is not None:
            # a spent reranked quota never refuses the search: it is served
            # unreranked and only `searches` is metered
            rrid, denied, _ = self.store.reserve(
                org_id, period, [(RERANKED_SEARCHES, searches, searches, hard(RERANKED_SEARCHES), None)],
                now=now, ttl_s=self.RESERVATION_TTL_S, live=self._live_ids(), rid=rid)
            if denied is not None:
                allow_rerank = False
                METRICS.inc("memd_quota_denials_total", meter=RERANKED_SEARCHES, **_OPS)
            else:
                rid = rrid
        adm = Admission(self, p, ns, rid, allow_rerank=allow_rerank, extract_limit=extract_limit)
        if extract and hard(MEMORIES_STORED) is not None:
            limit = float(hard(MEMORIES_STORED))
            adm.max_facts = lambda n: self._reserve_facts(adm, org_id, period, limit, n)
        return adm

    def _reserve_facts(self, adm: "Admission", org_id: str, period: str, limit: float, n: int) -> int:
        """Called by the engine once extraction finished: resize the close's
        memories reservation to exactly the facts it may write - min(facts
        extracted, remaining headroom) - and return that number. Until this
        point the close held room for one, so writes alongside a running
        close are not starved by a guess."""
        with self._stored_lock:
            base = self.stored_records(org_id, adm.ns)
            _, denied, granted = self.store.reserve(
                org_id, period, [(MEMORIES_STORED, max(0, int(n)), 0, limit, base)],
                now=self.clock(), ttl_s=self.RESERVATION_TTL_S, live=self._live_ids(), rid=adm.reservation)
        if denied is not None:
            return 0
        return int(granted.get(MEMORIES_STORED, 0))

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
                usage[EXTRACTIONS_OUR_KEY] = metered_extraction_turns(extraction)
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
                try:
                    st = self.engine.stats(namespace=row["ns"])
                except NamespaceBusyError:
                    # cluster: another node holds it; its jobs record the
                    # measure every tick (measure_open), use the latest
                    METRICS.inc("memd_billing_measure_remote_total",
                                help="gauge snapshots that used a peer node's last measure", **_OPS)
                    recs += int(row.get("records") or 0)
                    nbytes += int(row.get("bytes") or 0)
                    continue
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

    def measure_open(self, now: float | None = None) -> int:
        """Record the size of every org namespace open on THIS node (cluster
        mode: only the leaseholder can read a namespace without taking it
        over). Never opens one. Returns how many were measured."""
        now = self.clock() if now is None else now
        n = 0
        for ns in self.engine.open_namespaces():
            if self.store.ns_owner(ns) is None:
                continue
            try:
                st = self.engine.stats(namespace=ns)
            except Exception:  # noqa: BLE001 - evicted or handed off meanwhile
                continue
            r = int(st.get("records", 0))
            b = int(st.get("segment_bytes", 0) or 0) + int(st.get("wal_bytes", 0) or 0)
            self.store.set_ns_measure(ns, r, b, at=int(now))
            n += 1
        return n

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
