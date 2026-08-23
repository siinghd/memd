"""memd.engine.memory - the Memory facade.

One engine, three doors (SDK / REST / MCP all call this). Embedded mode =
zero external services; hosted mode = same API over HTTP.

Write path: append to WAL -> fsync -> index apply -> ack. No LLM, no
embedding on the critical path (SLO: embedded p99 <= 10ms).
Read path: plan -> fan-out (vector+BM25+time+entity) -> RRF fuse ->
validity filter -> budget-aware packing with provenance tags.
"""
from __future__ import annotations

import json
import os
import queue
import threading
import time
from collections import OrderedDict
from contextlib import ExitStack as _ExitStack
from dataclasses import dataclass, field
from typing import Any

import numpy as np

from memd.core.schema import ExtractorInfo, Kind, MemoryRecord, Scope, Source, now_ms
from memd.index.sqlite_index import IndexFilter
from memd.metrics import METRICS, auto_dumper_from_env, preset_core
from memd.pipeline.consolidation import ConsolidationResult, QuarantinePolicy, consolidate_facts
from memd.pipeline.embedder import Embedder, resolve_embedder
from memd.pipeline.extractor import ExtractedFact, Extractor, resolve_extractor
from memd.query.fusion import rrf_fuse
from memd.query.packing import PackedContext, count_tokens, pack_context
from memd.query.planner import plan_query
from memd.storage.audit import AuditLog, BufferedAuditLog
from memd.storage.crypto import LocalKeyEnvelope, NullKeyEnvelope
from memd.storage.engine import StorageEngine
from memd.storage.objectstore import count_io

DEFAULT_BUDGET_TOKENS = 2000
HARD_DELETE_PURGE_MS = 72 * 3600 * 1000  # D7 #8 default physical-purge window


@dataclass
class SearchHit:
    id: str
    content: str
    kind: str
    source: str
    actor_id: str | None
    t_event: int
    valid: bool
    score: float
    lanes: list[str]
    entity_keys: list[str]
    namespace: str


@dataclass
class SearchResult:
    packed_context: str
    items: list[SearchHit]
    tokens_used: int
    budget: int
    truncated: bool
    query_class: str
    latency_ms: float


@dataclass
class SessionTaint:
    """D7: explicit writes inherit the session's lowest ingested trust tier -
    an agent processing web content cannot mint user-tier facts from it."""
    min_tier: int = 5
    lock: threading.Lock = field(default_factory=threading.Lock)

    def observe(self, tier: int) -> None:
        with self.lock:
            self.min_tier = min(self.min_tier, tier)


class TaintStore:
    """Bounded session-taint map: sessions are dropped on close and on LRU
    overflow (oldest first) so a long-lived process can't leak memory.
    Dict mutations are serialized: server threadpools hit get() concurrently."""

    def __init__(self, max_sessions: int = 10_000):
        self._taints: "dict[str, SessionTaint]" = {}
        self._lock = threading.Lock()
        self.max_sessions = max_sessions

    def get(self, session_id: str) -> SessionTaint:
        with self._lock:
            t = self._taints.get(session_id)
            if t is None:
                if len(self._taints) >= self.max_sessions:
                    oldest = next(iter(self._taints))
                    self._taints.pop(oldest, None)
                t = SessionTaint()
                self._taints[session_id] = t
            else:
                # refresh LRU position
                self._taints.pop(session_id, None)
                self._taints[session_id] = t
            return t

    def drop(self, session_id: str) -> None:
        with self._lock:
            self._taints.pop(session_id, None)

    def __len__(self) -> int:
        with self._lock:
            return len(self._taints)


MAX_CONTENT_BYTES = 10 * 1024 * 1024  # engine-side hard cap (REST caps lower)
MAX_META_BYTES = 64 * 1024  # meta is serialized into WAL + index per record
MAX_BATCH_EVENTS = 10_000  # single durable-append bound per add_events call


def _guard_input(content: str, meta: dict[str, Any] | None) -> None:
    if len(content.encode("utf-8", errors="replace")) > MAX_CONTENT_BYTES:
        raise ValueError(f"content exceeds {MAX_CONTENT_BYTES} byte cap")
    if meta:
        try:
            blob = json.dumps(meta)
        except (TypeError, ValueError) as e:
            raise ValueError(f"meta not JSON-serializable: {e}") from e
        if len(blob.encode()) > MAX_META_BYTES:
            raise ValueError(f"serialized meta exceeds {MAX_META_BYTES} byte cap")


def _guard_kind(kind: str) -> str:
    """Schema-integrity guard at the engine boundary (all three doors):
    an unvalidated kind string would silently break every kind-keyed
    behavior downstream (planner lane filters, packing bonuses, stats)."""
    if kind not in Kind.ALL:
        raise ValueError(f"unknown kind {kind!r}; expected one of {list(Kind.ALL)}")
    return kind


class _EmbedWorker:
    """Async batched embedder: write ack never waits on embeddings.

    The queue is BOUNDED (max_queue texts). When the embedder falls behind a
    write burst (e.g. BYO API outage), submit() drops the embedding work for
    the overflow instead of buffering unbounded content strings in RAM - the
    record stays fully searchable via BM25/entity lanes and the vector lane
    heals via reembed() (the ADR-8 degradation story, now memory-safe)."""

    def __init__(self, embedder: Embedder, apply_fn, batch_size: int = 32, flush_s: float = 0.5,
                 max_retries: int = 3, max_queue: int = 5_000):
        self.embedder = embedder
        self.apply_fn = apply_fn  # callable(ns_name, ids, vecs)
        self.q: "queue.Queue[tuple[str, str, str]]" = queue.Queue(maxsize=max(1, max_queue))
        self.retries: dict[tuple[str, str], int] = {}
        self.max_retries = max_retries
        self.batch_size = batch_size
        self.flush_s = flush_s
        self._stop = threading.Event()
        # Batches are dequeued BEFORE they are embedded, so queue-empty alone
        # never meant "the work is done" - drain() returned while a batch was
        # still in flight and flush() inherited that broken promise.
        self._inflight = 0
        self._inflight_lock = threading.Lock()
        self._thread = threading.Thread(target=self._run, daemon=True, name="memd-embed")
        self._thread.start()

    def submit(self, ns_name: str, record_id: str, text: str) -> bool:
        """Enqueue embedding work; False when dropped (queue full) - callers
        treat that as 'vector lane deferred', never an error."""
        try:
            self.q.put_nowait((ns_name, record_id, text))
        except queue.Full:
            METRICS.inc("memd_embed_backlog_dropped_total", help="embeddings dropped: backlog over capacity")
            return False
        METRICS.set_gauge("memd_embed_queue_depth", self.q.qsize(), help="pending embedding texts")
        return True

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                ns_name, rid, text = self.q.get(timeout=self.flush_s)
            except queue.Empty:
                continue
            # batch PER NAMESPACE: vectors belong to the namespace that owns
            # the record - a shared cross-namespace batch would misfile them.
            # Common case is one bucket; batching efficiency is unchanged.
            batches: dict[str, dict[str, str]] = {ns_name: {rid: text}}
            while sum(len(b) for b in batches.values()) < self.batch_size:
                try:
                    ns_name, rid, text = self.q.get_nowait()
                except queue.Empty:
                    break
                batches.setdefault(ns_name, {})[rid] = text
            METRICS.set_gauge("memd_embed_queue_depth", self.q.qsize())
            with self._inflight_lock:
                self._inflight += 1
            try:
                for ns_bucket, batch in batches.items():
                    self._embed_one(ns_bucket, batch)
            finally:
                with self._inflight_lock:
                    self._inflight -= 1

    def _embed_one(self, ns_name: str, batch: dict[str, str]) -> None:
        t0 = time.monotonic()
        try:
            vecs = self.embedder.embed(list(batch.values()))
            self.apply_fn(ns_name, list(batch.keys()), vecs)
            METRICS.observe("memd_embed_batch_size", len(batch),
                            help="texts per embedding batch",
                            buckets=(1, 4, 8, 16, 32, 64, 128))
            METRICS.observe("memd_embed_apply_ms", (time.monotonic() - t0) * 1000,
                            help="embed + index apply duration (ms)")
            for rid in batch:
                self.retries.pop((ns_name, rid), None)
        except Exception:
            METRICS.inc("memd_embed_failures_total")
            # bounded retries: a poison text must not loop forever
            for rid in batch:
                key = (ns_name, rid)
                n = self.retries.get(key, 0) + 1
                if n <= self.max_retries:
                    self.retries[key] = n
                    try:
                        self.q.put_nowait((ns_name, rid, batch[rid]))
                        METRICS.inc("memd_embed_retries_total")
                    except queue.Full:
                        # backlog full: give up on this retry (dead letter);
                        # reembed() heals the vector lane later
                        self.retries.pop(key, None)
                        METRICS.inc("memd_embed_dead_letters_total")
                else:
                    self.retries.pop(key, None)
                    METRICS.inc("memd_embed_dead_letters_total")

    def _pending(self) -> int:
        with self._inflight_lock:
            return self.q.qsize() + self._inflight

    def drain(self, timeout_s: float = 30.0) -> int:
        """Block until the queue is empty AND no batch is still being applied.

        Waiting on queue-empty alone let drain() (and therefore flush())
        return while a dequeued batch was mid-embed, so "flushed" did not mean
        the vector lane was complete."""
        waited = 0.0
        while self._pending() and waited < timeout_s:
            time.sleep(0.05)
            waited += 0.05
        return self._pending()

    def stop(self, drain_timeout_s: float = 0.0) -> None:
        """Stop the worker, optionally draining first.

        Without a drain, everything still queued is discarded SILENTLY: a
        clean shutdown could throw away most of a namespace's vectors and
        nothing distinguished that from a complete vector lane. Whatever is
        still pending when the timeout expires is now counted, so a degraded
        lane is visible to an operator (reembed() heals it).
        """
        if drain_timeout_s > 0:
            self.drain(timeout_s=drain_timeout_s)
        left = self._pending()
        if left:
            METRICS.inc("memd_embed_dropped_on_close_total", left,
                        help="embeddings still pending when the worker stopped")
        self._stop.set()
        self._thread.join(timeout=5)


class _MaintenanceWorker:
    """Background runner for namespace-scale maintenance.

    D2 is explicit: "Per-write maintenance scope: O(entity cluster), never
    O(namespace)". Due hard-delete purges violated that by running
    `ns.compact()` INLINE on the writer's thread - compaction reads every live
    record and rewrites the whole live set, so the first ordinary write after
    a purge deadline came due paid 8131ms at 200K records (vs ~3ms for the
    identical call moments before), 813x the embedded write-ack SLO. In hosted
    mode that write holds the namespace lock, stalling every concurrent
    request on the tenant.

    The compliance guarantee (D7 #8) is unchanged: the purge still happens
    with no operator intervention, and is still proven by a metric plus an
    audit entry - it just no longer happens on a caller's latency path.
    Work is deduped per namespace; flush()/close() drain it.
    """

    def __init__(self, run_fn):
        self.run_fn = run_fn
        self._q: "queue.Queue[str]" = queue.Queue()
        self._queued: set[str] = set()
        self._lock = threading.Lock()
        self._inflight = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True, name="memd-maint")
        self._thread.start()

    def submit(self, ns_name: str) -> None:
        with self._lock:
            if ns_name in self._queued:
                return  # already scheduled: compaction is idempotent
            self._queued.add(ns_name)
        self._q.put(ns_name)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                ns_name = self._q.get(timeout=0.1)
            except queue.Empty:
                continue
            with self._lock:
                self._queued.discard(ns_name)
                self._inflight += 1
            try:
                self.run_fn(ns_name)
            except Exception:
                METRICS.inc("memd_maintenance_failures_total",
                            help="background maintenance runs that raised")
            finally:
                with self._lock:
                    self._inflight -= 1

    def pending(self) -> int:
        with self._lock:
            return self._q.qsize() + self._inflight

    def drain(self, timeout_s: float = 60.0) -> int:
        waited = 0.0
        while self.pending() and waited < timeout_s:
            time.sleep(0.02)
            waited += 0.02
        return self.pending()

    def stop(self, drain_timeout_s: float = 0.0) -> None:
        if drain_timeout_s > 0:
            self.drain(timeout_s=drain_timeout_s)
        self._stop.set()
        self._thread.join(timeout=5)


class _SearchCache:
    """Tiny LRU for identical repeat queries (agents re-ask similar prompts).
    Keyed by (ns, query, scope, budget, as_of, kinds); entries die on any
    write via an epoch counter - O(1) hit/miss, zero staleness.

    Bounded by BYTES, not just entries: a caller may legally request
    budget_tokens up to 128K, so one entry can approach ~0.5MB. Entry-count
    caps alone let 256 such entries pin >100MB of RAM."""

    MAX_ENTRY_BYTES = 262_144   # don't cache oversized packed contexts at all
    MAX_TOTAL_BYTES = 32 * 1024 * 1024

    def __init__(self, capacity: int = 256):
        import threading

        self.capacity = capacity
        self._lock = threading.Lock()
        self._map: "OrderedDict[tuple, Any]" = OrderedDict()
        self._bytes = 0

    @staticmethod
    def _size(value) -> int:
        try:
            n = len(value.packed_context) + sum(len(i.content) for i in value.items)
        except Exception:
            return 0
        return n or 1

    def get(self, key):
        import threading

        with self._lock:
            v = self._map.pop(key, None)
            if v is not None:
                self._map[key] = v  # refresh LRU
            return v

    def put(self, key, value) -> None:
        with self._lock:
            n = self._size(value)
            if n > self.MAX_ENTRY_BYTES:
                return  # oversized context: serve once, don't pin RAM
            while self._map and self._bytes + n > self.MAX_TOTAL_BYTES:
                _, old = self._map.popitem(last=False)
                self._bytes -= self._size(old)
            if key in self._map:  # replace: subtract old footprint first
                self._bytes -= self._size(self._map[key])
                del self._map[key]
            self._map[key] = value
            self._bytes += n
            while len(self._map) > self.capacity:
                _, old = self._map.popitem(last=False)
                self._bytes -= self._size(old)

    def clear(self) -> None:
        with self._lock:
            self._map.clear()
            self._bytes = 0


MAX_QUERY_CHARS = 64 * 1024  # embedder-cost guard for engine-side queries


class Memory:
    # hosted mode: set by __init__ when api_key= is given; every public
    # method then delegates to it so "one engine, three doors" holds at the
    # facade level too (not just via HostedMemory directly)
    _impl: Any = None

    def _hosted(self) -> Any:
        return self.__dict__.get("_impl")

    def __init__(
        self,
        path: str = "./memd-data",
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        namespace: str = "default",
        config: dict[str, Any] | None = None,
        encrypt: bool = True,
        transport: Any = None,
    ):
        cfg = dict(config or {})
        if api_key:
            from memd.sdk.client import HostedMemory

            self._impl = HostedMemory(api_key=api_key, base_url=base_url or "http://localhost:8700",
                                      namespace=namespace, transport=transport)
            return
        os.makedirs(path, exist_ok=True)
        envelope = LocalKeyEnvelope(os.path.join(path, "keys")) if encrypt else NullKeyEnvelope()
        self.engine = StorageEngine(os.path.join(path, "store"), envelope=envelope)
        self.namespace_name = namespace
        self.ns = self.engine.namespace(namespace)
        # the facade holds a direct reference to this store for its lifetime:
        # pin it so LRU churn of other namespaces can't close it underneath us
        self.engine.pin_namespace(namespace)
        # D7 #7 ledgers are PER NAMESPACE. They are held in an LRU keyed by
        # namespace (mirroring the engine's namespace table) and routed by the
        # OPERATION's target namespace - see _audit_for().
        self._audit_flush_every = int(cfg.get("audit_flush_every", 32))
        self._audit_max_open = int(cfg.get("audit_max_open", 64))
        self._audit_lock = threading.Lock()
        self._audits: "OrderedDict[str, BufferedAuditLog]" = OrderedDict()
        self.audit = self._audit_for(namespace)  # facade default, never evicted
        self.embedder: Embedder = resolve_embedder(cfg)
        self.extractor: Extractor = resolve_extractor(cfg)
        self.quarantine = QuarantinePolicy(
            rate_max_writes=int(cfg.get("rate_max_writes", 120)),
            dup_max_repeats=int(cfg.get("dup_max_repeats", 5)),
        )
        # Privacy: search queries are user content. Audit stores a short hash
        # by default (correlation without exposure); opt in to text for debug.
        self._audit_query_text = bool(cfg.get("audit_query_text", False))
        # D7 #8: physical-purge window for hard deletes (hosted compliance
        # tiers may tighten it; the deadline is self-enforced, see
        # _enforce_purge_deadlines)
        self._purge_deadline_ms = int(cfg.get("hard_delete_deadline_ms", HARD_DELETE_PURGE_MS))
        self._taints = TaintStore(max_sessions=int(cfg.get("max_tracked_sessions", 10_000)))
        self._metrics_dumper = auto_dumper_from_env()  # MEMD_METRICS_PATH opt-in
        preset_core(METRICS, ns=namespace)  # single registration: series exist from t=0
        self._qcache = _SearchCache()
        self._qepochs: dict[str, int] = {}
        self._embed_close_drain_s = float(cfg.get("embed_close_drain_s", 30.0))
        self._maint = _MaintenanceWorker(self._run_due_purge)
        self._embed_worker = _EmbedWorker(
            self.embedder,
            self._apply_vectors,
            batch_size=int(cfg.get("embed_batch", 32)),
            max_queue=int(cfg.get("embed_max_queue", 5_000)),
        )
        self.audit.append(actor="system", action="open", target=namespace, detail={"embedder": self.embedder.name})

    # ------------------------------------------------------------------ writes

    def add(
        self,
        content: str,
        *,
        session_id: str | None = None,
        user_id: str | None = None,
        agent_id: str | None = None,
        org_id: str | None = None,
        role: str = "user",
        kind: str = Kind.RAW_EVENT,
        source: Source | str | None = None,
        actor_id: str | None = None,
        t_event: int | None = None,
        meta: dict[str, Any] | None = None,
        namespace: str | None = None,
    ) -> list[str]:
        """Raw-lane capture. Durable + searchable (BM25) on return.
        For multi-event turns prefer add_events (one fsync per batch)."""
        impl = self._hosted()
        if impl is not None:
            return impl.add(content, session_id=session_id, user_id=user_id,
                            agent_id=agent_id, org_id=org_id, role=role, kind=kind,
                            source=source, actor_id=actor_id, t_event=t_event,
                            meta=meta, namespace=namespace)
        _guard_input(content, meta)
        _guard_kind(kind)
        ns = self._ns_for(namespace)
        src = self._resolve_source(source, role)
        org_id, agent_id = org_id or None, agent_id or None
        user_id, session_id = user_id or None, session_id or None
        scope = Scope(org=org_id, agent=agent_id, user=user_id, session=session_id)
        rec = MemoryRecord.create(
            namespace=ns.namespace,
            kind=kind,
            content=content,
            scope=scope,
            source=src,
            actor_id=actor_id or f"{role}:{user_id or session_id or 'anon'}",
            session_id=session_id,
            t_event=t_event,
            meta=meta or {},
        )
        verdicts = self.quarantine.check([rec])
        v = verdicts[rec.id]
        if v.quarantined:
            rec.meta["quarantined"] = True
            if v.expires_ms:
                rec.meta["quarantine_expires"] = v.expires_ms
            METRICS.inc("memd_quarantined_total", ns=ns.namespace, reason=v.reason or "unknown")
        t0 = time.monotonic()
        ns.append([rec])
        METRICS.observe("memd_write_ack_ms", (time.monotonic() - t0) * 1000,
                        help="durable write ack latency (ms)", ns=ns.namespace)
        self._bump_epoch(ns.namespace)
        METRICS.inc("memd_writes_total", help="raw/explicit lane writes", ns=ns.namespace, kind=kind, source=src.name.lower())
        if v.quarantined:
            ns.index.mark_quarantined(rec.id, True)
        else:
            self._embed_worker.submit(ns.namespace, rec.id, rec.content)
        if session_id:
            self._taint(session_id).observe(int(src))
        self._audit_for(ns.namespace).append(actor=actor_id or role, action="add", target=rec.id, detail={"kind": kind, "source": src.name})
        return [rec.id]

    def add_events(
        self,
        events: list[dict],
        *,
        namespace: str | None = None,
    ) -> list[str]:
        """Batch raw-lane capture: one durable append + one index commit for
        the whole batch (O(events) work, O(1) fsyncs), and quarantine checks
        run across the batch - which is exactly the shape MINJA attacks in."""
        impl = self._hosted()
        if impl is not None:
            return impl.add_events(events, namespace=namespace)
        ns = self._ns_for(namespace)
        if len(events) > MAX_BATCH_EVENTS:
            raise ValueError(f"batch exceeds {MAX_BATCH_EVENTS} events; split the call")
        records: list[MemoryRecord] = []
        taint_updates: list[tuple[str, int]] = []
        for e in events:
            _guard_input(e["content"], e.get("meta"))
            _guard_kind(e.get("kind", Kind.RAW_EVENT))
            src = self._resolve_source(e.get("source"), e.get("role", "user"))
            session_id = e.get("session_id") or None
            user_id = e.get("user_id") or None
            agent_id = e.get("agent_id") or None
            org_id = e.get("org_id") or None
            scope = Scope(org=org_id, agent=agent_id, user=user_id, session=session_id)
            rec = MemoryRecord.create(
                namespace=ns.namespace,
                kind=e.get("kind", Kind.RAW_EVENT),
                content=e["content"],
                scope=scope,
                source=src,
                actor_id=e.get("actor_id") or f"{e.get('role', 'user')}:{user_id or session_id or 'anon'}",
                session_id=session_id,
                t_event=e.get("t_event"),
                meta=e.get("meta") or {},
            )
            records.append(rec)
            if session_id:
                taint_updates.append((session_id, int(src)))
        verdicts = self.quarantine.check(records)
        quarantined_flags: dict[str, bool] = {}
        for rec in records:
            v = verdicts[rec.id]
            if v.quarantined:
                rec.meta["quarantined"] = True
                if v.expires_ms:
                    rec.meta["quarantine_expires"] = v.expires_ms
                quarantined_flags[rec.id] = True
        t0 = time.monotonic()
        ns.append(records)
        METRICS.observe("memd_write_ack_ms", (time.monotonic() - t0) * 1000,
                        help="durable write ack latency (ms)", ns=ns.namespace, batched="true")
        self._bump_epoch(ns.namespace)
        METRICS.inc("memd_writes_total", len(records), ns=ns.namespace, kind="batch", source="mixed")
        for rec in records:
            if rec.id in quarantined_flags:
                ns.index.mark_quarantined(rec.id, True)
            else:
                self._embed_worker.submit(ns.namespace, rec.id, rec.content)
        for sid, tier in taint_updates:
            self._taint(sid).observe(tier)
        self._audit_for(ns.namespace).append(
            actor="batch", action="add_events", target=f"{len(records)} events",
            detail={"quarantined": len(quarantined_flags)},
        )
        self._enforce_purge_deadlines(ns)
        return [r.id for r in records]

    def remember(
        self,
        content: str,
        *,
        kind: str = Kind.FACT,
        entity_keys: list[str] | None = None,
        session_id: str | None = None,
        user_id: str | None = None,
        agent_id: str | None = None,
        org_id: str | None = None,
        source: Source | str = Source.AGENT,
        actor_id: str | None = None,
        t_event: int | None = None,
        valid_from: int | None = None,
        namespace: str | None = None,
    ) -> str:
        """Explicit lane ("remember this"): high-trust write with taint cap."""
        impl = self._hosted()
        if impl is not None:
            return impl.remember(content, kind=kind, entity_keys=entity_keys,
                                 session_id=session_id, user_id=user_id,
                                 agent_id=agent_id, org_id=org_id, source=source,
                                 actor_id=actor_id, t_event=t_event,
                                 valid_from=valid_from, namespace=namespace)
        _guard_input(content, None)
        _guard_kind(kind)
        ns = self._ns_for(namespace)
        src = source if isinstance(source, Source) else Source.parse(source)
        if session_id:
            cap = self._taint(session_id).min_tier
            if int(src) > cap:
                src = Source(cap)
        org_id, agent_id = org_id or None, agent_id or None
        user_id, session_id = user_id or None, session_id or None
        ekeys = [k.strip().lower() for k in (entity_keys or []) if k.strip()][:8]
        rec = MemoryRecord.create(
            namespace=ns.namespace,
            kind=kind,
            content=content,
            scope=Scope(org=org_id, agent=agent_id, user=user_id, session=session_id),
            source=src,
            actor_id=actor_id or "explicit",
            session_id=session_id,
            entity_keys=ekeys,
            t_event=t_event,
            valid_from=valid_from,
        )
        # consolidate BEFORE the durable append so demotion metadata is atomic
        pairs = self._consolidate_explicit(ns, rec)
        t0 = time.monotonic()
        ns.append([rec])
        METRICS.observe("memd_write_ack_ms", (time.monotonic() - t0) * 1000,
                        help="durable write ack latency (ms)", ns=ns.namespace)
        self._bump_epoch(ns.namespace)
        self._apply_supersedence(ns, pairs)
        METRICS.inc("memd_writes_total", help="raw/explicit lane writes", ns=ns.namespace, kind=kind, source=src.name.lower())
        METRICS.inc("memd_remembers_total", ns=ns.namespace)
        self._embed_worker.submit(ns.namespace, rec.id, rec.content)
        self._audit_for(ns.namespace).append(actor=actor_id or "explicit", action="remember", target=rec.id, detail={"entity_keys": ekeys})
        return rec.id

    def _consolidate_explicit(self, ns, rec: MemoryRecord) -> list[tuple[str, str]]:
        """Cluster-local supersedence for explicit writes. Mutates rec.meta
        with lineage demotion; returns pairs for the caller to persist."""
        if not rec.entity_keys:
            return []
        cluster_scope = Scope(org=rec.scope.org, agent=rec.scope.agent, user=rec.scope.user)
        cluster: list[MemoryRecord] = []
        for ek in rec.entity_keys:
            cluster.extend(ns.index.entity_cluster(ek, scope=cluster_scope))
        fact = ExtractedFact(content=rec.content, entity_keys=list(rec.entity_keys), lineage=[])

        def make(f: ExtractedFact) -> MemoryRecord:
            return rec

        res = consolidate_facts(cluster, [fact], make)
        for old_id, new_id in res.superseded_pairs:
            old = ns.index.get_by_id(old_id, include_deleted=True)
            demoted: set[str] = set(old.provenance.lineage) if old else set()
            demoted.add(old_id)
            rec.meta["demotes"] = sorted(set(rec.meta.get("demotes", [])) | demoted)
        return res.superseded_pairs

    def _apply_supersedence(self, ns, pairs: list[tuple[str, str]]) -> None:
        """Persist + index supersedence pairs in one durable append and one
        index transaction (previously an fsync'd append + commit PER pair)."""
        if not pairs:
            return
        at = now_ms()
        ops = [{"op": "supersede", "old": old_id, "new": new_id, "at": at} for old_id, new_id in pairs]
        ns.append_ops(ops)
        ns.index.apply_ops_batch(ops)

    def observe(
        self,
        messages: list[dict],
        response: str,
        *,
        user_id: str | None = None,
        session_id: str | None = None,
        agent_id: str | None = None,
        org_id: str | None = None,
        namespace: str | None = None,
    ) -> list[str]:
        """Framework glue: capture a full turn after the LLM call. Batched:
        one durable append for the whole turn."""
        impl = self._hosted()
        if impl is not None:
            return impl.observe(messages, response, user_id=user_id,
                                session_id=session_id, agent_id=agent_id,
                                org_id=org_id, namespace=namespace)
        events = []
        for m in messages:
            if isinstance(m, dict) and m.get("content"):
                events.append({
                    "content": str(m["content"]),
                    "session_id": session_id,
                    "user_id": user_id,
                    "agent_id": agent_id,
                    "org_id": org_id,
                    "role": m.get("role", "user"),
                })
        events.append({
            "content": response,
            "session_id": session_id,
            "user_id": user_id,
            "agent_id": agent_id,
            "org_id": org_id,
            "role": "assistant",
        })
        return self.add_events(events, namespace=namespace)

    # ------------------------------------------------------------------ reads

    def _audit_for(self, ns_name: str) -> BufferedAuditLog:
        """The audit ledger belonging to `ns_name`.

        Ledgers are per-namespace objects (`ns/<ns>/audit`) and are exported
        per namespace, so they must be routed by the TARGET namespace of the
        operation. Binding one ledger to the facade's default namespace - the
        previous shape - meant a single facade serving many namespaces filed
        every tenant's adds/searches/deletes into the default namespace's
        ledger: one tenant's exportable, SIEM-bound trail carried another
        tenant's record ids, while the namespace that actually served the
        request had no trail at all. Same defect family as the pass-1 vector
        misfiling: facade default silently standing in for the real target.

        O(1) amortized; bounded by `audit_max_open` open ledgers (LRU, the
        facade default is pinned).
        """
        with self._audit_lock:
            log = self._audits.get(ns_name)
            if log is not None:
                self._audits.move_to_end(ns_name)
                return log
            log = BufferedAuditLog(
                self.engine.store, f"ns/{ns_name}/audit", self.engine.envelope,
                flush_every=self._audit_flush_every,
            )
            self._audits[ns_name] = log
            while len(self._audits) > self._audit_max_open:
                victim_name = next(
                    (k for k in self._audits if k != self.namespace_name), None)
                if victim_name is None:
                    break
                victim = self._audits.pop(victim_name)
                try:
                    victim.flush()  # never drop a tenant's entries on eviction
                except Exception:
                    METRICS.inc("memd_audit_flush_failures_total", ns=victim_name)
            return log

    def _flush_all_audits(self) -> None:
        with self._audit_lock:
            logs = list(self._audits.values())
        for lg in logs:
            try:
                lg.flush()
            except Exception:
                METRICS.inc("memd_audit_flush_failures_total")

    def _audit_target_for_query(self, query: str) -> str:
        if self._audit_query_text:
            return query[:80]
        import hashlib

        return "q:" + hashlib.sha256(query.encode()).hexdigest()[:12]

    def search(
        self,
        query: str,
        *,
        user_id: str | None = None,
        session_id: str | None = None,
        agent_id: str | None = None,
        org_id: str | None = None,
        budget_tokens: int = DEFAULT_BUDGET_TOKENS,
        as_of: int | None = None,
        kinds: list[str] | None = None,
        include_quarantined: bool = False,
        namespace: str | None = None,
    ) -> SearchResult:
        if len(query) > MAX_QUERY_CHARS:
            raise ValueError(f"query exceeds {MAX_QUERY_CHARS} char cap")
        impl = self._hosted()
        if impl is not None:
            return impl.search(query, user_id=user_id, session_id=session_id,
                               agent_id=agent_id, org_id=org_id,
                               budget_tokens=budget_tokens, as_of=as_of,
                               kinds=kinds, include_quarantined=include_quarantined,
                               namespace=namespace)
        t0 = time.monotonic()
        ns_name = self._ns_for(namespace).namespace
        cache_key = (
            ns_name, query,
            (user_id, session_id, agent_id, org_id),
            budget_tokens, as_of, tuple(kinds) if kinds else None,
            include_quarantined, self._qepochs.get(ns_name, 0),
        )
        cached = self._qcache.get(cache_key)
        if cached is not None:
            METRICS.inc("memd_search_cache_hits_total")
            METRICS.observe("memd_search_latency_ms", (time.monotonic() - t0) * 1000,
                            help="end-to-end search latency (ms)", ns=ns_name,
                            qclass=cached.query_class, cache="hit")
            return cached
        METRICS.inc("memd_search_cache_misses_total")
        io_stack = _ExitStack()
        io_tally = io_stack.enter_context(count_io())
        ns = self._ns_for(namespace)
        scope = Scope(org=org_id, agent=agent_id, user=user_id, session=session_id)
        _st0 = time.monotonic()
        plan = plan_query(query)
        METRICS.observe("memd_search_stage_ms", (time.monotonic() - _st0) * 1000,
                        help="per-stage search timing (ms): plan/fuse/pack",
                        ns=ns.namespace, stage="plan")
        filt_kwargs = dict(
            scope=scope,
            kinds=tuple(kinds) if kinds else plan.kinds,
            as_of=as_of,
            t_event_min=plan.t_event_min,
            t_event_max=plan.t_event_max,
            include_quarantined=include_quarantined,
        )
        from memd.index.sqlite_index import IndexFilter

        filt = IndexFilter(**filt_kwargs)
        lane_hits: dict[str, list] = {}
        tokens = [t for t in query.lower().split() if len(t) >= 3]
        # per-lane stage timing: end-to-end latency alone can't show WHICH
        # lane regressed (SLO triage needs the breakdown)
        for lane_name, lane_fn in (
            ("bm25", lambda: ns.index.search_bm25(query, filt, limit=plan.candidate_k)),
            ("entity", lambda: ns.index.search_by_entity_tokens(tokens, filt, limit=20)),
            # the documented third fan-out (D3 architecture: time/entity btree
            # scan): planner weights a "time" lane but nothing produced one -
            # temporal queries had NO recency-proximate candidates and relied
            # on lexical similarity surfacing fresh records by luck
            ("time", lambda: ns.index.search_time_lane(filt, limit=plan.candidate_k)),
        ):
            _lt0 = time.monotonic()
            lane_hits[lane_name] = lane_fn()
            METRICS.observe("memd_lane_ms", (time.monotonic() - _lt0) * 1000,
                            help="per-lane candidate fetch duration (ms)", ns=ns.namespace, lane=lane_name)
        # graceful degradation (ADR-8): if the embedder is unreachable
        # (BYO-key API outage), BM25 + entity lanes still serve - retrieval
        # degrades, it never dies
        try:
            _lt0 = time.monotonic()
            qvec = self.embedder.embed_one(query)
            lane_hits["vector"] = ns.index.search_vector(qvec, filt, limit=plan.candidate_k)
            METRICS.observe("memd_lane_ms", (time.monotonic() - _lt0) * 1000,
                            help="per-lane candidate fetch duration (ms)", ns=ns.namespace, lane="vector")
        except Exception:
            METRICS.inc("memd_embed_query_failures_total", ns=ns.namespace)
        _st0 = time.monotonic()
        fused = rrf_fuse(lane_hits, weights=plan.weights, limit=max(plan.candidate_k, 40))
        METRICS.observe("memd_search_stage_ms", (time.monotonic() - _st0) * 1000,
                        help="per-stage search timing (ms): plan/fuse/pack",
                        ns=ns.namespace, stage="fuse")
        _st0 = time.monotonic()
        packed = pack_context(fused, budget_tokens=budget_tokens, query_class=plan.qclass)
        METRICS.observe("memd_search_stage_ms", (time.monotonic() - _st0) * 1000,
                        help="per-stage search timing (ms): plan/fuse/pack",
                        ns=ns.namespace, stage="pack")
        items = [
            SearchHit(
                id=i.id,
                content=i.content,
                kind=i.kind,
                source=i.source,
                actor_id=i.actor_id,
                t_event=i.t_event,
                valid=i.valid,
                score=i.score,
                lanes=i.lanes,
                entity_keys=i.entity_keys,
                namespace=ns.namespace,
            )
            for i in packed.items
        ]
        METRICS.inc("memd_search_total", ns=ns.namespace, qclass=plan.qclass)
        METRICS.observe("memd_packed_tokens", packed.tokens_used, help="tokens injected per search",
                        buckets=(64, 128, 256, 512, 1024, 2048, 4096, 8192), ns=ns.namespace)
        METRICS.observe("memd_search_hits", len(items), help="packed items per search",
                        buckets=(1, 2, 5, 10, 20, 50), ns=ns.namespace)
        if packed.truncated:
            METRICS.inc("memd_packed_truncated_total", ns=ns.namespace)
        for lane, hits in lane_hits.items():
            METRICS.observe("memd_lane_candidates", len(hits), help="candidates per lane per search",
                            buckets=(0, 1, 5, 10, 25, 50, 100), ns=ns.namespace, lane=lane)
        self._audit_for(ns.namespace).append(actor="search", action="search",
                          target=self._audit_target_for_query(query), detail={"hits": len(items)})
        # measured LAST: the audit append is real per-request work and used to
        # sit outside the timer, so p99 under-reported every search
        latency = (time.monotonic() - t0) * 1000
        METRICS.observe("memd_search_latency_ms", latency, help="end-to-end search latency (ms)",
                        ns=ns.namespace, qclass=plan.qclass, cache="miss")
        io_stack.close()
        # I/O counts per request (the complexity budget's separate axis): a
        # search must stay O(1) in object-store round trips no matter how many
        # rows it touches. This is the metric that would catch a regression to
        # an N+1 pattern; the process-wide counter never could.
        METRICS.observe("memd_search_store_ops", float(sum(io_tally.values())),
                        help="object-store round trips per search",
                        buckets=(0, 1, 2, 4, 8, 16, 32, 64, 128), ns=ns.namespace)
        result = SearchResult(
            packed_context=packed.text,
            items=items,
            tokens_used=packed.tokens_used,
            budget=budget_tokens,
            truncated=packed.truncated,
            query_class=plan.qclass,
            latency_ms=round(latency, 3),
        )
        self._qcache.put(cache_key, result)
        return result

    def pack(
        self,
        messages: list[dict],
        *,
        user_id: str | None = None,
        session_id: str | None = None,
        budget_tokens: int = DEFAULT_BUDGET_TOKENS,
        namespace: str | None = None,
    ) -> list[dict]:
        """Inject packed memory context before the LLM call (the two-line glue)."""
        impl = self._hosted()
        if impl is not None:
            return impl.pack(messages, user_id=user_id, session_id=session_id,
                             budget_tokens=budget_tokens, namespace=namespace)
        last_user = next((m["content"] for m in reversed(messages) if m.get("role") == "user"), "")
        if not last_user:
            return messages
        res = self.search(
            str(last_user), user_id=user_id, session_id=session_id, budget_tokens=budget_tokens, namespace=namespace
        )
        if not res.items:
            return messages
        block = {"role": "system", "content": res.packed_context}
        out = list(messages)
        insert_at = 0
        for i, m in enumerate(out):
            if m.get("role") == "system":
                insert_at = i + 1
            else:
                break
        out.insert(insert_at, block)
        return out

    def get(self, record_id: str, *, history: bool = False, namespace: str | None = None) -> dict | None:
        impl = self._hosted()
        if impl is not None:
            return impl.get(record_id, history=history, namespace=namespace)
        ns = self._ns_for(namespace)
        rec = ns.index.get_by_id(record_id, include_deleted=True)
        if rec is None or (rec.deleted and not history):
            return None
        d = rec.to_dict()
        if history:
            d["history"] = [h.to_dict() for h in ns.index.history(record_id)]
        return d

    # ------------------------------------------------------------------ lifecycle

    def close_session(
        self,
        session_id: str,
        *,
        user_id: str | None = None,
        namespace: str | None = None,
    ) -> dict:
        """Segment-close boundary: extract facts, consolidate, rotate segment.
        Consolidation resolves at session end, never deferred past it.
        When user_id is supplied, extraction sweeps only that user's rows
        (blocks cross-user session-id injection into the fact lane)."""
        impl = self._hosted()
        if impl is not None:
            return impl.close_session(session_id, user_id=user_id, namespace=namespace)
        ns = self._ns_for(namespace)
        self._embed_worker.drain(timeout_s=60)
        # durable boundary: commit index + audit before folding the segment
        ns.index.flush()
        self._audit_for(ns.namespace).flush()
        t0 = time.monotonic()
        seg_records = ns.index.records_of_session(session_id, user_id=user_id)
        try:
            extracted = self.extractor.extract(seg_records) if seg_records else []
        except Exception as ex:
            # extraction is a REBUILDABLE derived index (raw lane is truth):
            # an extractor outage must never block the session boundary or
            # wedge the WAL. Facts can be regenerated later via reindex.
            METRICS.inc("memd_extraction_failures_total", ns=ns.namespace)
            self._audit_for(ns.namespace).append(actor="system", action="extraction_failed",
                              target=session_id, detail={"error": str(ex)[:200]})
            extracted = []
        facts_written, consolidation = self._write_facts(ns, extracted, seg_records)
        seg_name = ns.rotate(f"session-close:{session_id}")
        self._taints.drop(session_id)  # taint is per-session; closed = gone
        self._enforce_purge_deadlines(ns)  # segment fold enforces due purges too
        self._audit_for(ns.namespace).append(actor="system", action="close_session", target=session_id,
                          detail={"facts": facts_written, "segment": seg_name})
        METRICS.observe("memd_session_close_ms", (time.monotonic() - t0) * 1000,
                        help="session close: extract+consolidate+rotate (ms)", ns=ns.namespace)
        self._bump_epoch(ns.namespace)
        METRICS.inc("memd_sessions_closed_total", ns=ns.namespace)
        METRICS.inc("memd_facts_extracted_total", facts_written, ns=ns.namespace)
        METRICS.inc("memd_facts_superseded_total", len(consolidation.superseded_pairs), ns=ns.namespace)
        METRICS.inc("memd_dupes_dropped_total", consolidation.dropped_dupes, ns=ns.namespace)
        return {
            "segment": seg_name,
            "raw_considered": len(seg_records),
            "facts_extracted": len(extracted),
            "facts_written": facts_written,
            "superseded": len(consolidation.superseded_pairs),
            "dupes_dropped": consolidation.dropped_dupes,
        }

    @staticmethod
    def _resolve_subject_names(ns, clusters: dict[str, list[MemoryRecord]], scope: Scope) -> dict[str, str]:
        import re as _re

        out: dict[str, str] = {}
        for r in clusters.get("user.name", []):
            m = _re.match(r"(\S+) is called ([\w' -]+)$", r.content)
            if m and m.group(1) not in out:
                out[m.group(1)] = m.group(2).strip()
        return out

    @staticmethod
    def _subject_id_for(f: ExtractedFact, sources: list[MemoryRecord]) -> str | None:
        for s in sources:
            if s.id in f.lineage:
                return s.scope.user or s.scope.agent
        return None

    @staticmethod
    def _swap_subject(content: str, sid: str, display: str) -> str:
        return content.replace(f"{sid}'s", f"{display}'s").replace(f"{sid} ", f"{display} ", 1) \
            if sid != "the user" else content

    def _write_facts(self, ns, facts: list[ExtractedFact], sources: list[MemoryRecord]) -> tuple[int, ConsolidationResult]:
        if not facts:
            return 0, ConsolidationResult()
        tier_cap = min((int(s.provenance.source) for s in sources), default=int(Source.IMPORT))
        # Cluster scope = the sources' scope with the session component
        # stripped: supersedence is user-level knowledge that spans sessions,
        # while org/agent/user bindings still fence tenants and users.
        base = sources[0].scope if sources else Scope()
        cluster_scope = Scope(org=base.org, agent=base.agent, user=base.user)
        clusters: dict[str, list[MemoryRecord]] = {}
        for f in facts:
            for ek in f.entity_keys or ["fact.general"]:
                clusters.setdefault(ek, [])
        for ek in list(clusters):
            clusters[ek] = ns.index.entity_cluster(ek, scope=cluster_scope)
        # cross-session display-name resolution: "u7 works at X" reads better
        # (and retrieves better) as "Hank works at X" once a user.name fact
        # exists; facts are re-runnable derived data, so enrichment is legit
        subject_names = self._resolve_subject_names(ns, clusters, cluster_scope)
        if subject_names:
            for f in facts:
                if f.entity_keys == ["user.name"]:
                    continue  # never rewrite the name fact into "X is called X"
                sid = self._subject_id_for(f, sources)
                if sid and sid in subject_names:
                    disp = subject_names[sid]
                    f.content = self._swap_subject(f.content, sid, disp)
        results = []
        for f in facts:
            keys = f.entity_keys or ["fact.general"]
            cluster = []
            seen_ids: set[str] = set()
            for k in keys:
                for r in clusters.get(k, []):
                    if r.id not in seen_ids:
                        seen_ids.add(r.id)
                        cluster.append(r)

            def make(fact: ExtractedFact, _keys=keys) -> MemoryRecord:
                src = next((s for s in sources if s.id in fact.lineage), sources[0] if sources else None)
                return MemoryRecord.create(
                    namespace=ns.namespace,
                    kind=Kind.FACT,
                    content=fact.content,
                    scope=src.scope if src is not None else Scope(),
                    entity_keys=_keys,
                    lineage=fact.lineage,
                    source=Source(tier_cap),
                    actor_id=src.provenance.actor_id if src is not None else None,
                    session_id=src.provenance.session_id if src is not None else None,
                    t_event=max((s.time.t_event for s in sources if s.id in fact.lineage), default=None)
                    or (src.time.t_event if src is not None else None),
                    extractor=ExtractorInfo(model=self.extractor.name, prompt_version="v1"),
                )

            res = consolidate_facts(cluster, [f], make)
            results.append(res)
        written = 0
        kept_recs = [r for res in results for r in res.kept]
        # lineage demotion: a fact that supersedes another carries the old
        # fact's raw lineage in meta.demotes - packing then skips that stale
        # raw evidence (demotion, not deletion; D3 §3.6)
        superseded_all = [p for res in results for p in res.superseded_pairs]
        # lineage demotion: a fact that supersedes another carries the old
        # fact's raw lineage in meta.demotes - packing then skips that stale
        # raw evidence (demotion, not deletion; D3 §3.6)
        old_fact_cache: dict[str, MemoryRecord | None] = {}
        for r in kept_recs:
            demoted: set[str] = set()
            for old_id, new_id in superseded_all:
                if new_id != r.id:
                    continue
                if old_id not in old_fact_cache:
                    old_fact_cache[old_id] = ns.index.get_by_id(old_id, include_deleted=True)
                old_fact = old_fact_cache[old_id]
                if old_fact is not None:
                    demoted.update(old_fact.provenance.lineage)
                    demoted.add(old_id)
            if demoted:
                r.meta["demotes"] = sorted(demoted)
        self._apply_supersedence(ns, superseded_all)
        written = len(kept_recs)
        # persist kept facts in one durable batch (after demote annotation)
        if kept_recs:
            ns.append(kept_recs)
            for r in kept_recs:
                self._embed_worker.submit(ns.namespace, r.id, r.content)
        return written, ConsolidationResult(
            superseded_pairs=[p for res in results for p in res.superseded_pairs],
            dropped_dupes=sum(res.dropped_dupes for res in results),
        )

    # ------------------------------------------------------------------ deletion / export

    def delete(self, record_id: str, *, hard: bool = False, actor: str = "api", namespace: str | None = None) -> bool:
        impl = self._hosted()
        if impl is not None:
            return impl.delete(record_id, hard=hard, namespace=namespace)
        ns = self._ns_for(namespace)
        ns.append_op({"op": "tombstone", "id": record_id, "at": now_ms()})
        ok = ns.index.tombstone(record_id, now_ms())
        if hard:
            deadline = now_ms() + self._purge_deadline_ms
            ns.append_op({"op": "hard_delete", "id": record_id, "deadline": deadline})
            ns.index.hard_delete(record_id)
        METRICS.inc("memd_deletes_total", hard=hard, ns=ns.namespace)
        self._bump_epoch(ns.namespace)
        self._audit_for(ns.namespace).append(actor=actor, action="hard_delete" if hard else "delete", target=record_id)
        if hard:
            self._enforce_purge_deadlines(ns)
        return ok

    def delete_many(
        self,
        record_ids: list[str],
        *,
        hard: bool = False,
        actor: str = "api",
        namespace: str | None = None,
    ) -> int:
        """Batched deletion (embedded mode): ONE durable op-append (one fsync)
        + ONE index transaction for the whole set.

        The destructive-sweep path previously called delete() per id - two
        fsync'd appends and two index commits per record (O(n) round trips on
        a request path). Complexity here: O(n) arithmetic, O(1) fsyncs,
        O(1) commits. Single-id delete() is unchanged."""
        impl = self._hosted()
        if impl is not None:
            # hosted door has no batch endpoint yet: keep correctness, accept
            # the per-id cost server-side
            return sum(1 for rid in record_ids if impl.delete(rid, hard=hard, namespace=namespace))
        ids = [rid for rid in record_ids if rid]
        if not ids:
            return 0
        ns = self._ns_for(namespace)
        now = now_ms()
        ops: list[dict] = [{"op": "tombstone", "id": rid, "at": now} for rid in ids]
        if hard:
            deadline = now + self._purge_deadline_ms
            ops.extend({"op": "hard_delete", "id": rid, "deadline": deadline} for rid in ids)
        ns.append_ops(ops)
        ns.index.apply_ops_batch(ops)
        METRICS.inc("memd_deletes_total", len(ids), hard=hard, ns=ns.namespace, batched="true")
        self._bump_epoch(ns.namespace)
        self._audit_for(ns.namespace).append(
            actor=actor, action="hard_delete_batch" if hard else "delete_batch",
            target=f"{len(ids)} records", detail={"count": len(ids), "hard": hard},
        )
        if hard:
            self._enforce_purge_deadlines(ns)
        return len(ids)

    def _enforce_purge_deadlines(self, ns) -> bool:
        """Self-enforce the physical-purge guarantee (D7 #8) OFF the caller's
        thread: when a scheduled hard delete comes due, schedule the
        compaction that erases it. Cheap no-op otherwise (one bounded list
        scan). See _MaintenanceWorker for why this must not run inline."""
        if not ns.has_due_deletes():
            return False
        self._maint.submit(ns.namespace)
        METRICS.inc("memd_purges_scheduled_total",
                    help="due hard-delete purges handed to background maintenance",
                    ns=ns.namespace)
        return True

    def _run_due_purge(self, ns_name: str) -> bool:
        """Background body of the purge deadline. Resolves the namespace by
        PEEK so a namespace destroyed between scheduling and running stays
        destroyed rather than being re-materialized by name."""
        ns = self.engine.peek_namespace(ns_name)
        if ns is None or not ns.has_due_deletes():
            return False
        t0 = time.monotonic()
        rep = ns.compact(force=False)  # force=False still enforces due deadlines
        METRICS.inc("memd_auto_compactions_total", reason="hard_delete_deadline", ns=ns_name)
        METRICS.observe("memd_compaction_ms", (time.monotonic() - t0) * 1000,
                        help="compaction duration (ms)", ns=ns_name, forced="auto")
        self._bump_epoch(ns_name)
        self._audit_for(ns_name).append(actor="system", action="auto_compact", target=ns_name,
                          detail={"reason": "hard_delete_deadline",
                                  "purged": rep.records_purged,
                                  "hard_deleted_purged": rep.hard_deleted_purged})
        return True

    def find_ids(
        self,
        query: str,
        *,
        user_id: str | None = None,
        session_id: str | None = None,
        agent_id: str | None = None,
        org_id: str | None = None,
        as_of: int | None = None,
        kinds: list[str] | None = None,
        namespace: str | None = None,
    ) -> list[str]:
        """Unbounded id resolution for destructive flows (forget): fusion over
        all lanes with NO token-budget packing cap - a delete-by-query must
        never silently miss matches beyond a packing budget."""
        impl = self._hosted()
        if impl is not None:
            return impl.find_ids(query, user_id=user_id, session_id=session_id,
                                 agent_id=agent_id, org_id=org_id, as_of=as_of,
                                 kinds=kinds, namespace=namespace)
        if len(query) > MAX_QUERY_CHARS:
            raise ValueError(f"query exceeds {MAX_QUERY_CHARS} char cap")
        ns = self._ns_for(namespace)
        scope = Scope(org=org_id, agent=agent_id, user=user_id, session=session_id)
        _st0 = time.monotonic()
        plan = plan_query(query)
        METRICS.observe("memd_search_stage_ms", (time.monotonic() - _st0) * 1000,
                        help="per-stage search timing (ms): plan/fuse/pack",
                        ns=ns.namespace, stage="plan")
        t0 = time.monotonic()
        filt = IndexFilter(
            scope=scope, kinds=tuple(kinds) if kinds else plan.kinds, as_of=as_of,
            include_quarantined=False,
        )
        # no packing budget here: a delete-by-query sweep must see every match
        sweep_limit = 10_000
        try:
            qvec = self.embedder.embed_one(query)
            vector_hits = ns.index.search_vector(qvec, filt, limit=sweep_limit)
        except Exception:
            METRICS.inc("memd_embed_query_failures_total", ns=ns.namespace)
            vector_hits = []
        lane_hits = {
            "bm25": ns.index.search_bm25(query, filt, limit=sweep_limit),
            "vector": vector_hits,
            "entity": ns.index.search_by_entity_tokens(
                [t for t in query.lower().split() if len(t) >= 3], filt, limit=sweep_limit),
        }
        fused = rrf_fuse(lane_hits, weights=plan.weights, limit=sweep_limit)
        # destructive sweep evidence rule: a candidate must have LEXICAL
        # grounding (bm25/entity lane) OR a strong vector match. Vector-only
        # weak similarity must never drive mass deletion.
        vec_scores = {h.record.id: h.score for h in lane_hits["vector"]}
        strong_vec = 0.35
        ids = []
        for it in fused:
            lexical = bool(set(it.lanes) & {"bm25", "entity"})
            if lexical or vec_scores.get(it.record.id, 0.0) >= strong_vec:
                ids.append(it.record.id)
        METRICS.observe("memd_find_ids_ms", (time.monotonic() - t0) * 1000,
                        help="find_ids sweep duration (ms)", ns=ns.namespace)
        METRICS.inc("memd_find_ids_total", ns=ns.namespace)
        METRICS.inc("memd_find_ids_matched_total", len(ids), ns=ns.namespace)
        return ids

    def forget(
        self,
        query: str,
        *,
        user_id: str | None = None,
        session_id: str | None = None,
        agent_id: str | None = None,
        org_id: str | None = None,
        actor: str = "api",
        namespace: str | None = None,
    ) -> list[str]:
        """User-driven deletion by query. Uses find_ids (unbounded), NOT the
        budget-capped packed view - partial destruction is unacceptable."""
        impl = self._hosted()
        if impl is not None:
            return impl.forget(query, confirm=True, user_id=user_id,
                               session_id=session_id, agent_id=agent_id,
                               org_id=org_id, namespace=namespace)
        ids = self.find_ids(
            query, user_id=user_id, session_id=session_id, agent_id=agent_id,
            org_id=org_id, namespace=namespace,
        )
        # one durable batch for the whole sweep (delete_many), not an
        # fsync-per-id loop
        self.delete_many(ids, actor=actor, namespace=namespace)
        self._audit_for(self._ns_for(namespace).namespace).append(
            actor=actor, action="forget",
            target=self._audit_target_for_query(query), detail={"deleted": len(ids)})
        return ids

    def destroy_namespace(self, namespace: str | None = None, actor: str = "api") -> bool:
        impl = self._hosted()
        if impl is not None:
            return impl.destroy_namespace(namespace=namespace)
        name = namespace or self.namespace_name
        ns_store = self._ns_for(name)
        # block new ops + serialize against in-flight exports/reads before
        # shredding (an export racing destroy would otherwise recreate the
        # namespace mid-teardown)
        # Lock order in this engine is StorageEngine._lock > NamespaceStore._lock
        # (> _wlock / _sync_lock / index._lock). Taking the ns lock HERE and
        # then blocking on the engine lock inside destroy_namespace was the one
        # place that inverted it - an ABBA deadlock against StorageEngine.close(),
        # which holds the engine lock and then blocks on each ns lock. Both
        # directions are reachable from the public surface: DELETE /v1/ns/{ns}
        # racing the lifespan shutdown hook wedged BOTH threads permanently -
        # no timeout, no error, no metric, and the container lost the manifest
        # flush that close() was performing.
        # StorageEngine.destroy_namespace already marks destroyed and takes the
        # ns lock itself, under the engine lock, in the correct order.
        ns_store.mark_destroyed()
        ok = self.engine.destroy_namespace(name)
        self._bump_epoch(name)
        # The shredded namespace's ledger was encrypted under a data key that
        # no longer exists: its buffered tail is undecryptable, and appending
        # to it would re-create BOTH the object and a fresh data key beneath a
        # crypto-shredded namespace (D7 #9). Drop it without flushing.
        with self._audit_lock:
            self._audits.pop(name, None)
        if name == self.namespace_name:
            self.ns = self.engine.namespace(name)
            self.audit = self._audit_for(name)
        METRICS.inc("memd_destroys_total", ns=name)
        # destroy is an ADMINISTRATIVE act on the engine, so it lands in the
        # facade's own ledger - never in the ledger of the namespace just shredded
        self.audit.append(actor=actor, action="destroy_namespace", target=name)
        return ok

    def export_jsonl_iter(self, namespace: str | None = None):
        """Streaming export (embedded mode): NDJSON lines, O(1) line-sized
        buffers. Hosted mode has no streaming REST surface yet; callers there
        fall back to the buffered blob."""
        impl = self._hosted()
        if impl is not None:
            yield impl.export_jsonl(namespace=namespace)
            return
        name = namespace or self.namespace_name
        t0 = time.monotonic()
        n = 0
        for line in self.engine.namespace(name).export_jsonl_iter():
            n += 1
            yield line
        METRICS.observe("memd_export_ms", (time.monotonic() - t0) * 1000,
                        help="export duration (ms)", ns=name)
        METRICS.inc("memd_exports_total", ns=name)
        METRICS.inc("memd_export_records_total", n, ns=name,
                    help="records streamed by exports")
        self._audit_for(name).append(actor="export", action="export", target=name,
                          detail={"records": n})

    def export_jsonl(self, namespace: str | None = None) -> bytes:
        """Buffered variant (CLI/SDK convenience). Emits its own metrics +
        one audit record; prefer export_jsonl_iter() on streaming surfaces."""
        impl = self._hosted()
        if impl is not None:
            return impl.export_jsonl(namespace=namespace)
        name = namespace or self.namespace_name
        t0 = time.monotonic()
        buf = bytearray()
        n = 0
        for line in self.engine.namespace(name).export_jsonl_iter():
            n += 1
            buf += line
        data = bytes(buf)
        METRICS.observe("memd_export_ms", (time.monotonic() - t0) * 1000,
                        help="export duration (ms)", ns=name)
        METRICS.inc("memd_exports_total", ns=name)
        METRICS.inc("memd_export_records_total", n, ns=name,
                    help="records streamed by exports")
        self._audit_for(name).append(actor="export", action="export", target=name,
                          detail={"bytes": len(data), "records": n})
        return data

    def compact(self, force: bool = False, namespace: str | None = None) -> dict:
        impl = self._hosted()
        if impl is not None:
            return impl.compact(force=force, namespace=namespace)
        ns = self._ns_for(namespace)
        ns.index.flush()
        self._audit_for(ns.namespace).flush()
        t0 = time.monotonic()
        rep = ns.compact(force=force)
        METRICS.observe("memd_compaction_ms", (time.monotonic() - t0) * 1000,
                        help="compaction duration (ms)", ns=ns.namespace, forced=str(force))
        self._bump_epoch(ns.namespace)
        METRICS.inc("memd_compactions_total", ns=ns.namespace, forced=str(force))
        METRICS.inc("memd_records_purged_total", rep.records_purged, ns=ns.namespace)
        self._audit_for(ns.namespace).append(actor="maintenance", action="compact", target=ns.namespace, detail=rep.__dict__)
        return rep.__dict__

    def stats(self, namespace: str | None = None) -> dict:
        impl = self._hosted()
        if impl is not None:
            return impl.stats(namespace=namespace)
        ns = self._ns_for(namespace)
        st = ns.stats()
        st["namespace"] = ns.namespace
        st["embedder"] = self.embedder.name
        st["extractor"] = self.extractor.name
        # live gauges so /metrics and stats() agree on current state
        METRICS.set_gauge("memd_records", st.get("records", 0), ns=ns.namespace)
        METRICS.set_gauge("memd_vectors", st.get("vectors", 0), ns=ns.namespace)
        METRICS.set_gauge("memd_quarantined", st.get("quarantined", 0), ns=ns.namespace)
        return st

    def status(self, ns_filter: str | None = None) -> dict:
        """Engine status. `ns_filter` restricts the inventory to one namespace:
        the namespace list is TENANT information and must not be handed to a
        key that is 403'd on every entry in it. Scoping also avoids the
        O(namespaces) object-store walk that list_namespaces() performs - a
        scoped key costs one exists() probe instead."""
        impl = self._hosted()
        if impl is not None:
            return impl.status()
        if ns_filter is not None:
            names = ([ns_filter]
                     if self.engine.store.exists(f"ns/{ns_filter}/manifest.json")
                     else [])
            return {
                "mode": "embedded",
                "namespaces": names,
                "default_namespace": ns_filter,
                "embedder": self.embedder.name,
                "extractor": self.extractor.name,
                "version": "0.1.0",
            }
        names = self.engine.list_namespaces()
        return {
            "mode": "embedded",
            "namespaces": names,
            "default_namespace": self.namespace_name,
            "embedder": self.embedder.name,
            "extractor": self.extractor.name,
            "version": "0.1.0",
        }

    # ------------------------------------------------------------------ internals

    def _ns_for(self, namespace: str | None):
        if namespace is None or namespace == self.namespace_name:
            return self.ns
        return self.engine.namespace(namespace)

    def _taint(self, session_id: str) -> SessionTaint:
        return self._taints.get(session_id)

    def _bump_epoch(self, ns: str | None = None) -> None:
        """Invalidate the search cache for one namespace on write (O(1));
        other namespaces' cached contexts stay valid."""
        name = ns or self.namespace_name
        self._qepochs[name] = self._qepochs.get(name, 0) + 1
        # drop stale entries lazily: full clear only when cache grows past cap
        if len(self._qcache._map) > self._qcache.capacity:
            self._qcache.clear()

    @staticmethod
    def _resolve_source(source: Source | str | None, role: str) -> Source:
        if source is not None:
            return source if isinstance(source, Source) else Source.parse(source)
        return {"user": Source.USER, "assistant": Source.AGENT, "agent": Source.AGENT}.get(role, Source.TOOL)

    def _apply_vectors(self, ns_name: str, ids: list[str], vecs: np.ndarray) -> None:
        # resolve the OWNING namespace's index: vectors are per-namespace
        # derived state; writing them into the facade default misfiled every
        # non-default namespace's lane (orphan rows there, dead lane here).
        # peek, never materialize: a namespace destroyed between submit and
        # apply must stay destroyed (re-materializing it by name raced the
        # teardown and resurrected shredded records into a ghost index).
        ns = self.engine.peek_namespace(ns_name)
        if ns is None:
            METRICS.inc("memd_embed_target_missing_total",
                        help="embeddings dropped because the target namespace is gone")
            return
        for i, rid in enumerate(ids):
            ns.index.set_vector(rid, vecs[i], self.embedder.name)

    def reembed(self, *, namespace: str | None = None, batch_size: int = 256) -> dict:
        """Batch re-embedding job (ADR-8): rebuild the vector lane from raw.

        Needed after restoring segments onto a machine without the derived
        index cache, after switching embedder models, or whenever records
        lack a current-version embedding. O(records); BM25 lane unaffected."""
        impl = self._hosted()
        if impl is not None:
            raise RuntimeError("reembed is embedded-only (no REST surface)")
        ns = self._ns_for(namespace)
        stale = ns.index.records_missing_embedding(self.embedder.name)
        done = 0
        t0 = time.monotonic()
        for i in range(0, len(stale), batch_size):
            chunk = stale[i : i + batch_size]
            texts = [r.content for r in chunk]
            vecs = self.embedder.embed(texts)
            for j, rec in enumerate(chunk):
                ns.index.set_vector(rec.id, vecs[j], self.embedder.name)
                done += 1
        METRICS.observe("memd_reembed_ms", (time.monotonic() - t0) * 1000,
                        help="re-embedding batch duration (ms)", ns=ns.namespace)
        METRICS.inc("memd_reembed_total", done, ns=ns.namespace)
        if done:
            self._bump_epoch(ns.namespace)  # vector lane changed -> cached contexts are stale
        self._audit_for(ns.namespace).append(actor="maintenance", action="reembed", target=ns.namespace,
                          detail={"embedded": done, "model": self.embedder.name})
        return {"namespace": ns.namespace, "missing": len(stale), "embedded": done,
                "model": self.embedder.name}

    def flush(self) -> None:
        impl = self._hosted()
        if impl is not None:
            return None  # hosted mode: nothing buffered locally
        self._embed_worker.drain(timeout_s=60)
        self._maint.drain(timeout_s=60)
        self.ns.index.flush()
        self._flush_all_audits()

    def close(self) -> None:
        impl = self._hosted()
        if impl is not None:
            return impl.close()
        if getattr(self, "_metrics_dumper", None):
            self._metrics_dumper.stop()
        # bounded drain before stopping: a clean close should not silently
        # discard the vector lane it was asked to persist
        self._embed_worker.stop(drain_timeout_s=float(self._embed_close_drain_s))
        self._maint.stop(drain_timeout_s=float(self._embed_close_drain_s))
        self.ns.index.flush()
        self._flush_all_audits()
        self.ns.close()
        self.engine.close()
