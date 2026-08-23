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


class _EmbedWorker:
    """Async batched embedder: write ack never waits on embeddings."""

    def __init__(self, embedder: Embedder, apply_fn, batch_size: int = 32, flush_s: float = 0.5,
                 max_retries: int = 3):
        self.embedder = embedder
        self.apply_fn = apply_fn
        self.q: queue.Queue[tuple[str, str]] = queue.Queue()
        self.retries: dict[str, int] = {}
        self.max_retries = max_retries
        self.batch_size = batch_size
        self.flush_s = flush_s
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True, name="memd-embed")
        self._thread.start()

    def submit(self, record_id: str, text: str) -> None:
        self.q.put((record_id, text))
        METRICS.set_gauge("memd_embed_queue_depth", self.q.qsize(), help="pending embedding texts")

    def _run(self) -> None:
        while not self._stop.is_set():
            batch: dict[str, str] = {}
            try:
                rid, text = self.q.get(timeout=self.flush_s)
                batch[rid] = text
                while len(batch) < self.batch_size:
                    try:
                        rid, text = self.q.get_nowait()
                        batch[rid] = text
                    except queue.Empty:
                        break
            except queue.Empty:
                continue
            METRICS.set_gauge("memd_embed_queue_depth", self.q.qsize())
            t0 = time.monotonic()
            try:
                vecs = self.embedder.embed(list(batch.values()))
                self.apply_fn(list(batch.keys()), vecs)
                METRICS.observe("memd_embed_batch_size", len(batch),
                                help="texts per embedding batch",
                                buckets=(1, 4, 8, 16, 32, 64, 128))
                METRICS.observe("memd_embed_apply_ms", (time.monotonic() - t0) * 1000,
                                help="embed + index apply duration (ms)")
                for rid in batch:
                    self.retries.pop(rid, None)
            except Exception:
                METRICS.inc("memd_embed_failures_total")
                # bounded retries: a poison text must not loop forever
                for rid in batch:
                    n = self.retries.get(rid, 0) + 1
                    if n <= self.max_retries:
                        self.retries[rid] = n
                        self.q.put((rid, batch[rid]))
                        METRICS.inc("memd_embed_retries_total")
                    else:
                        self.retries.pop(rid, None)
                        METRICS.inc("memd_embed_dead_letters_total")

    def drain(self, timeout_s: float = 30.0) -> int:
        """Block until queue empty (used by close/flush paths)."""
        waited = 0.0
        while not self.q.empty() and waited < timeout_s:
            threading.Event().wait(0.05)
            waited += 0.05
        return self.q.qsize()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)


class _SearchCache:
    """Tiny LRU for identical repeat queries (agents re-ask similar prompts).
    Keyed by (ns, query, scope, budget, as_of, kinds); entries die on any
    write via an epoch counter - O(1) hit/miss, zero staleness."""

    def __init__(self, capacity: int = 256):
        import threading

        self.capacity = capacity
        self._lock = threading.Lock()
        self._map: "OrderedDict[tuple, Any]" = OrderedDict()

    def get(self, key):
        import threading

        with self._lock:
            v = self._map.pop(key, None)
            if v is not None:
                self._map[key] = v  # refresh LRU
            return v

    def put(self, key, value) -> None:
        with self._lock:
            self._map[key] = value
            while len(self._map) > self.capacity:
                self._map.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._map.clear()


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
        self.audit = BufferedAuditLog(
            self.engine.store, f"ns/{namespace}/audit", envelope,
            flush_every=int(cfg.get("audit_flush_every", 32)),
        )
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
        preset_core(METRICS, ns=namespace)
        self._qcache = _SearchCache()
        self._qepochs: dict[str, int] = {}
        preset_core(METRICS, ns=namespace)
        self._embed_worker = _EmbedWorker(
            self.embedder,
            self._apply_vectors,
            batch_size=int(cfg.get("embed_batch", 32)),
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
        METRICS.observe("memd_write_ack_seconds", (time.monotonic() - t0) * 1000,
                        help="durable write ack latency (ms)", ns=ns.namespace)
        self._bump_epoch(ns.namespace)
        METRICS.inc("memd_writes_total", help="raw/explicit lane writes", ns=ns.namespace, kind=kind, source=src.name.lower())
        if v.quarantined:
            ns.index.mark_quarantined(rec.id, True)
        else:
            self._embed_worker.submit(rec.id, rec.content)
        if session_id:
            self._taint(session_id).observe(int(src))
        self.audit.append(actor=actor_id or role, action="add", target=rec.id, detail={"kind": kind, "source": src.name})
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
        METRICS.observe("memd_write_ack_seconds", (time.monotonic() - t0) * 1000,
                        help="durable write ack latency (ms)", ns=ns.namespace, batched="true")
        self._bump_epoch(ns.namespace)
        METRICS.inc("memd_writes_total", len(records), ns=ns.namespace, kind="batch", source="mixed")
        for rec in records:
            if rec.id in quarantined_flags:
                ns.index.mark_quarantined(rec.id, True)
            else:
                self._embed_worker.submit(rec.id, rec.content)
        for sid, tier in taint_updates:
            self._taint(sid).observe(tier)
        self.audit.append(
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
        METRICS.observe("memd_write_ack_seconds", (time.monotonic() - t0) * 1000,
                        help="durable write ack latency (ms)", ns=ns.namespace)
        self._bump_epoch(ns.namespace)
        for old_id, new_id in pairs:
            ns.append_op({"op": "supersede", "old": old_id, "new": new_id, "at": now_ms()})
            ns.index.mark_superseded(old_id, new_id, now_ms())
        METRICS.inc("memd_writes_total", help="raw/explicit lane writes", ns=ns.namespace, kind=kind, source=src.name.lower())
        METRICS.inc("memd_remembers_total", ns=ns.namespace)
        self._embed_worker.submit(rec.id, rec.content)
        self.audit.append(actor=actor_id or "explicit", action="remember", target=rec.id, detail={"entity_keys": ekeys})
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
        ns = self._ns_for(namespace)
        scope = Scope(org=org_id, agent=agent_id, user=user_id, session=session_id)
        plan = plan_query(query)
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
        fused = rrf_fuse(lane_hits, weights=plan.weights, limit=max(plan.candidate_k, 40))
        packed = pack_context(fused, budget_tokens=budget_tokens, query_class=plan.qclass)
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
        latency = (time.monotonic() - t0) * 1000
        METRICS.observe("memd_search_latency_ms", latency, help="end-to-end search latency (ms)",
                        ns=ns.namespace, qclass=plan.qclass, cache="miss")
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
        self.audit.append(actor="search", action="search",
                          target=self._audit_target_for_query(query), detail={"hits": len(items)})
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
        self.audit.flush()
        t0 = time.monotonic()
        seg_records = ns.index.records_of_session(session_id, user_id=user_id)
        try:
            extracted = self.extractor.extract(seg_records) if seg_records else []
        except Exception as ex:
            # extraction is a REBUILDABLE derived index (raw lane is truth):
            # an extractor outage must never block the session boundary or
            # wedge the WAL. Facts can be regenerated later via reindex.
            METRICS.inc("memd_extraction_failures_total", ns=ns.namespace)
            self.audit.append(actor="system", action="extraction_failed",
                              target=session_id, detail={"error": str(ex)[:200]})
            extracted = []
        facts_written, consolidation = self._write_facts(ns, extracted, seg_records)
        seg_name = ns.rotate(f"session-close:{session_id}")
        self._taints.drop(session_id)  # taint is per-session; closed = gone
        self._enforce_purge_deadlines(ns)  # segment fold enforces due purges too
        self.audit.append(actor="system", action="close_session", target=session_id,
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
        for old_id, new_id in superseded_all:
            ns.append_op({"op": "supersede", "old": old_id, "new": new_id, "at": now_ms()})
            ns.index.mark_superseded(old_id, new_id, now_ms())
        written = len(kept_recs)
        # persist kept facts in one durable batch (after demote annotation)
        if kept_recs:
            ns.append(kept_recs)
            for r in kept_recs:
                self._embed_worker.submit(r.id, r.content)
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
        self.audit.append(actor=actor, action="hard_delete" if hard else "delete", target=record_id)
        if hard:
            self._enforce_purge_deadlines(ns)
        return ok

    def _enforce_purge_deadlines(self, ns) -> bool:
        """Self-enforce the physical-purge guarantee (D7 #8): when a scheduled
        hard delete comes due, run an unforced compaction NOW instead of
        waiting for an operator to remember /compact. Cheap no-op otherwise
        (one bounded list scan)."""
        if not ns.has_due_deletes():
            return False
        t0 = time.monotonic()
        rep = ns.compact(force=False)  # force=False still enforces due deadlines
        METRICS.inc("memd_auto_compactions_total", reason="hard_delete_deadline", ns=ns.namespace)
        METRICS.observe("memd_compaction_seconds", time.monotonic() - t0,
                        help="compaction duration (s)", ns=ns.namespace, forced="auto")
        self._bump_epoch(ns.namespace)
        self.audit.append(actor="system", action="auto_compact", target=ns.namespace,
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
        plan = plan_query(query)
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
        for rid in ids:
            self.delete(rid, actor=actor, namespace=namespace)
        self.audit.append(actor=actor, action="forget",
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
        ns_store.mark_destroyed()
        with ns_store._lock:
            ok = self.engine.destroy_namespace(name)
        self._bump_epoch(name)
        if name == self.namespace_name:
            self.ns = self.engine.namespace(name)
            self.audit = BufferedAuditLog(
                self.engine.store, f"ns/{name}/audit", self.engine.envelope,
                flush_every=self.audit.flush_every,
            )
        METRICS.inc("memd_destroys_total", ns=name)
        self.audit.append(actor=actor, action="destroy_namespace", target=name)
        return ok

    def export_jsonl(self, namespace: str | None = None) -> bytes:
        impl = self._hosted()
        if impl is not None:
            return impl.export_jsonl(namespace=namespace)
        name = namespace or self.namespace_name
        t0 = time.monotonic()
        data = self.engine.namespace(name).export_jsonl()
        METRICS.observe("memd_export_seconds", time.monotonic() - t0,
                        help="export duration (s)", ns=name)
        METRICS.inc("memd_exports_total", ns=name)
        self.audit.append(actor="export", action="export", target=name, detail={"bytes": len(data)})
        return data

    def compact(self, force: bool = False, namespace: str | None = None) -> dict:
        impl = self._hosted()
        if impl is not None:
            return impl.compact(force=force, namespace=namespace)
        ns = self._ns_for(namespace)
        ns.index.flush()
        self.audit.flush()
        t0 = time.monotonic()
        rep = ns.compact(force=force)
        METRICS.observe("memd_compaction_seconds", time.monotonic() - t0,
                        help="compaction duration (s)", ns=ns.namespace, forced=str(force))
        self._bump_epoch(ns.namespace)
        METRICS.inc("memd_compactions_total", ns=ns.namespace, forced=str(force))
        METRICS.inc("memd_records_purged_total", rep.records_purged, ns=ns.namespace)
        self.audit.append(actor="maintenance", action="compact", target=ns.namespace, detail=rep.__dict__)
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

    def status(self) -> dict:
        impl = self._hosted()
        if impl is not None:
            return impl.status()
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

    def _apply_vectors(self, ids: list[str], vecs: np.ndarray) -> None:
        for i, rid in enumerate(ids):
            self.ns.index.set_vector(rid, vecs[i], self.embedder.name)

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
        METRICS.observe("memd_reembed_seconds", time.monotonic() - t0,
                        help="re-embedding batch duration (s)", ns=ns.namespace)
        METRICS.inc("memd_reembed_total", done, ns=ns.namespace)
        if done:
            self._bump_epoch(ns.namespace)  # vector lane changed -> cached contexts are stale
        self.audit.append(actor="maintenance", action="reembed", target=ns.namespace,
                          detail={"embedded": done, "model": self.embedder.name})
        return {"namespace": ns.namespace, "missing": len(stale), "embedded": done,
                "model": self.embedder.name}

    def flush(self) -> None:
        impl = self._hosted()
        if impl is not None:
            return None  # hosted mode: nothing buffered locally
        self._embed_worker.drain(timeout_s=60)
        self.ns.index.flush()
        self.audit.flush()

    def close(self) -> None:
        impl = self._hosted()
        if impl is not None:
            return impl.close()
        if getattr(self, "_metrics_dumper", None):
            self._metrics_dumper.stop()
        self._embed_worker.stop()
        self.ns.index.flush()
        self.audit.flush()
        self.ns.close()
        self.engine.close()
