"""memd.engine.memory - the Memory facade.

One engine, three doors (SDK / REST / MCP all call this). Embedded mode =
zero external services; hosted mode = same API over HTTP.

Write path: append to WAL -> fsync -> index apply -> ack. No LLM, no
embedding on the critical path (SLO: embedded p99 <= 10ms).
Read path: plan -> fan-out (BM25+entity, time on recency intent, vector
with a real embedder) -> RRF fuse -> optional rerank of the lexical top-30
-> validity filter -> budget-aware packing with provenance tags: dated
session excerpts by default, a flat ranked list on request (gated evidence
packing as an experimental opt-in).
"""
from __future__ import annotations

import contextlib
import json
import logging
import os
import queue
import threading
import time
from collections import OrderedDict
from dataclasses import replace as _dc_replace
from contextlib import ExitStack as _ExitStack
from dataclasses import dataclass, field
from typing import Any, Callable

import numpy as np

from memd.core.schema import ExtractorInfo, Kind, MemoryRecord, Scope, Source, now_ms, ulid_new
from memd.engine import forward as _fw
from memd.index.ann_usearch import requested_vector_index, resolve_vector_index, vector_index_config
from memd.index.sqlite_index import IndexFilter
from memd.index.tantivy_lexical import (
    DEFAULT_COMMIT_DOCS,
    DEFAULT_COMMIT_MS,
    requested_lexical_backend,
    resolve_lexical_backend,
)
from memd.metrics import METRICS, auto_dumper_from_env, preset_core
from memd.pipeline.consolidation import ConsolidationResult, QuarantinePolicy, consolidate_facts
from memd.pipeline.embedder import Embedder, requested_embedder, resolve_embedder
from memd.pipeline.extractor import ExtractedFact, Extractor, resolve_extractor
from memd.query.fusion import FusedItem, rrf_fuse
from memd.query.packing import (
    PackedContext,
    count_tokens,
    gate_candidates,
    pack_context,
    pack_gated,
    pack_sessions,
    rank_for_packing,
)
from memd.query.planner import plan_query
from memd.query.rerank import (
    DEFAULT_RERANK_K,
    RerankStage,
    candidate_from_record,
    requested_reranker,
    resolve_reranker,
)
from memd.storage.audit import AuditLog, BufferedAuditLog
from memd.storage.crypto import envelope_from_config
from memd.storage.engine import NamespaceBusyError, StorageEngine
from memd.storage.objectstore import LocalObjectStore, ReadOnlyError, count_io

_log = logging.getLogger(__name__)

# the packed context's default size, with session packing. Measured on
# LongMemEval_S end-to-end QA: 0.875 for a 12K session pack, but with
# bge-small embeddings, a local cross-encoder reranker and the relative-date
# annotations on (memd ships them off), against 0.779 for the old defaults
# (hash embedder, no reranker, 2K flat). The new defaults as shipped were not
# measured end to end (README-engine.md, "Packing and the budget"). Pass
# budget_tokens=2000 and packing="flat" for the old footprint.
DEFAULT_BUDGET_TOKENS = 12_000
HARD_DELETE_PURGE_MS = 72 * 3600 * 1000  # default physical-purge window for hard deletes
# the embed worker's / maintenance jobs' name for a namespace's READ REPLICA
# in a writer process (a cluster node serves both): "#" is never in a name
_REPLICA_TAG = "#replica"

# The record ids of a call this thread runs for ANOTHER process (see
# Memory._serve_forwarded); unset when the thread runs its own calls.
_SERVING = threading.local()
_NOT_SERVING = object()


@contextlib.contextmanager
def _serving_forwarded(ids):
    prev = getattr(_SERVING, "ids", _NOT_SERVING)
    _SERVING.ids = ids
    try:
        yield
    finally:
        _SERVING.ids = prev


class _Run:
    """Where a call runs (Memory._forward): here (`here`; `ids`: the record
    ids to create it with, when it was - or was tried to be - forwarded), or
    in the namespace's writer in another process, which answered `result`."""

    __slots__ = ("here", "result", "ids")

    def __init__(self, here: bool, result: Any = None, ids: list[str] | None = None):
        self.here, self.result, self.ids = here, result, ids


_HERE = _Run(True)


def _source_name(source) -> str | None:
    """A source as it travels to another process (Source is an IntEnum: by
    name, never by number)."""
    if source is None:
        return None
    return source.name if isinstance(source, Source) else str(source)


class _ForwardedAudit:
    """The facade's ledger while another process holds its namespace: the
    entries are buffered here and appended by the holder (forwarded call
    "audit") - every `flush_every` entries, in the background (an append
    on a read path never waits for the holder), and on flush() / close().
    A ledger has one writer, like the namespace."""

    MAX_BUFFER = 10_000

    def __init__(self, mem: "Memory", namespace: str, flush_every: int):
        self._mem = mem
        self._ns = namespace
        self.flush_every = max(1, int(flush_every))
        self._lock = threading.Lock()
        self._buffer: list[dict] = []
        self._flushing = False

    def append(self, actor: str, action: str, target: str, detail: dict | None = None) -> None:
        d = dict(detail or {})
        d.setdefault("at_ms", now_ms())
        with self._lock:
            self._buffer.append({"actor": actor, "action": action, "target": target, "detail": d})
            if len(self._buffer) > self.MAX_BUFFER:
                self._buffer.pop(0)
                METRICS.inc("memd_audit_entries_dropped_total", ns=self._ns)
            due = len(self._buffer) >= self.flush_every and not self._flushing
            if due:
                self._flushing = True
        if due:
            threading.Thread(target=self._flush_in_background, daemon=True,
                             name="memd-forward-audit").start()

    def _flush_in_background(self) -> None:
        try:
            self.flush()
        finally:
            with self._lock:
                self._flushing = False

    def flush(self) -> None:
        with self._lock:
            entries, self._buffer = self._buffer, []
        if not entries:
            return
        try:
            self._mem._append_forwarded_audit(self._ns, entries)
        except Exception as ex:  # noqa: BLE001 - kept for the next flush
            METRICS.inc("memd_audit_flush_failures_total", ns=self._ns)
            _log.warning("memd: the audit entries for %r could not be sent to its writer (%s); "
                         "kept for the next flush", self._ns, ex)
            with self._lock:
                self._buffer[:0] = entries

    def drain_into(self, ledger) -> None:
        """This process took the namespace: its own ledger takes the rest."""
        with self._lock:
            entries, self._buffer = self._buffer, []
        for e in entries:
            ledger.append(actor=e["actor"], action=e["action"], target=e["target"], detail=e["detail"])

    def read(self) -> list:
        return []

    def verify(self) -> bool:
        return True


def _opt_float(cfg: dict, key: str) -> float | None:
    v = cfg.get(key)
    return None if v is None or v == "" else float(v)


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
    # the reranker actually ran for THIS call (not a cache hit, not a
    # fallback): hosted mode meters reranked searches on it
    reranked: bool = False
    # who served it: "leader" - the namespace's writer - or
    # "replica", with the seq it had applied and its age (ms) at the read
    served_by: str = "leader"
    replica_seq: int | None = None
    replica_age_ms: int | None = None


@dataclass
class SessionTaint:
    """Session taint: explicit writes inherit the session's lowest ingested
    trust tier - an agent processing web content cannot mint user-tier facts
    from it."""
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
# Text handed to the embedder is cut here (config embed_max_chars; 0 = off).
# A local ONNX model pads a batch to its longest member: on LongMemEval turns
# (mean 1083 chars) raw bge-small managed 1.2 texts/s batched against 3.4
# one at a time on this host. Truncating the tail and sorting each batch by
# length bound the padding; the text itself is stored and searched whole.
DEFAULT_EMBED_MAX_CHARS = 2000


def embed_order(batch: dict[str, str], max_chars: int) -> tuple[list[str], list[str]]:
    """(ids, texts) for one embedding call: texts truncated to max_chars
    (0 = no cap) and sorted by length so similar lengths share a padded
    batch. The id list stays aligned with the texts."""
    items = [(rid, t[:max_chars] if max_chars > 0 else t) for rid, t in batch.items()]
    items.sort(key=lambda it: len(it[1]))
    return [rid for rid, _ in items], [t for _, t in items]


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
    heals via reembed() (graceful degradation, now memory-safe)."""

    def __init__(self, embedder: Embedder, apply_fn, batch_size: int = 32, flush_s: float = 0.5,
                 max_retries: int = 3, max_queue: int = 5_000,
                 max_chars: int = DEFAULT_EMBED_MAX_CHARS):
        self.embedder = embedder
        self.apply_fn = apply_fn  # callable(ns_name, ids, vecs)
        self.max_chars = int(max_chars)
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
        # set once the embedder's load() has returned or raised
        self._load_done = threading.Event()
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

    def _load_embedder(self) -> None:
        """Warm the embedder HERE, on the worker's own thread. A local ONNX
        model costs seconds to import and load; built inside Memory() it sat
        ahead of the first write ack (the pass-19 SIGKILL test saw nothing
        acked within 0.5s). Writes queue meanwhile, and search serves from the
        lanes that need no query vector until embedder.ready() flips."""
        try:
            self.embedder.load()
        except Exception:
            # every batch now fails against it and dead-letters; the bm25 and
            # entity lanes still serve, and reembed() heals the vector lane
            # once a working embedder is configured
            _log.exception("memd: embedder %s failed to load; the vector lane is degraded",
                           self.embedder.name)
        finally:
            self._load_done.set()

    def wait_ready(self, timeout_s: float) -> bool:
        """Wait (bounded) for the embedder load to finish; True if it did."""
        return self._load_done.wait(timeout=max(0.0, timeout_s))

    def _run(self) -> None:
        self._load_embedder()
        while not self._stop.is_set():
            try:
                ns_name, rid, text = self.q.get(timeout=self.flush_s)
            except queue.Empty:
                continue
            # Count the work as in-flight the moment it LEAVES the queue.
            # Incrementing after the batch was assembled left dequeued items in
            # neither q.qsize() nor _inflight, so drain() - and therefore
            # flush() - could report the vector lane complete while a batch was
            # still in hand. The window is narrow (it lost to drain()'s 50ms
            # poll in 80 trials) but it is real, and this is where the count
            # belongs.
            with self._inflight_lock:
                self._inflight += 1
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
            try:
                for ns_bucket, batch in batches.items():
                    self._embed_one(ns_bucket, batch)
            finally:
                with self._inflight_lock:
                    self._inflight -= 1

    def _embed_one(self, ns_name: str, batch: dict[str, str]) -> None:
        t0 = time.monotonic()
        try:
            ids, texts = embed_order(batch, self.max_chars)
            vecs = self.embedder.embed(texts)
            self.apply_fn(ns_name, ids, vecs)
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


class _NullAuditLog:
    """Ledger for a namespace that no longer exists. Appends are discarded
    rather than encrypted, because encrypting would re-create the data key the
    crypto-shred just destroyed."""

    flush_every = 0

    def append(self, *a, **kw) -> None:
        METRICS.inc("memd_audit_appends_dropped_total",
                    help="audit appends discarded for a shredded namespace")

    def flush(self) -> None:
        return None

    def read(self) -> list:
        return []

    def verify(self) -> bool:
        return True


def forget_fingerprint(ids: list[str]) -> str:
    """Identifies the exact set of ids a forget() preview showed: a confirm
    that passes it back deletes nothing unless it would delete that set."""
    import hashlib

    return hashlib.sha256("\n".join(sorted(ids)).encode()).hexdigest()[:32]


class ExportStream:
    """An export whose fold has run (Memory.export_stream). Iterating yields
    its NDJSON lines. `skipped_frames`: the unreadable WAL frames it left out
    (log, byte offset, fault); `skipped`: how many (also set where only the
    count is known - a hosted client). 0: the export is complete."""

    def __init__(self, lines, skipped_frames: list[dict], skipped: int | None = None):
        self._lines = lines
        self.skipped_frames = list(skipped_frames)
        self.skipped = len(self.skipped_frames) if skipped is None else int(skipped)

    def __iter__(self):
        return self._lines


class ForgetPreviewMismatch(Exception):
    """A confirmed forget() would delete a different set than its preview."""


class _MaintenanceWorker:
    """Background runner for namespace-scale maintenance.

    The SLOs are explicit: "Per-write maintenance scope: O(entity cluster),
    never O(namespace)". Due hard-delete purges violated that by running
    `ns.compact()` INLINE on the writer's thread - compaction reads every live
    record and rewrites the whole live set, so the first ordinary write after
    a purge deadline came due paid 8131ms at 200K records (vs ~3ms for the
    identical call moments before), 813x the embedded write-ack SLO. In hosted
    mode that write holds the namespace lock, stalling every concurrent
    request on the tenant.

    The compliance guarantee is unchanged: the purge still happens
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


PACK_MODES = ("auto", "ranked", "gated")
PACKINGS = ("sessions", "flat")
DEFAULT_RERANK_GATE = 0.5


def resolve_fuse_vector(config: dict | None, embedder: Embedder) -> bool:
    """Whether the vector lane is fused into ranking: config["fuse_vector"]
    (or env MEMD_FUSE_VECTOR) = "auto" | true | false.

    auto = fuse unless the embedder is the HashEmbedder. Once the bm25 lane
    ranked by real bm25, fusing the hash "vector" lane (feature-hashed
    n-grams - a noisier copy of the lexical signal) into RRF cost ~0.07
    ndcg@5 on LongMemEval_S. Hash vectors are still computed and stored:
    forget() and dedupe use them."""
    cfg = config or {}
    v = cfg.get("fuse_vector")
    if v is None:
        v = os.environ.get("MEMD_FUSE_VECTOR", "auto")
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s == "auto":
        return embedder.kind != "hash"
    if s in ("1", "true", "yes", "on"):
        return True
    if s in ("0", "false", "no", "off"):
        return False
    raise ValueError(f"fuse_vector must be auto|true|false, not {v!r}")


def resolve_pack_mode(config: dict | None) -> str:
    """config["pack_mode"] (or env MEMD_PACK_MODE): "auto" | "ranked" |
    "gated". "auto" means ranked, for every reranker.

    "gated" is an EXPERIMENTAL opt-in for token savings. In an experiment it
    matched top-k QA accuracy at 27% fewer tokens, but over a 100-candidate
    shortlist; over the product's top-30 it DROPS second evidence sessions:
    Jev + gated scored session ndcg@5 0.906 / recall_all@5 0.803 on
    LongMemEval_S dev (below no reranker's 0.835 recall_all@5), while Jev +
    ranked scored 0.955 / 0.928 (experiments 020/021)."""
    cfg = config or {}
    mode = str(cfg.get("pack_mode") or os.environ.get("MEMD_PACK_MODE") or "auto").strip().lower()
    if mode not in PACK_MODES:
        raise ValueError(f"unknown pack_mode {mode!r}; expected one of {list(PACK_MODES)}")
    return mode


def resolve_packing(config: dict | None) -> str:
    """config["packing"] (or env MEMD_PACKING): the packed context's layout.

    "sessions" (default): dated session excerpts - each retrieved turn with
    its neighbouring turns, a fact under the turn it came from, sessions
    oldest first, the speaker on every line. "flat": one provenance-tagged
    <memory> element per candidate in rank order (the layout memd used
    before session packing)."""
    cfg = config or {}
    return check_packing(cfg.get("packing") or os.environ.get("MEMD_PACKING") or "sessions")


def check_packing(value: str) -> str:
    v = str(value).strip().lower()
    if v not in PACKINGS:
        raise ValueError(f"unknown packing {value!r}; expected one of {list(PACKINGS)}")
    return v


def resolve_pack_resolve_dates(config: dict | None) -> bool:
    """config["pack_resolve_dates"] (or env MEMD_PACK_RESOLVE_DATES): annotate
    relative time expressions in packed user turns with the date they refer
    to ("yesterday [= Fri 2023-05-19]"). Off by default: in the LongMemEval_S
    measurement there was no sign that the annotations help."""
    cfg = config or {}
    v = cfg.get("pack_resolve_dates")
    if v is None:
        v = os.environ.get("MEMD_PACK_RESOLVE_DATES", "false")
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "on"):
        return True
    if s in ("0", "false", "no", "off", ""):
        return False
    raise ValueError(f"pack_resolve_dates must be true|false, not {v!r}")


MAX_QUERY_CHARS = 64 * 1024  # embedder-cost guard for engine-side queries
# session packing reads the neighbouring turns of this many anchors in its
# first statement (doubling with each further one)
_NEIGHBOUR_READ_AHEAD = 4
# find_ids: a vector-only candidate must beat the median of the sweep's
# vector sample by this much (see Memory._vector_only_floor); the median is
# only trusted once the sample has a few points in it
_VECTOR_ONLY_MEDIAN_MARGIN = 0.1
_VECTOR_ONLY_MIN_SAMPLE = 5


class Memory:
    # hosted mode: set by __init__ when api_key= is given; every public
    # method then delegates to it so "one engine, three doors" holds at the
    # facade level too (not just via HostedMemory directly)
    _impl: Any = None
    # write forwarding (memd.engine.forward): the router to namespaces
    # another process holds, this process's endpoint, and the facade's
    # ledger while another process holds its namespace
    _fwd: "_fw.Forwarder | None" = None
    _fwd_server: "_fw.ForwardServer | None" = None
    _fwd_audit: "_ForwardedAudit | None" = None

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
        read_only: bool = False,
        forwarding: str | None = None,
    ):
        """`read_only` (or config `read_only`): open every namespace as a
        READ REPLICA - no lease, nothing written or deleted
        in the store or the keys, every mutating call refused with
        ReadOnlyError; reads follow the namespace's writer within
        `replica_refresh_s` (default 2 s).

        `forwarding` (or config `forwarding`, MEMD_FORWARDING): "auto" (the
        default) - a namespace another process on the data root holds has
        its writes and strong reads run by that process (see
        memd.engine.forward), and the next forwarded call takes it over
        when that process goes away; "off" - such a namespace raises
        NamespaceBusyError, as before forwarding existed."""
        cfg = dict(config or {})
        if api_key:
            from memd.sdk.client import HostedMemory

            self._impl = HostedMemory(api_key=api_key, base_url=base_url or "http://localhost:8700",
                                      namespace=namespace, transport=transport)
            return
        store = None
        if str(path).startswith("s3://"):
            # Remote source of truth. Two things stay LOCAL and must, so the
            # local directory is part of the configuration rather than an
            # implementation detail:
            #   - the SQLite derived index (rebuildable by contract; the pass-22
            #     snapshot is what makes a cold node cheap), and
            #   - envelope keys WITH THE DEFAULT `local` KEY PROVIDER, which
            #     makes this "one node with remote durability". A remote
            #     provider (key_provider="aws-kms" | "vault-transit")
            #     keeps the wrapped keys in the bucket instead, and then any
            #     authorised node can serve any namespace.
            from memd.storage.s3store import S3ObjectStore

            rest = str(path)[len("s3://"):]
            bucket, _, s3_prefix = rest.partition("/")
            if not bucket:
                raise ValueError(f"malformed s3 url {path!r}: expected s3://bucket[/prefix]")
            store = S3ObjectStore(
                bucket=bucket, prefix=s3_prefix,
                endpoint_url=cfg.get("s3_endpoint_url") or os.environ.get("MEMD_S3_ENDPOINT"),
                # separate from AWS_* so a MinIO / R2 bucket and AWS KMS can
                # use different credentials in one process
                access_key=cfg.get("s3_access_key") or os.environ.get("MEMD_S3_ACCESS_KEY"),
                secret_key=cfg.get("s3_secret_key") or os.environ.get("MEMD_S3_SECRET_KEY"),
                region=cfg.get("s3_region") or os.environ.get("AWS_REGION"),
                lease_ttl_s=float(cfg.get("lease_ttl_s") or os.environ.get("MEMD_LEASE_TTL_S") or 60.0),
                lease_holder=cfg.get("lease_holder"),
            )
            path = str(cfg.get("local_dir")
                       or os.environ.get("MEMD_LOCAL_DIR")
                       or os.path.join(".memd-local", bucket, s3_prefix or "_"))
        read_only = bool(read_only or cfg.get("read_only"))
        self.read_only = read_only
        os.makedirs(path, exist_ok=True)
        remote = store is not None
        if store is None:
            store = LocalObjectStore(os.path.join(path, "store"))
        # Write forwarding: this process's endpoint is up - and its address
        # is what it writes into the locks and leases it takes - before it
        # opens any namespace. A configured lease holder (a cluster node)
        # advertises no endpoint: the cluster router routes between nodes.
        fcfg = _fw.ForwardConfig.from_config(
            cfg, forwarding, lease_ttl_s=getattr(store, "lease_ttl_s", None) if remote else None)
        self._fwd_ready = threading.Event()
        fwd_secret = holder = None
        if fcfg.enabled and not read_only and not os.environ.get("MEMD_ALLOW_MULTI_PROCESS"):
            try:
                fwd_secret = _fw.load_secret(fcfg, path)
            except OSError as ex:
                # (a data directory shared with another user, whose secret
                # this one cannot read): without it, as with forwarding off
                _log.warning("memd: write forwarding is off for this process: the forwarding "
                             "secret cannot be read or created (%s)", ex)
        if fwd_secret is not None:
            holder = getattr(store, "lease_holder", None)
            if not holder:
                self._fwd_server = _fw.ForwardServer(fcfg, fwd_secret, self._serve_forwarded)
                holder = store.lease_holder = self._fwd_server.holder
        # Key custody: who holds the root key. `local` (default) keeps it in a file
        # under <path>/keys exactly as before; a remote provider (aws-kms,
        # vault-transit) keeps wrapped data keys as objects in `store`, so any
        # node authorised on the provider can open any namespace. (With
        # encryption off, the keys directory - and the store's wrapped keys -
        # still tell an open that a namespace was written with it on: see
        # NamespaceStore._verify_key)
        envelope = envelope_from_config(cfg, path, store, encrypt=encrypt, read_only=read_only)
        # resolved before any namespace opens: the accelerators attach at open
        self.lexical_backend = resolve_lexical_backend(cfg)
        self.vector_index = resolve_vector_index(cfg)
        self.engine = StorageEngine(os.path.join(path, "store"), envelope=envelope,
                                    store=store,
                                    cache_dir=os.path.join(path, "_cache") if remote else None,
                                    lexical={
                                        "backend": self.lexical_backend,
                                        "commit_ms": int(cfg.get("lexical_commit_ms", DEFAULT_COMMIT_MS)),
                                        "commit_docs": int(cfg.get("lexical_commit_docs", DEFAULT_COMMIT_DOCS)),
                                    },
                                    vector_index=vector_index_config(cfg, self.vector_index),
                                    cache_sweep_s=cfg.get("cache_sweep_s"),
                                    read_only=read_only,
                                    replica_refresh_s=_opt_float(cfg, "replica_refresh_s"),
                                    max_replicas=cfg.get("max_replicas"),
                                    replica_idle_s=_opt_float(cfg, "replica_idle_s"),
                                    replica_max_staleness_s=(
                                        None if cfg.get("replica_max_staleness_ms") is None
                                        else float(cfg["replica_max_staleness_ms"]) / 1000.0),
                                    replica_refresh_wait_s=(
                                        None if cfg.get("replica_refresh_wait_ms") is None
                                        else float(cfg["replica_refresh_wait_ms"]) / 1000.0),
                                    replica_connect_timeout_s=_opt_float(cfg, "replica_connect_timeout_s"),
                                    replica_read_timeout_s=_opt_float(cfg, "replica_read_timeout_s"),
                                    replica_max_attempts=cfg.get("replica_max_attempts"))
        self.namespace_name = namespace
        # Audit ledgers are PER NAMESPACE. They are held in an LRU keyed by
        # namespace (mirroring the engine's namespace table) and routed by the
        # OPERATION's target namespace - see _audit_for(). Set up before any
        # namespace opens: opening one can already produce audit events
        # (segments a crashed compaction left, collected at open).
        self._audit_flush_every = int(cfg.get("audit_flush_every", 32))
        self._audit_max_open = int(cfg.get("audit_max_open", 64))
        self._audit_lock = threading.Lock()
        self._audits: "OrderedDict[str, BufferedAuditLog]" = OrderedDict()
        # Namespaces crypto-shredded by this process. An audit append encrypts
        # its payload, and encrypting for a shredded namespace MINTS A FRESH
        # DATA KEY - resurrecting what the crypto-shred just destroyed. Pass 16 dropped
        # the ledger on destroy, but any later _audit_for() rebuilt it: a
        # compaction whose audit entry lands after the destroy (the pass-22
        # snapshot widened that window) was enough. Bounded; cleared when the
        # namespace legitimately exists again.
        self._shredded: "OrderedDict[str, None]" = OrderedDict()
        self.engine.audit_hook = self._audit_engine_event
        self.engine.close_hook = self._namespace_closing
        self.engine.replica_hook = self._replica_event
        # the facade holds a direct reference to this store for its lifetime:
        # pin it so LRU churn of other namespaces can't close it underneath us
        self.engine.pin_namespace(namespace)
        if fwd_secret is not None:
            self._fwd = _fw.Forwarder(self.engine, fcfg, fwd_secret, holder)
            self._fwd_audit = _ForwardedAudit(self, namespace, self._audit_flush_every)
            if self._fwd_server is not None:
                self._fwd_server.start()
        try:
            # None: another process holds it - this facade's calls on it are
            # forwarded there, until the holder goes away and one takes it
            self.ns: Any = self._open_facade_namespace(namespace)
        except BaseException:
            self._stop_forwarding()
            raise
        self.embedder: Embedder = resolve_embedder(cfg)
        self.fuse_vector: bool = resolve_fuse_vector(cfg, self.embedder)
        reranker = resolve_reranker(cfg)
        self.rerank: RerankStage | None = (
            RerankStage(reranker, k=int(cfg.get("rerank_k", DEFAULT_RERANK_K))) if reranker else None)
        self.pack_mode: str = resolve_pack_mode(cfg)
        self.packing: str = resolve_packing(cfg)
        self.pack_resolve_dates: bool = resolve_pack_resolve_dates(cfg)
        self.rerank_gate: float = float(cfg.get("rerank_gate", DEFAULT_RERANK_GATE))
        self._lexical_flush_drain_s = float(cfg.get("lexical_flush_drain_s", 60.0))
        self._vector_flush_drain_s = float(cfg.get("vector_flush_drain_s", 60.0))
        self.extractor: Extractor = resolve_extractor(cfg)
        self.quarantine = QuarantinePolicy(
            rate_max_writes=int(cfg.get("rate_max_writes", 120)),
            dup_max_repeats=int(cfg.get("dup_max_repeats", 5)),
        )
        # Privacy: search queries are user content. Audit stores a short hash
        # by default (correlation without exposure); opt in to text for debug.
        self._audit_query_text = bool(cfg.get("audit_query_text", False))
        # Physical-purge window for hard deletes (hosted compliance
        # tiers may tighten it; the deadline is self-enforced, see
        # _enforce_purge_deadlines)
        self._purge_deadline_ms = int(cfg.get("hard_delete_deadline_ms", HARD_DELETE_PURGE_MS))
        self._taints = TaintStore(max_sessions=int(cfg.get("max_tracked_sessions", 10_000)))
        self._metrics_dumper = auto_dumper_from_env()  # MEMD_METRICS_PATH opt-in
        preset_core(METRICS, ns=namespace)  # single registration: series exist from t=0
        self._qcache = _SearchCache()
        self._qepochs: dict[str, int] = {}
        self._embed_close_drain_s = float(cfg.get("embed_close_drain_s", 30.0))
        self._embed_flush_drain_s = float(cfg.get("embed_flush_drain_s", 60.0))
        self._maint = _MaintenanceWorker(self._run_maintenance)
        # Purge deadlines at open, too: a purge that came due while the process was down
        # - or one the format-1 migration recovered from the audit ledger, due
        # at once - is scheduled as the namespace opens, not left on disk
        # until the next write there happens to check the deadline
        self.engine.open_hook = lambda _name, store: self._enforce_purge_deadlines(store)
        if self.ns is not None:
            self._enforce_purge_deadlines(self.ns)
        self._embed_max_chars = int(cfg.get("embed_max_chars", DEFAULT_EMBED_MAX_CHARS))
        self._embed_worker = _EmbedWorker(
            self.embedder,
            self._apply_vectors,
            batch_size=int(cfg.get("embed_batch", 32)),
            max_queue=int(cfg.get("embed_max_queue", 5_000)),
            max_chars=self._embed_max_chars,
        )
        if self.ns is not None:
            self.audit.append(actor="system", action="open", target=namespace,
                              detail={"embedder": self.embedder.name})
        # which embedder is active decides retrieval AND forget() semantics,
        # and "auto" depends on what happens to be installed: say it once
        _log.info("memd: opened namespace %r with embedder %s (kind=%s, requested=%s)",
                  namespace, self.embedder.name, self.embedder.kind, requested_embedder(cfg))
        # the reranker decides whether search text leaves the machine: say so
        _log.info("memd: reranker %s (model=%s, requested=%s, pack_mode=%s, packing=%s); "
                  "lexical backend %s (requested=%s); vector index %s (requested=%s); "
                  "fuse_vector=%s",
                  self.rerank.name if self.rerank else "none",
                  self.rerank.model if self.rerank else "-", requested_reranker(cfg),
                  self.pack_mode, self.packing, self.lexical_backend, requested_lexical_backend(cfg),
                  self.vector_index, requested_vector_index(cfg), self.fuse_vector)
        if self.rerank is not None and callable(getattr(self.rerank.reranker, "load", None)):
            # warm the reranker off the caller's path: a local cross-encoder
            # takes seconds to load (a first download is ~1GB) and the Jev SDK
            # ~1s to import - either would eat the first search's deadline
            threading.Thread(target=self._load_reranker, daemon=True, name="memd-rerank-load").start()
        self._vector_selfheal = bool(cfg.get("vector_selfheal", True))
        if self.ns is not None:
            self._report_vector_health(self.ns)
        self._fwd_ready.set()

    # ------------------------------------------------------------ forwarding

    def _open_facade_namespace(self, name: str):
        """The facade's own namespace, opened here - or None when another
        process holds it and forwarding is on (raises when forwarding to
        that process cannot work: see Forwarder.probe)."""
        try:
            return self.engine.namespace(name)
        except NamespaceBusyError:
            if self._fwd is None:
                raise
        return self.engine.namespace(name) if self._fwd.probe(name) else None

    def _stop_forwarding(self) -> None:
        if self._fwd_server is not None:
            self._fwd_server.stop(drain_s=float(getattr(self, "_embed_close_drain_s", 30.0)))
        if self._fwd is not None:
            self._fwd.close()

    def _forward(self, namespace: str | None, op: str, args: Any = None, n_ids: int = 0, *,
                 write: bool = True) -> _Run:
        """Where a call on `namespace` runs: here when this process holds
        it (or takes it now - its lock or lease was free), else in the
        process that holds it, whose answer comes back as the result.
        `args` (a dict, or a callable building it) is the call as it
        travels; `n_ids`: how many records it creates - their ids are
        generated here, so that every retry of it creates the same records
        and a repeat is recognised (Memory._applied). A call this thread
        runs for another process always runs here."""
        ids = getattr(_SERVING, "ids", _NOT_SERVING)
        if ids is not _NOT_SERVING:
            return _Run(True, None, ids if n_ids else None)
        fwd = self._fwd
        if fwd is None:
            return _HERE
        name = namespace or self.namespace_name
        if self.engine.holds(name):
            return _HERE
        ids = [ulid_new() for _ in range(n_ids)] if n_ids else None
        out = fwd.call(name, op, args() if callable(args) else dict(args or {}), ids, write=write)
        if out is _fw.LOCAL:
            return _Run(True, None, ids)
        return _Run(False, out)

    @staticmethod
    def _applied(ns, ids: list[str] | None) -> bool:
        """A forwarded write's records were written to the namespace already
        - any of them: a batch is one append - by an earlier attempt of the
        same call, in this process or in a holder before it, whose log this
        one replayed. A record deleted since counts, a hard-deleted one too
        (its row is gone, its id is remembered: NamespaceIndex.known_ids):
        a retry must not bring back what was deleted after the write."""
        return bool(ids) and bool(ns.index.known_ids(ids))

    def _serve_forwarded(self, op: str, ns: str, args: dict, ids: list[str] | None):
        """ForwardServer handler: run a call another process forwarded -
        only on a namespace this process holds (NamespaceBusyError: not
        the writer, nothing applied; the caller finds the writer again)."""
        if not self._fwd_ready.wait(10.0):
            raise _fw.ForwardNotReady("the namespace's writer is still opening")
        if not self.engine.holds(ns):
            raise NamespaceBusyError(f"namespace {ns!r} is not held by this process")
        fn = _SERVED.get(op)
        if fn is None:
            raise ValueError(f"unknown forwarded call {op!r}")
        with _serving_forwarded(ids):
            return fn(self, ns, args)

    def _append_forwarded_audit(self, ns_name: str, entries: list[dict]) -> None:
        run = self._forward(ns_name, "audit", {"entries": entries})
        if run.here:
            ledger = self._audit_for(ns_name)
            for e in entries:
                ledger.append(actor=e["actor"], action=e["action"], target=e["target"], detail=e["detail"])

    @property
    def audit(self):
        """The facade's own ledger - its namespace's. While another process
        holds that namespace, its entries are appended by that process."""
        fa = self._fwd_audit
        if fa is None or self.engine.holds(self.namespace_name):
            ledger = self._audit_for(self.namespace_name)
            if fa is not None and fa._buffer:
                fa.drain_into(ledger)
            return ledger
        return fa

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
        self._writable("add")
        _guard_input(content, meta)
        _guard_kind(kind)
        run = self._forward(namespace, "add", lambda: dict(
            content=content, session_id=session_id, user_id=user_id, agent_id=agent_id,
            org_id=org_id, role=role, kind=kind, source=_source_name(source), actor_id=actor_id,
            t_event=t_event, meta=meta), n_ids=1)
        if not run.here:
            return run.result
        ns = self._ns_for(namespace)
        if self._applied(ns, run.ids):
            return list(run.ids)
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
            record_id=run.ids[0] if run.ids else None,
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
        self._writable("add_events")
        if len(events) > MAX_BATCH_EVENTS:
            raise ValueError(f"batch exceeds {MAX_BATCH_EVENTS} events; split the call")
        run = self._forward(namespace, "add_events", lambda: {"events": [
            dict(e, source=_source_name(e["source"])) if isinstance(e, dict) and "source" in e else e
            for e in events]}, n_ids=len(events))
        if not run.here:
            return run.result
        ns = self._ns_for(namespace)
        if self._applied(ns, run.ids):
            return list(run.ids)
        records: list[MemoryRecord] = []
        taint_updates: list[tuple[str, int]] = []
        for i, e in enumerate(events):
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
                record_id=run.ids[i] if run.ids else None,
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
        self._writable("remember")
        _guard_input(content, None)
        _guard_kind(kind)
        run = self._forward(namespace, "remember", lambda: dict(
            content=content, kind=kind, entity_keys=entity_keys, session_id=session_id,
            user_id=user_id, agent_id=agent_id, org_id=org_id, source=_source_name(source),
            actor_id=actor_id, t_event=t_event, valid_from=valid_from), n_ids=1)
        if not run.here:
            return run.result
        ns = self._ns_for(namespace)
        if self._applied(ns, run.ids):
            return run.ids[0]
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
            record_id=run.ids[0] if run.ids else None,
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
        self._writable("observe")
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
        if self.read_only:
            return _NullAuditLog()   # a replica writes no object - the ledger is the writer's
        with self._audit_lock:
            if ns_name in self._shredded:
                return _NullAuditLog()   # never resurrect a shredded namespace
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

    def _namespace_closing(self, ns_name: str, lost: bool) -> None:
        """The engine is closing `ns_name` (LRU eviction, shutdown) or dropped
        it after another writer took it over. Its ledger's buffered entries
        are written now, while this process still holds the namespace - or
        dropped when it no longer does (writing them would be a write without
        the lease) - and the ledger object goes: the next tenure reloads the
        tail another node may have extended. Cached searches go stale too."""
        if ns_name == self.namespace_name and not lost:
            return   # the facade's pinned namespace is never evicted
        with self._audit_lock:
            log = self._audits.pop(ns_name, None)
        if log is not None and not lost:
            try:
                log.flush()
            except Exception:
                METRICS.inc("memd_audit_flush_failures_total", ns=ns_name)
        elif log is not None:
            METRICS.inc("memd_audit_entries_dropped_total",
                        help="buffered audit entries of a namespace lost to another writer",
                        ns=ns_name)
        self._bump_epoch(ns_name)

    def _audit_engine_event(self, ns_name: str, action: str, target: str, detail: dict) -> None:
        """StorageEngine.audit_hook: engine-initiated events land in the
        ledger this facade owns, so its hash chain stays one chain."""
        self._audit_for(ns_name).append(actor="maintenance", action=action, target=target,
                                        detail=detail)

    def _flush_all_audits(self) -> None:
        if self._fwd_audit is not None and self._fwd_audit._buffer:
            self.audit.flush()     # the holder's, or (taken over since) this process's
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
        rerank: bool = True,
        consistency: str | None = None,
        max_staleness_ms: int | None = None,
        packing: str | None = None,
    ) -> SearchResult:
        """`packing` lays out the packed context: "sessions" (dated session
        excerpts, the turns around each hit) or "flat" (one tagged element
        per hit, in rank order); None = the Memory's `packing` config.
        `budget_tokens` caps it either way (default 12,000).

        `rerank=False` skips the reranker for this call (hosted mode: the
        org's reranked-search quota is spent); the result is the unreranked
        order, counted as a fallback with reason "quota" and not cached.

        `consistency="eventual"` accepts a read replica: the
        namespace's writer when it is open here, else this process's
        replica of it, no staler than `max_staleness_ms` (default
        replica_max_staleness_ms); the result says which (`served_by`,
        `replica_seq`, `replica_age_ms`). "strong" (the default; a read-only
        Memory is always eventual) reads the writer."""
        if len(query) > MAX_QUERY_CHARS:
            raise ValueError(f"query exceeds {MAX_QUERY_CHARS} char cap")
        if packing is not None:
            packing = check_packing(packing)
        impl = self._hosted()
        if impl is not None:
            return impl.search(query, user_id=user_id, session_id=session_id,
                               agent_id=agent_id, org_id=org_id,
                               budget_tokens=budget_tokens, as_of=as_of,
                               kinds=kinds, include_quarantined=include_quarantined,
                               namespace=namespace, consistency=consistency,
                               max_staleness_ms=max_staleness_ms, packing=packing)
        t0 = time.monotonic()
        if (consistency or ("eventual" if self.read_only else "strong")) == "strong":
            # every search parameter travels; the layout is the caller's
            # (its per-call choice, else its own default), not the holder's
            run = self._forward(namespace, "search", lambda: dict(
                query=query, user_id=user_id, session_id=session_id, agent_id=agent_id,
                org_id=org_id, budget_tokens=budget_tokens, as_of=as_of, kinds=kinds,
                include_quarantined=include_quarantined, rerank=rerank,
                packing=packing or self.packing), write=False)
            if not run.here:
                return _search_result(run.result)
        ns_idx, info = self._reader(namespace, consistency, max_staleness_ms)
        with self._serving(ns_idx, info):
            return self._search(ns_idx, info, t0, query, user_id=user_id, session_id=session_id,
                                agent_id=agent_id, org_id=org_id, budget_tokens=budget_tokens,
                                as_of=as_of, kinds=kinds,
                                include_quarantined=include_quarantined, rerank=rerank,
                                packing=packing or self.packing)

    def _search(self, ns_idx, info: dict | None, t0: float, query: str, *, user_id, session_id,
                agent_id, org_id, budget_tokens, as_of, kinds, include_quarantined,
                rerank, packing) -> SearchResult:
        ns_name = ns_idx.namespace
        cache_key = (
            ns_name, query,
            (user_id, session_id, agent_id, org_id),
            budget_tokens, packing, as_of, tuple(kinds) if kinds else None,
            include_quarantined, self._qepochs.get(ns_name, 0),
            # a result served before the model loaded has no vector lane: it
            # must not outlive the load - nor one served while the lane skips
            # queries because the ANN sidecar is still loading or rebuilding
            self.embedder.ready(), ns_idx.index.vector_lane_degraded(),
            # a replica's results never stand in for the writer's, and go
            # stale with every refresh that changed what it serves
            (info or {}).get("served_by", "leader"), getattr(ns_idx, "data_epoch", 0),
        )
        cached = self._qcache.get(cache_key)
        if cached is not None:
            hit_ms = (time.monotonic() - t0) * 1000
            METRICS.inc("memd_search_cache_hits_total")
            METRICS.observe("memd_search_latency_ms", hit_ms,
                            help="end-to-end search latency (ms)", ns=ns_name,
                            qclass=cached.query_class, cache="hit")
            # latency_ms must describe THIS call. Returning the cached object
            # verbatim replayed the original MISS's latency to every
            # subsequent hit - so a caller (and bench/slo_bench.py, which
            # grades the retrieval SLO from this field) read a stale number
            # that described neither the hit nor a fresh query.
            return _dc_replace(cached, latency_ms=round(hit_ms, 3), reranked=False,
                               **self._info_fields(info))
        METRICS.inc("memd_search_cache_misses_total")
        # entered/exited explicitly rather than via `with`, because the tally
        # must close AFTER the latency observation below. count_io()'s reset
        # tolerates being unwound in another context, so an early return or an
        # exception leaves at most an orphan dict that the next search replaces.
        io_ctx = count_io()
        io_tally = io_ctx.__enter__()
        ns = ns_idx
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
        lanes = [
            ("bm25", lambda: ns.index.search_bm25(query, filt, limit=plan.candidate_k)),
            ("entity", lambda: ns.index.search_by_entity_tokens(tokens, filt, limit=20)),
        ]
        if plan.use_time_lane:
            # the documented third fan-out (time/entity btree
            # scan). It returns the newest rows REGARDLESS of the query, so it
            # runs only on recency intent or an explicit time bound: invoked
            # unconditionally it injected query-independent rows into fusion
            # (measured -0.048 ndcg@5 on LongMemEval)
            lanes.append(("time", lambda: ns.index.search_time_lane(filt, limit=plan.candidate_k)))
        # per-lane stage timing: end-to-end latency alone can't show WHICH
        # lane regressed (SLO triage needs the breakdown)
        for lane_name, lane_fn in lanes:
            _lt0 = time.monotonic()
            lane_hits[lane_name] = lane_fn()
            METRICS.observe("memd_lane_ms", (time.monotonic() - _lt0) * 1000,
                            help="per-lane candidate fetch duration (ms)", ns=ns.namespace, lane=lane_name)
        # graceful degradation: if the embedder is unreachable
        # (BYO-key API outage) or its model is still loading in the embed
        # worker, BM25 + entity lanes still serve - retrieval degrades, it
        # never dies, and it never waits on a model load. The hash embedder's
        # lane is not fused at all (see resolve_fuse_vector).
        if self.fuse_vector:
            if not self.embedder.ready():
                METRICS.inc("memd_embed_query_not_ready_total", ns=ns.namespace)
            else:
                try:
                    _lt0 = time.monotonic()
                    qvec = self.embedder.embed_one(query)
                    lane_hits["vector"] = ns.index.search_vector(qvec, filt, limit=plan.candidate_k)
                    METRICS.observe("memd_lane_ms", (time.monotonic() - _lt0) * 1000,
                                    help="per-lane candidate fetch duration (ms)", ns=ns.namespace,
                                    lane="vector")
                except Exception:
                    METRICS.inc("memd_embed_query_failures_total", ns=ns.namespace)
        _st0 = time.monotonic()
        fused = rrf_fuse(lane_hits, weights=plan.weights, limit=max(plan.candidate_k, 40))
        METRICS.observe("memd_search_stage_ms", (time.monotonic() - _st0) * 1000,
                        help="per-stage search timing (ms): plan/fuse/pack",
                        ns=ns.namespace, stage="fuse")
        rerank_order, degraded = self._rerank(query, lane_hits, fused, ns.namespace, allowed=rerank)
        rerank_scores: dict[str, float] | None = None
        if rerank_order is not None:
            rerank_scores = {it.record.id: p for it, p in rerank_order}
            # the reranked shortlist leads, in the reranker's order; the rest
            # of the fused list follows in its own order
            fused = [it for it, _ in rerank_order] + [
                it for it in fused if it.record.id not in rerank_scores]
        _st0 = time.monotonic()
        if self._pack_mode_for(rerank_order) == "gated":
            kept = gate_candidates(rerank_order, self.rerank_gate)
            neighbours, positions = ns.index.session_neighbours(
                [it.record.id for it, _ in kept if it.record.kind == "raw_event"], filt, radius=1)
            packed = pack_gated(kept, neighbours, positions, budget_tokens=budget_tokens,
                                query_class=plan.qclass)
        elif packing == "flat":
            # as_of anchors the recency tilt; without it packing is data-relative
            packed = pack_context(fused, budget_tokens=budget_tokens, now=as_of, query_class=plan.qclass,
                                  rerank_scores=rerank_scores)
        else:
            packed = self._pack_sessions(ns, fused, rerank_scores, scope=scope, as_of=as_of, kinds=kinds,
                                         include_quarantined=include_quarantined,
                                         budget_tokens=budget_tokens, query_class=plan.qclass)
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
        if info is not None and info.get("served_by") == "replica":
            # a replica cannot append to the namespace's ledger (the writer's
            # object): a writer process audits the read in its own ledger
            # (a cluster node's memd-node.<id>), naming the namespace
            if not self.read_only:
                self.audit.append(actor="search", action="replica_search",
                                  target=self._audit_target_for_query(query),
                                  detail={"namespace": ns.namespace, "hits": len(items),
                                          "applied_seq": info.get("applied_seq")})
        else:
            self._audit_for(ns.namespace).append(actor="search", action="search",
                              target=self._audit_target_for_query(query), detail={"hits": len(items)})
        # measured LAST: the audit append is real per-request work and used to
        # sit outside the timer, so p99 under-reported every search
        latency = (time.monotonic() - t0) * 1000
        METRICS.observe("memd_search_latency_ms", latency, help="end-to-end search latency (ms)",
                        ns=ns.namespace, qclass=plan.qclass, cache="miss")
        io_ctx.__exit__(None, None, None)
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
            reranked=rerank_order is not None,
            **self._info_fields(info),
        )
        if not degraded:
            # a result served while the reranker was failing must not
            # outlive the outage
            self._qcache.put(cache_key, result)
        return result

    def _rerank(self, query: str, lane_hits: dict[str, list], fused: list[FusedItem],
                ns_name: str, allowed: bool = True) -> tuple[list[tuple[FusedItem, float]] | None, bool]:
        """(shortlist in reranker order with scores, degraded?). The
        shortlist is the top-k of the bm25 lane, then the vector lane when a
        real embedder is fused, deduped, bm25 first. None: no reranker, or it
        failed (then degraded=True and the fused order stands)."""
        if self.rerank is None:
            return None, False
        if not allowed:
            # degraded: the caller's entitlement, not the reranker - never
            # cache it (a later caller with quota must get the reranked order)
            METRICS.inc("memd_rerank_fallback_total",
                        help="searches that kept the unreranked order: reranker failed, timed out or was not ready",
                        ns=ns_name, reranker=self.rerank.name, reason="quota")
            return None, True
        hits = list(lane_hits.get("bm25", []))
        if self.embedder.kind != "hash":
            hits += lane_hits.get("vector", [])
        shortlist, seen = [], set()
        for h in hits:
            if h.record.id not in seen:
                seen.add(h.record.id)
                shortlist.append(h)
                if len(shortlist) >= self.rerank.k:
                    break
        if not shortlist:
            return None, False
        _st0 = time.monotonic()
        vals = self.rerank.run(query, [candidate_from_record(h.record) for h in shortlist], ns=ns_name)
        METRICS.observe("memd_search_stage_ms", (time.monotonic() - _st0) * 1000,
                        help="per-stage search timing (ms): plan/fuse/pack", ns=ns_name, stage="rerank")
        if vals is None:
            return None, True
        by_id = {it.record.id: it for it in fused}
        items = [by_id.get(h.record.id) or FusedItem(record=h.record, score=0.0, lanes=[h.lane],
                                                     ranks={h.lane: i + 1})
                 for i, h in enumerate(shortlist)]
        order = sorted(range(len(items)), key=lambda i: (-vals[i], i))
        return [(items[i], vals[i]) for i in order], False

    def _pack_sessions(self, ns, fused: list[FusedItem], rerank_scores: dict[str, float] | None, *,
                       scope: Scope, as_of: int | None, kinds: list[str] | None,
                       include_quarantined: bool, budget_tokens: int, query_class: str) -> PackedContext:
        """Session packing over the flat layout's candidate order. The turns
        it adds - a fact's source turns, a hit's neighbours - pass the same
        scope / validity / quarantine filter as the hits (never the planner's
        kinds or time window: a neighbour is context, not a hit). A `kinds`
        filter without raw turns adds none."""
        ranked = rank_for_packing(fused, now=as_of, rerank_scores=rerank_scores)
        known = {it.record.id: it.record for _, it in ranked}
        sources: dict[str, list[MemoryRecord]] = {}
        expand = not kinds or Kind.RAW_EVENT in kinds
        filt = IndexFilter(scope=scope, as_of=as_of, include_quarantined=include_quarantined)
        want = list(dict.fromkeys(x for _, it in ranked if it.record.kind != Kind.RAW_EVENT
                                  for x in it.record.provenance.lineage)) if expand else []
        if want:
            vis = {r.id: r for r in ns.index.get_visible(want, filt) if r.kind == Kind.RAW_EVENT}
            known.update(vis)
            for _, it in ranked:
                if it.record.kind != Kind.RAW_EVENT:
                    got = [vis[x] for x in dict.fromkeys(it.record.provenance.lineage) if x in vis]
                    if got:
                        sources[it.record.id] = got
        positions = ns.index.rowids(list(known))
        neighbours = None
        if expand:
            # a unit's neighbours are looked up when it is packed (two index
            # seeks per anchor), together with the next anchors' in rank
            # order, 4 then 8, 16, ... at a time, and their rows read in one
            # statement per batch; rows already in hand (candidates, sources)
            # are never read again
            anchors = list(dict.fromkeys(
                a.id for _, it in ranked
                for a in ([it.record] if it.record.kind == Kind.RAW_EVENT else sources.get(it.record.id, []))
                if a.id in positions))
            at = {a: i for i, a in enumerate(anchors)}
            adj: dict[str, list[tuple[str, int]]] = {}
            tried: set[str] = set()
            batch = [_NEIGHBOUR_READ_AHEAD]

            def neighbours(ids: list[str]):
                if any(i not in adj for i in ids):
                    k = min((at[i] for i in ids if i in at), default=len(anchors))
                    ahead = [known[a] for a in dict.fromkeys([*ids, *anchors[k:k + batch[0]]])
                             if a in known and a in positions and a not in adj]
                    batch[0] = min(batch[0] * 2, 64)
                    adj.update(ns.index.adjacent_turns(
                        [(a.id, a.scope.session, a.time.t_event, positions[a.id]) for a in ahead], filt))
                    for a in ahead:
                        adj.setdefault(a.id, [])
                    fresh = list(dict.fromkeys(x for a in ahead for x, _ in adj[a.id]
                                               if x not in known and x not in tried))
                    if fresh:
                        tried.update(fresh)
                        known.update((r.id, r) for r in ns.index.get_visible(fresh, filt))
                got = {i: adj.get(i, []) for i in ids}
                return ({i: [known[x] for x, _ in v if x in known] for i, v in got.items()},
                        {x: p for v in got.values() for x, p in v})
        return pack_sessions(ranked, sources=sources, positions=positions, neighbours=neighbours,
                             budget_tokens=budget_tokens, query_class=query_class,
                             resolve_dates=self.pack_resolve_dates)

    def _pack_mode_for(self, rerank_order) -> str:
        """Gated only on explicit opt-in, and only with reranker scores to
        gate on (see resolve_pack_mode for why auto is ranked)."""
        if rerank_order is None or self.rerank is None:
            return "ranked"
        return "gated" if self.pack_mode == "gated" else "ranked"

    def _load_reranker(self) -> None:
        try:
            self.rerank.reranker.load()
        except Exception:
            _log.exception("memd: reranker %s failed to load; searches keep their own order",
                           self.rerank.name)

    def pack(
        self,
        messages: list[dict],
        *,
        user_id: str | None = None,
        session_id: str | None = None,
        budget_tokens: int = DEFAULT_BUDGET_TOKENS,
        namespace: str | None = None,
        packing: str | None = None,
    ) -> list[dict]:
        """Inject packed memory context before the LLM call (the two-line glue)."""
        impl = self._hosted()
        if impl is not None:
            return impl.pack(messages, user_id=user_id, session_id=session_id,
                             budget_tokens=budget_tokens, namespace=namespace, packing=packing)
        last_user = next((m["content"] for m in reversed(messages) if m.get("role") == "user"), "")
        if not last_user:
            return messages
        res = self.search(
            str(last_user), user_id=user_id, session_id=session_id, budget_tokens=budget_tokens, namespace=namespace,
            packing=packing,
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

    def get(self, record_id: str, *, history: bool = False, include_deleted: bool = False,
            namespace: str | None = None, consistency: str | None = None,
            max_staleness_ms: int | None = None, read_info: dict | None = None) -> dict | None:
        """One record, or None. history=True adds its supersedence chain.
        A deleted record - and a deleted version in a chain - is served only
        with include_deleted (an administrative read: `history` used to serve
        soft-deleted content until compaction purged it).

        `consistency` / `max_staleness_ms` as for search(); `read_info`, if
        a dict, is filled with who served the read: {"served_by",
        "applied_seq", "age_ms"}."""
        impl = self._hosted()
        if impl is not None:
            got = impl.get(record_id, history=history, include_deleted=include_deleted,
                           namespace=namespace, consistency=consistency,
                           max_staleness_ms=max_staleness_ms)
            if read_info is not None:
                read_info.update(impl.last_read or {"served_by": "leader"})
            return got
        if (consistency or ("eventual" if self.read_only else "strong")) == "strong":
            run = self._forward(namespace, "get", lambda: dict(
                record_id=record_id, history=history, include_deleted=include_deleted), write=False)
            if not run.here:
                if read_info is not None:
                    read_info.update({"served_by": "leader"})
                return run.result
        ns, info = self._reader(namespace, consistency, max_staleness_ms)
        with self._serving(ns, info):
            if read_info is not None:
                read_info.update(info or {"served_by": "leader"})
            rec = ns.index.get_by_id(record_id, include_deleted=True)
            if rec is None or (rec.deleted and not include_deleted):
                return None
            d = rec.to_dict()
            if history:
                d["history"] = [h.to_dict() for h in ns.index.history(record_id)
                                if include_deleted or not h.deleted]
            return d

    # ------------------------------------------------------------ read replicas

    def _reader(self, namespace: str | None, consistency: str | None,
                max_staleness_ms: int | None):
        """(store, info) a read is served from - see search()."""
        c = consistency or ("eventual" if self.read_only else "strong")
        if c not in ("strong", "eventual"):
            raise ValueError(f"consistency must be 'strong' or 'eventual', got {c!r}")
        if c == "strong":
            if self.read_only:
                raise ReadOnlyError("a read-only Memory (a read replica) serves eventual reads only")
            return self._ns_for(namespace), None
        bound = None if max_staleness_ms is None else max(0.0, float(max_staleness_ms) / 1000.0)
        return self.engine.reader(namespace or self.namespace_name, bound,
                                  wrap_errors=not self.read_only)

    @staticmethod
    @contextlib.contextmanager
    def _serving(store, info: dict | None = None):
        """Hold while reading a store's index to SERVE a read: a replica's
        rebuild swaps it, one that failed leaves nothing to serve, and one
        whose refresh has not applied the log tail yet is waited for (see
        ReplicaStore.reading). `info` (a replica read's) is brought up to
        the state the read was admitted to: a rebuild and a refresh may have
        run since its freshness check."""
        reading = getattr(store, "reading", None)
        if not callable(reading):
            yield
            return
        with reading():
            if info is not None and info.get("served_by") == "replica":
                info.update(store.serving_info())
            yield

    @staticmethod
    def _holding(store):
        """Hold a store's index for maintenance (flush, derived vectors): a
        replica's rebuild swaps it, but there is no read to refuse."""
        holding = getattr(store, "holding", None)
        return holding() if callable(holding) else contextlib.nullcontext()

    @staticmethod
    def _info_fields(info: dict | None) -> dict:
        if not info or info.get("served_by") != "replica":
            return {"served_by": "leader", "replica_seq": None, "replica_age_ms": None}
        return {"served_by": "replica", "replica_seq": info.get("applied_seq"),
                "replica_age_ms": info.get("age_ms")}

    def _writable(self, what: str) -> None:
        if self.read_only:
            raise ReadOnlyError(f"{what}: this Memory is read-only (a read replica) "
                                "- write through the namespace's writer")

    def _replica_event(self, ns_name: str, store, records) -> None:
        """StorageEngine.replica_hook: a replica opened (records None) or
        applied records - their vectors are derived here, locally (vectors
        are derived state, never logged)."""
        if not getattr(self, "fuse_vector", False) or getattr(self, "_embed_worker", None) is None:
            return
        tag = ns_name if self.read_only else ns_name + _REPLICA_TAG
        if records is None:
            self._report_vector_health(store)
            return
        for rec in records:
            if not rec.deleted and not rec.meta.get("quarantined"):
                self._embed_worker.submit(tag, rec.id, rec.content)

    # ------------------------------------------------------------------ lifecycle

    def session_raw_count(self, session_id: str, *, user_id: str | None = None,
                          namespace: str | None = None) -> int:
        """How many raw records close_session() would hand the extractor."""
        run = self._forward(namespace, "session_raw_count",
                            lambda: dict(session_id=session_id, user_id=user_id), write=False)
        if not run.here:
            return run.result
        ns = self._ns_for(namespace)
        with self._serving(ns):
            return ns.index.count_session_raw(session_id, user_id=user_id)

    def close_session(
        self,
        session_id: str,
        *,
        user_id: str | None = None,
        namespace: str | None = None,
        extract_limit: int | None = None,
        max_facts: "int | Callable[[int], int] | None" = None,
    ) -> dict:
        """Segment-close boundary: extract facts, consolidate, rotate segment.
        Consolidation resolves at session end, never deferred past it.
        When user_id is supplied, extraction sweeps only that user's rows
        (blocks cross-user session-id injection into the fact lane).

        Quota limits (hosted mode): at most `extract_limit` raw records go to
        the extractor (the rest stay raw-only: searchable, never extracted -
        `raw_skipped`), and at most `max_facts` facts are written
        (`facts_capped`); a callable `max_facts` is asked, once extraction
        finished, how many of the n extracted facts may be written. The raw
        lane is never touched by either.

        `extraction_errors` counts the extraction calls that failed and
        `raw_failed` their turns: the LLM extractor's turns of a failed call
        go through the pattern extractor instead; an extractor that raised
        outright counts 1 call, all its turns, and adds no facts."""
        impl = self._hosted()
        if impl is not None:
            return impl.close_session(session_id, user_id=user_id, namespace=namespace)
        self._writable("close_session")

        def args() -> dict:
            if callable(max_facts):
                raise _fw.ForwardingError(
                    "close_session: a callable max_facts runs only where the namespace's writer "
                    "runs, and that is another process")
            return dict(session_id=session_id, user_id=user_id, extract_limit=extract_limit,
                        max_facts=max_facts)
        run = self._forward(namespace, "close_session", args)
        if not run.here:
            return run.result
        ns = self._ns_for(namespace)
        self._embed_worker.drain(timeout_s=60)
        # durable boundary: commit index + audit before folding the segment
        ns.index.flush()
        self._audit_for(ns.namespace).flush()
        t0 = time.monotonic()
        seg_records = ns.index.records_of_session(session_id, user_id=user_id)
        to_extract = seg_records if extract_limit is None else seg_records[:max(0, int(extract_limit))]
        try:
            extracted = self.extractor.extract(to_extract) if to_extract else []
            # provider calls that failed (LLM extractor): their turns went
            # through the pattern extractor instead - reported, not silent
            extraction_errors = list(getattr(extracted, "errors", None) or [])
            raw_failed = int(getattr(extracted, "failed_records", 0) or 0)
            if extraction_errors:
                self._audit_for(ns.namespace).append(
                    actor="system", action="extraction_degraded", target=session_id,
                    detail={"failed_calls": len(extraction_errors), "reasons": sorted(set(extraction_errors))})
        except Exception as ex:
            # extraction is a REBUILDABLE derived index (raw lane is truth):
            # an extractor outage must never block the session boundary or
            # wedge the WAL. Facts can be regenerated later via reindex.
            METRICS.inc("memd_extraction_failures_total", ns=ns.namespace)
            self._audit_for(ns.namespace).append(actor="system", action="extraction_failed",
                              target=session_id, detail={"error": str(ex)[:200]})
            extracted = []
            extraction_errors = ["error"]
            raw_failed = len(to_extract)
        facts_capped = 0
        if callable(max_facts):
            max_facts = max_facts(len(extracted))
        if max_facts is not None and len(extracted) > max(0, int(max_facts)):
            facts_capped = len(extracted) - max(0, int(max_facts))
            extracted = extracted[:max(0, int(max_facts))]
        if len(to_extract) < len(seg_records) or facts_capped:
            METRICS.inc("memd_extraction_capped_total", ns=ns.namespace,
                        help="session closes whose extraction a quota limited")
            self._audit_for(ns.namespace).append(actor="system", action="extraction_capped", target=session_id,
                                                 detail={"raw_skipped": len(seg_records) - len(to_extract),
                                                         "facts_capped": facts_capped})
        facts_written, consolidation = self._write_facts(ns, extracted, seg_records)
        # the facts are durable: a fold that refuses (a damaged log frame)
        # leaves the session unfolded - "" - and is surfaced, never raised
        seg_name = ns.maintain_rotate(f"session-close:{session_id}")
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
            "raw_considered": len(to_extract),
            "raw_skipped": len(seg_records) - len(to_extract),
            "raw_failed": raw_failed,
            "facts_extracted": len(extracted),
            "extraction_errors": len(extraction_errors),
            "facts_capped": facts_capped,
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

        def source_of(fact: ExtractedFact) -> MemoryRecord | None:
            # the fact's own turn: its scope, actor and time
            return next((s for s in sources if s.id in fact.lineage), sources[0] if sources else None)

        def cluster_scope_of(fact: ExtractedFact) -> Scope:
            # the fact's source scope with the session component stripped:
            # supersedence is user-level knowledge that spans sessions, while
            # org/agent/user bindings still fence tenants and users. Per fact:
            # in a session with several users (closed without user_id), one
            # user's fact must never supersede another's
            src = source_of(fact)
            b = src.scope if src is not None else Scope()
            return Scope(org=b.org, agent=b.agent, user=b.user)

        clusters: dict[Scope, dict[str, list[MemoryRecord]]] = {}
        for f in facts:
            by_key = clusters.setdefault(cluster_scope_of(f), {})
            for ek in f.entity_keys or ["fact.general"]:
                by_key.setdefault(ek, [])
        for cscope, by_key in clusters.items():
            for ek in list(by_key):
                by_key[ek] = ns.index.entity_cluster(ek, scope=cscope)
        # cross-session display-name resolution: "u7 works at X" reads better
        # (and retrieves better) as "Hank works at X" once a user.name fact
        # exists; facts are re-runnable derived data, so enrichment is legit
        subject_names: dict[str, str] = {}
        for cscope, by_key in clusters.items():
            subject_names.update(self._resolve_subject_names(ns, by_key, cscope))
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
            by_key = clusters[cluster_scope_of(f)]
            cluster = []
            seen_ids: set[str] = set()
            for k in keys:
                for r in by_key.get(k, []):
                    if r.id not in seen_ids:
                        seen_ids.add(r.id)
                        cluster.append(r)

            def make(fact: ExtractedFact, _keys=keys) -> MemoryRecord:
                src = source_of(fact)
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
                    extractor=fact.extractor or ExtractorInfo(
                        model=self.extractor.name, prompt_version=getattr(self.extractor, "prompt_version", "v1")),
                )

            res = consolidate_facts(cluster, [f], make)
            results.append(res)
        written = 0
        kept_recs = [r for res in results for r in res.kept]
        # lineage demotion: a fact that supersedes another carries the old
        # fact's raw lineage in meta.demotes - packing then skips that stale
        # raw evidence (demotion, not deletion)
        superseded_all = [p for res in results for p in res.superseded_pairs]
        # lineage demotion: a fact that supersedes another carries the old
        # fact's raw lineage in meta.demotes - packing then skips that stale
        # raw evidence (demotion, not deletion)
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
        self._writable("delete")
        run = self._forward(namespace, "delete", lambda: dict(record_id=record_id, hard=hard, actor=actor))
        if not run.here:
            return run.result
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
        self._writable("delete_many")
        ids = [rid for rid in record_ids if rid]
        if not ids:
            return 0
        run = self._forward(namespace, "delete_many", lambda: dict(record_ids=ids, hard=hard, actor=actor))
        if not run.here:
            return run.result
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
        """Self-enforce the physical-purge guarantee OFF the caller's
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

    def _run_maintenance(self, job: str) -> bool:
        if job.startswith("reembed:"):
            ns_name = job.split(":", 1)[1]
            if ns_name.endswith(_REPLICA_TAG):
                store = self.engine.peek_replica(ns_name[:-len(_REPLICA_TAG)])
                if store is None:
                    return False
                self._reembed_store(store)
                return True
            if self.engine.peek_namespace(ns_name) is None:
                return False
            self.reembed(namespace=ns_name)
            return True
        return self._run_due_purge(job)

    def _report_vector_health(self, ns) -> int:
        """Publish how much of the vector lane is missing, and schedule the heal.

        Losing the derived index cache (a node restore, a wiped volume) replays
        every RECORD from segments but zero VECTORS - the lane is rebuildable
        by contract, so nothing was wrong, and nothing said anything either. A
        namespace could serve every query with one of its four fusion lanes
        empty and look FASTER while doing it (no vector matrix to build), so
        latency monitoring could never surface it. reembed() existed, but only
        behind the CLI: a hosted operator had no door on the node that needed it.
        """
        try:
            missing = ns.index.count_missing_embedding(self.embedder.name)
        except Exception:
            return 0
        METRICS.set_gauge("memd_vectors_missing", float(missing),
                          help="live records with no current-version vector", ns=ns.namespace)
        if missing and self._vector_selfheal:
            # heal on the maintenance thread, never at open time on the
            # caller's path (that is the pass-17 lesson)
            tag = _REPLICA_TAG if getattr(ns, "read_only", False) and not self.read_only else ""
            self._maint.submit(f"reembed:{ns.namespace}{tag}")
        return missing

    def _run_due_purge(self, ns_name: str) -> bool:
        """Background body of the purge deadline. Resolves the namespace by
        PEEK so a namespace destroyed between scheduling and running stays
        destroyed rather than being re-materialized by name."""
        ns = self.engine.peek_namespace(ns_name)
        if ns is None or not ns.has_due_deletes():
            return False
        t0 = time.monotonic()
        rep = ns.maintain_compact()  # force=False still enforces due deadlines
        if rep is None:
            return False  # a failed fold's backoff: the next purge trigger retries
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
        run = self._forward(namespace, "find_ids", lambda: dict(
            query=query, user_id=user_id, session_id=session_id, agent_id=agent_id,
            org_id=org_id, as_of=as_of, kinds=kinds), write=False)
        if not run.here:
            return run.result
        ns = self._ns_for(namespace)
        with self._serving(ns):
            return self._find_ids(ns, query, user_id=user_id, session_id=session_id,
                                  agent_id=agent_id, org_id=org_id, as_of=as_of, kinds=kinds)

    def _find_ids(self, ns, query: str, *, user_id, session_id, agent_id, org_id, as_of,
                  kinds) -> list[str]:
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
        vector_hits = []
        if not self.embedder.ready():
            # model still loading: sweep on lexical evidence only (the
            # conservative direction for a delete)
            METRICS.inc("memd_embed_query_not_ready_total", ns=ns.namespace)
        else:
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
        strong_vec = self._vector_only_floor(list(vec_scores.values()))
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

    def _vector_only_floor(self, scores: list[float]) -> float:
        """Minimum cosine for a vector-ONLY candidate in a destructive sweep.

        Absolute part: the embedder's strong_match_cosine. A fixed 0.35 was
        calibrated on the hash embedder, where unrelated strings score ~0;
        bge-small scores unrelated short strings ~0.5-0.6, so the same
        number had forget() delete 120 records instead of 60.
        Relative part: the candidate must also beat the median similarity of
        the sweep's own vector sample by a margin, so a namespace whose
        records all sit close together in embedding space (a homogeneous
        corpus, or a model with an unusually high baseline) cannot be swept
        wholesale on vector evidence. The sample is the scoped top
        `sweep_limit`, which only biases the median upward - more
        conservative, never less."""
        floor = float(self.embedder.strong_match_cosine)
        if len(scores) >= _VECTOR_ONLY_MIN_SAMPLE:
            floor = max(floor, float(np.median(scores)) + _VECTOR_ONLY_MEDIAN_MARGIN)
        return floor

    def forget(
        self,
        query: str,
        *,
        user_id: str | None = None,
        session_id: str | None = None,
        agent_id: str | None = None,
        org_id: str | None = None,
        as_of: int | None = None,
        kinds: list[str] | None = None,
        expected: str | None = None,
        actor: str = "api",
        namespace: str | None = None,
    ) -> list[str]:
        """User-driven deletion by query. Uses find_ids (unbounded), NOT the
        budget-capped packed view - partial destruction is unacceptable.

        Deletes exactly what find_ids returns for the SAME filters (as_of,
        kinds, scope) - a preview's. `expected`: the preview's
        forget_fingerprint(); if what would be deleted now differs, nothing
        is deleted and ForgetPreviewMismatch is raised."""
        impl = self._hosted()
        if impl is not None:
            return impl.forget(query, confirm=True, user_id=user_id,
                               session_id=session_id, agent_id=agent_id,
                               org_id=org_id, as_of=as_of, kinds=kinds,
                               fingerprint=expected, namespace=namespace)
        self._writable("forget")
        ids = self.find_ids(
            query, user_id=user_id, session_id=session_id, agent_id=agent_id,
            org_id=org_id, as_of=as_of, kinds=kinds, namespace=namespace,
        )
        if expected is not None and forget_fingerprint(ids) != expected:
            raise ForgetPreviewMismatch(
                f"the query now matches {len(ids)} record(s), not the previewed set: "
                "preview again and confirm that")
        self._forget_ids(ids, self._audit_target_for_query(query), actor, namespace)
        return ids

    def _forget_ids(self, ids: list[str], target: str, actor: str, namespace: str | None) -> None:
        """forget()'s deletion, in the namespace's writer: one durable batch
        for the whole sweep (delete_many), not an fsync-per-id loop, and the
        forget entry of its ledger."""
        run = self._forward(namespace, "forget_ids", lambda: dict(ids=ids, target=target, actor=actor))
        if not run.here:
            return
        self.delete_many(ids, actor=actor, namespace=namespace)
        self._audit_for(self._ns_for(namespace).namespace).append(
            actor=actor, action="forget", target=target, detail={"deleted": len(ids)})

    def has_namespace(self, namespace: str) -> bool:
        """True if `namespace` exists; never creates it."""
        impl = self._hosted()
        if impl is not None:
            raise RuntimeError("has_namespace is embedded-only")
        return self.engine.has_namespace(namespace)

    def open_namespaces(self) -> list[str]:
        """Namespaces open in this process (embedded-only)."""
        if self._hosted() is not None:
            raise RuntimeError("open_namespaces is embedded-only")
        return self.engine.open_namespaces()

    def destroy_namespace(self, namespace: str | None = None, actor: str = "api") -> bool:
        impl = self._hosted()
        if impl is not None:
            return impl.destroy_namespace(namespace=namespace)
        self._writable("destroy_namespace")
        name = namespace or self.namespace_name
        run = self._forward(name, "destroy_namespace", lambda: dict(actor=actor))
        if not run.here:
            self._bump_epoch(name)
            return run.result
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
        # crypto-shredded namespace. Drop it without flushing.
        with self._audit_lock:
            self._audits.pop(name, None)
            self._shredded[name] = None
            while len(self._shredded) > 1024:
                self._shredded.popitem(last=False)
        if name == self.namespace_name:
            self.ns = self.engine.namespace(name)
        METRICS.inc("memd_destroys_total", ns=name)
        # destroy is an ADMINISTRATIVE act on the engine, so it lands in the
        # facade's own ledger - never in the ledger of the namespace just
        # shredded. It records what the key provider did: the wrapped
        # key deleted, and any KMS-side disable / scheduled deletion
        env = self.engine.envelope
        shred = env.destroy_report(name) if env is not None and hasattr(env, "destroy_report") else {}
        self.audit.append(actor=actor, action="destroy_namespace", target=name,
                          detail={"key_shred": shred} if shred else None)
        return ok

    # how many unreadable WAL frames the last export left out (0: complete) -
    # see export_stream
    last_export_skipped_frames = 0

    def export_stream(self, namespace: str | None = None) -> "ExportStream":
        """Streaming export whose fold has already run: iterate it for the
        NDJSON lines (O(1) line-sized buffers). Its `skipped` - the
        unreadable WAL frames it left out (see NamespaceStore._wal_events) -
        is known before the first line, so a streaming surface can say the
        export is incomplete up front (REST: X-Memd-Export-Skipped-Frames).
        Metered and audited once the last line is out. Hosted mode has no
        streaming REST surface yet: one buffered blob."""
        impl = self._hosted()
        if impl is not None:
            blob = impl.export_jsonl(namespace=namespace)
            n = int(getattr(impl, "last_export_skipped_frames", 0) or 0)
            self.last_export_skipped_frames = n
            return ExportStream(iter([blob]), [], skipped=n)
        name = namespace or self.namespace_name
        run = self._forward(name, "export", write=False)
        if not run.here:
            return self._forwarded_export(run.result)
        t0 = time.monotonic()
        nstore = self.engine.namespace(name)
        recs, skipped = nstore.export_records()
        self.last_export_skipped_frames = len(skipped)

        def lines():
            n = 0
            for rec in recs:
                n += 1
                yield nstore.export_line(rec)
            self._exported(name, t0, skipped, records=n)

        return ExportStream(lines(), skipped)

    def export_jsonl_iter(self, namespace: str | None = None):
        """Streaming export: NDJSON lines (see export_stream)."""
        yield from self.export_stream(namespace=namespace)

    def export_jsonl(self, namespace: str | None = None) -> bytes:
        """Buffered variant (CLI/SDK convenience). Emits its own metrics +
        one audit record; prefer export_stream() on streaming surfaces.
        last_export_skipped_frames says whether it is complete."""
        impl = self._hosted()
        if impl is not None:
            data = impl.export_jsonl(namespace=namespace)
            self.last_export_skipped_frames = int(getattr(impl, "last_export_skipped_frames", 0) or 0)
            return data
        name = namespace or self.namespace_name
        run = self._forward(name, "export", write=False)
        if not run.here:
            return b"".join(self._forwarded_export(run.result))
        t0 = time.monotonic()
        nstore = self.engine.namespace(name)
        recs, skipped = nstore.export_records()
        self.last_export_skipped_frames = len(skipped)
        data = b"".join(nstore.export_line(rec) for rec in recs)
        self._exported(name, t0, skipped, bytes=len(data), records=len(recs))
        return data

    def _forwarded_export(self, reader) -> "ExportStream":
        """An export the namespace's writer in another process streams
        (memd.engine.forward._StreamReader): metered and audited there."""
        frames = list(reader.meta.get("skipped_frames") or [])
        self.last_export_skipped_frames = int(reader.meta.get("skipped") or len(frames))
        return ExportStream(iter(reader), frames, skipped=self.last_export_skipped_frames)

    def _exported(self, name: str, t0: float, skipped: list[dict], **detail) -> None:
        """Meter and audit one export. Its audit detail lists the unreadable
        WAL frames it left out (log, byte offset, fault), if any."""
        METRICS.observe("memd_export_ms", (time.monotonic() - t0) * 1000,
                        help="export duration (ms)", ns=name)
        METRICS.inc("memd_exports_total", ns=name)
        METRICS.inc("memd_export_records_total", int(detail.get("records", 0)), ns=name,
                    help="records streamed by exports")
        if skipped:
            detail["skipped_frames"] = list(skipped)
        self._audit_for(name).append(actor="export", action="export", target=name, detail=detail)

    def compact(self, force: bool = False, namespace: str | None = None) -> dict:
        impl = self._hosted()
        if impl is not None:
            return impl.compact(force=force, namespace=namespace)
        self._writable("compact")
        run = self._forward(namespace, "compact", lambda: dict(force=force))
        if not run.here:
            return run.result
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
        run = self._forward(namespace, "stats", write=False)
        if not run.here:
            return run.result
        ns = self._ns_for(namespace)
        st = ns.stats()
        st["namespace"] = ns.namespace
        st["embedder"] = self.embedder.name
        st["embedder_kind"] = self.embedder.kind
        st["embedder_ready"] = self.embedder.ready()
        # embeddings queued or mid-batch: non-zero after flush() means the
        # drain timed out and the vector lane is still catching up
        st["embed_pending"] = self._embed_worker._pending()
        st["fuse_vector"] = self.fuse_vector
        st["reranker"] = self.rerank.stats() if self.rerank is not None else {"name": "none"}
        st["pack_mode"] = self.pack_mode
        st["packing"] = self.packing
        st["pack_resolve_dates"] = self.pack_resolve_dates
        lex = ns.index.lexical
        st["lexical"] = lex.stats() if lex is not None else {"backend": "fts5"}
        ann = ns.index.ann
        st["vector_index"] = ann.stats() if ann is not None else {
            "kind": "flat", "mode": self.vector_index, "ready": True, "size": 0, "rebuilds": 0,
            "last_build_ms": None, "fallback_exact_total": 0}
        # vector-lane queries skipped while the sidecar was not serving (a
        # namespace over flat_max_vectors never loads the exact scan's matrix)
        st["vector_index"]["skipped_total"] = ns.index.vector_lane_skipped
        st["vector_index"]["flat_max_vectors"] = ns.index.flat_max_vectors
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
        # open namespaces whose automatic maintenance (rotate/compaction) is
        # failing: writes succeed meanwhile, the logs grow (see
        # NamespaceStore.maintain_rotate)
        failing = self.engine.maintenance_failing()
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
                "version": __import__("memd").__version__,
                "maintenance": {k: v for k, v in failing.items() if k == ns_filter},
            }
        names = self.engine.list_namespaces()
        return {
            "mode": "embedded",
            "namespaces": names,
            "default_namespace": self.namespace_name,
            "embedder": self.embedder.name,
            "extractor": self.extractor.name,
            "version": __import__("memd").__version__,
            "maintenance": failing,
        }

    # ------------------------------------------------------------------ internals

    def _ns_for(self, namespace: str | None):
        if self.read_only:
            # a replica, refreshed first if it is staler than the bound: a
            # read never sees a replica mid-rebuild (or one that failed one)
            return self.engine.reader(namespace or self.namespace_name, wrap_errors=False)[0]
        if namespace is None or namespace == self.namespace_name:
            ns = self.ns
            if ns is None or ns._closed:
                # another process held it (forwarding) or this one lost it
                # since: the engine's open store - which takes it here
                ns = self.ns = self.engine.namespace(self.namespace_name)
            return ns
        store = self.engine.namespace(namespace)
        if namespace in self._shredded:
            # materialized again: it legitimately exists, so stop tombstoning it
            with self._audit_lock:
                self._shredded.pop(namespace, None)
        return store

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
        if ns_name.endswith(_REPLICA_TAG):
            ns = self.engine.peek_replica(ns_name[:-len(_REPLICA_TAG)])
        else:
            ns = self.engine.peek_namespace(ns_name)
        if ns is None:
            METRICS.inc("memd_embed_target_missing_total",
                        help="embeddings dropped because the target namespace is gone")
            return
        # one batch: one index lock hold and one ANN sidecar change
        with self._holding(ns):
            ns.index.set_vectors(list(ids), [vecs[i] for i in range(len(ids))], self.embedder.name)

    def reembed(self, *, namespace: str | None = None, batch_size: int = 256) -> dict:
        """Batch re-embedding job: rebuild the vector lane from raw.

        Needed after restoring segments onto a machine without the derived
        index cache, after switching embedder models, or whenever records
        lack a current-version embedding. O(records); BM25 lane unaffected."""
        impl = self._hosted()
        if impl is not None:
            raise RuntimeError("reembed is embedded-only (no REST surface)")
        run = self._forward(namespace, "reembed", lambda: dict(batch_size=batch_size))
        if not run.here:
            return run.result
        return self._reembed_store(self._ns_for(namespace))

    def _reembed_store(self, ns, batch_size: int = 256) -> dict:
        with self._holding(ns):
            return self._reembed_locked(ns, batch_size)

    def _reembed_locked(self, ns, batch_size: int) -> dict:
        stale = ns.index.records_missing_embedding(self.embedder.name)
        done = 0
        t0 = time.monotonic()
        # same text preparation as the embed worker (truncated, and chunks of
        # similar length so a padded batch wastes little)
        stale.sort(key=lambda r: len(r.content))
        for i in range(0, len(stale), batch_size):
            chunk = stale[i : i + batch_size]
            ids, texts = embed_order({r.id: r.content for r in chunk}, self._embed_max_chars)
            vecs = self.embedder.embed(texts)
            ns.index.set_vectors(list(ids), [vecs[j] for j in range(len(ids))], self.embedder.name)
            done += len(ids)
        METRICS.observe("memd_reembed_ms", (time.monotonic() - t0) * 1000,
                        help="re-embedding batch duration (ms)", ns=ns.namespace)
        try:
            METRICS.set_gauge(
                "memd_vectors_missing",
                float(ns.index.count_missing_embedding(self.embedder.name)),
                help="live records with no current-version vector", ns=ns.namespace)
        except Exception:
            pass
        METRICS.inc("memd_reembed_total", done, ns=ns.namespace)
        if done:
            self._bump_epoch(ns.namespace)  # vector lane changed -> cached contexts are stale
        if not getattr(ns, "read_only", False):
            self._audit_for(ns.namespace).append(actor="maintenance", action="reembed",
                                                 target=ns.namespace,
                                                 detail={"embedded": done, "model": self.embedder.name})
        return {"namespace": ns.namespace, "missing": len(stale), "embedded": done,
                "model": self.embedder.name}

    def flush(self) -> None:
        impl = self._hosted()
        if impl is not None:
            return None  # hosted mode: nothing buffered locally
        budget = self._embed_flush_drain_s
        deadline = time.monotonic() + budget
        left = self._embed_worker.drain(timeout_s=budget)
        # nothing queued does not mean the vector lane is serving: the model
        # may still be loading. flush() is the caller's "make it consistent"
        # point, so it waits for that too (within the same budget).
        self._embed_worker.wait_ready(deadline - time.monotonic())
        if left:
            # drain() gave up with work still queued or mid-batch: the
            # vector lane is incomplete (it keeps draining in the background)
            # and "flushed" must not silently claim otherwise
            METRICS.inc("memd_flush_embed_pending_total",
                        help="flushes that returned with embeddings still pending")
            _log.warning("memd: flush() returned with %d embedding(s) still pending after %.1fs; "
                         "the vector lane is incomplete until the embed worker catches up",
                         left, budget)
        self._maint.drain(timeout_s=60)
        ns = self.ns if self.ns is not None and not self.ns._closed else None
        if ns is not None:
            with self._holding(ns):
                ns.index.flush()
            lex = ns.index.lexical
            if lex is not None:
                # the tantivy accelerator is derived and serves its tail from
                # FTS5 meanwhile, so this is about speed, not visibility
                lex.drain(timeout_s=self._lexical_flush_drain_s)
            ann = ns.index.ann
            if ann is not None:
                # likewise the ANN sidecar (the exact scan serves while it builds)
                ann.drain(timeout_s=self._vector_flush_drain_s)
        self._flush_all_audits()
        if self._fwd is not None and getattr(_SERVING, "ids", _NOT_SERVING) is _NOT_SERVING:
            # the namespaces this process wrote through their writers in
            # other processes: theirs to flush (their vector lane, purges)
            for name in self._fwd.take_written():
                try:
                    self._forward(name, "flush", write=False)
                except Exception as ex:  # noqa: BLE001 - one writer gone must not fail the rest
                    _log.warning("memd: flush of %r in its writer failed (%s)", name, ex)

    def close(self) -> None:
        impl = self._hosted()
        if impl is not None:
            return impl.close()
        # first: forwarded calls already running finish while this process
        # still holds their namespaces; new ones are answered "not the
        # writer" and their callers take the namespaces once released below
        if self._fwd_server is not None:
            self._fwd_server.stop(drain_s=float(self._embed_close_drain_s))
        if self._fwd_audit is not None:
            self._fwd_audit.flush()
        if getattr(self, "_metrics_dumper", None):
            self._metrics_dumper.stop()
        # bounded drain before stopping: a clean close should not silently
        # discard the vector lane it was asked to persist
        self._embed_worker.stop(drain_timeout_s=float(self._embed_close_drain_s))
        # a purge's scrub waiting for a reader of the index (one memd does
        # not control may hold on indefinitely) would hold the drain below:
        # it stops waiting, and the next open finishes it
        self.engine.stop_waiting()
        self._maint.stop(drain_timeout_s=float(self._embed_close_drain_s))
        if self.rerank is not None:
            self.rerank.close()
        ns = self.ns
        if ns is not None and not ns._closed:
            with self._holding(ns):
                ns.index.flush()
        self._flush_all_audits()
        if ns is not None:
            ns.close()
        self.engine.close()
        if self._fwd is not None:
            self._fwd.close()
        # a clean close collects what a crash orphaned (segment_gc /
        # snapshot_gc entries): make those entries durable too
        self._flush_all_audits()


# ---------------------------------------------------------------- forwarding


def _search_result(d: dict) -> SearchResult:
    d = dict(d)
    d["items"] = [SearchHit(**i) for i in d.get("items") or []]
    return SearchResult(**d)


def _search_dict(res: SearchResult) -> dict:
    """A search result as it travels back (shallow: nothing is mutated)."""
    return {**vars(res), "items": [vars(i) for i in res.items]}


def _served_export(m: Memory, ns: str, a: dict):
    st = m.export_stream(namespace=ns)
    return _fw._Stream({"skipped": st.skipped, "skipped_frames": st.skipped_frames}, iter(st))


# what a forwarded call runs in the holder (Memory._serve_forwarded): the
# same public methods a local call runs, on the namespace it names
_SERVED: dict[str, Callable[[Memory, str, dict], Any]] = {
    "ping": lambda m, ns, a: {"holds": True},
    "add": lambda m, ns, a: m.add(namespace=ns, **a),
    "add_events": lambda m, ns, a: m.add_events(a["events"], namespace=ns),
    "remember": lambda m, ns, a: m.remember(namespace=ns, **a),
    "close_session": lambda m, ns, a: m.close_session(namespace=ns, **a),
    "delete": lambda m, ns, a: m.delete(namespace=ns, **a),
    "delete_many": lambda m, ns, a: m.delete_many(namespace=ns, **a),
    "forget_ids": lambda m, ns, a: m._forget_ids(a["ids"], a["target"], a["actor"], ns),
    "destroy_namespace": lambda m, ns, a: m.destroy_namespace(namespace=ns, **a),
    "compact": lambda m, ns, a: m.compact(namespace=ns, **a),
    "reembed": lambda m, ns, a: m.reembed(namespace=ns, **a),
    "flush": lambda m, ns, a: m.flush(),
    "audit": lambda m, ns, a: m._append_forwarded_audit(ns, a["entries"]),
    "search": lambda m, ns, a: _search_dict(m.search(namespace=ns, consistency="strong", **a)),
    "get": lambda m, ns, a: m.get(namespace=ns, consistency="strong", **a),
    "find_ids": lambda m, ns, a: m.find_ids(namespace=ns, **a),
    "session_raw_count": lambda m, ns, a: m.session_raw_count(namespace=ns, **a),
    "stats": lambda m, ns, a: m.stats(namespace=ns),
    "export": _served_export,
}
