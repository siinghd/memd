"""memd.metrics - runtime observability (counters, histograms, gauges).

Dependency-free, thread-safe, O(1) per observation. Every hot path reports
here; surfaces:
  - Prometheus text exposition via the REST server at GET /metrics
  - JSON snapshot via metrics.snapshot() (harness results embed it)
  - optional periodic JSON dump: MEMD_METRICS_PATH env (for graphing)

UNIT CONTRACT (deliberate, and a deliberate deviation from Prometheus's
seconds-base convention): every duration in memd is measured, named and
bucketed in MILLISECONDS, suffix `_ms`. memd's SLO table is written in ms
(write ack p99 <= 10ms, retrieval p50 <= 20ms / p99 <= 100ms, cold open
p90 <= 1.5s); one unit end-to-end is what keeps a dashboard honest. Mixing
the two is what previously made every latency quantile wrong - see
LATENCY_MS_BUCKETS below.
"""
from __future__ import annotations

import bisect
import itertools
import json
import os
import threading
import time
from collections import OrderedDict
from typing import Any

# Millisecond ladder, deliberately dense where the SLOs are graded:
#   write ack p99 <= 10ms | retrieval p50 <= 20ms, p99 <= 100ms | cold open
#   p90 <= 1500ms | compaction/reembed seconds | extraction lag <= 60s p95.
# A duration that lands past the last bucket is counted in `overflow` and
# reported as >= the top bound - never averaged away (see _quantile).
LATENCY_MS_BUCKETS = (
    0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 15, 20, 35, 50, 75, 100, 250, 500,
    1000, 2500, 5000, 10_000, 30_000, 60_000,
)
# Back-compat alias: `observe()` defaults to the latency ladder.
DEFAULT_BUCKETS = LATENCY_MS_BUCKETS


def _labels_key(labels: dict[str, Any]) -> tuple:
    return tuple(sorted((k, str(v)) for k, v in labels.items()))


class _Series:
    __slots__ = ("value", "buckets", "bucket_counts", "overflow", "sum", "count")

    def __init__(self, buckets: tuple[float, ...]):
        self.value = 0.0
        self.buckets = buckets
        self.bucket_counts = [0] * len(buckets)
        self.overflow = 0  # observations above the last bucket (the +Inf band)
        self.sum = 0.0
        self.count = 0


class Registry:
    def __init__(self, max_series: int = 20_000):
        self._lock = threading.Lock()
        self._series: "OrderedDict[tuple, _Series]" = OrderedDict()
        self._meta: dict[str, tuple[str, str]] = {}  # name -> (type, help)
        # keys registered by preset_core(): the SLO series a dashboard graphs.
        # A FIFO guard evicted these first (they are inserted at t=0), so a
        # burst of label churn silently deleted exactly the series that must
        # never disappear. They are exempt from cardinality eviction.
        self._protected: set[tuple] = set()
        self.max_series = max_series
        # LRU bookkeeping only matters once eviction is possible; below this
        # watermark the recency touch is pure overhead on the hottest path in
        # the process (one observe() per lane per search).
        self._lru_watermark = max(1, (max_series * 8) // 10)
        self.started = time.time()
        self._res_sampled = 0.0

    def preset(self, name: str, kind: str, help: str = "", *,
               buckets: tuple[float, ...] = DEFAULT_BUCKETS,
               samples: list[dict[str, Any]] | None = None) -> None:
        """Pre-register a metric family so graphs get series from t=0 even at
        zero traffic (Prometheus best practice)."""
        with self._lock:
            self._meta.setdefault(name, (kind, help or name))
            for labels in (samples or [{}]):
                key = (name, _labels_key(labels))
                self._protected.add(key)
                if key not in self._series:
                    self._series[key] = _Series(buckets if kind == "histogram" else ())
                    self._series.move_to_end(key)

    def _get(self, name: str, kind: str, help_text: str, labels: dict[str, Any],
             buckets: tuple[float, ...]) -> _Series:
        key = (name, _labels_key(labels))
        s = self._series.get(key)
        if s is not None:
            if len(self._series) >= self._lru_watermark:
                self._series.move_to_end(key)  # LRU only under eviction pressure
            return s
        if len(self._series) >= self.max_series:
            victim = next((k for k in self._series if k not in self._protected), None)
            if victim is not None:
                del self._series[victim]  # evict least-recently-USED, never a preset
        b = buckets if buckets else ()
        s = _Series(b)
        self._series[key] = s
        self._meta.setdefault(name, (kind, help_text))
        return s

    # ------------------------------------------------------------------ API

    def inc(self, name: str, amount: float = 1.0, *, help: str = "", **labels: Any) -> None:
        with self._lock:
            self._get(name, "counter", help, labels, ()).value += amount

    def dec(self, name: str, amount: float = 1.0, *, help: str = "", **labels: Any) -> None:
        with self._lock:
            self._get(name, "gauge", help, labels, ()).value -= amount

    def set_gauge(self, name: str, value: float, *, help: str = "", **labels: Any) -> None:
        with self._lock:
            self._get(name, "gauge", help, labels, ()).value = value

    def add_gauge(self, name: str, delta: float, *, help: str = "", **labels: Any) -> None:
        with self._lock:
            self._get(name, "gauge", help, labels, ()).value += delta

    def observe(self, name: str, value: float, *, help: str = "",
                buckets: tuple[float, ...] = DEFAULT_BUCKETS, **labels: Any) -> None:
        with self._lock:
            s = self._get(name, "histogram", help, labels, buckets)
            v = max(0.0, float(value))
            s.sum += v
            s.count += 1
            i = bisect.bisect_left(s.buckets, v)
            if i < len(s.buckets):
                s.bucket_counts[i] += 1
            else:
                # Above the top bound. Prometheus exposition derives +Inf from
                # s.count, but the JSON snapshot's quantiles need to KNOW the
                # band is populated: without this counter a fully-overflowed
                # histogram silently reported p50 == p95 == p99 == mean.
                s.overflow += 1

    def timer(self, name: str, *, help: str = "", buckets: tuple[float, ...] = DEFAULT_BUCKETS,
              **labels: Any):
        """Context manager timing a block in MILLISECONDS (unit contract):
        `with METRICS.timer('memd_x_ms', ns=ns): ...`"""
        return _Timer(self, name, help, buckets, labels)

    # ----------------------------------------------------------- resources

    def sample_resources(self, min_interval_s: float = 1.0) -> None:
        """Process resource use - RSS, open fds, threads, CPU seconds.

        Observability needs resource use alongside latency/throughput/errors;
        none of it was ever gauged, so a memory or fd leak was invisible in the
        very surface built to catch it. Sampled lazily at export time and
        throttled, so a scrape storm cannot turn into a /proc storm. Linux
        /proc is the fast path; every read degrades silently elsewhere.
        """
        now = time.monotonic()
        with self._lock:
            if now - self._res_sampled < min_interval_s:
                return
            self._res_sampled = now
        try:
            with open("/proc/self/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        self.set_gauge("memd_process_rss_bytes",
                                       float(line.split()[1]) * 1024,
                                       help="resident set size (bytes)")
                    elif line.startswith("Threads:"):
                        self.set_gauge("memd_process_threads", float(line.split()[1]),
                                       help="OS threads in this process")
        except OSError:
            pass
        try:
            self.set_gauge("memd_process_open_fds", float(len(os.listdir("/proc/self/fd"))),
                           help="open file descriptors")
        except OSError:
            pass
        try:
            t = os.times()
            self.set_gauge("memd_process_cpu_seconds", float(t.user + t.system),
                           help="process CPU seconds (user+system)")
        except OSError:
            pass

    # ------------------------------------------------------------- exports

    @staticmethod
    def _visible(lkey: tuple, ns_filter: "set[str] | None") -> bool:
        """Tenancy filter for the export surfaces.

        Metric labels carry namespace names, so an unfiltered export hands one
        tenant every other tenant's traffic shape, record counts and error
        rates - through a key that is 403'd on those same namespaces. Series
        with NO ns label are process-global and stay visible.
        """
        if ns_filter is None:
            return True
        for k, v in lkey:
            if k == "ns":
                return v in ns_filter
        return True

    def snapshot(self, ns_filter: "set[str] | None" = None) -> dict[str, Any]:
        """Point-in-time JSON view.

        The lock is held ONLY to copy each series' primitives - quantile
        interpolation and dict building happen outside it. Holding the global
        lock across the whole O(series x buckets) computation stalled every
        concurrent observe() on every hot path for the duration of a scrape
        (162-184ms at cap cardinality): the observability surface was itself a
        latency source on the request path. Copy cost is one pass over the
        bucket arrays; the math is three.
        """
        self.sample_resources()
        with self._lock:
            uptime = round(time.time() - self.started, 3)
            # O(series) POINTER copy only - the bucket arrays are read outside
            # the lock, exactly as render_prometheus() already does. A metrics
            # reader may observe a series mid-update (count/sum skewed by one
            # observation); that is the deliberate trade for never stalling a
            # request path behind a scrape.
            items = list(self._series.items())
            meta = dict(self._meta)
        raw = [
            (name, lkey, meta.get(name, ("", ""))[0], s.buckets,
             list(s.bucket_counts), s.overflow, s.sum, s.count, s.value)
            for (name, lkey), s in items if self._visible(lkey, ns_filter)
        ]

        def _quantile(buckets, counts, overflow, total, ssum, q: float) -> float:
            if total == 0:
                return 0.0
            target = q * total
            cum = 0.0
            prev = 0.0
            for b, c in zip(buckets, counts):
                if cum + c >= target:
                    frac = (target - cum) / c if c else 0.0
                    return round(prev + (b - prev) * frac, 6)
                cum += c
                prev = b
            # target lies in the +Inf band: report at least the top bound.
            # Returning the MEAN here (the previous behaviour) hid every tail -
            # a histogram whose values all exceeded the ladder reported
            # p50 == p95 == p99 == avg.
            if not buckets:
                return round(ssum / total, 6)
            return round(max(float(buckets[-1]), ssum / total), 6)

        out: dict[str, Any] = {
            "_process_uptime_s": uptime,
            "counters": {},
            "gauges": {},
            "histograms": {},
        }
        for name, lkey, kind, buckets, counts, overflow, ssum, count, value in raw:
            labels = dict(lkey)
            if kind == "histogram":
                out["histograms"].setdefault(name, []).append({
                    "labels": labels,
                    "count": count,
                    "sum": round(ssum, 6),
                    "avg": round(ssum / count, 6) if count else 0.0,
                    "p50": _quantile(buckets, counts, overflow, count, ssum, 0.50),
                    "p95": _quantile(buckets, counts, overflow, count, ssum, 0.95),
                    "p99": _quantile(buckets, counts, overflow, count, ssum, 0.99),
                    "buckets": dict(zip(map(str, buckets), counts)),
                    "overflow": overflow,  # observations past the top bound
                })
            elif kind == "counter":
                out["counters"].setdefault(name, []).append({"labels": labels, "value": value})
            else:
                out["gauges"].setdefault(name, []).append({"labels": labels, "value": value})
        return out

    def render_prometheus(self, ns_filter: "set[str] | None" = None) -> str:
        self.sample_resources()
        lines: list[str] = []
        snap_meta = self._meta
        seen_help: set[str] = set()
        counter_acc: dict[str, float] = {}
        gauge_acc: dict[str, float] = {}
        # histograms keep their label sets: per-route/per-ns quantiles must be
        # graphable, so merging across labels here would silently destroy the
        # very dimensions the cardinality guard exists to allow
        hist_series: dict[str, list[tuple[tuple, _Series]]] = {}
        with self._lock:
            items = list(self._series.items())
        for (name, lkey), s in items:
            if not self._visible(lkey, ns_filter):
                continue
            kind = snap_meta.get(name, ("gauge", ""))[0]
            lbl = ",".join(f'{k}="{v}"' for k, v in lkey) if lkey else ""
            lblc = "{" + lbl + "}" if lbl else ""
            if kind == "histogram":
                hist_series.setdefault(name, []).append((lkey, s))
            elif kind == "counter":
                counter_acc[f"{name}{lblc}"] = counter_acc.get(f"{name}{lblc}", 0.0) + s.value
            else:
                gauge_acc[f"{name}{lblc}"] = gauge_acc.get(f"{name}{lblc}", 0.0) + s.value

        def emit_help(name: str, kind: str, help_text: str) -> None:
            if name not in seen_help:
                lines.append(f"# HELP {name} {help_text or name}")
                lines.append(f"# TYPE {name} {kind}")
                seen_help.add(name)

        for name in sorted(set(counter_acc) | set(gauge_acc)):
            base = name.split("{")[0]
            kind, help_text = snap_meta.get(base, ("gauge", ""))
            emit_help(base, kind, help_text)
            val = counter_acc.get(name, gauge_acc.get(name))
            if val is not None and float(val).is_integer():
                val = int(val)
            lines.append(f"{name} {val}")
        for name in sorted(hist_series):
            _, help_text = snap_meta.get(name, ("histogram", name))
            emit_help(name, "histogram", help_text)
            for lkey, s in sorted(hist_series[name], key=lambda x: x[0]):
                lbl = ",".join(f'{k}="{v}"' for k, v in lkey)

                def hline(suffix: str, value) -> str:
                    return f"{name}_{suffix}{{{lbl}}} {value}" if lbl else f"{name}_{suffix} {value}"

                cum = 0
                for b, c in zip(s.buckets, s.bucket_counts):
                    cum += c
                    le = f',le="{b}"' if lbl else f'le="{b}"'
                    lines.append(f"{name}_bucket{{{lbl}{le}}} {cum}" if lbl
                                 else f'{name}_bucket{{le="{b}"}} {cum}')
                inf = ',le="+Inf"' if lbl else 'le="+Inf"'
                lines.append(f"{name}_bucket{{{lbl}{inf}}} {s.count}")
                lines.append(hline("sum", s.sum))
                lines.append(hline("count", s.count))
        return "\n".join(lines) + "\n"


class _Timer:
    __slots__ = ("reg", "name", "help", "buckets", "labels", "t0")

    def __init__(self, reg: Registry, name: str, help_text: str, buckets: tuple[float, ...],
                 labels: dict[str, Any]):
        self.reg = reg
        self.name = name
        self.help = help_text
        self.buckets = buckets
        self.labels = labels
        self.t0 = 0.0

    def __enter__(self) -> "_Timer":
        self.t0 = time.monotonic()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        dt = (time.monotonic() - self.t0) * 1000
        self.reg.observe(self.name, dt, help=self.help or self.name,
                         buckets=self.buckets, **self.labels)


METRICS = Registry()


class PeriodicDumper:
    """Append-only metrics history for trend graphing: one timestamped JSON
    snapshot per line (JSONL - queryable with jq / duckdb / pandas), appended
    every `interval_s`. Lines are small O_APPEND writes (atomic on local
    POSIX). Set MEMD_METRICS_PATH to enable.

    Retention is the point of a trend store, so it is sized deliberately: the
    dump is COMPACT by default (count/sum/avg/quantiles/overflow per series,
    without the raw bucket vector, which dominates the line and is not what a
    trend graph reads) and `keep` rotations are retained rather than one. At
    cap cardinality the previous shape - full buckets, a single rotation -
    held roughly ten minutes of history, which cannot show a trend. Set
    MEMD_METRICS_FULL=1 to keep raw buckets when re-deriving quantiles
    offline matters more than retention."""

    def __init__(self, path: str, interval_s: float = 10.0, registry: Registry | None = None,
                 rotate_bytes: int = 64 * 1024 * 1024, keep: int = 8, compact: bool = True):
        self.path = path
        self.interval_s = interval_s
        self.reg = registry or METRICS
        self.rotate_bytes = rotate_bytes
        self.keep = max(1, keep)
        self.compact = compact
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True, name="memd-metrics-dump")

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
        self._dump()

    def _run(self) -> None:
        while not self._stop.wait(self.interval_s):
            self._dump()

    def _rotate(self) -> None:
        """Shift .1 .. .keep down by one and seal the live file as .1."""
        oldest = f"{self.path}.{self.keep}"
        if os.path.exists(oldest):
            os.remove(oldest)
        for i in range(self.keep - 1, 0, -1):
            src = f"{self.path}.{i}"
            if os.path.exists(src):
                os.replace(src, f"{self.path}.{i + 1}")
        os.replace(self.path, f"{self.path}.1")

    def _dump(self) -> None:
        try:
            snap = self.reg.snapshot()
            if self.compact:
                for series in snap.get("histograms", {}).values():
                    for h in series:
                        h.pop("buckets", None)
            snap["_epoch_ms"] = int(time.time() * 1000)
            line = json.dumps(snap, separators=(",", ":")) + "\n"
            if os.path.exists(self.path) and os.path.getsize(self.path) >= self.rotate_bytes:
                self._rotate()
            with open(self.path, "a") as f:  # O_APPEND: atomic per line here
                f.write(line)
        except OSError:
            pass


def auto_dumper_from_env(registry: Registry | None = None) -> PeriodicDumper | None:
    path = os.environ.get("MEMD_METRICS_PATH")
    if not path:
        return None
    interval = float(os.environ.get("MEMD_METRICS_INTERVAL_S", "10"))
    d = PeriodicDumper(
        path, interval, registry,
        keep=int(os.environ.get("MEMD_METRICS_KEEP", "8")),
        compact=not os.environ.get("MEMD_METRICS_FULL"),
    )
    d.start()
    return d


CORE_HISTOGRAMS = {
    "memd_write_ack_ms": ("durable write ack latency (ms)", DEFAULT_BUCKETS),
    "memd_search_latency_ms": ("end-to-end search latency (ms)", DEFAULT_BUCKETS),
    "memd_find_ids_ms": ("find_ids sweep duration (ms)", DEFAULT_BUCKETS),
    "memd_mcp_tool_ms": ("MCP tool duration (ms)", DEFAULT_BUCKETS),
    "memd_packed_tokens": ("tokens injected per search",
                           (64, 128, 256, 512, 1024, 2048, 4096, 8192)),
    "memd_search_hits": ("packed items per search", (1, 2, 5, 10, 20, 50)),
    "memd_search_store_ops": ("object-store round trips per search",
                              (0, 1, 2, 4, 8, 16, 32, 64, 128)),
    "memd_lane_candidates": ("candidates per lane per search",
                             (0, 1, 5, 10, 25, 50, 100)),
    "memd_session_close_ms": ("session close duration (ms)", DEFAULT_BUCKETS),
    "memd_compaction_ms": ("compaction duration (ms)", DEFAULT_BUCKETS),
    "memd_export_ms": ("export duration (ms)", DEFAULT_BUCKETS),
    "memd_reembed_ms": ("re-embedding batch duration (ms)", DEFAULT_BUCKETS),
    "memd_search_stage_ms": ("per-stage search timing (ms): plan/fuse/pack", DEFAULT_BUCKETS),
    "memd_index_snapshot_bytes": ("index snapshot size (bytes)",
                                  (1e5, 1e6, 1e7, 5e7, 1e8, 5e8)),
    "memd_embed_batch_size": ("texts per embedding batch", (1, 4, 8, 16, 32, 64, 128)),
    "memd_embed_apply_ms": ("embed + index apply duration (ms)", DEFAULT_BUCKETS),
    "memd_http_request_ms": ("HTTP request duration (ms)", DEFAULT_BUCKETS),
    "memd_lane_ms": ("per-lane candidate fetch duration (ms)", DEFAULT_BUCKETS),
    "memd_rerank_ms": ("reranker call duration (ms), including fallbacks", DEFAULT_BUCKETS),
    "memd_lexical_commit_ms": ("tantivy indexer batch: read + index + commit (ms)", DEFAULT_BUCKETS),
    "memd_vector_index_build_ms": ("usearch sidecar build from SQLite (ms)", DEFAULT_BUCKETS),
    "memd_vector_index_save_ms": ("vector sidecar save (ms)", DEFAULT_BUCKETS),
    "memd_vector_index_gil_hold_ms": ("usearch save/restore calls, which hold the GIL (ms)", DEFAULT_BUCKETS),
}

CORE_COUNTERS = {
    "memd_source_downgrades_total": "client trust-tier claims downgraded",
    "memd_oversized_requests_total": "requests rejected by body-size cap",
    "memd_embed_query_failures_total": "query-embedding failures (degraded mode)",
    "memd_embed_query_not_ready_total": "queries served without the vector lane: embedder model still loading",
    "memd_extraction_failures_total": "extraction runs that failed",
    "memd_extraction_chunks_failed_total": "extraction calls that failed (their turns went through the pattern extractor)",
    "memd_extraction_items_malformed_total": "items of an extraction reply dropped as malformed (the reply's other facts kept)",
    "memd_writes_total": "raw/explicit lane writes",
    "memd_search_total": "searches served",
    "memd_search_cache_hits_total": "repeat-query cache hits",
    "memd_search_cache_misses_total": "repeat-query cache misses",
    "memd_find_ids_total": "destructive-sweep resolutions",
    "memd_find_ids_matched_total": "ids matched by sweeps",
    "memd_rotations_total": "WAL rotations (segment closes)",
    "memd_destroys_total": "namespace crypto-shreds",
    "memd_reembed_total": "records re-embedded",
    "memd_mcp_calls_total": "MCP tool calls",
    "memd_remembers_total": "explicit-lane writes",
    "memd_deletes_total": "tombstones/hard deletes",
    "memd_quarantined_total": "writes quarantined by anomaly policy",
    "memd_facts_extracted_total": "facts written by extraction",
    "memd_facts_superseded_total": "facts superseded cluster-locally",
    "memd_dupes_dropped_total": "near-duplicate facts dropped",
    "memd_sessions_closed_total": "sessions closed (extraction boundary)",
    "memd_packed_truncated_total": "searches truncated at budget",
    "memd_wal_bytes_total": "WAL bytes appended",
    "memd_wal_frames_total": "WAL frames appended",
    "memd_manifest_writes_total": "manifest persists",
    "memd_index_commits_total": "index commits (lazy threshold + explicit)",
    "memd_compactions_total": "compactions run",
    "memd_compactions_skipped_total": "compactions skipped: provably nothing to do",
    "memd_index_snapshots_written_total": "derived-index snapshots persisted",
    "memd_index_snapshots_loaded_total": "cold starts served from a snapshot",
    "memd_index_snapshot_failures_total": "snapshot write/load failures (falls back to replay)",
    "memd_records_purged_total": "records physically purged",
    "memd_embed_failures_total": "embedding batch failures",
    "memd_embed_retries_total": "embedding retry attempts",
    "memd_embed_dead_letters_total": "embeddings dropped after retry cap",
    "memd_embed_dropped_on_close_total": "embeddings still pending when the worker stopped",
    "memd_flush_embed_pending_total": "flushes that returned with embeddings still pending",
    "memd_bm25_queries_total": "bm25 lane queries (one ranked FTS5 OR query each)",
    "memd_embed_backlog_dropped_total": "embeddings deferred: backlog over capacity (reembed heals)",
    "memd_auto_compactions_total": "opportunistic compactions (self-enforced deadlines)",
    "memd_purges_scheduled_total": "due hard-delete purges handed to background maintenance",
    "memd_maintenance_failures_total": "background maintenance runs that raised",
    "memd_storage_parse_errors_total": "corrupt frames skipped in replay/load",
    "memd_audit_flush_failures_total": "audit entries lost to I/O errors",
    "memd_audit_checkpoint_failures_total": "audit tail checkpoints not persisted (slow open next time)",
    "memd_audit_rotations_total": "audit ledger segments sealed",
    "memd_audit_appends_dropped_total": "audit appends discarded for a shredded namespace",
    "memd_audit_segments_pruned_total": "audit segments dropped past the retention bound",
    "memd_audit_rotation_failures_total": "audit ledger rotations that failed (ledger keeps growing)",
    "memd_session_truncated_total": "sessions exceeding extraction row limit",
    "memd_http_requests_total": "HTTP requests",
    "memd_http_errors_total": "HTTP 5xx errors",
    "memd_auth_failures_total": "authentication failures",
    "memd_authz_denials_total": "namespace authorization denials (valid key, wrong namespace)",
    "memd_rate_limited_total": "requests rejected by rate limiter",
    "memd_heavy_throttled_total": "heavy maintenance calls rejected by throttle",
    "memd_keystore_load_failures_total": "keys-file loads that failed (stale map served)",
    "memd_orphan_segments_adopted_total": "unreferenced segments healed on open",
    "memd_index_write_after_close_total": "index writes skipped after close",
    "memd_store_ops_total": "object-store operations by type",
    "memd_embed_target_missing_total": "embeddings dropped because the target namespace is gone",
    "memd_forgets_total": "query-driven forget sweeps executed",
    "memd_rerank_calls_total": "searches that asked the reranker",
    "memd_rerank_fallback_total": "searches that kept the unreranked order: reranker failed, timed out or was not ready",
    "memd_lexical_searches_total": "bm25-lane queries served by tantivy",
    "memd_lexical_fallback_total": "bm25-lane queries served by FTS5 instead of tantivy",
    "memd_lexical_tail_hits_total": "bm25-lane hits served from the FTS5 tail (not yet in tantivy)",
    "memd_lexical_rebuilds_total": "tantivy lexical index rebuilds",
    "memd_lexical_indexed_total": "rows (re)indexed into tantivy",
    "memd_lexical_index_failures_total": "tantivy indexer steps that failed (the lane serves from FTS5)",
    "memd_lexical_attach_failures_total": "namespaces opened without the tantivy accelerator (FTS5 serves)",
    "memd_vector_index_searches_total": "vector-lane queries served by the usearch sidecar",
    "memd_vector_index_fallback_total": "vector-lane queries answered exactly instead of by the usearch sidecar",
    "memd_vector_index_rebuilds_total": "usearch sidecar rebuilds",
    "memd_vector_index_failures_total": "usearch sidecar operations that failed (rebuilt; the exact scan serves)",
    "memd_vector_index_attach_failures_total": "namespaces opened without the usearch sidecar (the exact scan serves)",
    "memd_vector_index_activations_total": "namespaces that crossed ann_min_vectors (auto: flat -> usearch)",
    "memd_vector_index_snapshots_written_total": "vector sidecar images published with an index snapshot",
    "memd_vector_index_snapshots_loaded_total": "vector sidecars installed from a published snapshot",
    "memd_vector_index_snapshot_failures_total": "vector sidecar snapshots that could not be used (rebuilt instead)",
    "memd_vector_lane_skipped_total": "vector-lane queries skipped: the sidecar is not serving and the namespace is over flat_max_vectors (other lanes serve)",
    "memd_vector_index_corrupt_total": "sidecar files and snapshots rejected as corrupt (rebuilt from SQLite)",
    "memd_vector_index_repaired_total": "poorly linked HNSW nodes re-inserted after a build",
}

CORE_GAUGES = {
    "memd_process_rss_bytes": "resident set size (bytes)",
    "memd_process_open_fds": "open file descriptors",
    "memd_process_threads": "OS threads in this process",
    "memd_process_cpu_seconds": "process CPU seconds (user+system)",
    "memd_embed_queue_depth": "pending embedding texts",
    "memd_records": "live record count",
    "memd_vectors": "vector count",
    "memd_vectors_missing": "live records with no current-version vector",
    "memd_quarantined": "quarantined record count",
    "memd_pending_purges": "scheduled physical purges not yet due",
    "memd_lexical_lag": "changes not yet in the tantivy index (served from FTS5)",
}


def preset_core(registry: Registry, ns: str | None = None) -> None:
    """Pre-register every core family so /metrics shows series from t=0."""
    lbl_ns = {"ns": ns} if ns else {}
    for name, (_help, buckets) in CORE_HISTOGRAMS.items():
        registry.preset(name, "histogram", _help, buckets=buckets,
                        samples=[dict(lbl_ns)] if lbl_ns else [{}])
    for name, help_text in CORE_COUNTERS.items():
        registry.preset(name, "counter", help_text,
                        samples=[dict(lbl_ns)] if lbl_ns else [{}])
    for name, help_text in CORE_GAUGES.items():
        registry.preset(name, "gauge", help_text,
                        samples=[dict(lbl_ns)] if lbl_ns else [{}])
