"""memd.metrics - runtime observability (counters, histograms, gauges).

Dependency-free, thread-safe, O(1) per observation. Every hot path reports
here; surfaces:
  - Prometheus text exposition via the REST server at GET /metrics
  - JSON snapshot via metrics.snapshot() (harness results embed it)
  - optional periodic JSON dump: MEMD_METRICS_PATH env (for graphing)

Naming follows Prometheus convention: memd_<thing>_<unit>_total/_seconds.
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

DEFAULT_BUCKETS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)


def _labels_key(labels: dict[str, Any]) -> tuple:
    return tuple(sorted((k, str(v)) for k, v in labels.items()))


class _Series:
    __slots__ = ("value", "buckets", "bucket_counts", "sum", "count")

    def __init__(self, buckets: tuple[float, ...]):
        self.value = 0.0
        self.buckets = buckets
        self.bucket_counts = [0] * len(buckets)
        self.sum = 0.0
        self.count = 0


class Registry:
    def __init__(self, max_series: int = 20_000):
        self._lock = threading.Lock()
        self._series: "OrderedDict[tuple, _Series]" = OrderedDict()
        self._meta: dict[str, tuple[str, str]] = {}  # name -> (type, help)
        self.max_series = max_series
        self.started = time.time()

    def preset(self, name: str, kind: str, help: str = "", *,
               buckets: tuple[float, ...] = DEFAULT_BUCKETS,
               samples: list[dict[str, Any]] | None = None) -> None:
        """Pre-register a metric family so graphs get series from t=0 even at
        zero traffic (Prometheus best practice)."""
        with self._lock:
            self._meta.setdefault(name, (kind, help or name))
            for labels in (samples or [{}]):
                key = (name, _labels_key(labels))
                if key not in self._series:
                    self._series[key] = _Series(buckets if kind == "histogram" else ())
                    self._series.move_to_end(key)

    def _get(self, name: str, kind: str, help_text: str, labels: dict[str, Any],
             buckets: tuple[float, ...]) -> _Series:
        key = (name, _labels_key(labels))
        s = self._series.get(key)
        if s is None:
            if len(self._series) >= self.max_series:
                self._series.popitem(last=False)  # evict oldest (cardinality guard)
            b = buckets if buckets else ()
            s = _Series(b)
            self._series[key] = s
            self._meta.setdefault(name, (kind, help_text))
        elif name in self._meta and self._meta[name][0] != kind:
            pass
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
            else:  # above last bucket: count into +Inf implicitly via sum/count
                s.bucket_counts[-1] += 0

    def timer(self, name: str, *, help: str = "", buckets: tuple[float, ...] = DEFAULT_BUCKETS,
              **labels: Any):
        """Context manager: metrics.timer('memd_x_seconds').__enter__/exit."""
        return _Timer(self, name, help, buckets, labels)

    # ------------------------------------------------------------- exports

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            out: dict[str, Any] = {
                "_process_uptime_s": round(time.time() - self.started, 3),
                "counters": {},
                "gauges": {},
                "histograms": {},
            }
            for (name, lkey), s in self._series.items():
                labels = dict(lkey)
                kind = self._meta.get(name, ("", ""))[0]
                if kind == "histogram":
                    # quantiles estimated by linear interpolation within buckets
                    def _quantile(s: _Series, q: float) -> float:
                        if s.count == 0:
                            return 0.0
                        target = q * s.count
                        cum = 0.0
                        prev = 0.0
                        for b, c in zip(s.buckets, s.bucket_counts):
                            if cum + c >= target:
                                frac = (target - cum) / c if c else 0.0
                                return round(prev + (b - prev) * frac, 6)
                            cum += c
                            prev = b
                        return round(s.sum / s.count, 6)

                    h = out["histograms"].setdefault(name, [])
                    h.append({
                        "labels": labels,
                        "count": s.count,
                        "sum": round(s.sum, 6),
                        "avg": round(s.sum / s.count, 6) if s.count else 0.0,
                        "p50": _quantile(s, 0.50),
                        "p95": _quantile(s, 0.95),
                        "p99": _quantile(s, 0.99),
                        "buckets": dict(zip(map(str, s.buckets), s.bucket_counts)),
                    })
                elif kind == "counter":
                    out["counters"].setdefault(name, []).append({"labels": labels, "value": s.value})
                else:
                    out["gauges"].setdefault(name, []).append({"labels": labels, "value": s.value})
            return out

    def render_prometheus(self) -> str:
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
    POSIX); the file rotates at `rotate_bytes` so a long-lived process can't
    grow it without bound. Set MEMD_METRICS_PATH to enable."""

    def __init__(self, path: str, interval_s: float = 10.0, registry: Registry | None = None,
                 rotate_bytes: int = 64 * 1024 * 1024):
        self.path = path
        self.interval_s = interval_s
        self.reg = registry or METRICS
        self.rotate_bytes = rotate_bytes
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

    def _dump(self) -> None:
        try:
            snap = self.reg.snapshot()
            snap["_epoch_ms"] = int(time.time() * 1000)
            line = json.dumps(snap, separators=(",", ":")) + "\n"
            if os.path.exists(self.path) and os.path.getsize(self.path) >= self.rotate_bytes:
                os.replace(self.path, self.path + ".1")  # keep one older window
            with open(self.path, "a") as f:  # O_APPEND: atomic per line here
                f.write(line)
        except OSError:
            pass


def auto_dumper_from_env(registry: Registry | None = None) -> PeriodicDumper | None:
    path = os.environ.get("MEMD_METRICS_PATH")
    if not path:
        return None
    interval = float(os.environ.get("MEMD_METRICS_INTERVAL_S", "10"))
    d = PeriodicDumper(path, interval, registry)
    d.start()
    return d


CORE_HISTOGRAMS = {
    "memd_write_ack_seconds": ("durable write ack latency (ms)", DEFAULT_BUCKETS),
    "memd_search_latency_ms": ("end-to-end search latency (ms)", DEFAULT_BUCKETS),
    "memd_find_ids_ms": ("find_ids sweep duration (ms)", DEFAULT_BUCKETS),
    "memd_mcp_tool_ms": ("MCP tool duration (ms)", DEFAULT_BUCKETS),
    "memd_packed_tokens": ("tokens injected per search",
                           (64, 128, 256, 512, 1024, 2048, 4096, 8192)),
    "memd_search_hits": ("packed items per search", (1, 2, 5, 10, 20, 50)),
    "memd_lane_candidates": ("candidates per lane per search",
                             (0, 1, 5, 10, 25, 50, 100)),
    "memd_session_close_ms": ("session close duration (ms)", DEFAULT_BUCKETS),
    "memd_compaction_seconds": ("compaction duration (s)", DEFAULT_BUCKETS),
    "memd_embed_batch_size": ("texts per embedding batch", (1, 4, 8, 16, 32, 64, 128)),
    "memd_embed_apply_ms": ("embed + index apply duration (ms)", DEFAULT_BUCKETS),
    "memd_http_request_ms": ("HTTP request duration (ms)", DEFAULT_BUCKETS),
    "memd_lane_ms": ("per-lane candidate fetch duration (ms)", DEFAULT_BUCKETS),
}

CORE_COUNTERS = {
    "memd_source_downgrades_total": "client trust-tier claims downgraded",
    "memd_oversized_requests_total": "requests rejected by body-size cap",
    "memd_embed_query_failures_total": "query-embedding failures (degraded mode)",
    "memd_extraction_failures_total": "extraction runs that failed",
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
    "memd_records_purged_total": "records physically purged",
    "memd_embed_failures_total": "embedding batch failures",
    "memd_embed_retries_total": "embedding retry attempts",
    "memd_embed_dead_letters_total": "embeddings dropped after retry cap",
    "memd_embed_backlog_dropped_total": "embeddings deferred: backlog over capacity (reembed heals)",
    "memd_auto_compactions_total": "opportunistic compactions (self-enforced deadlines)",
    "memd_storage_parse_errors_total": "corrupt frames skipped in replay/load",
    "memd_audit_flush_failures_total": "audit entries lost to I/O errors",
    "memd_session_truncated_total": "sessions exceeding extraction row limit",
    "memd_http_requests_total": "HTTP requests",
    "memd_http_errors_total": "HTTP 5xx errors",
    "memd_auth_failures_total": "authentication failures",
    "memd_rate_limited_total": "requests rejected by rate limiter",
    "memd_heavy_throttled_total": "heavy maintenance calls rejected by throttle",
    "memd_keystore_load_failures_total": "keys-file loads that failed (stale map served)",
    "memd_orphan_segments_adopted_total": "unreferenced segments healed on open",
    "memd_index_write_after_close_total": "index writes skipped after close",
    "memd_forgets_total": "query-driven forget sweeps executed",
}

CORE_GAUGES = {
    "memd_embed_queue_depth": "pending embedding texts",
    "memd_records": "live record count",
    "memd_vectors": "vector count",
    "memd_quarantined": "quarantined record count",
    "memd_pending_purges": "scheduled physical purges not yet due",
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
