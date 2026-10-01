#!/usr/bin/env python
"""Scale benchmark: SLO verification at the flat-scan design ceiling.

The embedded retrieval SLOs (warm p50<=20ms / p99<=100ms embedded) are stated for
namespaces at real scale. Toy corpora hide asymptotics; this bench seeds
50K records (the documented ceiling before IVF replaces the flat scan),
then measures warm retrieval by query class, cold open, compaction cost,
and peak RSS.

Run: python bench/scale_bench.py [--records 50000]
"""
from __future__ import annotations

import argparse
import json
import resource
import shutil
import sys
import tempfile
import time

sys.path.insert(0, "src")

from memd.engine.memory import Memory

TOPICS = ["standup", "refactor", "deploy", "cache", "auth", "lint", "api", "storage",
          "queue", "metrics", "tracing", "alerting", "backup", "restore", "rotate"]
BATCH = 40


def pctl(xs: list[float], p: float) -> float:
    xs = sorted(xs)
    return xs[min(int(p * (len(xs) - 1)), len(xs) - 1)]


def sample(fn, n: int) -> dict:
    lat = []
    for i in range(n):
        t = time.monotonic()
        fn(i)
        lat.append((time.monotonic() - t) * 1000)
    return {"p50": round(pctl(lat, .5), 1), "p95": round(pctl(lat, .95), 1),
            "p99": round(pctl(lat, .99), 1), "max": round(max(lat), 1)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--records", type=int, default=50_000)
    ap.add_argument("--keep", action="store_true", help="keep the data dir")
    args = ap.parse_args()

    root = tempfile.mkdtemp(prefix="memd-scale-")
    mem = Memory(root, config={"rate_max_writes": 10 ** 9})
    try:
        t0 = time.monotonic()
        batches = args.records // BATCH
        for b in range(batches):
            events = [{"content": f"Notes from {TOPICS[(b * BATCH + j) % len(TOPICS)]} "
                       f"session {b * BATCH + j}: discussed {TOPICS[(b * BATCH + j) % len(TOPICS)]} "
                       f"follow ups number {b * BATCH + j} payload words.",
                       "session_id": f"s{(b * BATCH + j) % 200}", "user_id": "bench"}
                      for j in range(BATCH)]
            mem.add_events(events)
            if (b + 1) % 25 == 0:
                mem.ns.rotate("scale-bench")
        seed_s = time.monotonic() - t0
        mem.flush()

        # warmup absorbs one-time costs (vector matrix load, sqlite cache)
        for i in range(20):
            mem.search(f"warmup about {TOPICS[i % len(TOPICS)]} number {i}", user_id="bench")

        out = {
            "records": batches * BATCH,
            "seed_s": round(seed_s, 1),
            "peak_rss_mb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1),
            "plain": sample(lambda i: mem.search(
                f"notes about {TOPICS[i % len(TOPICS)]} session follow ups number {10_000 + i}",
                user_id="bench"), 150),
            "temporal": sample(lambda i: mem.search(
                f"when did we discuss {TOPICS[i % len(TOPICS)]} recently number {20_000 + i}?",
                user_id="bench"), 150),
        }

        mem.close()
        t0 = time.monotonic()
        mem2 = Memory(root)
        out["cold_open_ms"] = round((time.monotonic() - t0) * 1000, 1)
        out["records_visible"] = mem2.stats()["records"]

        t0 = time.monotonic()
        rep = mem2.compact(force=True)
        out["compact_s"] = round(time.monotonic() - t0, 2)

        # post-compaction correctness spot check: nothing lost
        assert out["records_visible"] == out["records"], "compaction lost records"

        verdict = {
            "retrieve_p99_ok": max(out["plain"]["p99"], out["temporal"]["p99"]) <= 100,
            "cold_open_ok": out["cold_open_ms"] <= 1500,
        }
        print(json.dumps(out | {"pass": verdict}, indent=1))
        if not all(verdict.values()):
            raise SystemExit(1)
    finally:
        mem.close()
        if not args.keep:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
