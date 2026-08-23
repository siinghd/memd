"""SLO micro-benchmarks (D2 acceptance): measures, doesn't guess.

Embedded-mode targets from ../02-slos.md:
  - durable write ack p99 <= 10ms (local fsync)
  - warm retrieval p50 <= 20ms / p99 <= 100ms
  - cold-namespace first query p90 <= 1.5s (embedded: index rebuild path)
  - read-your-writes immediate

Run: python bench/slo_bench.py [--vectors N]
"""
from __future__ import annotations

import argparse
import json
import shutil
import statistics
import tempfile
import time

from memd.engine.memory import Memory


def pctl(xs: list[float], p: float) -> float:
    xs = sorted(xs)
    return xs[min(int(p * (len(xs) - 1)), len(xs) - 1)]


def bench_writes(mem: Memory, n: int = 500) -> dict:
    lat = []
    for i in range(n):
        t0 = time.monotonic()
        mem.add(f"benchmark write event {i} with realistic payload text " * 2,
                session_id=f"bench-s{i % 10}", user_id="bench")
        lat.append((time.monotonic() - t0) * 1000)
    return {"n": n, "p50": round(pctl(lat, .5), 3), "p95": round(pctl(lat, .95), 3),
            "p99": round(pctl(lat, .99), 3), "max": round(max(lat), 3)}


def bench_retrieval(mem: Memory, queries: int = 200) -> dict:
    # ensure embeddings flushed so the vector lane is warm
    mem.flush()
    lat = []
    for i in range(queries):
        q = f"benchmark query about topic {i % 25} deployment details"
        t0 = time.monotonic()
        res = mem.search(q, user_id="bench", budget_tokens=2000)
        lat.append(res.latency_ms)
    return {"n": queries, "p50": round(pctl(lat, .5), 3), "p95": round(pctl(lat, .95), 3),
            "p99": round(pctl(lat, .99), 3)}


def bench_cold_open(root: str, namespace: str = "default") -> dict:
    """Kill everything, reopen: replay wal tail + index availability."""
    t0 = time.monotonic()
    m2 = Memory(root)
    st = m2.stats(namespace=namespace)
    dt = (time.monotonic() - t0) * 1000
    res = m2.search("benchmark query about topic 1 deployment details", user_id="bench")
    total = (time.monotonic() - t0) * 1000
    m2.close()
    return {"open_ms": round(dt, 3), "open_plus_first_query_ms": round(total, 3),
            "records_visible": st["records"]}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=3000)
    ap.add_argument("--writes", type=int, default=500)
    ap.add_argument("--queries", type=int, default=200)
    args = ap.parse_args()

    root = tempfile.mkdtemp(prefix="memd-slo-")
    try:
        mem = Memory(root, config={"rate_max_writes": 10**9})
        # seed a realistic corpus
        topics = ["standup", "refactor", "deploy", "cache", "auth", "lint"]
        for i in range(args.events):
            mem.add(f"Notes from {topics[i%len(topics)]} session {i}: discussed {topics[i%len(topics)]} "
                    f"and agreed on follow ups regarding {topics[i%len(topics)]} number {i}.",
                    session_id=f"s{i%40}", user_id="bench")
        mem.flush()

        w = bench_writes(mem, args.writes)
        r = bench_retrieval(mem, args.queries)

        # read-your-writes: search immediately after add, no flush allowed
        mem.add("the secret deployment codeword is zanzibar", session_id="ryw", user_id="bench")
        t0 = time.monotonic()
        ryw = mem.search("secret deployment codeword", user_id="bench")
        ryw_ms = (time.monotonic() - t0) * 1000
        assert any("zanzibar" in i.content for i in ryw.items), "read-your-writes failed"

        mem.close()  # quiesce first writer before the cold-open probe
        c = bench_cold_open(root)
        report = {
            "slo_targets": {"write_p99_ms": 10, "retrieve_p50_ms": 20, "retrieve_p99_ms": 100},
            "write_ack": w,
            "retrieval": r,
            "read_your_writes": {"found": True, "latency_ms": round(ryw_ms, 3)},
            "cold_open": c,
            "pass": {
                "write_p99<=10": w["p99"] <= 10,
                "retrieve_p50<=20": r["p50"] <= 20,
                "retrieve_p99<=100": r["p99"] <= 100,
            },
        }
        print(json.dumps(report, indent=1))
        if not all(report["pass"].values()):
            raise SystemExit(1)
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
