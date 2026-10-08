"""SLO micro-benchmarks: measures, doesn't guess.

memd's embedded-mode SLO targets:
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


TOPICS = ["standup", "refactor", "deploy", "cache", "auth", "lint"]


def bench_retrieval(mem: Memory, queries: int = 200, corpus_n: int = 3000, **search_kw) -> dict:
    """Warm retrieval: the NAMESPACE is warm, not the query. `search_kw`:
    the search's budget and layout; none = the defaults a caller gets
    (12,000 tokens, session packing), which the SLO is graded on.

    Two measurement defects fixed here, both of which made this gate grade
    something other than retrieval:
      - it recorded `res.latency_ms`, memd's self-report, and discarded the
        wall clock it had already started. Anything outside memd's own timer
        was invisible to the SLO.
      - it asked only 25 distinct questions across 200 iterations, so ~87% of
        the samples were repeat-query CACHE HITS, and a hit used to replay the
        original miss's latency_ms. The reported p50/p99 described neither.
    Queries are now distinct (this is retrieval, not cache) and timed by the
    caller's clock. Cache-hit latency is reported separately, because that
    number is real and useful - it is just not the retrieval SLO.
    """
    mem.flush()
    lat = []
    step = max(1, corpus_n // max(1, queries))
    for i in range(queries):
        # distinct (so this measures retrieval, not the repeat-query cache) but
        # drawn from the seeded corpus vocabulary (so it stays representative)
        n = (i * step) % corpus_n
        q = f"{TOPICS[i % len(TOPICS)]} session discussed follow ups number {n}"
        t0 = time.monotonic()
        mem.search(q, user_id="bench", **search_kw)
        lat.append((time.monotonic() - t0) * 1000)
    hits = []
    warm_q = f"{TOPICS[0]} session discussed follow ups number 0"
    for _ in range(50):
        t0 = time.monotonic()
        mem.search(warm_q, user_id="bench", **search_kw)
        hits.append((time.monotonic() - t0) * 1000)
    return {"n": queries, "p50": round(pctl(lat, .5), 3), "p95": round(pctl(lat, .95), 3),
            "p99": round(pctl(lat, .99), 3),
            "cache_hit_p50": round(pctl(hits, .5), 3),
            "cache_hit_p99": round(pctl(hits, .99), 3)}


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
        r = bench_retrieval(mem, args.queries, corpus_n=args.events)
        # the previous defaults, for comparison (not graded)
        r_old = bench_retrieval(mem, args.queries, corpus_n=args.events, budget_tokens=2000, packing="flat")

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
            "retrieval_2k_flat": r_old,
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
