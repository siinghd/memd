#!/usr/bin/env python
"""Lexical backend bench: FTS5 vs the tantivy accelerator, through memd.

An earlier measurement compared the two engines bare (index-only, unfiltered).
This measures what a caller gets: FILTERED search (user-scoped, the common
case) through Memory.search and through the bm25 lane alone, at 10K / 50K / 150K
records, plus the durable write ack with each backend (tantivy must not move
it: FTS5 stays on the ack path, tantivy indexes in the background).

One store per size is built with the tantivy backend, then opened once per
backend, so both answer over the same rows (same ids: the top-10 overlap is
exact). Corpus: real LongMemEval turns when available (--corpus, one JSON per
line with "content"), else synthetic. Queries: real LongMemEval questions
when available (--questions), else drawn from the corpus.

Run: python bench/lexical_bench.py [--sizes 10000,50000,150000] [--queries 200]
"""
from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import statistics
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "src"))

from memd.core.schema import Scope  # noqa: E402
from memd.engine.memory import Memory  # noqa: E402
from memd.index.sqlite_index import IndexFilter  # noqa: E402
from memd.query.planner import plan_query  # noqa: E402

# LongMemEval turns, one JSON object per line with "content"
DEFAULT_CORPUS = os.path.join(os.path.expanduser("~"), ".cache", "memd", "longmemeval", "turns.jsonl")
LME_CACHE = os.path.join(os.path.expanduser("~"), ".cache", "memd", "longmemeval",
                         "longmemeval_s_cleaned.json")
WORDS = ("kumquat mango orchard ferry harbor violin rehearsal passport visa dentist invoice "
         "mortgage marathon blister recipe sourdough telescope nebula keyboard firmware bicycle "
         "gardening compost tomato basil espresso grinder thermostat boiler insurance premium "
         "flight layover museum sculpture yoga pilates budget savings concert tickets").split()
FILLER = "the a of and to in we i it was is that for on with this my you have be".split()


def pctl(xs: list[float], p: float) -> float:
    xs = sorted(xs)
    return round(xs[min(int(p * (len(xs) - 1)), len(xs) - 1)], 2)


def summary(lat: list[float]) -> dict:
    return {"p50": pctl(lat, .5), "p90": pctl(lat, .9), "p99": pctl(lat, .99),
            "mean": round(statistics.fmean(lat), 2), "n": len(lat)}


def load_corpus(path: str | None, n: int, seed: int) -> list[str]:
    texts: list[str] = []
    if path and os.path.exists(path):
        with open(path) as f:
            for line in f:
                try:
                    c = json.loads(line).get("content")
                except ValueError:
                    continue
                if c and c.strip():
                    texts.append(c[:4000])
                if len(texts) >= n:
                    break
    if len(texts) < n:
        rng = random.Random(seed)
        while len(texts) < n:
            k = rng.randint(6, 30)
            w = [rng.choice(WORDS) for _ in range(k // 2)] + [rng.choice(FILLER) for _ in range(k)]
            rng.shuffle(w)
            texts.append(" ".join(w))
    return texts[:n]


def load_queries(path: str | None, corpus: list[str], n: int, seed: int) -> list[str]:
    rng = random.Random(seed)
    if path and os.path.exists(path):
        with open(path) as f:
            data = json.load(f)
        qs = sorted({str(q["question"]) for q in data if isinstance(q, dict) and q.get("question")})
        rng.shuffle(qs)
        if len(qs) >= n:
            return qs[:n]
    out = []
    for _ in range(n):
        words = [w for w in rng.choice(corpus).split() if len(w) >= 4]
        out.append(" ".join(rng.sample(words, min(len(words), rng.randint(3, 6)))) or "notes")
    return out


def config(backend: str) -> dict:
    return {"embedder": "hash", "reranker": "none", "lexical_backend": backend,
            "rate_max_writes": 10 ** 9, "dup_max_repeats": 10 ** 9, "vector_selfheal": False}


def build(root: str, texts: list[str], users: int) -> dict:
    mem = Memory(root, encrypt=False, config=config("tantivy"))
    try:
        t0 = time.monotonic()
        for i in range(0, len(texts), 1000):
            mem.add_events([{"content": t, "user_id": f"u{(i + j) % users}",
                             "session_id": f"s{(i + j) // 20}", "t_event": 1_600_000_000_000 + i + j}
                            for j, t in enumerate(texts[i:i + 1000])])
        ingest_s = time.monotonic() - t0
        t1 = time.monotonic()
        mem.flush()
        drain_s = time.monotonic() - t1
        lex = mem.ns.index.lexical
        st = lex.stats() if lex else {}
    finally:
        mem.close()
    size = sum(os.path.getsize(os.path.join(dp, f)) for dp, _d, fs in os.walk(root)
               for f in fs if ".tantivy" in dp)
    return {"ingest_s": round(ingest_s, 1), "tantivy_catchup_after_ingest_s": round(drain_s, 1),
            "tantivy_bytes": size, "tantivy_ready": st.get("ready")}


def measure(root: str, backend: str, queries: list[str], users: int, writes: int) -> dict:
    mem = Memory(root, encrypt=False, config=config(backend))
    try:
        t0 = time.monotonic()
        mem.flush()  # tantivy: rebuilds after the FTS5-only session (see engine)
        open_drain_s = round(time.monotonic() - t0, 1)
        idx = mem.ns.index
        if backend == "tantivy":
            assert idx.lexical is not None and idx.lexical.ready(), "tantivy not serving"
        for q in queries[:20]:  # warm-up (page cache, searcher)
            mem.search(q, user_id="u0", budget_tokens=2001)
        e2e, lane, tops = [], [], []
        for i, q in enumerate(queries):
            user = f"u{i % users}"
            t = time.monotonic()
            mem.search(q, user_id=user)
            e2e.append((time.monotonic() - t) * 1000)
            f = IndexFilter(scope=Scope(user=user), kinds=plan_query(q).kinds)
            t = time.monotonic()
            hits = idx.search_bm25(q, f, limit=plan_query(q).candidate_k)
            lane.append((time.monotonic() - t) * 1000)
            tops.append([h.record.id for h in hits[:10]])
        acks = []
        for i in range(writes):
            t = time.monotonic()
            mem.add(f"bench write {i} about {random.choice(WORDS)}", user_id=f"u{i % users}")
            acks.append((time.monotonic() - t) * 1000)
        batch = []
        for i in range(max(1, writes // 10)):
            t = time.monotonic()
            mem.add_events([{"content": f"batched write {i}.{j} {random.choice(WORDS)}",
                             "user_id": f"u{j % users}"} for j in range(100)])
            batch.append((time.monotonic() - t) * 1000)
        return {"open_drain_s": open_drain_s, "search_ms": summary(e2e), "bm25_lane_ms": summary(lane),
                "add_ack_ms": summary(acks), "add_events_100_ack_ms": summary(batch),
                "_tops": tops}
    finally:
        mem.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="10000,50000,150000")
    ap.add_argument("--queries", type=int, default=200)
    ap.add_argument("--users", type=int, default=20)
    ap.add_argument("--writes", type=int, default=300)
    ap.add_argument("--corpus", default=DEFAULT_CORPUS)
    ap.add_argument("--questions", default=LME_CACHE)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--out")
    args = ap.parse_args()
    sizes = [int(x) for x in args.sizes.split(",") if x]
    corpus = load_corpus(args.corpus, max(sizes), args.seed)
    queries = load_queries(args.questions, corpus, args.queries, args.seed)
    report = {"corpus": args.corpus if os.path.exists(args.corpus or "") else "synthetic",
              "questions": args.questions if os.path.exists(args.questions or "") else "from corpus",
              "users": args.users, "queries": len(queries), "sizes": {}}
    for n in sizes:
        root = tempfile.mkdtemp(prefix=f"memd-lexbench-{n}-")
        try:
            b = build(root, corpus[:n], args.users)
            print(f"[{n}] built: {b}", file=sys.stderr, flush=True)
            res = {}
            for backend in ("fts5", "tantivy"):
                res[backend] = measure(root, backend, queries, args.users, args.writes)
                print(f"[{n}] {backend}: search {res[backend]['search_ms']} "
                      f"lane {res[backend]['bm25_lane_ms']} ack {res[backend]['add_ack_ms']}",
                      file=sys.stderr, flush=True)
            ov = [len(set(a) & set(b_)) / max(1, len(a)) for a, b_ in
                  zip(res["fts5"].pop("_tops"), res["tantivy"].pop("_tops")) if a]
            report["sizes"][n] = {"build": b, **res,
                                  "top10_overlap_mean": round(statistics.fmean(ov), 3) if ov else None}
        finally:
            shutil.rmtree(root, ignore_errors=True)
    print(json.dumps(report, indent=1))
    if args.out:
        with open(args.out, "w") as f:
            json.dump(report, f, indent=1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
