#!/usr/bin/env python
"""Vector-lane bench: the usearch ANN sidecar vs the exact flat scan.

Per namespace size (default 50K / 200K / 1M vectors), one store is built
with the exact scan (vector_index=flat), then opened with the sidecar
(vector_index=usearch) - which builds it from SQLite, the rebuild path - and
measured:
  - recall@10 of the vector lane against the exact answer (the lane's own
    exact path, streamed from SQLite: the same math as the flat scan without
    its float32 matrix), per query FILTERED to one of `--users` users - the
    common case, as Memory.search calls the lane (IndexFilter with the
    planner's kinds, limit = candidate_k);
  - lane latency p50/p99, and end-to-end Memory.search latency with the
    vector lane fused;
  - build time from SQLite, sidecar file size, process RSS (VmRSS/VmHWM;
    each size runs in its own process);
  - the write path: add_events(100) and add() ack p50/p99 while the embed
    worker applies each batch's vectors to the sidecar, against the same
    with vector_index=flat.
The exact flat scan's lane latency is measured too while its float32 matrix
fits (--flat-max, default 200K: at 1M it is 1.5 GB on its own).

Vectors: --corpus synthetic (default) = unit vectors around 256 random
centers (clustered, like real embeddings); --corpus lme = real LongMemEval
turns (--turns, one JSON per line with "content", ~200K lines) embedded with
the hash embedder (no ONNX), queries = 3-6 words drawn from other turns.
Keeps RSS under --max-rss-gb (default 3): a size whose estimate exceeds it
is skipped and reported as such.

Run: python bench/ann_bench.py [--sizes 50000,200000,1000000] [--corpus synthetic|lme]
"""
from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import statistics
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "src"))

import numpy as np  # noqa: E402

from memd.core.schema import MemoryRecord, Scope  # noqa: E402
from memd.engine.memory import Memory  # noqa: E402
from memd.index.sqlite_index import IndexFilter  # noqa: E402
from memd.query.planner import plan_query  # noqa: E402

DEFAULT_TURNS = "/tmp/claude-1000/prof-corpus/turns.jsonl"
WORDS = ("kumquat mango orchard ferry harbor violin rehearsal passport visa dentist invoice "
         "mortgage marathon blister recipe sourdough telescope nebula keyboard firmware bicycle "
         "gardening compost tomato basil espresso grinder thermostat boiler insurance premium "
         "flight layover museum sculpture yoga pilates budget savings concert tickets").split()
CENTERS = 256
CHUNK = 5000


def pctl(xs: list[float], p: float) -> float:
    xs = sorted(xs)
    return round(xs[min(int(p * (len(xs) - 1)), len(xs) - 1)], 2)


def summary(lat: list[float]) -> dict:
    return {"p50": pctl(lat, .5), "p90": pctl(lat, .9), "p99": pctl(lat, .99),
            "mean": round(statistics.fmean(lat), 2), "n": len(lat)}


def rss_mb() -> dict:
    out = {}
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith(("VmRSS:", "VmHWM:")):
                k, v = line.split(":")
                out[k.strip()] = round(int(v.split()[0]) / 1024, 1)
    return out


EXPANSION_SEARCH = 0  # --expansion-search (0: usearch's default)
OVERFETCH = 4         # --overfetch


def config(vector_index: str, users: int, build_threads: int) -> dict:
    return {"embedder": "hash", "reranker": "none", "lexical_backend": "fts5",
            "vector_index": vector_index, "fuse_vector": True, "ann_build_threads": build_threads,
            "ann_expansion_search": EXPANSION_SEARCH, "ann_overfetch": OVERFETCH,
            "rate_max_writes": 10 ** 9, "dup_max_repeats": 10 ** 9, "vector_selfheal": False,
            "vector_flush_drain_s": 3600.0}


def _centers(seed: int, dim: int) -> np.ndarray:
    return np.random.default_rng(seed).standard_normal((CENTERS, dim)).astype(np.float32)


def synthetic(centers: np.ndarray, n: int, draw: int) -> np.ndarray:
    rng = np.random.default_rng(draw)
    x = centers[rng.integers(0, len(centers), n)] + 0.55 * rng.standard_normal(
        (n, centers.shape[1])).astype(np.float32)
    return x / np.linalg.norm(x, axis=1, keepdims=True)


def turns(path: str, n: int) -> list[str]:
    out = []
    with open(path) as f:
        for line in f:
            try:
                c = json.loads(line).get("content")
            except ValueError:
                continue
            if c and c.strip():
                out.append(c[:4000])
            if len(out) >= n:
                break
    return out


def ingest(root: str, args, n: int) -> dict:
    """Records straight through the namespace store (WAL + index), vectors
    straight into the index: the embed worker is not what is measured."""
    mem = Memory(root, encrypt=False, config=config("flat", args.users, args.build_threads))
    ns, model = mem.ns, mem.embedder.name
    t0 = time.monotonic()
    texts = turns(args.turns, n) if args.corpus == "lme" else None
    if texts is not None and len(texts) < n:
        raise SystemExit(f"--corpus lme has {len(texts)} turns, fewer than {n}")
    centers = _centers(args.seed, args.dim)
    for start in range(0, n, CHUNK):
        m = min(CHUNK, n - start)
        if texts is not None:
            body = texts[start:start + m]
            vecs = mem.embedder.embed(body)
        else:
            body = [f"synthetic record {start + j} {WORDS[(start + j) % len(WORDS)]}" for j in range(m)]
            vecs = synthetic(centers, m, draw=args.seed * 1_000_003 + start)
        recs = [MemoryRecord.create(namespace=ns.namespace, kind="raw_event", content=body[j],
                                    scope=Scope(user=f"u{(start + j) % args.users}"),
                                    session_id=f"s{(start + j) // 20}",
                                    t_event=1_600_000_000_000 + start + j) for j in range(m)]
        ns.append(recs)
        ns.index.set_vectors([r.id for r in recs], vecs, model)
    mem.flush()
    mem.close()
    return {"ingest_s": round(time.monotonic() - t0, 1)}


def queries(args, n_store: int) -> list[tuple[str, np.ndarray | None]]:
    rng = random.Random(args.seed + 7)
    if args.corpus == "lme":
        pool = turns(args.turns, min(n_store + 5000, 250_000))[n_store:] or turns(args.turns, 5000)
        out = []
        while len(out) < args.queries:
            words = [w for w in rng.choice(pool).split() if len(w) >= 4 and w.isalpha()]
            if len(words) >= 3:
                out.append((" ".join(rng.sample(words, min(len(words), rng.randint(3, 6)))), None))
        return out
    qv = synthetic(_centers(args.seed, args.dim), args.queries, draw=args.seed + 99)
    return [(" ".join(rng.sample(WORDS, 4)), qv[i]) for i in range(args.queries)]


def lane(mem: Memory, qs, args, *, exact: bool) -> dict:
    idx = mem.ns.index
    lat, exact_lat, recalls, e2e, tops = [], [], [], [], []
    for i, (text, qv) in enumerate(qs):
        if qv is None:
            qv = mem.embedder.embed_one(text)
        qv = np.asarray(qv, dtype=np.float32)
        qn = qv / (np.linalg.norm(qv) or 1.0)
        plan = plan_query(text)
        f = IndexFilter(scope=Scope(user=f"u{i % args.users}"), kinds=plan.kinds)
        t = time.perf_counter()
        hits = idx.search_vector(qv, f, limit=plan.candidate_k)
        lat.append((time.perf_counter() - t) * 1000)
        top = [h.record.id for h in hits[:10]]
        tops.append(top)
        if exact:
            t = time.perf_counter()
            want = [h.record.id for h in idx._exact_vector(qn, f, 10)]
            exact_lat.append((time.perf_counter() - t) * 1000)
            if want:
                recalls.append(len(set(top) & set(want)) / len(want))
    for i, (text, _qv) in enumerate(qs[:args.e2e]):
        t = time.perf_counter()
        mem.search(text, user_id=f"u{i % args.users}")
        e2e.append((time.perf_counter() - t) * 1000)
    out = {"lane_ms": summary(lat), "search_e2e_ms": summary(e2e), "_tops": tops}
    if exact:
        out["recall_at_10"] = round(statistics.fmean(recalls), 4) if recalls else None
        out["recall_at_10_min"] = round(min(recalls), 2) if recalls else None
        out["exact_streamed_ms"] = summary(exact_lat)
    return out


def writes(mem: Memory, args) -> dict:
    # other users than the queries': the flat pass then answers over the
    # same visible rows as the usearch pass did
    rng = random.Random(args.seed + 3)
    batch, single = [], []
    for b in range(args.writes):
        evs = [{"content": f"bench write {b}.{j} about {rng.choice(WORDS)} and {rng.choice(WORDS)}",
                "user_id": f"w{j % args.users}"} for j in range(100)]
        t = time.perf_counter()
        mem.add_events(evs)
        batch.append((time.perf_counter() - t) * 1000)
        t = time.perf_counter()
        mem.add(f"single write {b} {rng.choice(WORDS)}", user_id=f"w{b % args.users}")
        single.append((time.perf_counter() - t) * 1000)
    t = time.perf_counter()
    mem.flush()  # the embed worker (and the sidecar applier) catching up
    return {"add_events_100_ack_ms": summary(batch), "add_ack_ms": summary(single),
            "flush_after_writes_s": round(time.perf_counter() - t, 2)}


def one(args, n: int) -> dict:
    root = tempfile.mkdtemp(prefix=f"memd-annbench-{n}-", dir=args.tmp)
    rep: dict = {"size": n, "corpus": args.corpus}
    try:
        rep["ingest"] = ingest(root, args, n)
        rep["rss_after_ingest_mb"] = rss_mb()
        qs = queries(args, n)
        # --- the sidecar: built from SQLite at open
        t0 = time.monotonic()
        mem = Memory(root, encrypt=False, config=config("usearch", args.users, args.build_threads))
        ann = mem.ns.index.ann
        assert ann is not None and ann.drain(3600) and ann.ready(), "sidecar not serving"
        st = ann.stats()
        sdir = ann.path
        rep["usearch"] = {"build_ms": st["last_build_ms"], "open_to_ready_s": round(time.monotonic() - t0, 1),
                          "size": st["size"], "dtype": st["dtype"],
                          "file_mb": round(sum(os.path.getsize(os.path.join(sdir, f))
                                               for f in os.listdir(sdir)) / 1e6, 1),
                          "rss_after_build_mb": rss_mb()}
        for text, _qv in qs[:10]:  # warm-up
            mem.search(text, user_id="u0")
        res = lane(mem, qs, args, exact=True)
        fb = ann.stats()["fallback_exact_total"]
        rep["usearch"].update({k: v for k, v in res.items() if k != "_tops"})
        rep["usearch"]["fallback_exact_total"] = fb
        rep["usearch"]["writes"] = writes(mem, args)
        rep["usearch"]["rss_after_queries_mb"] = rss_mb()
        t = time.monotonic()
        mem.close()  # saves the sidecar
        rep["usearch"]["close_s"] = round(time.monotonic() - t, 2)
        # --- the exact flat scan (this open drops the sidecar: flat keeps none)
        mem = Memory(root, encrypt=False, config=config("flat", args.users, args.build_threads))
        try:
            flat: dict = {}
            if n <= args.flat_max:
                t = time.monotonic()
                mem.ns.index.search_vector(np.ones(args.dim, dtype=np.float32), IndexFilter(), limit=1)
                flat["matrix_load_s"] = round(time.monotonic() - t, 1)
                fres = lane(mem, qs, args, exact=False)
                flat.update({k: v for k, v in fres.items() if k != "_tops"})
                ov = [len(set(a) & set(b)) / len(b) for a, b in zip(res["_tops"], fres["_tops"]) if b]
                flat["top10_overlap_with_usearch"] = round(statistics.fmean(ov), 4) if ov else None
                flat["rss_after_queries_mb"] = rss_mb()
                mem.ns.index.invalidate_vec_cache()
            else:
                flat["lane"] = f"skipped: its float32 matrix alone is {n * args.dim * 4 / 1e9:.1f} GB"
            flat["writes"] = writes(mem, args)
            rep["flat"] = flat
        finally:
            mem.close()
        rep["rss_peak_mb"] = rss_mb().get("VmHWM")
    finally:
        shutil.rmtree(root, ignore_errors=True)
    return rep


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="50000,200000,1000000")
    ap.add_argument("--corpus", choices=("synthetic", "lme"), default="synthetic")
    ap.add_argument("--turns", default=DEFAULT_TURNS)
    ap.add_argument("--dim", type=int, default=384)
    ap.add_argument("--users", type=int, default=8)
    ap.add_argument("--queries", type=int, default=200)
    ap.add_argument("--e2e", type=int, default=100, help="Memory.search calls timed end to end")
    ap.add_argument("--writes", type=int, default=50, help="add_events(100) batches (and add() calls)")
    ap.add_argument("--flat-max", type=int, default=200_000)
    ap.add_argument("--max-rss-gb", type=float, default=3.0)
    ap.add_argument("--build-threads", type=int, default=4)
    ap.add_argument("--expansion-search", type=int, default=0, help="ann_expansion_search (HNSW ef floor)")
    ap.add_argument("--overfetch", type=int, default=4, help="ann_overfetch")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--tmp", default=None)
    ap.add_argument("--one", type=int, help=argparse.SUPPRESS)
    ap.add_argument("--out")
    args = ap.parse_args()
    if args.corpus == "lme":
        args.dim = 384  # the hash embedder's
    global EXPANSION_SEARCH, OVERFETCH
    EXPANSION_SEARCH, OVERFETCH = args.expansion_search, args.overfetch
    if args.one:
        print(json.dumps(one(args, args.one)))
        return 0
    report = {"corpus": args.corpus, "dim": args.dim, "users": args.users, "queries": args.queries,
              "build_threads": args.build_threads, "expansion_search": args.expansion_search,
              "overfetch": args.overfetch,
              "sizes": {}}
    for n in [int(x) for x in args.sizes.split(",") if x]:
        # sidecar (f16 vectors + HNSW links) + the flat matrix when measured + ~0.4 GB baseline
        est_gb = (n * (args.dim * 2 + 200) + (n * args.dim * 4 if n <= args.flat_max else 0)) / 1e9 + 0.4
        if est_gb > args.max_rss_gb:
            report["sizes"][n] = {"skipped": f"estimated RSS {est_gb:.1f} GB > {args.max_rss_gb} GB"}
            print(f"[{n}] skipped (estimated RSS {est_gb:.1f} GB)", file=sys.stderr, flush=True)
            continue
        passed = []
        skip = False
        for a in sys.argv[1:]:  # everything but --sizes / --out (and their values)
            if skip:
                skip = False
                continue
            if a in ("--sizes", "--out"):
                skip = True
                continue
            if a.startswith(("--sizes=", "--out=")):
                continue
            passed.append(a)
        cmd = [sys.executable, os.path.abspath(__file__), "--one", str(n)] + passed
        t = time.monotonic()
        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.returncode != 0:
            report["sizes"][n] = {"error": p.stderr[-3000:]}
            print(f"[{n}] failed:\n{p.stderr[-3000:]}", file=sys.stderr, flush=True)
            continue
        rep = json.loads(p.stdout.strip().splitlines()[-1])
        rep["wall_s"] = round(time.monotonic() - t, 1)
        report["sizes"][n] = rep
        u, fl = rep["usearch"], rep["flat"]
        print(f"[{n}] recall@10 {u['recall_at_10']} lane {u['lane_ms']} build {u['build_ms']}ms "
              f"rss peak {rep['rss_peak_mb']}MB ack usearch {u['writes']['add_events_100_ack_ms']['p50']} "
              f"flat {fl['writes']['add_events_100_ack_ms']['p50']}", file=sys.stderr, flush=True)
    print(json.dumps(report, indent=1))
    if args.out:
        with open(args.out, "w") as f:
            json.dump(report, f, indent=1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
