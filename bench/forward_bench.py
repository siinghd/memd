"""Write forwarding: what a call costs when another process holds its
namespace, against the same call made by the holder itself.

Setup: a corpus namespace of --corpus records. Then two phases, each
writing into a fresh namespace of its own (so both write into namespaces
of the same size) and reading the same corpus:

  1. local      - this process holds both namespaces;
  2. forwarded  - a holder process holds both, and this process forwards
                  every call to it.

Per phase: the write ack latency of one writer thread (p50/p90/p99), the
throughput of 1/4/8 writer threads, a strong get of an existing record and
a strong search of the corpus (distinct queries: no cache hits).

    python bench/forward_bench.py [--n 2000] [--seconds 5] [--s3]   # --s3: MEMD_TEST_S3_ENDPOINT

Everything runs on one machine: the forwarded hop is loopback TCP.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

from memd.engine.memory import Memory  # noqa: E402

CORPUS = "corpus"
TEXT = "benchmark write event {i} with realistic payload text about deploys and caches"

HOLDER = r"""
import json, sys
sys.path.insert(0, sys.argv[3])
from memd.engine.memory import Memory
m = Memory(sys.argv[1], namespace=sys.argv[4], config=json.loads(sys.argv[2]))
m.stats(namespace="corpus")      # holds the corpus too
print("ready", flush=True)
sys.stdin.read()                 # until the bench closes our stdin
m.close()
"""


def pctl(xs: list[float], p: float) -> float:
    xs = sorted(xs)
    return xs[min(int(p * (len(xs) - 1)), len(xs) - 1)]


def timed(fn, n: int) -> list[float]:
    lat = []
    for i in range(n):
        t0 = time.perf_counter()
        fn(i)
        lat.append((time.perf_counter() - t0) * 1000)
    return lat


def throughput(mem: Memory, ns: str, threads: int, seconds: float, tag: str) -> float:
    stop = time.monotonic() + seconds
    counts = [0] * threads

    def run(k: int) -> None:
        i = 0
        while time.monotonic() < stop:
            mem.add(TEXT.format(i=f"{tag}-{k}-{i}"), user_id="bench", namespace=ns)
            i += 1
        counts[k] = i
    ts = [threading.Thread(target=run, args=(k,)) for k in range(threads)]
    t0 = time.monotonic()
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    return round(sum(counts) / (time.monotonic() - t0), 1)


def phase(mem: Memory, ns: str, n: int, seconds: float, corpus_ids: list[str]) -> dict:
    w = timed(lambda i: mem.add(TEXT.format(i=f"{ns}-{i}"), user_id="bench", namespace=ns), n)
    out = {"write_ms": {"p50": round(pctl(w, .5), 2), "p90": round(pctl(w, .9), 2),
                        "p99": round(pctl(w, .99), 2)}}
    for t in (1, 4, 8):
        out[f"writes_per_s_{t}"] = throughput(mem, ns, t, seconds, f"{ns}-t{t}")
    g = timed(lambda i: mem.get(corpus_ids[i % len(corpus_ids)], namespace=CORPUS), min(n, 1000))
    out["get_ms"] = {"p50": round(pctl(g, .5), 2), "p99": round(pctl(g, .99), 2)}
    s = timed(lambda i: mem.search(f"deploys caches {i}", user_id="bench", namespace=CORPUS), 300)
    out["search_ms"] = {"p50": round(pctl(s, .5), 2), "p99": round(pctl(s, .99), 2)}
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=2000)
    ap.add_argument("--corpus", type=int, default=2000)
    ap.add_argument("--seconds", type=float, default=5.0)
    ap.add_argument("--s3", action="store_true")
    args = ap.parse_args()
    tmp = tempfile.mkdtemp(prefix="memd-fwd-bench-")
    cfg: dict = {"rate_max_writes": 10 ** 9, "embedder": "hash", "reranker": "none"}
    if args.s3:
        ep = os.environ["MEMD_TEST_S3_ENDPOINT"]
        root = f"s3://{os.environ.get('MEMD_TEST_S3_BUCKET', 'memd-engine')}/fwdbench-{uuid.uuid4().hex[:8]}"
        cfg.update(s3_endpoint_url=ep, s3_access_key=os.environ.get("MEMD_TEST_S3_KEY", "minioadmin"),
                   s3_secret_key=os.environ.get("MEMD_TEST_S3_SECRET", "minioadmin"),
                   s3_region="us-east-1", forward_secret="forward-bench-shared-secret-0001")
    else:
        root = os.path.join(tmp, "data")
    # the holder's local dir (and, on S3, its data keys) is the one the
    # local phase writes with; the forwarding process has its own
    held = dict(cfg, local_dir=os.path.join(tmp, "local-h"))
    results = {}
    try:
        mem = Memory(root, namespace=CORPUS, config=held)
        ids = [mem.add(TEXT.format(i=f"corpus-{i}"), user_id="bench")[0] for i in range(args.corpus)]
        mem.flush()
        results["local"] = phase(mem, "w-local", args.n, args.seconds, ids)
        mem.close()
        src = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
        holder = subprocess.Popen([sys.executable, "-c", HOLDER, root, json.dumps(held), src, "w-fwd"],
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        assert holder.stdout.readline().strip() == "ready"
        mem = Memory(root, namespace="w-fwd", config=dict(cfg, local_dir=os.path.join(tmp, "local-b")))
        assert mem.ns is None and not mem.engine.holds(CORPUS), "the holder process should hold both"
        results["forwarded"] = phase(mem, "w-fwd", args.n, args.seconds, ids)
        mem.close()
        holder.stdin.close()
        holder.wait(timeout=60)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    results["backend"] = "s3 (MinIO, loopback)" if args.s3 else "local filesystem"
    results["cpus"] = os.cpu_count()
    print(json.dumps(results, indent=1))


if __name__ == "__main__":
    main()
