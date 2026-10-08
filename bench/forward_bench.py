"""Write forwarding: what a write costs when another process holds its
namespace, against the same write made by the holder itself.

One holder process opens the namespace and serves forwarded calls; this
process then writes - first as the holder (the holder process is not
started yet), then as a forwarder to it. Per row: the write ack latency of
one writer thread (p50/p90/p99), and the throughput of 1/4/8 writer
threads. Plus a strong search through forwarding against a local one.

    python bench/forward_bench.py [--n 2000] [--s3]     # --s3: MEMD_TEST_S3_ENDPOINT (MinIO)

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

NS = "bench"
TEXT = "benchmark write event {i} with realistic payload text about deploys and caches"

HOLDER = r"""
import json, sys
sys.path.insert(0, sys.argv[3])
from memd.engine.memory import Memory
m = Memory(sys.argv[1], namespace="bench", config=json.loads(sys.argv[2]))
print("ready", flush=True)
sys.stdin.read()          # until the bench closes our stdin
m.close()
"""


def pctl(xs: list[float], p: float) -> float:
    xs = sorted(xs)
    return xs[min(int(p * (len(xs) - 1)), len(xs) - 1)]


def latency(mem: Memory, n: int, tag: str) -> dict:
    lat = []
    for i in range(n):
        t0 = time.perf_counter()
        mem.add(TEXT.format(i=f"{tag}-{i}"), user_id="bench", namespace=NS)
        lat.append((time.perf_counter() - t0) * 1000)
    return {"p50": round(pctl(lat, .5), 2), "p90": round(pctl(lat, .9), 2),
            "p99": round(pctl(lat, .99), 2)}


def throughput(mem: Memory, threads: int, seconds: float, tag: str) -> float:
    stop = time.monotonic() + seconds
    counts = [0] * threads

    def run(k: int) -> None:
        i = 0
        while time.monotonic() < stop:
            mem.add(TEXT.format(i=f"{tag}-{k}-{i}"), user_id="bench", namespace=NS)
            i += 1
        counts[k] = i
    ts = [threading.Thread(target=run, args=(k,)) for k in range(threads)]
    t0 = time.monotonic()
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    return round(sum(counts) / (time.monotonic() - t0), 1)


def search_latency(mem: Memory, n: int) -> dict:
    lat = []
    for i in range(n):
        t0 = time.perf_counter()
        mem.search(f"deploys caches {i}", user_id="bench", namespace=NS)   # distinct: no cache hit
        lat.append((time.perf_counter() - t0) * 1000)
    return {"p50": round(pctl(lat, .5), 2), "p99": round(pctl(lat, .99), 2)}


def row(mem: Memory, n: int, seconds: float, tag: str) -> dict:
    out = {"write_ms": latency(mem, n, tag)}
    for t in (1, 4, 8):
        out[f"writes_per_s_{t}"] = throughput(mem, t, seconds, f"{tag}-t{t}")
    out["search_ms"] = search_latency(mem, min(n, 300))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=2000)
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
    results = {}
    try:
        # 1. this process holds the namespace (S3: with the local dir - and
        # the data key - the holder process uses next)
        mem = Memory(root, namespace=NS, config=dict(cfg, local_dir=os.path.join(tmp, "local-h")))
        results["local"] = row(mem, args.n, args.seconds, "local")
        mem.close()
        # 2. another process holds it; this one forwards
        src = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
        holder = subprocess.Popen(
            [sys.executable, "-c", HOLDER, root, json.dumps(dict(cfg, local_dir=os.path.join(tmp, "local-h"))),
             src], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        assert holder.stdout.readline().strip() == "ready"
        mem = Memory(root, namespace=NS, config=dict(cfg, local_dir=os.path.join(tmp, "local-b")))
        assert mem.ns is None, "the holder process should hold the namespace"
        results["forwarded"] = row(mem, args.n, args.seconds, "fwd")
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
