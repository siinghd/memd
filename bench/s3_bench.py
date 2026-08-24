#!/usr/bin/env python
"""Hosted-path SLOs against a real S3 API.

02-slos.md states hosted numbers (durable write ack p50<=150ms/p90<=300ms,
warm retrieval p50<=100ms/p99<=400ms, cold first query p90<=1.5s) that were
UNEVIDENCED by construction until an S3 backend existed - LocalObjectStore was
the only implementation.

Read the output with the caveat stated in it: an S3 server on localhost is not
AWS across a WAN. What this measures honestly is the SHAPE of the hosted path -
how many round trips a write and a read cost, and how the engine behaves when
the source of truth is remote. The absolute latencies are a floor, not a
forecast.

    docker run -d -p 9000:9000 -e MINIO_ROOT_USER=minioadmin \
      -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
    MEMD_TEST_S3_ENDPOINT=http://127.0.0.1:9000 python bench/s3_bench.py
"""
from __future__ import annotations

import json
import os
import shutil
import statistics
import sys
import tempfile
import time
import uuid

sys.path.insert(0, "src")

from memd.engine.memory import Memory  # noqa: E402
from memd.storage.objectstore import count_io  # noqa: E402

ENDPOINT = os.environ.get("MEMD_TEST_S3_ENDPOINT")
BUCKET = os.environ.get("MEMD_TEST_S3_BUCKET", "memd-bench")
KEY = os.environ.get("MEMD_TEST_S3_KEY", "minioadmin")
SECRET = os.environ.get("MEMD_TEST_S3_SECRET", "minioadmin")


def pctl(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p * len(xs)))]


def main() -> None:
    if not ENDPOINT:
        print("set MEMD_TEST_S3_ENDPOINT (e.g. http://127.0.0.1:9000)", file=sys.stderr)
        raise SystemExit(2)
    import boto3

    c = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET, region_name="us-east-1")
    try:
        c.create_bucket(Bucket=BUCKET)
    except Exception:
        pass

    prefix = f"bench-{uuid.uuid4().hex[:8]}"
    local = tempfile.mkdtemp(prefix="memd-s3-bench-")
    cfg = {"s3_endpoint_url": ENDPOINT, "s3_access_key": KEY, "s3_secret_key": SECRET,
           "s3_region": "us-east-1", "local_dir": local, "rate_max_writes": 10 ** 9}
    out: dict = {"endpoint": ENDPOINT, "note": "localhost S3 server; NOT a WAN measurement"}

    m = Memory(f"s3://{BUCKET}/{prefix}", encrypt=False, config=dict(cfg))
    try:
        # seed
        t0 = time.monotonic()
        for i in range(0, 2000, 200):
            m.add_events([{"content": f"Notes from standup {i + j}: discussed deploy "
                                      f"and agreed follow ups number {i + j}.",
                           "user_id": "bench", "session_id": f"s{(i + j) % 20}"}
                          for j in range(200)])
        m.flush()
        out["seed_s"] = round(time.monotonic() - t0, 2)

        # durable write ack: one PUT per append on this backend
        acks = []
        for i in range(120):
            t0 = time.monotonic()
            m.add(f"an ordinary later turn {i}", user_id="bench")
            acks.append((time.monotonic() - t0) * 1000)
        out["write_ack"] = {"n": len(acks), "p50": round(pctl(acks, .5), 2),
                            "p90": round(pctl(acks, .9), 2), "p99": round(pctl(acks, .99), 2)}

        # I/O shape per operation - the part that does not change on a WAN
        with count_io() as w:
            m.add("io probe write", user_id="bench")
        m.flush()
        with count_io() as r:
            m.search("standup deploy follow ups number 7", user_id="bench")
        out["io_per_write"] = dict(w)
        out["io_per_warm_search"] = dict(r)

        # warm retrieval (served by the local derived index)
        lat = []
        for i in range(200):
            q = f"standup discussed follow ups number {i * 7 % 2000}"
            t0 = time.monotonic()
            m.search(q, user_id="bench")
            lat.append((time.monotonic() - t0) * 1000)
        out["retrieval"] = {"n": len(lat), "p50": round(pctl(lat, .5), 2),
                            "p95": round(pctl(lat, .95), 2), "p99": round(pctl(lat, .99), 2)}
        m.compact(force=True)
    finally:
        m.close()

    # cold node: bucket intact, local cache gone
    shutil.rmtree(local, ignore_errors=True)
    cold_dir = tempfile.mkdtemp(prefix="memd-s3-cold-")
    t0 = time.monotonic()
    m2 = Memory(f"s3://{BUCKET}/{prefix}", encrypt=False,
                config=dict(cfg, local_dir=cold_dir))
    open_ms = (time.monotonic() - t0) * 1000
    try:
        t0 = time.monotonic()
        hits = m2.search("standup deploy", user_id="bench")
        first_q = (time.monotonic() - t0) * 1000
        out["cold_node"] = {"open_ms": round(open_ms, 1),
                            "first_query_ms": round(first_q, 1),
                            "open_plus_first_query_ms": round(open_ms + first_q, 1),
                            "records_visible": m2.ns.index.stats()["records"],
                            "hits": len(hits.items)}
    finally:
        m2.close()

    out["slo_reference"] = {
        "hosted_write_ack_p50_ms": 150, "hosted_write_ack_p90_ms": 300,
        "hosted_retrieval_p50_ms": 100, "hosted_retrieval_p99_ms": 400,
        "cold_first_query_p90_ms": 1500,
    }
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
