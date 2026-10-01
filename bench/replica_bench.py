#!/usr/bin/env python
"""Read replicas, measured on real node processes.

Three `memd serve --http --node-id` processes on one MinIO bucket with a
moto KMS (the multi-node setup), one hot namespace:

  lag         leader ack -> first eventual GET of the record that a
              non-owner node's replica serves (200). Writes go straight to
              the leader one at a time, at a random phase against the
              refresh cycle; every replica node polls every 10 ms.
  throughput  eventual searches against the hot namespace from a fixed
              pool of client threads in separate processes, spread over 1
              (the leader alone), 2 or 3 serving nodes: requests/s and
              latency per configuration.
  leader      the leader's write-ack and search latency with no replica
              open, then with the other nodes' replicas attached (refreshing),
              then with them serving a read load.

    MEMD_TEST_S3_ENDPOINT=http://127.0.0.1:9310 python bench/replica_bench.py \\
        [--writes 200] [--records 2000] [--seconds 20] [--clients 12] [--refresh 2]

A loopback MinIO and every process on one machine: the absolute latencies
are a floor, and the throughput ceiling is this machine's cores, shared by
the nodes, MinIO and the load generators (reported with the result).
"""
from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import random
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "src")
ENDPOINT = os.environ.get("MEMD_TEST_S3_ENDPOINT")
BUCKET = os.environ.get("MEMD_TEST_S3_BUCKET", "memd-bench")
KEY = os.environ.get("MEMD_TEST_S3_KEY", "minioadmin")
SECRET = os.environ.get("MEMD_TEST_S3_SECRET", "minioadmin")
ADMIN = "memd-admin-" + uuid.uuid4().hex
H = {"Authorization": f"Bearer {ADMIN}"}
EV = dict(H, **{"X-Memd-Read-Consistency": "eventual"})


def pctl(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p * len(xs)))] if xs else float("nan")


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Fleet:
    def __init__(self, tmp: str, kms_url: str, arn: str, refresh_s: float):
        self.tmp = tmp
        self.prefix = f"rb-{uuid.uuid4().hex[:10]}"
        os.makedirs(os.path.join(tmp, "state"), exist_ok=True)
        self.env = dict(os.environ)
        self.env.update({
            "PYTHONPATH": SRC, "MEMD_DATA": f"s3://{BUCKET}/{self.prefix}",
            "MEMD_S3_ENDPOINT": ENDPOINT, "AWS_ACCESS_KEY_ID": KEY, "AWS_SECRET_ACCESS_KEY": SECRET,
            "AWS_REGION": "us-east-1", "MEMD_KEY_PROVIDER": "aws-kms", "MEMD_KMS_KEY_ID": arn,
            "MEMD_KMS_ENDPOINT": kms_url, "MEMD_LEASE_TTL_S": "10",
            "MEMD_CLUSTER_SECRET": "cluster-" + uuid.uuid4().hex,
            "MEMD_STATE_DIR": os.path.join(tmp, "state"), "MEMD_ADMIN_KEY": ADMIN,
            "MEMD_EMBEDDER": "hash", "MEMD_RERANKER": "none", "MEMD_NS_RATE_LIMIT_PER_MIN": "100000000",
            "MEMD_REPLICA_REFRESH_S": str(refresh_s),
        })
        self.env.pop("MEMD_HOSTED", None)
        self.procs: dict[str, subprocess.Popen] = {}
        self.urls: dict[str, str] = {}

    def start(self, nid: str) -> None:
        port = _free_port()
        env = dict(self.env, MEMD_LOCAL_DIR=os.path.join(self.tmp, f"local-{nid}"))
        log = open(os.path.join(self.tmp, f"{nid}.log"), "ab")
        # the per-key rate limit (600/min per node) would cap every
        # configuration at the same few requests/s: the bench measures the
        # nodes, so it is lifted in the node processes (bench only)
        boot = ("import sys; import memd.server.auth as a; "
                "a.RateLimiter.allow = lambda self, k, n: True; "
                "from memd.cli import main; sys.argv = ['memd'] + sys.argv[1:]; sys.exit(main())")
        self.procs[nid] = subprocess.Popen(
            [sys.executable, "-c", boot, "serve", "--http", "--node-id", nid, "--port", str(port)],
            env=env, stdout=log, stderr=subprocess.STDOUT)
        url = f"http://127.0.0.1:{port}"
        self.urls[nid] = url
        import httpx

        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                if httpx.get(f"{url}/health", timeout=1).status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.2)
        raise SystemExit(f"node {nid} did not start; see {self.tmp}/{nid}.log")

    def owner(self, s3, ns: str) -> str | None:
        try:
            body = s3.get_object(Bucket=BUCKET, Key=f"{self.prefix}/ns/{ns}/.owner")["Body"].read()
        except Exception:
            return None
        return body.decode().split("\n", 1)[0].split("@", 1)[0]

    def close(self) -> None:
        for p in self.procs.values():
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
        for p in self.procs.values():
            try:
                p.wait(timeout=30)
            except subprocess.TimeoutExpired:
                p.kill()


# ------------------------------------------------------------------ lag


def measure_lag(fleet: Fleet, ns: str, owner: str, others: list[str], writes: int,
                refresh_s: float) -> dict:
    import httpx

    lags: dict[str, list[float]] = {n: [] for n in others}
    misses = 0
    with httpx.Client(timeout=30) as c:
        for i in range(writes):
            time.sleep(random.uniform(0, refresh_s))       # a random phase
            r = c.post(f"{fleet.urls[owner]}/v1/ns/{ns}/memories",
                       json={"content": f"lag probe {i} {uuid.uuid4().hex[:8]}"}, headers=H)
            t_ack = time.monotonic()
            assert r.status_code == 201, r.text
            rid = r.json()["id"]
            done: dict[str, float] = {}

            def poll(nid: str) -> None:
                with httpx.Client(timeout=30) as pc:
                    while time.monotonic() - t_ack < 30:
                        g = pc.get(f"{fleet.urls[nid]}/v1/ns/{ns}/memories/{rid}", headers=EV)
                        if g.status_code == 200:
                            if g.headers.get("X-Memd-Served-By") != "replica":
                                raise AssertionError(f"{nid} did not serve from its replica")
                            done[nid] = time.monotonic() - t_ack
                            return
                        time.sleep(0.01)

            ts = [threading.Thread(target=poll, args=(n,)) for n in others]
            for t in ts:
                t.start()
            for t in ts:
                t.join()
            for n in others:
                if n in done:
                    lags[n].append(done[n] * 1000)
                else:
                    misses += 1
    every = [x for v in lags.values() for x in v]
    return {"samples": len(every), "misses": misses,
            "p50_ms": round(pctl(every, 0.50), 1), "p90_ms": round(pctl(every, 0.90), 1),
            "p99_ms": round(pctl(every, 0.99), 1), "max_ms": round(max(every), 1),
            "mean_ms": round(statistics.mean(every), 1)}


# ------------------------------------------------------------ throughput

QUERIES = ["how do we deploy", "who owns the billing service", "where is the staging api",
           "what did we decide about retries", "which region is primary", "rollback procedure",
           "on call rotation", "database migration plan"]


def _client_proc(urls: list[str], ns: str, seconds: float, threads: int, eventual: bool, q,
                 admin: str):
    import httpx

    # a child process re-imports this module (forkserver/spawn): ADMIN there
    # is another random value - use the parent's
    h = {"Authorization": f"Bearer {admin}"}
    ev = dict(h, **{"X-Memd-Read-Consistency": "eventual"})

    out: list[float] = []
    errs = [0]
    served = {"replica": 0, "leader": 0}
    lock = threading.Lock()
    stop_at = time.monotonic() + seconds

    def run(k: int) -> None:
        url = urls[k % len(urls)]
        hdr = ev if eventual else h
        with httpx.Client(timeout=30) as c:
            while time.monotonic() < stop_at:
                t0 = time.monotonic()
                try:
                    r = c.post(f"{url}/v1/ns/{ns}/search", headers=hdr,
                               json={"query": random.choice(QUERIES)})
                    ok = r.status_code == 200
                except Exception:
                    ok = False
                dt = (time.monotonic() - t0) * 1000
                with lock:
                    if ok:
                        out.append(dt)
                        served[r.headers.get("X-Memd-Served-By", "leader")] += 1
                    else:
                        errs[0] += 1

    base = os.getpid()
    ts = [threading.Thread(target=run, args=(base + i,)) for i in range(threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    q.put((out, errs[0], served))


def measure_throughput(urls: list[str], ns: str, seconds: float, clients: int, eventual: bool) -> dict:
    procs = max(1, min(4, clients // 3))
    per = max(1, clients // procs)
    q: mp.Queue = mp.Queue()
    ps = [mp.Process(target=_client_proc, args=(urls, ns, seconds, per, eventual, q, ADMIN))
          for _ in range(procs)]
    for p in ps:
        p.start()
    lat: list[float] = []
    errs = 0
    served = {"replica": 0, "leader": 0}
    for _ in ps:
        o, e, s = q.get()
        lat += o
        errs += e
        for k, v in s.items():
            served[k] = served.get(k, 0) + v
    for p in ps:
        p.join()
    return {"nodes": len(urls), "clients": procs * per, "req_per_s": round(len(lat) / seconds, 1),
            "p50_ms": round(pctl(lat, 0.5), 2), "p99_ms": round(pctl(lat, 0.99), 2), "errors": errs,
            "served_by": served}


# ----------------------------------------------------------------- leader


def measure_leader(url: str, ns: str, n: int) -> dict:
    import httpx

    w, s = [], []
    with httpx.Client(timeout=30) as c:
        for i in range(n):
            t0 = time.monotonic()
            r = c.post(f"{url}/v1/ns/{ns}/memories", json={"content": f"leader latency {i} {uuid.uuid4().hex}"},
                       headers=H)
            w.append((time.monotonic() - t0) * 1000)
            assert r.status_code == 201
            t0 = time.monotonic()
            r = c.post(f"{url}/v1/ns/{ns}/search", json={"query": random.choice(QUERIES)}, headers=H)
            s.append((time.monotonic() - t0) * 1000)
            assert r.status_code == 200
    return {"write_p50_ms": round(pctl(w, .5), 2), "write_p99_ms": round(pctl(w, .99), 2),
            "search_p50_ms": round(pctl(s, .5), 2), "search_p99_ms": round(pctl(s, .99), 2), "n": n}


def _background_reads(urls, ns, stop: threading.Event):
    import httpx

    def run(url):
        with httpx.Client(timeout=30) as c:
            while not stop.is_set():
                try:
                    c.post(f"{url}/v1/ns/{ns}/search", headers=EV, json={"query": random.choice(QUERIES)})
                except Exception:
                    pass

    ts = [threading.Thread(target=run, args=(u,), daemon=True) for u in urls for _ in range(2)]
    for t in ts:
        t.start()
    return ts


# ------------------------------------------------------------------- main


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--writes", type=int, default=200)
    ap.add_argument("--records", type=int, default=2000)
    ap.add_argument("--seconds", type=float, default=20.0)
    ap.add_argument("--clients", type=int, default=12)
    ap.add_argument("--refresh", type=float, default=2.0)
    ap.add_argument("--leader-n", type=int, default=300)
    ap.add_argument("--only", default="lag,throughput,leader")
    args = ap.parse_args()
    if not ENDPOINT:
        raise SystemExit("set MEMD_TEST_S3_ENDPOINT")
    import boto3
    import httpx
    from moto.server import ThreadedMotoServer

    s3 = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=KEY, aws_secret_access_key=SECRET,
                      region_name="us-east-1")
    try:
        s3.create_bucket(Bucket=BUCKET)
    except Exception:
        pass
    port = _free_port()
    kms = ThreadedMotoServer(ip_address="127.0.0.1", port=port, verbose=False)
    kms.start()
    kms_url = f"http://127.0.0.1:{port}"
    arn = boto3.client("kms", endpoint_url=kms_url, region_name="us-east-1", aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET).create_key(Description="bench")["KeyMetadata"]["Arn"]
    tmp = tempfile.mkdtemp(prefix="memd-replica-bench-")
    fleet = Fleet(tmp, kms_url, arn, args.refresh)
    results: dict = {"setup": {"cores": os.cpu_count(), "refresh_s": args.refresh, "nodes": 3,
                               "store": "MinIO on loopback", "kms": "moto server", "embedder": "hash"}}
    try:
        for nid in ("n1", "n2", "n3"):
            fleet.start(nid)
        ns = "hot"
        with httpx.Client(timeout=60) as c:
            batch = []
            for i in range(args.records):
                batch.append({"content": f"{random.choice(QUERIES)} note {i} {uuid.uuid4().hex[:6]}"})
                if len(batch) == 200:
                    r = c.post(f"{fleet.urls['n1']}/v1/ns/{ns}/events", json={"events": batch}, headers=H)
                    assert r.status_code == 202, r.text
                    batch = []
        owner = fleet.owner(s3, ns)
        others = [n for n in ("n1", "n2", "n3") if n != owner]
        results["setup"].update({"records": args.records, "owner": owner})
        print(f"owner {owner}, replicas on {others}", flush=True)
        if "leader" in args.only:
            # before any replica exists
            results["leader_no_replicas"] = measure_leader(fleet.urls[owner], ns, args.leader_n)
            print("leader, no replicas:", results["leader_no_replicas"], flush=True)
        # open the replicas
        for n in others:
            r = httpx.post(f"{fleet.urls[n]}/v1/ns/{ns}/search", headers=EV, json={"query": "x"}, timeout=60)
            assert r.headers.get("X-Memd-Served-By") == "replica", r.headers
        if "lag" in args.only:
            results["lag"] = measure_lag(fleet, ns, owner, others, args.writes, args.refresh)
            print("lag:", results["lag"], flush=True)
        if "leader" in args.only:
            results["leader_replicas_attached"] = measure_leader(fleet.urls[owner], ns, args.leader_n)
            print("leader, 2 replicas attached:", results["leader_replicas_attached"], flush=True)
            stop = threading.Event()
            _background_reads([fleet.urls[n] for n in others], ns, stop)
            time.sleep(2)
            results["leader_replicas_under_read_load"] = measure_leader(fleet.urls[owner], ns, args.leader_n)
            stop.set()
            time.sleep(1)
            print("leader, replicas serving reads:", results["leader_replicas_under_read_load"], flush=True)
        if "throughput" in args.only:
            tp = []
            for k in (1, 2, 3):
                urls = [fleet.urls[owner]] + [fleet.urls[n] for n in others][:k - 1]
                res = measure_throughput(urls, ns, args.seconds, args.clients, eventual=True)
                print("throughput:", res, flush=True)
                tp.append(res)
            results["throughput"] = tp
        print(json.dumps(results, indent=2))
    finally:
        fleet.close()
        kms.stop()


if __name__ == "__main__":
    main()
