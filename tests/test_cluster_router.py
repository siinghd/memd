"""ADR-12: several memd nodes (real processes) on ONE bucket, with KMS keys.

What is pinned here, end to end over HTTP:
  - any node serves any namespace: the router proxies a request to the
    node holding the namespace's lease (or hashes a free one to a live
    node), and the data keys are unwrapped by whichever node serves it;
  - failover: SIGKILL the leaseholder and another node takes the namespace
    over within TTL + epsilon, with every ACKED write still readable
    (an oracle journal of acks, as in tests/fuzz);
  - graceful handoff: SIGTERM releases the lease, so a peer takes over
    without waiting out the TTL;
  - no split brain: a leaseholder frozen (SIGSTOP) past its TTL and then
    resumed cannot land a write in the new owner's log - what the new owner
    serves is exactly what a cold replay of the bucket finds;
  - hosted billing through the router: usage is metered once, by the node
    that executed the request, however many hops it took.

Needs MEMD_TEST_S3_ENDPOINT (MinIO) and moto[server] (a KMS every node
process shares); skipped otherwise.
"""
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid

import pytest

pytestmark = pytest.mark.s3

ENDPOINT = os.environ.get("MEMD_TEST_S3_ENDPOINT")
BUCKET = os.environ.get("MEMD_TEST_S3_BUCKET", "memd-engine")
KEY = os.environ.get("MEMD_TEST_S3_KEY", "minioadmin")
SECRET = os.environ.get("MEMD_TEST_S3_SECRET", "minioadmin")
if not ENDPOINT:
    pytest.skip("no S3 endpoint (set MEMD_TEST_S3_ENDPOINT)", allow_module_level=True)
if not hasattr(signal, "SIGSTOP"):
    pytest.skip("needs POSIX signals", allow_module_level=True)
boto3 = pytest.importorskip("boto3")
pytest.importorskip("moto.server")
httpx = pytest.importorskip("httpx")

SRC = os.path.join(os.path.dirname(__file__), "..", "src")
TTL = 4.0
ADMIN = "memd-admin-" + uuid.uuid4().hex
EPSILON = 3.0      # routing retry cadence + reclaim + cold open from the bucket


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture(scope="module")
def kms():
    """One moto server = one KMS all node processes share."""
    from moto.server import ThreadedMotoServer

    port = _free_port()
    srv = ThreadedMotoServer(ip_address="127.0.0.1", port=port, verbose=False)
    srv.start()
    url = f"http://127.0.0.1:{port}"
    c = boto3.client("kms", endpoint_url=url, region_name="us-east-1",
                     aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    arn = c.create_key(Description="memd cluster test")["KeyMetadata"]["Arn"]
    try:
        yield url, arn
    finally:
        srv.stop()


@pytest.fixture(scope="module")
def s3():
    c = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET, region_name="us-east-1")
    try:
        c.create_bucket(Bucket=BUCKET)
    except Exception:
        pass
    return c


class Fleet:
    """N `memd serve --http --node-id` processes on one s3:// prefix."""

    def __init__(self, tmp_path, s3, kms, *, hosted: bool = False, extra_env: dict | None = None):
        self.tmp = tmp_path
        self.s3 = s3
        self.prefix = f"cl-{uuid.uuid4().hex[:10]}"
        self.state = tmp_path / "state"
        self.state.mkdir()
        self.env = dict(os.environ)
        self.env.update({
            "PYTHONPATH": SRC, "MEMD_DATA": f"s3://{BUCKET}/{self.prefix}",
            "MEMD_S3_ENDPOINT": ENDPOINT, "AWS_ACCESS_KEY_ID": KEY,
            "AWS_SECRET_ACCESS_KEY": SECRET, "AWS_REGION": "us-east-1",
            "MEMD_KEY_PROVIDER": "aws-kms", "MEMD_KMS_KEY_ID": kms[1], "MEMD_KMS_ENDPOINT": kms[0],
            "MEMD_LEASE_TTL_S": str(TTL), "MEMD_CLUSTER_SECRET": "cluster-" + uuid.uuid4().hex,
            "MEMD_STATE_DIR": str(self.state), "MEMD_ADMIN_KEY": ADMIN, "MEMD_EMBEDDER": "hash",
            "MEMD_ROUTE_RETRY_S": "3", "MEMD_SHUTDOWN_GRACE_S": "10",
            "MEMD_NS_RATE_LIMIT_PER_MIN": "1000000",
        })
        self.env.pop("MEMD_HOSTED", None)
        if hosted:
            self.env["MEMD_HOSTED"] = "1"
        self.env.update(extra_env or {})
        self.procs: dict[str, subprocess.Popen] = {}
        self.urls: dict[str, str] = {}
        self.logs: dict[str, str] = {}

    def start(self, nid: str) -> str:
        port = _free_port()
        env = dict(self.env, MEMD_LOCAL_DIR=str(self.tmp / f"local-{nid}"))
        log = str(self.tmp / f"{nid}.log")
        self.logs[nid] = log
        self.procs[nid] = subprocess.Popen(
            [sys.executable, "-m", "memd.cli", "serve", "--http", "--node-id", nid,
             "--port", str(port)], env=env, stdout=open(log, "ab"), stderr=subprocess.STDOUT)
        url = f"http://127.0.0.1:{port}"
        self.urls[nid] = url
        deadline = time.time() + 60
        while time.time() < deadline:
            if self.procs[nid].poll() is not None:
                raise AssertionError(f"node {nid} exited:\n{open(log).read()[-3000:]}")
            try:
                if httpx.get(f"{url}/health", timeout=1).status_code == 200:
                    return url
            except httpx.HTTPError:
                pass
            time.sleep(0.2)
        raise AssertionError(f"node {nid} did not come up:\n{open(log).read()[-3000:]}")

    def send(self, nid: str, sig) -> None:
        self.procs[nid].send_signal(sig)

    def stop(self, nid: str, sig=signal.SIGTERM, wait: bool = True) -> None:
        p = self.procs.get(nid)
        if p is None or p.poll() is not None:
            return
        p.send_signal(sig)
        if wait:
            try:
                p.wait(timeout=30)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait(timeout=10)

    def close(self) -> None:
        for nid, p in self.procs.items():
            if p.poll() is None:
                try:
                    p.send_signal(signal.SIGCONT)
                except Exception:
                    pass
                p.terminate()
        for p in self.procs.values():
            try:
                p.wait(timeout=30)
            except subprocess.TimeoutExpired:
                p.kill()

    def owner(self, ns: str) -> str | None:
        """The node holding `ns`'s lease, read straight from the bucket."""
        try:
            body = self.s3.get_object(Bucket=BUCKET, Key=f"{self.prefix}/ns/{ns}/.owner")["Body"].read()
        except Exception:
            return None
        holder = body.decode().split("\n", 1)[0]
        return holder.split("@", 1)[0] if "@" in holder else holder

    def log_tail(self) -> str:
        return "\n".join(f"--- {n}\n{open(p).read()[-2500:]}" for n, p in self.logs.items())


@pytest.fixture()
def fleet_factory(tmp_path, s3, kms):
    made = []

    def make(**kw):
        f = Fleet(tmp_path / f"f{len(made)}", s3, kms, **kw) if made else Fleet(tmp_path, s3, kms, **kw)
        made.append(f)
        return f
    yield make
    for f in made:
        f.close()


H = {"Authorization": f"Bearer {ADMIN}"}


def _remember(url: str, ns: str, content: str, headers=H, timeout=30.0):
    return httpx.post(f"{url}/v1/ns/{ns}/memories", json={"content": content}, headers=headers,
                      timeout=timeout)


def _get(url: str, ns: str, rid: str, headers=H):
    return httpx.get(f"{url}/v1/ns/{ns}/memories/{rid}", headers=headers, timeout=30)


class Writer(threading.Thread):
    """Writes unique facts through one entry node and journals every ACK
    (the oracle: an acked write must survive whatever happens next)."""

    def __init__(self, url: str, ns: str, tag: str, pause: float = 0.05):
        super().__init__(daemon=True)
        self.url, self.ns, self.tag, self.pause = url, ns, tag, pause
        self.acked: list[tuple[str, str, float]] = []
        self.failed: list[tuple[int, float]] = []
        self.sent_at: dict[str, float] = {}
        self.stop = threading.Event()

    def run(self):
        i = 0
        with httpx.Client(timeout=60.0) as c:
            while not self.stop.is_set():
                i += 1
                content = f"{self.tag} fact number {i} {uuid.uuid4().hex[:8]}"
                sent = time.monotonic()
                try:
                    r = c.post(f"{self.url}/v1/ns/{self.ns}/memories", json={"content": content}, headers=H)
                    if r.status_code == 201:
                        self.acked.append((r.json()["id"], content, time.monotonic()))
                        self.sent_at[r.json()["id"]] = sent
                    else:
                        self.failed.append((r.status_code, time.monotonic()))
                except httpx.HTTPError:
                    self.failed.append((-1, time.monotonic()))
                time.sleep(self.pause)


def _assert_all_acked_readable(fleet: Fleet, ns: str, acked, nodes):
    """Every acked (id, content) is in each node's view of the namespace:
    one export per node (the reads are routed to the owner either way), plus
    a direct GET of a sample - cheap enough for the per-key rate limit."""
    assert acked, "the writer never got an ack"
    for nid in nodes:
        url = fleet.urls[nid]
        r = httpx.post(f"{url}/v1/ns/{ns}/export", headers=H, timeout=120)
        assert r.status_code == 200, (nid, r.status_code, r.text[:300])
        have = {}
        for line in r.text.splitlines():
            if line.strip():
                rec = json.loads(line)
                have[rec["id"]] = rec["content"]
        missing = [(rid, c) for rid, c, _t in acked if have.get(rid) != c]
        assert not missing, (f"ACKED writes lost (via {nid}): {missing[:5]} of {len(missing)}"
                             f"\n{fleet.log_tail()}")
        for rid, content, _t in acked[:: max(1, len(acked) // 5)]:
            g = _get(url, ns, rid)
            assert g.status_code == 200 and g.json()["content"] == content, (nid, rid, g.status_code)


def _export_ids(url: str, ns: str) -> set[str]:
    r = httpx.post(f"{url}/v1/ns/{ns}/export", headers=H, timeout=60)
    assert r.status_code == 200, r.text
    return {json.loads(line)["id"] for line in r.text.splitlines() if line.strip()}


# ----------------------------------------------------------------- routing


def test_any_node_serves_any_namespace(fleet_factory):
    f = fleet_factory()
    nodes = ["n1", "n2", "n3"]
    for n in nodes:
        f.start(n)
    nss = [f"tenant{i}" for i in range(9)]
    ids = {}
    for i, ns in enumerate(nss):
        r = _remember(f.urls[nodes[i % 3]], ns, f"{ns} deploys with make ship on fridays")
        assert r.status_code == 201, (r.status_code, r.text, f.log_tail())
        ids[ns] = r.json()["id"]
    for ns in nss:
        for n in nodes:
            r = _get(f.urls[n], ns, ids[ns])
            assert r.status_code == 200 and "make ship" in r.json()["content"], (n, ns, r.text)
    owners = {ns: f.owner(ns) for ns in nss}
    assert set(owners.values()) <= set(nodes), owners
    assert len(set(owners.values())) >= 2, f"rendezvous put everything on one node: {owners}"
    # searches through a non-owner are proxied and see the owner's data
    for ns in nss[:3]:
        other = next(n for n in nodes if n != owners[ns])
        r = httpx.post(f"{f.urls[other]}/v1/ns/{ns}/search", json={"query": "how do we deploy"},
                       headers=H, timeout=30)
        assert r.status_code == 200 and "make ship" in r.json()["packed_context"], r.text
    # the keys: wrapped by KMS, in the bucket - no node holds a local key file
    for ns in nss:
        rec = json.loads(f.s3.get_object(Bucket=BUCKET, Key=f"{f.prefix}/keys/{ns}.dek")["Body"].read())
        assert rec["provider"] == "aws-kms"
    for n in nodes:
        kd = f.tmp / f"local-{n}" / f"node-{n}" / "keys"
        assert not kd.exists() or not [p for p in os.listdir(kd) if p.startswith("ns-tenant")]
        assert (f.tmp / f"local-{n}" / f"node-{n}" / "_cache").is_dir(), "node-local cache not per node" 
    # a node's own facade namespace is never served to a client
    assert httpx.get(f"{f.urls['n1']}/v1/ns/memd-node.n2/stats", headers=H).status_code == 404
    # registry objects exist for every node
    listed = f.s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{f.prefix}/_cluster/nodes/")
    assert {o["Key"].rsplit("/", 1)[1] for o in listed.get("Contents", [])} >= {f"{n}.json" for n in nodes}


def test_a_forged_route_header_is_ignored(fleet_factory):
    f = fleet_factory()
    for n in ("n1", "n2"):
        f.start(n)
    assert _remember(f.urls["n1"], "forge", "the forge namespace").status_code == 201
    owner = f.owner("forge")
    other = "n2" if owner == "n1" else "n1"
    # a client claiming to be a routed peer: the signature does not verify, so
    # the request is routed normally (proxied to the owner), not served here
    bad = dict(H, **{"X-Memd-Route": f"{owner}|{time.time()}|1.2.3.4|deadbeef"})
    r = httpx.post(f"{f.urls[other]}/v1/ns/forge/memories", json={"content": "spoofed hop"},
                   headers=bad, timeout=30)
    assert r.status_code == 201
    assert f.owner("forge") == owner, "a forged header moved the namespace"


# ---------------------------------------------------------------- failover


def _takeover_after(writer: Writer, t0: float, timeout: float) -> float:
    """Seconds from t0 to the first ack of a request SENT after t0 (a
    request already in flight at t0 may still be answered by the old owner)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        after = [t for rid, _c, t in list(writer.acked) if writer.sent_at.get(rid, 0) > t0]
        if after:
            return after[0] - t0
        time.sleep(0.05)
    raise AssertionError("no write was acknowledged after the leaseholder went away")


def test_kill_the_leaseholder_another_node_takes_over_within_ttl(fleet_factory):
    f = fleet_factory()
    nodes = ["n1", "n2", "n3"]
    for n in nodes:
        f.start(n)
    ns = "failover"
    assert _remember(f.urls["n1"], ns, "first fact before any failure").status_code == 201
    owner = f.owner(ns)
    entry = next(n for n in nodes if n != owner)
    w = Writer(f.urls[entry], ns, "fo")
    w.start()
    time.sleep(1.5)
    assert w.acked, f"no steady-state acks through the router: {w.failed[:5]}\n{f.log_tail()}"
    t_kill = time.monotonic()
    f.stop(owner, signal.SIGKILL)
    took = _takeover_after(w, t_kill, TTL + 20)
    time.sleep(1.5)
    w.stop.set()
    w.join(timeout=60)
    new_owner = f.owner(ns)
    print(f"\nFAILOVER(SIGKILL): owner {owner} -> {new_owner} in {took:.2f}s "
          f"(TTL {TTL}s); acked {len(w.acked)}, failed during the gap {len(w.failed)}")
    assert new_owner in nodes and new_owner != owner
    assert took <= TTL + EPSILON, f"takeover took {took:.2f}s > TTL {TTL} + {EPSILON}"
    survivors = [n for n in nodes if n != owner]
    _assert_all_acked_readable(f, ns, w.acked, survivors)


def test_graceful_shutdown_hands_off_without_waiting_for_the_ttl(fleet_factory):
    f = fleet_factory()
    nodes = ["n1", "n2", "n3"]
    for n in nodes:
        f.start(n)
    ns = "handoff"
    assert _remember(f.urls["n2"], ns, "first fact before the handoff").status_code == 201
    owner = f.owner(ns)
    entry = next(n for n in nodes if n != owner)
    w = Writer(f.urls[entry], ns, "ho")
    w.start()
    time.sleep(1.5)
    t_stop = time.monotonic()
    f.stop(owner, signal.SIGTERM, wait=False)
    took = _takeover_after(w, t_stop, TTL + 20)
    f.procs[owner].wait(timeout=60)
    time.sleep(1.0)
    w.stop.set()
    w.join(timeout=60)
    print(f"\nHANDOFF(SIGTERM): owner {owner} -> {f.owner(ns)} in {took:.2f}s; "
          f"acked {len(w.acked)}, failed {len(w.failed)}")
    # uvicorn re-raises the SIGTERM it handled once the shutdown is done
    assert f.procs[owner].returncode in (0, -signal.SIGTERM), f.log_tail()
    assert "Application shutdown complete" in open(f.logs[owner]).read()
    assert took < TTL, f"graceful handoff took {took:.2f}s - the lease was not released"
    reg = f.s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{f.prefix}/_cluster/nodes/{owner}.json")
    assert not reg.get("Contents"), "a gracefully stopped node stayed registered"
    _assert_all_acked_readable(f, ns, w.acked, [n for n in nodes if n != owner])


def test_a_frozen_leaseholder_cannot_split_brain(fleet_factory, kms):
    """SIGSTOP the owner mid-traffic (its in-flight write frozen at an
    arbitrary point), let another node take over and write, then SIGCONT.
    The resumed node must not land anything in the new owner's log: every
    ack from either side survives, and what the new owner serves equals what
    a cold replay of the bucket contains."""
    f = fleet_factory()
    nodes = ["n1", "n2", "n3"]
    for n in nodes:
        f.start(n)
    ns = "splitbrain"
    assert _remember(f.urls["n3"], ns, "first fact before the stall").status_code == 201
    a = f.owner(ns)
    b = next(n for n in nodes if n != a)
    wa = Writer(f.urls[a], ns, "from-a", pause=0.01)    # straight at the owner
    wa.start()
    time.sleep(1.0)
    f.send(a, signal.SIGSTOP)
    t_stop = time.monotonic()
    wb = Writer(f.urls[b], ns, "from-b")
    wb.start()
    took = _takeover_after(wb, t_stop, TTL + 20)
    time.sleep(1.5)                                      # B writes while A is frozen
    f.send(a, signal.SIGCONT)
    time.sleep(3.0)                                      # A resumes, is fenced, routes to B
    wa.stop.set()
    wb.stop.set()
    wa.join(timeout=90)
    wb.join(timeout=90)
    new_owner = f.owner(ns)
    print(f"\nSTALL(SIGSTOP): owner {a} -> {new_owner} in {took:.2f}s; acked A {len(wa.acked)} "
          f"B {len(wb.acked)}; A failures {len(wa.failed)}")
    assert new_owner != a
    acked = wa.acked + wb.acked
    _assert_all_acked_readable(f, ns, acked, nodes)
    served = _export_ids(f.urls[new_owner], ns)
    assert {rid for rid, _c, _t in acked} <= served
    # stop everything and replay the bucket cold: nothing A wrote after the
    # takeover may appear that the new owner was not serving
    for n in nodes:
        f.stop(n)
    from memd.engine.memory import Memory

    kms_url, arn = kms
    c = boto3.client("kms", endpoint_url=kms_url, region_name="us-east-1",
                     aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    cold = Memory(f"s3://{BUCKET}/{f.prefix}", namespace=ns, config={
        "s3_endpoint_url": ENDPOINT, "s3_access_key": KEY, "s3_secret_key": SECRET,
        "s3_region": "us-east-1", "local_dir": str(f.tmp / "cold"), "embedder": "hash",
        "key_provider": "aws-kms", "kms_client": c, "kms_key_id": arn})
    try:
        replayed = {json.loads(line)["id"] for line in cold.export_jsonl(namespace=ns).splitlines()
                    if line.strip()}
    finally:
        cold.close()
    assert replayed == served, (f"cold replay differs from what the owner served: "
                                f"+{len(replayed - served)} -{len(served - replayed)}")


# ------------------------------------------------------ hosted, via router


def test_hosted_usage_is_metered_once_through_the_router(fleet_factory):
    from memd.hosted.store import AdminStore, period_of

    f = fleet_factory(hosted=True)
    st = AdminStore.for_data_root(str(f.state))
    org = st.create_org("acme", plan="free")
    key, _kid = st.create_key(org, "acme-mem", scopes=["memory"])
    st.close()
    nodes = ["n1", "n2", "n3"]
    for n in nodes:
        f.start(n)
    hk = {"Authorization": f"Bearer {key}"}
    writes = searches = 0
    for i in range(12):
        r = _remember(f.urls[nodes[i % 3]], "acme-mem", f"acme fact {i} about the billing run",
                      headers=hk)
        assert r.status_code == 201, (r.status_code, r.text, f.log_tail())
        writes += 1
    for i in range(7):
        r = httpx.post(f"{f.urls[nodes[i % 3]]}/v1/ns/acme-mem/search", json={"query": "billing run"},
                       headers=hk, timeout=30)
        assert r.status_code == 200, r.text
        searches += 1
    # a key of this org cannot reach another namespace through any node
    r = _remember(f.urls["n2"], "someone-else", "nope", headers=hk)
    assert r.status_code == 403
    owner = f.owner("acme-mem")
    assert owner in nodes
    st = AdminStore.for_data_root(str(f.state))
    try:
        period = period_of(time.time())
        assert st.rollup(org, "writes", period) == writes, "writes metered more (or less) than once"
        assert st.rollup(org, "searches", period) == searches, "searches metered more (or less) than once"
        evs = [e for e in st.events(org) if e["meter"] in ("writes", "searches")]
        assert sum(e["quantity"] for e in evs) == writes + searches
        assert not st.reservations(org), "a reservation leaked across nodes"
    finally:
        st.close()
