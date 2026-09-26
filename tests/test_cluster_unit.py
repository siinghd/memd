"""ADR-12 router and lease pieces, without processes.

Local: rendezvous hashing, lease-holder parsing, route-header signing,
config validation. S3 (MinIO, skipped without MEMD_TEST_S3_ENDPOINT): the
lease compare-and-swap (one winner among racing reclaimers), the takeover
fence (a stalled writer's resumed append conflicts instead of landing in
the new owner's log), the fenced-store self-heal, and a router resolving
against real leases and registry objects.
"""
import os
import threading
import time
import uuid

import pytest

from memd.server.cluster import (ClusterConfig, _sign, namespace_of_path, node_of,
                                 rendezvous)

ENDPOINT = os.environ.get("MEMD_TEST_S3_ENDPOINT")
BUCKET = os.environ.get("MEMD_TEST_S3_BUCKET", "memd-engine")
KEY = os.environ.get("MEMD_TEST_S3_KEY", "minioadmin")
SECRET = os.environ.get("MEMD_TEST_S3_SECRET", "minioadmin")


# ------------------------------------------------------------------- local


def test_rendezvous_moves_only_the_departed_nodes_namespaces():
    nodes = [f"n{i}" for i in range(5)]
    nss = [f"t{i}" for i in range(2000)]
    before = {ns: rendezvous(ns, nodes)[0] for ns in nss}
    after = {ns: rendezvous(ns, [n for n in nodes if n != "n3"])[0] for ns in nss}
    moved = [ns for ns in nss if before[ns] != after[ns]]
    assert moved and all(before[ns] == "n3" for ns in moved)
    counts = {n: sum(1 for v in before.values() if v == n) for n in nodes}
    assert min(counts.values()) > 300, f"badly unbalanced: {counts}"   # ~400 each


def test_lease_holders_map_to_nodes():
    assert node_of("n1@host:12:ab") == "n1"
    assert node_of("host:12") is None            # a non-cluster writer
    assert node_of("../x@y") is None


def test_namespace_of_path():
    assert namespace_of_path("/v1/ns/acme/memories/abc") == "acme"
    assert namespace_of_path("/v1/ns/acme") == "acme"
    assert namespace_of_path("/v1/status") is None
    assert namespace_of_path("/v1/ns/") is None


def test_route_signature_binds_node_time_client_method_and_path():
    s = _sign("k" * 32, "n1", "100.0", "1.2.3.4", "POST", "/v1/ns/a/memories")
    assert s == _sign("k" * 32, "n1", "100.0", "1.2.3.4", "POST", "/v1/ns/a/memories")
    for args in (("n2", "100.0", "1.2.3.4", "POST", "/v1/ns/a/memories"),
                 ("n1", "101.0", "1.2.3.4", "POST", "/v1/ns/a/memories"),
                 ("n1", "100.0", "5.6.7.8", "POST", "/v1/ns/a/memories"),
                 ("n1", "100.0", "1.2.3.4", "DELETE", "/v1/ns/a/memories"),
                 ("n1", "100.0", "1.2.3.4", "POST", "/v1/ns/b/memories")):
        assert _sign("k" * 32, *args) != s
    assert _sign("x" * 32, "n1", "100.0", "1.2.3.4", "POST", "/v1/ns/a/memories") != s


def test_cluster_config_validation():
    ok = ClusterConfig("n1", "http://10.0.0.1:8700", "s" * 32)
    assert ok.holder.startswith("n1@") and ok.node_namespace == "memd-node.n1"
    with pytest.raises(ValueError, match="SECRET"):
        ClusterConfig("n1", "http://10.0.0.1:8700", "short")
    with pytest.raises(ValueError, match="node id"):
        ClusterConfig("../n1", "http://10.0.0.1:8700", "s" * 32)
    with pytest.raises(ValueError, match="advertise"):
        ClusterConfig("n1", "10.0.0.1:8700", "s" * 32)


def test_cluster_mode_refuses_a_local_data_root(tmp_path):
    from memd.server.http import create_app

    cfg = ClusterConfig("n1", "http://127.0.0.1:1", "s" * 32)
    with pytest.raises(ValueError, match="s3://"):
        create_app(str(tmp_path / "d"), cluster=cfg)


class _Scripted:
    """A router that always answers the given decisions in turn."""

    def __init__(self, *decisions):
        self.decisions = list(decisions)
        self.forgotten = 0

    def resolve(self, ns, exclude=frozenset()):
        return self.decisions.pop(0) if len(self.decisions) > 1 else self.decisions[0]

    def forget(self, ns):
        self.forgotten += 1


def _drive(mw, path="/v1/ns/a/memories", body=b"payload", method="POST"):
    import asyncio

    sent = []
    chunks = [{"type": "http.request", "body": body, "more_body": False}]

    async def receive():
        if chunks:
            return chunks.pop(0)
        await asyncio.sleep(3600)

    async def send(msg):
        sent.append(msg)

    scope = {"type": "http", "method": method, "path": path, "raw_path": path.encode(),
             "headers": [], "client": ("1.2.3.4", 5), "query_string": b""}
    asyncio.run(mw(scope, receive, send))
    return [m["status"] for m in sent if m["type"] == "http.response.start"], \
        b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")


def test_a_lost_lease_race_on_the_local_path_is_retried_invisibly():
    """Resolve said "local", but another node won the lease between resolve
    and open: the app's 503 not-owner must never reach the client - the
    request is re-resolved and served (here: locally, on the second try)."""
    from memd.server.cluster import ClusterMiddleware, Decision

    seen = []

    async def app(scope, receive, send):
        seen.append((await receive())["body"])
        if len(seen) == 1:
            await send({"type": "http.response.start", "status": 503,
                        "headers": [(b"x-memd-not-owner", b"1")]})
            await send({"type": "http.response.body", "body": b"not owner"})
            return
        await send({"type": "http.response.start", "status": 201, "headers": []})
        await send({"type": "http.response.body", "body": b"created"})

    router = _Scripted(Decision("local", "n1"))
    mw = ClusterMiddleware(app, router, ClusterConfig("n1", "http://127.0.0.1:1", "s" * 32))
    statuses, body = _drive(mw)
    assert statuses == [201] and body == b"created"
    assert seen == [b"payload", b"payload"], "the body was not replayed to the second attempt"
    assert router.forgotten == 1


def test_middleware_reserved_namespaces_body_cap_and_giving_up():
    from memd.server.cluster import ClusterMiddleware, Decision

    async def app(scope, receive, send):
        raise AssertionError("must not reach the app")

    cfg = ClusterConfig("n1", "http://127.0.0.1:1", "s" * 32, route_retry_s=0.2)
    mw = ClusterMiddleware(app, _Scripted(Decision("wait", reason="test")), cfg, max_body=10)
    assert _drive(mw, path="/v1/ns/memd-node.n2/stats")[0] == [404]
    assert _drive(mw, body=b"x" * 11)[0] == [413]
    statuses, body = _drive(mw, body=b"ok")
    assert statuses == [503] and b"namespace_unavailable" in body


# ---------------------------------------------------------------------- S3


def _s3(prefix=None, **kw):
    if not ENDPOINT:
        pytest.skip("no S3 endpoint (set MEMD_TEST_S3_ENDPOINT)")
    boto3 = pytest.importorskip("boto3")
    from memd.storage.s3store import S3ObjectStore

    c = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET, region_name="us-east-1")
    try:
        c.create_bucket(Bucket=BUCKET)
    except Exception:
        pass
    return S3ObjectStore(bucket=BUCKET, prefix=prefix or f"cu-{uuid.uuid4().hex[:10]}",
                         endpoint_url=ENDPOINT, access_key=KEY, secret_key=SECRET,
                         region="us-east-1", **kw)


@pytest.mark.s3
def test_racing_reclaimers_of_a_stale_lease_have_exactly_one_winner():
    first = _s3(lease_ttl_s=1.0)
    first._raw_put(first._owner_key("ns1"), f"dead:1\n{time.time() - 60}".encode())
    racers = [_s3(prefix=first.prefix, lease_ttl_s=1.0) for _ in range(6)]
    barrier = threading.Barrier(len(racers))
    won: list[int] = []

    def go(i):
        barrier.wait()
        if racers[i].try_acquire_owner("ns1", f"racer-{i}:1"):
            won.append(i)

    ts = [threading.Thread(target=go, args=(i,)) for i in range(len(racers))]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert len(won) == 1, f"{len(won)} racers all believe they own the namespace: {won}"
    holder = first.read_owner("ns1")["holder"]
    assert holder == f"racer-{won[0]}:1"
    racers[won[0]].release_owner("ns1")


@pytest.mark.s3
def test_a_renewal_never_overwrites_a_lease_someone_else_took():
    a = _s3(lease_ttl_s=2.0)
    b = _s3(prefix=a.prefix, lease_ttl_s=2.0)
    assert a.try_acquire_owner("ns1", "node-A:1")
    a._lease_stop.set()                     # A stalls
    time.sleep(2.5)
    assert b.try_acquire_owner("ns1", "node-B:2")
    assert a._renew_one("ns1", "node-A:1") is False     # A wakes and renews: CAS fails
    assert "ns1" in a._fenced
    assert a.read_owner("ns1")["holder"] == "node-B:2", "the stalled holder took the lease back"
    b.release_owner("ns1")


@pytest.mark.s3
def test_takeover_fences_a_stalled_writers_resumed_append(tmp_path):
    """The window self-fencing cannot close: a writer frozen AFTER its lease
    check and before its PUT. The new owner takes the next part number of
    every append log first, so the resumed PUT conflicts - loudly, unacked -
    instead of landing in the new owner's log unseen."""
    from memd.core.schema import Kind, MemoryRecord
    from memd.storage.engine import NamespaceStore

    a = _s3(lease_ttl_s=2.0, lease_holder="node-A@1")     # two "processes"
    b = _s3(prefix=a.prefix, lease_ttl_s=2.0, lease_holder="node-B@2")
    na = NamespaceStore("ns1", a, str(tmp_path / "a"))
    na.append([MemoryRecord.create(namespace="ns1", kind=Kind.FACT, content="written by A")])
    a._lease_stop.set()                                   # A freezes...
    a._check_fence = lambda key: None                     # ...already past its check
    time.sleep(2.5)
    nb = NamespaceStore("ns1", b, str(tmp_path / "b"))    # B reclaims: takeover fence
    with pytest.raises(RuntimeError, match="append conflict"):
        a.append(na.wal_key, b"a frame from the frozen writer")
    nb.append([MemoryRecord.create(namespace="ns1", kind=Kind.FACT, content="written by B")])
    got = {r.content for r in nb._visible_records()}
    assert got == {"written by A", "written by B"}
    nb.close()


@pytest.mark.s3
def test_a_fenced_namespace_is_dropped_and_reopened_not_reused(tmp_path):
    from memd.core.schema import Kind, MemoryRecord
    from memd.storage.engine import NamespaceBusyError, StorageEngine

    a = _s3(lease_ttl_s=2.0, lease_holder="node-A@1")
    b = _s3(prefix=a.prefix, lease_ttl_s=2.0, lease_holder="node-B@2")
    ea = StorageEngine(str(tmp_path / "a"), store=a, cache_dir=str(tmp_path / "a" / "c"))
    ns_a = ea.namespace("ns1")
    ns_a.append([MemoryRecord.create(namespace="ns1", kind=Kind.FACT, content="one")])
    a._lease_stop.set()
    time.sleep(2.5)
    eb = StorageEngine(str(tmp_path / "b"), store=b, cache_dir=str(tmp_path / "b" / "c"))
    eb.namespace("ns1")                                   # B takes it over
    a._renew_one("ns1", a._leases.get("ns1") or "x")     # A notices: fenced
    assert "ns1" in a._fenced
    with pytest.raises(NamespaceBusyError):
        ea.namespace("ns1")                               # the stale store is not handed out
    assert ns_a._closed
    eb.close()
    a._fenced.discard("ns1")
    assert ea.namespace("ns1") is not ns_a                # reopens fresh once B is gone
    ea.close()


@pytest.mark.s3
def test_router_resolves_against_real_leases_and_registry(tmp_path):
    from memd.server.cluster import NodeRegistry, Router

    store = _s3(lease_ttl_s=3.0)
    c1 = ClusterConfig("n1", "http://127.0.0.1:1", "s" * 32, lease_ttl_s=3.0)
    c2 = ClusterConfig("n2", "http://127.0.0.1:2", "s" * 32, lease_ttl_s=3.0)
    r1 = Router(c1, store, NodeRegistry(store, c1))
    reg2 = NodeRegistry(store, c2)
    reg2.start()
    try:
        # a free namespace goes where the hash says
        free = next(ns for ns in (f"t{i}" for i in range(50)) if rendezvous(ns, {"n1", "n2"})[0] == "n2")
        d = r1.resolve(free)
        assert (d.kind, d.node, d.url) == ("proxy", "n2", "http://127.0.0.1:2")
        # a held namespace goes to the holder, whatever the hash says
        mine = next(ns for ns in (f"u{i}" for i in range(50)) if rendezvous(ns, {"n1", "n2"})[0] == "n1")
        other = _s3(prefix=store.prefix, lease_ttl_s=3.0)
        assert other.try_acquire_owner(mine, c2.holder)
        r1.forget(mine)
        assert r1.resolve(mine).node == "n2"
        # the holder is unreachable: wait out the TTL, never pick another node
        assert r1.resolve(mine, frozenset({"n2"})).kind == "wait"
        other.release_owner(mine)
        r1.forget(mine)
        assert r1.resolve(mine).kind == "local"
        assert r1.is_leader("_billing") == (rendezvous("_billing", {"n1", "n2"})[0] == "n1")
    finally:
        reg2.stop()
    NodeRegistry.CACHE_S = 0.0
    try:
        r1.forget(free)
        assert r1.resolve(free).kind == "local", "a deregistered node still gets namespaces"
    finally:
        NodeRegistry.CACHE_S = 1.0


@pytest.mark.s3
def test_a_second_node_with_local_keys_refuses_instead_of_rekeying(tmp_path):
    """`local` keys on a shared bucket: the node that did not create a
    namespace has no key for it. It used to MINT one - new writes under a
    key the old data was never under, and the replay skipping the frames it
    could not decrypt. Now the open is refused."""
    from memd.engine.memory import Memory
    from memd.storage.crypto import KeyCustodyError

    store = _s3()
    cfg = {"s3_endpoint_url": ENDPOINT, "s3_access_key": KEY, "s3_secret_key": SECRET,
           "s3_region": "us-east-1", "embedder": "hash"}
    m = Memory(f"s3://{BUCKET}/{store.prefix}", config=dict(cfg, local_dir=str(tmp_path / "a")))
    m.remember("only node A holds the key for this")
    m.close()
    with pytest.raises(KeyCustodyError, match="no local key"):
        Memory(f"s3://{BUCKET}/{store.prefix}", config=dict(cfg, local_dir=str(tmp_path / "b")))
    m = Memory(f"s3://{BUCKET}/{store.prefix}", config=dict(cfg, local_dir=str(tmp_path / "a")))
    assert any("node A" in i.content for i in m.search("who holds the key").items)
    m.close()
