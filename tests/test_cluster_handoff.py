"""ADR-12 re-verification: a namespace handed back and forth between nodes.

The defect: an S3ObjectStore cached per-log part counters for the life of
the PROCESS, not of the lease. A node that held a namespace, released it,
and took it back later kept numbering parts from where IT had left off -
below the parts other nodes wrote meanwhile - and the takeover fence landed
below a paused writer's next part. Acked writes were lost on A -> B -> A,
even with no faults at all; a retaking node also failed client writes with
"append conflict" once per part the others had written.

Two Memory objects over one bucket = two nodes (each its own store, lease
holder and local dir). Plus key custody failing closed (local roots too),
and the small routing/CLI notes from the same verification.
"""
import json
import os
import subprocess
import sys
import threading
import time
import uuid

import pytest

ENDPOINT = os.environ.get("MEMD_TEST_S3_ENDPOINT")
BUCKET = os.environ.get("MEMD_TEST_S3_BUCKET", "memd-engine")
KEY = os.environ.get("MEMD_TEST_S3_KEY", "minioadmin")
SECRET = os.environ.get("MEMD_TEST_S3_SECRET", "minioadmin")
NS = "t"


def _need_s3():
    if not ENDPOINT:
        pytest.skip("no S3 endpoint (set MEMD_TEST_S3_ENDPOINT)")
    boto3 = pytest.importorskip("boto3")
    c = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET, region_name="us-east-1")
    try:
        c.create_bucket(Bucket=BUCKET)
    except Exception:
        pass
    return c


class Cluster:
    """Nodes = Memory objects on one s3:// root, each evicting every
    namespace but its facade (max_open_namespaces=1): touching another
    namespace is a clean close + lease release."""

    def __init__(self, tmp_path, ttl=10.0):
        self.s3 = _need_s3()
        self.tmp = tmp_path
        self.ttl = ttl
        self.prefix = f"ho-{uuid.uuid4().hex[:8]}"
        self.root = f"s3://{BUCKET}/{self.prefix}"
        self.nodes = []

    def node(self, name):
        from memd.engine.memory import Memory

        m = Memory(self.root, namespace=f"facade-{name}", encrypt=False, config={
            "embedder": "hash", "rate_max_writes": 10 ** 9,
            "local_dir": str(self.tmp / f"{name}-{uuid.uuid4().hex[:4]}"),
            "lease_ttl_s": self.ttl, "lease_holder": f"{name}@{uuid.uuid4().hex[:6]}",
            "s3_endpoint_url": ENDPOINT, "s3_access_key": KEY, "s3_secret_key": SECRET,
            "s3_region": "us-east-1"})
        m.engine.max_open_namespaces = 1
        self.nodes.append(m)
        return m

    def parts(self, log):
        r = self.s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{self.prefix}/ns/{NS}/{log}.__part-")
        return [(int(o["Key"].rsplit("-", 1)[1]), o["Size"]) for o in r.get("Contents", [])]

    def cold(self):
        m = self.node(f"cold{len(self.nodes)}")
        try:
            return {json.loads(line)["id"]: json.loads(line)["content"]
                    for line in m.export_jsonl(namespace=NS).splitlines() if line.strip()}
        finally:
            m.close()
            self.nodes.remove(m)

    def close(self):
        for m in self.nodes:
            try:
                m.close()
            except Exception:
                pass


@pytest.fixture()
def cluster(tmp_path):
    c = Cluster(tmp_path)
    yield c
    c.close()


def evict(m):
    """Touch another namespace: the LRU closes NS - a clean release."""
    m.remember("x", namespace=f"other-{uuid.uuid4().hex[:6]}")


def _check(acked: dict, deleted: set, views: dict):
    expect = {r: c for r, c in acked.items() if r not in deleted}
    for label, view in views.items():
        lost = sorted(c for r, c in expect.items() if view.get(r) != c)
        back = sorted(acked[r] for r in deleted if r in view)
        assert not lost, f"{label}: ACKED writes lost: {lost}"
        assert not back, f"{label}: acked deletes undone: {back}"


def _served(m, acked):
    return {json.loads(line)["id"]: json.loads(line)["content"]
            for line in m.export_jsonl(namespace=NS).splitlines() if line.strip()}


# ------------------------------------------------ handoffs without faults


@pytest.mark.s3
def test_a_b_a_through_clean_releases_loses_nothing(cluster):
    """No faults at all: A -> B (who compacts) -> A. A's writes and its delete
    of one of B's records must land ABOVE everything B wrote, succeed first
    time, and survive a cold replay (before and after a compaction)."""
    A, B = cluster.node("A"), cluster.node("B")
    acked, deleted = {}, set()
    for i in range(5):
        acked[A.remember(f"a-{i}", namespace=NS)] = f"a-{i}"
    evict(A)
    for i in range(20):
        acked[B.remember(f"b-{i}", namespace=NS)] = f"b-{i}"
    B.compact(force=True, namespace=NS)
    for i in range(3):
        acked[B.remember(f"b2-{i}", namespace=NS)] = f"b2-{i}"
    top_before = max(n for n, _ in cluster.parts("wal"))
    evict(B)
    for i in range(6):   # every one must succeed - no "append conflict"
        acked[A.remember(f"a2-{i}", namespace=NS)] = f"a2-{i}"
    victim = next(r for r, c in acked.items() if c == "b-3")
    A.delete(victim, namespace=NS)
    deleted.add(victim)
    new = [n for n, sz in cluster.parts("wal") if sz > 0 and n > top_before]
    assert len(new) == 6, f"A's parts did not all land above B's ({top_before}): {cluster.parts('wal')}"
    served = _served(A, acked)
    A.close()
    B.close()
    cold = cluster.cold()
    c = cluster.node("C")
    c.compact(force=True, namespace=NS)
    c.close()
    _check(acked, deleted, {"A_served": served, "cold": cold, "cold_after_compact": cluster.cold()})


@pytest.mark.s3
def test_a_b_c_a_triple_handoff_numbering_never_goes_backwards(cluster):
    A, B, C = cluster.node("A"), cluster.node("B"), cluster.node("C")
    acked = {}
    high = -1                     # highest WAL part number that ever existed
    for rnd, n in enumerate([A, B, C, A, B, A]):
        for i in range(4):
            acked[n.remember(f"r{rnd}-{i}", namespace=NS)] = f"r{rnd}-{i}"
        mine = [p for p, sz in cluster.parts("wal") if sz > 0]
        assert len(mine) >= 4 and all(p > high for p in mine[-4:]), \
            f"round {rnd}: new parts {mine[-4:]} not above every earlier part ({high})"
        high = max([high] + [p for p, _ in cluster.parts("wal")])
        if rnd in (1, 3):
            n.compact(force=True, namespace=NS)     # deletes the parts it folded
        evict(n)
    _check(acked, set(), {"cold": cluster.cold()})


# ------------------------------------ a retaken namespace's warm index cache


def _local_bytes(m) -> bytes:
    """Every byte node `m` keeps locally (index cache, tantivy copy, keys)."""
    out = []
    for d, _, files in os.walk(os.path.dirname(m.engine.cache_dir)):
        for f in files:
            try:
                with open(os.path.join(d, f), "rb") as fh:
                    out.append(fh.read())
            except OSError:
                pass
    return b"\0".join(out)


def _view(m, ids) -> dict:
    """What `m` serves per id: None, or the record's superseded_by."""
    out = {}
    for r in ids:
        d = m.get(r, namespace=NS)
        out[r] = None if d is None else ("served", d["time"]["superseded_by"])
    return out


@pytest.mark.s3
@pytest.mark.parametrize("fold", ["compact", "rotate"])
@pytest.mark.parametrize("b_writes", [False, True])
def test_a_retaken_namespace_never_serves_what_another_node_deleted(cluster, fold, b_writes):
    """A -> B -> A through clean releases, A's local index cache kept warm.
    A writes and deletes (soft and hard) and releases; B deletes more (soft
    and hard; with b_writes it also writes new records and supersedes one of
    A's), then folds and releases. A compaction RETIRES every op it folds:
    the deletes survive only as absence from its segment, so replaying the
    tail past A's old watermark cannot redo them. A's warm cache served
    every record B deleted - hard deletes included - through get and search,
    while export (folded from durable state) did not. Now: nothing either
    node deleted comes back, B's writes and supersede are served, A agrees
    with a cold node, and no hard-deleted text is left in A's local files."""
    A, B = cluster.node("A"), cluster.node("B")
    tag = uuid.uuid4().hex[:8]
    word = [f"zq{tag}w{i:02d}" for i in range(30)]   # one unique token per record
    ids = [A.remember(f"note {i} {word[i]}", namespace=NS) for i in range(30)]
    a_soft, a_hard, b_soft, b_hard = ids[0:2], ids[2:4], ids[4:6], ids[6:8]
    for r in a_soft:
        assert A.delete(r, namespace=NS)
    for r in a_hard:
        assert A.delete(r, hard=True, namespace=NS)
    A.search(" ".join(word[:10]), namespace=NS, budget_tokens=4000)   # warm A's caches
    evict(A)
    for r in b_soft:
        assert B.delete(r, namespace=NS)
    for r in b_hard:
        assert B.delete(r, hard=True, namespace=NS)
    new = []
    if b_writes:
        new = [B.remember(f"from b {i} zq{tag}b{i}", namespace=NS) for i in range(3)]
        ns_b = B.engine.namespace(NS)
        B._apply_supersedence(ns_b, [(ids[8], new[0])])
    if fold == "compact":
        B.compact(force=True, namespace=NS)
    else:
        B.engine.namespace(NS).rotate("test")
    b_view = _view(B, ids + new)
    evict(B)

    deleted = a_soft + a_hard + b_soft + b_hard
    a_view = _view(A, ids + new)
    back = [r for r in deleted if a_view[r] is not None]
    assert not back, f"A serves {len(back)} deleted record(s) again: {back}"
    assert a_view == b_view, "A's view of the namespace differs from B's"
    for r in new:
        assert a_view[r] == ("served", None), f"A lost B's write {r}"
    if b_writes:
        assert a_view[ids[8]] == ("served", new[0]), "A lost B's supersede"
    hits = A.search(" ".join(word[:10]), namespace=NS, budget_tokens=4000)
    got = {h.id for h in hits.items}
    assert not got & set(deleted), f"search serves deleted records: {sorted(got & set(deleted))}"
    assert set(ids[8:10]) & got, "the search found nothing at all"
    exported = {json.loads(line)["id"] for line in A.export_jsonl(namespace=NS).splitlines()
                if line.strip()}
    assert exported == {r for r, v in a_view.items() if v is not None}, "get and export disagree"
    if fold == "compact":
        # the purge B's compaction made must reach A's copies too (D7)
        local = _local_bytes(A)
        assert word[9].encode() in local, "the scan does not see A's index cache"
        kept = [word[ids.index(r)] for r in a_hard + b_hard if word[ids.index(r)].encode() in local]
        assert not kept, f"hard-deleted text left in A's local files: {kept}"
    A.close()
    B.close()
    cold = cluster.node("cold")
    try:
        assert _view(cold, ids + new) == a_view, "A disagrees with a cold node"
    finally:
        cold.close()


def test_a_reopen_no_other_tenure_came_between_keeps_its_warm_cache(tmp_path):
    """Control for the lineage check: it discards a cache only when another
    tenure opened the namespace since. A close + reopen in between which
    nobody else wrote keeps the same file and replays nothing."""
    from memd.core.schema import MemoryRecord
    from memd.storage.engine import StorageEngine

    root, cache = str(tmp_path / "store"), str(tmp_path / "cache")
    e = StorageEngine(root, cache_dir=cache)
    ns = e.namespace("t")
    for i in range(5):
        ns.append([MemoryRecord.create(namespace="t", kind="raw_event", content=f"r {i}")])
    path = ns.index.path
    e.close()
    ino = os.stat(path).st_ino
    for _ in range(2):
        e = StorageEngine(root, cache_dir=cache)
        ns = e.namespace("t")
        try:
            assert not ns._replayed_at_open, "a warm reopen replayed or rebuilt the cache"
            assert os.stat(path).st_ino == ino, "a warm reopen replaced the cache file"
            assert ns.index.stats()["records"] == 5
        finally:
            e.close()


def test_a_discarded_cache_takes_the_ann_sidecar_with_it(tmp_path, monkeypatch):
    """The lineage check (ADR-12) with the usearch sidecar: a cache of
    another lineage is deleted and rebuilt - and so are the sidecar's files,
    derived from it: they hold the vectors of records another node may have
    hard-deleted and purged since (D7). Removed before the sidecar attaches,
    so nothing of the old lineage is loaded from them either."""
    pytest.importorskip("usearch")
    import numpy as np

    from memd.core.schema import MemoryRecord
    from memd.storage.engine import StorageEngine

    root, cache = str(tmp_path / "store"), str(tmp_path / "cache")
    vcfg = {"mode": "usearch", "min_vectors": 0, "overfetch": 4, "exact_max": 0,
            "dtype": "f16", "build_threads": 1}
    e = StorageEngine(root, cache_dir=cache, vector_index=vcfg)
    ns = e.namespace("t")
    recs = [MemoryRecord.create(namespace="t", kind="raw_event", content=f"r {i}") for i in range(50)]
    ns.append(recs)
    x = np.random.default_rng(0).standard_normal((50, 16)).astype(np.float32)
    ns.index.set_vectors([r.id for r in recs], x, "m")
    assert ns.index.ann.drain(60) and ns.index.ann.size() == 50
    side = os.path.splitext(ns.index.path)[0] + ".usearch"
    e.close()
    assert os.path.isdir(side) and os.listdir(side), "no sidecar files to discard"
    sentinel = os.path.join(side, "stale-lineage")
    with open(sentinel, "wb") as f:
        f.write(b"vectors of another lineage")
    # another tenure opened the namespace since (another node, a handoff)
    mpath = os.path.join(root, "ns", "t", "manifest.json")
    with open(mpath) as f:
        m = json.load(f)
    m["lineage"] = "01OTHERTENURE0000000000000"
    with open(mpath, "w") as f:
        json.dump(m, f)
    from memd.index.ann_usearch import UsearchSidecar

    seen = []
    real = UsearchSidecar._open_step

    def spy(self, job):
        seen.append(os.path.exists(sentinel))   # what the sidecar's load finds
        return real(self, job)

    monkeypatch.setattr(UsearchSidecar, "_open_step", spy)
    e = StorageEngine(root, cache_dir=cache, vector_index=vcfg)
    try:
        ns = e.namespace("t")
        assert ns._replayed_at_open and ns.index.stats()["records"] == 50
        assert ns.index.ann.drain(60)
        assert seen == [False], "the new lineage's sidecar found the discarded cache's files"
        assert not os.path.exists(sentinel)
    finally:
        e.close()


# ------------------------------------------- handoffs with a paused writer


def _pause_after_check(store, method, key_suffix):
    """Make `store`'s next `method` on a key ending in key_suffix stop right
    after its fence check (the window no check can close) until released."""
    from memd.storage import s3store

    gate, at_gate = threading.Event(), threading.Event()
    orig = getattr(s3store.S3ObjectStore, method)
    armed = {"on": True}

    def paused(self, key, *a, **kw):
        if self is store and armed["on"] and key.endswith(key_suffix):
            armed["on"] = False
            real = self._check_fence

            def check_then_pause(k):
                real(k)
                at_gate.set()
                gate.wait()
            self._check_fence = check_then_pause
            try:
                return orig(self, key, *a, **kw)
            finally:
                del self._check_fence
        return orig(self, key, *a, **kw)
    return gate, at_gate, paused, orig


def _freeze_heartbeat(m):
    st = m.engine.store
    st._lease_stop.set()
    if st._lease_thread is not None:
        st._lease_thread.join(timeout=5)


@pytest.mark.s3
@pytest.mark.parametrize("then_rotate", [False, True])
def test_double_handoff_with_a_paused_append_loses_no_ack(tmp_path, monkeypatch, then_rotate):
    """A holds NS, releases; B takes it, writes, compacts; B's next append is
    frozen AFTER its fence check; A re-takes NS (stale lease) and writes; B
    resumes. B's frozen append must either land where A replays it or fail
    unacked - never land unseen - and A must write without conflicts."""
    from memd.storage import s3store

    cl = Cluster(tmp_path, ttl=2.0)
    try:
        A, B = cl.node("A"), cl.node("B")
        acked = {}
        for i in range(5):
            acked[A.remember(f"a-{i}", namespace=NS)] = f"a-{i}"
        evict(A)
        for i in range(20):
            acked[B.remember(f"b-{i}", namespace=NS)] = f"b-{i}"
        B.compact(force=True, namespace=NS)
        for i in range(3):
            acked[B.remember(f"b2-{i}", namespace=NS)] = f"b2-{i}"
        gate, at_gate, paused, _orig = _pause_after_check(B.engine.store, "append", "/wal")
        monkeypatch.setattr(s3store.S3ObjectStore, "append", paused)
        result = {}

        def b_op():
            try:
                rid = B.remember("b-PAUSED", namespace=NS)
                acked[rid] = "b-PAUSED"
                result["b"] = rid
            except Exception as ex:  # noqa: BLE001
                result["b"] = f"{type(ex).__name__}: {ex}"
        t = threading.Thread(target=b_op, daemon=True)
        t.start()
        assert at_gate.wait(30)
        _freeze_heartbeat(B)
        time.sleep(cl.ttl + 0.6)
        for i in range(4):      # A re-takes: every write succeeds first time
            acked[A.remember(f"a-after-{i}", namespace=NS)] = f"a-after-{i}"
        if then_rotate:
            A.engine.namespace(NS).rotate("test")
        gate.set()
        t.join(30)
        # the frozen append either landed where A replays it, or failed as a
        # LOST LEASE (503 lease_lost over REST) - never a generic error
        assert result["b"] in acked or result["b"].split(":")[0] in (
            "AppendConflict", "LeaseLostError", "PreconditionFailed"), result
        for i in range(3):
            acked[A.remember(f"a-post-{i}", namespace=NS)] = f"a-post-{i}"
        A.engine.namespace(NS).rotate("test")
        served = _served(A, acked)
        A.close()
        try:
            B.close()
        except Exception:
            pass
        _check(acked, set(), {"A_served": served, "cold": cl.cold()})
    finally:
        cl.close()


@pytest.mark.s3
def test_an_aborted_takeover_is_not_released_as_clean(tmp_path, monkeypatch):
    """A takeover whose fence cannot complete must not leave a CLEAN
    tombstone: the next holder would skip the fence and the manifest claim
    while the paused holder it took over from may still resume."""
    from memd.storage.engine import NamespaceBusyError, NamespaceStore
    from memd.storage.s3store import _ABORTED

    cl = Cluster(tmp_path, ttl=2.0)
    try:
        A = cl.node("A")
        A.remember("one", namespace=NS)
        st = A.engine.store
        _freeze_heartbeat(A)           # A "stalls" holding NS
        time.sleep(cl.ttl + 0.6)
        B = cl.node("B")

        def always_busy(self):
            raise NamespaceBusyError("simulated: the previous writer keeps appending")
        monkeypatch.setattr(NamespaceStore, "_fence_previous_writer", always_busy)
        with pytest.raises(NamespaceBusyError):
            B.engine.namespace(NS)
        body = cl.s3.get_object(Bucket=BUCKET, Key=f"{cl.prefix}/ns/{NS}/.owner")["Body"].read()
        assert body == _ABORTED, f"an aborted takeover left {body!r}"
        monkeypatch.undo()
        C = cl.node("C")
        C.engine.namespace(NS)          # must fence + claim again
        assert C.engine.store._leases.get(NS)
        assert st is not None
    finally:
        cl.close()


# ------------------------------------------------ key custody fails closed


def test_a_garbage_custody_marker_refuses_to_open_a_local_root(tmp_path):
    from memd.engine.memory import Memory
    from memd.storage.crypto import KeyCustodyError

    data = str(tmp_path / "d")
    m = Memory(data, config={"embedder": "hash"})
    m.remember("secret stuff")
    m.close()
    os.makedirs(os.path.join(data, "store", "keys"), exist_ok=True)
    with open(os.path.join(data, "store", "keys", "_custody.json"), "wb") as f:
        f.write(b"\x00\x13not json")
    with pytest.raises(KeyCustodyError, match="unreadable"):
        Memory(data, config={"embedder": "hash"})


def test_a_local_root_never_mints_a_key_over_existing_data(tmp_path):
    from memd.engine.memory import Memory
    from memd.storage.crypto import KeyCustodyError

    data = str(tmp_path / "d")
    m = Memory(data, config={"embedder": "hash"})
    m.remember("under the original key")
    m.close()
    os.unlink(os.path.join(data, "keys", "ns-default.key"))   # the key is lost
    wal = os.path.join(data, "store", "ns", "default", "wal")
    before = open(wal, "rb").read()
    with pytest.raises(KeyCustodyError, match="no data key"):
        Memory(data, config={"embedder": "hash"})
    assert open(wal, "rb").read() == before, "a refused open changed the log"
    assert not os.path.exists(os.path.join(data, "keys", "ns-default.key")), "a key was minted"


def test_complete_frames_that_do_not_decrypt_are_never_truncated(tmp_path):
    """The key file replaced by another one (a restore mix-up): every frame
    is complete but undecryptable. The torn-tail repair used to cut them all
    off. Now the open is refused and not a byte is cut."""
    from memd.engine.memory import Memory
    from memd.storage.crypto import KeyCustodyError, LocalKeyEnvelope

    data = str(tmp_path / "d")
    m = Memory(data, config={"embedder": "hash"})
    for i in range(3):
        m.remember(f"fact {i}")
    m.close()
    other = str(tmp_path / "other")
    LocalKeyEnvelope(os.path.join(other, "keys")).data_key("default")
    os.replace(os.path.join(other, "keys", "ns-default.key"), os.path.join(data, "keys", "ns-default.key"))
    os.replace(os.path.join(other, "keys", "root.key"), os.path.join(data, "keys", "root.key"))
    logs = {n: open(os.path.join(data, "store", "ns", "default", n), "rb").read()
            for n in ("wal", "ops") if os.path.exists(os.path.join(data, "store", "ns", "default", n))}
    # (master's key custody: the manifest's key check refuses it before any
    # frame is read - "nothing was read, truncated or deleted")
    with pytest.raises(KeyCustodyError, match=r"nothing was (read, )?truncated"):
        Memory(data, config={"embedder": "hash"})
    for n, before in logs.items():
        assert open(os.path.join(data, "store", "ns", "default", n), "rb").read() == before, n


def test_a_data_key_under_another_local_root_is_a_custody_error():
    """Not a raw cryptography InvalidTag (an opaque 500): the data key was
    wrapped by another root key - held elsewhere, which is what
    KeyCustodyError says (and what the HTTP layer answers as such)."""
    from memd.storage.crypto import KeyCustodyError, LocalKeyProvider

    wk = LocalKeyProvider(os.urandom(32)).wrap("a", b"k" * 32)
    with pytest.raises(KeyCustodyError, match="different one"):
        LocalKeyProvider(os.urandom(32)).unwrap("a", wk)


def test_a_torn_tail_is_still_repaired(tmp_path):
    """Control: a frame cut short by its length (a torn write) is still cut."""
    from memd.engine.memory import Memory

    data = str(tmp_path / "d")
    m = Memory(data, config={"embedder": "hash"})
    m.remember("kept")
    m.close()
    wal = os.path.join(data, "store", "ns", "default", "wal")
    good = open(wal, "rb").read()
    with open(wal, "ab") as f:
        f.write((500).to_bytes(4, "big") + b"\x01" * 20)     # length says 500
    m = Memory(data, config={"embedder": "hash"})
    assert any("kept" in i.content for i in m.search("kept").items)
    m.close()
    assert open(wal, "rb").read()[:len(good)] == good


# ----------------------------------------------------------------- notes


@pytest.mark.s3
def test_a_refused_open_of_a_nonexistent_namespace_leaves_no_lease(tmp_path):
    from memd.storage.crypto import KeyUnavailableError

    cl = Cluster(tmp_path)
    try:
        A = cl.node("A")
        env = A.engine.envelope = type("Down", (), {
            "enabled": True, "prefetch_at_open": True,
            "data_key": staticmethod(lambda ns: (_ for _ in ()).throw(KeyUnavailableError("KMS down"))),
            "has_key": staticmethod(lambda ns: False)})()
        with pytest.raises(KeyUnavailableError):
            A.engine.namespace("never-created")
        assert env is not None
        r = cl.s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{cl.prefix}/ns/never-created/")
        assert not r.get("Contents"), f"left behind: {[o['Key'] for o in r['Contents']]}"
    finally:
        cl.close()


def test_an_invalid_namespace_is_answered_before_any_routing():
    import asyncio

    from memd.server.cluster import ClusterConfig, ClusterMiddleware

    calls = []

    class Router:
        def resolve(self, ns, exclude=frozenset()):
            calls.append(ns)
            raise AssertionError("routed an invalid namespace")

        def forget(self, ns):
            pass

    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 400, "headers": []})
        await send({"type": "http.response.body", "body": b"invalid namespace"})

    mw = ClusterMiddleware(app, Router(), ClusterConfig("n1", "http://127.0.0.1:1", "s" * 32))
    sent = []

    async def receive():
        return {"type": "http.request", "body": b"{}", "more_body": False}

    async def send(msg):
        sent.append(msg)
    for path in ("/v1/ns/..%2F/memories", "/v1/ns/-bad/memories", "/v1/ns/" + "x" * 200 + "/stats"):
        sent.clear()
        asyncio.run(mw({"type": "http", "method": "POST", "path": path, "raw_path": path.encode(),
                        "headers": [], "client": ("1.2.3.4", 1), "query_string": b""}, receive, send))
        assert sent[0]["status"] == 400
    assert not calls


def test_a_single_server_does_not_import_the_cluster_module(tmp_path):
    code = ("import sys, memd.cli as c\n"
            "c.uvicorn = None\n"
            "import uvicorn\n"
            "uvicorn.run = lambda *a, **k: None\n"
            "c.main(['serve', '--http'])\n"
            "print('memd.server.cluster' in sys.modules)")
    env = dict(os.environ, MEMD_DATA=str(tmp_path / "d"), MEMD_EMBEDDER="hash")
    env.pop("MEMD_NODE_ID", None)
    out = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True, timeout=120)
    assert out.stdout.strip().splitlines()[-1] == "False", out.stdout + out.stderr
