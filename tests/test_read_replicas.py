"""Read replicas - the read-only namespace store.

A replica (`Memory(..., read_only=True)`, `ReplicaStore`) opens a namespace
WITHOUT its lease and follows the writer's tail from the bucket:

  - it sees the leader's writes and deletes within the staleness bound;
  - it never writes or deletes any object (store instrumented: zero
    mutating calls; S3: only GET / HEAD / LIST reach the endpoint) and
    refuses every mutating call;
  - a compaction, rotation or takeover on the leader is followed correctly
    (rebuild or catch-up) and never resurrects a delete;
  - a hard delete's text leaves the replica's local files;
  - a wrong key refuses (KeyCustodyError), never an empty replica;
  - a destroyed namespace's replica cache is dropped.

Every test runs on the local store; the S3 variants need MinIO
(MEMD_TEST_S3_ENDPOINT).
"""
import hashlib
import os
import shutil
import threading
import time
import uuid

import pytest

from memd.engine.memory import Memory
from memd.storage.crypto import KeyCustodyError

ENDPOINT = os.environ.get("MEMD_TEST_S3_ENDPOINT")
BUCKET = os.environ.get("MEMD_TEST_S3_BUCKET", "memd-engine")
KEY = os.environ.get("MEMD_TEST_S3_KEY", "minioadmin")
SECRET = os.environ.get("MEMD_TEST_S3_SECRET", "minioadmin")

REFRESH_S = 0.2
BOUND_S = 5.0     # generous: CI machines stall; bench/replica_bench.py measures the lag


def _wait(cond, timeout=BOUND_S, every=0.05):
    deadline = time.monotonic() + timeout
    while True:
        got = cond()
        if got:
            return got
        if time.monotonic() > deadline:
            return got
        time.sleep(every)


class Backend:
    """A data root two Memory objects (a writer and a replica) share."""

    def __init__(self, kind: str, tmp_path):
        self.kind = kind
        self.tmp = tmp_path
        self.local = str(tmp_path / "local")
        if kind == "s3":
            if not ENDPOINT:
                pytest.skip("no S3 endpoint (set MEMD_TEST_S3_ENDPOINT)")
            boto3 = pytest.importorskip("boto3")
            c = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=KEY,
                             aws_secret_access_key=SECRET, region_name="us-east-1")
            try:
                c.create_bucket(Bucket=BUCKET)
            except Exception:
                pass
            self.s3 = c
            self.prefix = f"rr-{uuid.uuid4().hex[:10]}"
            self.path = f"s3://{BUCKET}/{self.prefix}"
        else:
            self.path = str(tmp_path / "data")

    def cfg(self, **extra):
        cfg = {"embedder": "hash", "rate_max_writes": 10 ** 9, "replica_refresh_s": REFRESH_S,
               "reranker": "none"}
        if self.kind == "s3":
            cfg.update({"s3_endpoint_url": ENDPOINT, "s3_access_key": KEY, "s3_secret_key": SECRET,
                        "s3_region": "us-east-1", "local_dir": self.local, "lease_ttl_s": 10.0})
        cfg.update(extra)
        return cfg

    def leader(self, namespace="default", **extra):
        return Memory(self.path, namespace=namespace, config=self.cfg(**extra))

    def replica(self, namespace="default", **extra):
        return Memory(self.path, namespace=namespace, read_only=True, config=self.cfg(**extra))

    def objects(self) -> dict:
        """Every durable object (and key file) with a fingerprint - never the
        local derived caches."""
        out = {}
        if self.kind == "s3":
            pag = self.s3.get_paginator("list_objects_v2")
            for page in pag.paginate(Bucket=BUCKET, Prefix=self.prefix + "/"):
                for o in page.get("Contents", []) or []:
                    out[o["Key"]] = (o["ETag"], o["Size"], str(o["LastModified"]))
            roots = [os.path.join(self.local, "keys")]
        else:
            roots = [self.path]
        for root in roots:
            for dirpath, dirs, files in os.walk(root):
                dirs[:] = [d for d in dirs if d != "_cache"]
                for fn in files:
                    p = os.path.join(dirpath, fn)
                    try:
                        st = os.stat(p)
                        with open(p, "rb") as f:
                            digest = hashlib.sha256(f.read()).hexdigest()
                    except FileNotFoundError:
                        continue
                    out[os.path.relpath(p, root)] = (digest, st.st_size, st.st_mtime_ns)
        return out


@pytest.fixture(params=["local", "s3"])
def be(request, tmp_path):
    return Backend(request.param, tmp_path)


def _texts(mem, query, **kw):
    return {i.content for i in mem.search(query, **kw).items}


def _cache_bytes(mem) -> bytes:
    """Every byte of the replica's local derived cache files."""
    base = os.path.splitext(mem.ns.index.path)[0]
    blob = b""
    for p in (base + ".sqlite", base + ".sqlite-wal", base + ".sqlite-shm"):
        try:
            with open(p, "rb") as f:
                blob += f.read()
        except FileNotFoundError:
            pass
    for d in (base + ".tantivy", base + ".usearch"):
        for dirpath, _dirs, files in os.walk(d):
            for fn in files:
                try:
                    with open(os.path.join(dirpath, fn), "rb") as f:
                        blob += f.read()
                except FileNotFoundError:
                    pass   # a background rebuild replaced it mid-walk
    return blob


# ------------------------------------------------------------- following


def test_a_replica_sees_the_leaders_writes_and_deletes_within_the_bound(be):
    lead = be.leader()
    try:
        lead.add("the deploy command is make ship alpha")
        rep = be.replica()
        try:
            assert "the deploy command is make ship alpha" in _texts(rep, "deploy command")
            t0 = time.monotonic()
            rid = lead.add("the rollback command is make undo bravo")[0]
            assert _wait(lambda: "the rollback command is make undo bravo" in _texts(rep, "rollback command"))
            assert time.monotonic() - t0 < BOUND_S
            assert rep.get(rid)["content"] == "the rollback command is make undo bravo"
            lead.delete(rid)
            assert _wait(lambda: rep.get(rid) is None)
            assert "the rollback command is make undo bravo" not in _texts(rep, "rollback command")
            hid = lead.add("the hard delete target charlie")[0]
            assert _wait(lambda: rep.get(hid) is not None)
            lead.delete(hid, hard=True)
            assert _wait(lambda: rep.get(hid) is None and rep.get(hid, include_deleted=True) is None)
            # the replica says how fresh it is
            st = rep.stats()["replica"]
            assert st["applied_seq"] >= 1 and 0 <= st["age_ms"] < BOUND_S * 1000
        finally:
            rep.close()
    finally:
        lead.close()


def test_a_replica_opened_before_the_namespace_exists_follows_its_creation(be):
    rep = be.replica(namespace="later")
    try:
        assert rep.search("anything").items == []
        lead = be.leader(namespace="later")
        try:
            lead.add("created after the replica opened delta")
            assert _wait(lambda: "created after the replica opened delta" in _texts(rep, "replica opened"))
        finally:
            lead.close()
    finally:
        rep.close()


def test_replica_reads_respect_the_one_seq_order_across_both_logs(be):
    """A tombstone in the ops log after a frame the replica has not read yet
    must not be applied before it (the delete would be undone when the frame
    lands). The leader writes R and deletes it between the replica's first
    WAL read and its ops read; the refresh must apply both, in order, without
    a rebuild."""
    from memd.storage import replica as _replica

    lead = be.leader()
    rep = be.replica(replica_refresh_s=3600)   # refreshed by hand only
    try:
        lead.add("warmup echo")
        rep.ns.refresh()
        injected = {}
        orig = _replica.ReplicaStore._read_log

        def read_log(self, which, cursor):
            out = orig(self, which, cursor)
            if which == "wal" and not injected:
                injected["rid"] = lead.add("written then deleted foxtrot")[0]
                lead.delete(injected["rid"])
            return out

        _replica.ReplicaStore._read_log = read_log
        try:
            rebuilds = rep.ns.rebuilds
            rep.ns.refresh()
        finally:
            _replica.ReplicaStore._read_log = orig
        assert rep.get(injected["rid"]) is None
        assert "written then deleted foxtrot" not in _texts(rep, "deleted foxtrot")
        assert rep.ns.rebuilds == rebuilds, "the ordering needed a rebuild to come out right"
        rep.ns.refresh()
        assert rep.get(injected["rid"]) is None
        assert rep.ns.applied_seq == lead.ns.manifest.seq
        assert rep.ns.rebuilds == rebuilds, "the ordering needed a rebuild to come out right"

    finally:
        rep.close()
        lead.close()


def test_a_fold_between_the_manifest_read_and_the_tail_read_is_never_half_seen(be):
    """The replica reads the manifest, then the logs. A rotation landing in
    between folds frames the replica has not read into a segment the
    manifest it holds does not name, and deletes them from the log: the
    tail it reads skips them. The second manifest read sees the fold, and
    the refresh starts over from the new manifest."""
    lead = be.leader()
    lead.ns.wal_rotate_frames = 10 ** 6
    lead.ns.wal_rotate_bytes = 10 ** 9
    rep = be.replica(replica_refresh_s=3600)
    try:
        lead.add("seen before golf")
        rep.ns.refresh()
        folded = [lead.add(f"folded unseen hotel {i}")[0] for i in range(3)]
        store = rep.ns.store
        orig = type(store).get_versioned
        fired = []

        def get_versioned(key):
            got = orig(store, key)
            if key.endswith("manifest.json") and not fired:
                fired.append(1)
                lead.ns.rotate("test")                     # folds + deletes the logs
                fired.append(lead.add("after the fold india")[0])
            return got

        store.get_versioned = get_versioned
        try:
            rep.ns.refresh()
        finally:
            del store.get_versioned
        assert fired, "the hook did not run"
        for rid in folded + [fired[1]]:
            assert rep.get(rid), f"{rid} lost by the replica"
        assert rep.ns.applied_seq == lead.ns.manifest.seq
    finally:
        rep.close()
        lead.close()


def test_a_segment_deleted_under_a_replicas_read_is_read_again(be):
    """A compaction can delete a segment between the replica's manifest read
    and its read of that segment. The writer may skip a segment it cannot
    find (it holds the lock: that is damage); a replica must not - it reads
    the new manifest instead."""
    lead = be.leader()
    lead.ns.wal_rotate_frames = 10 ** 6
    lead.ns.wal_rotate_bytes = 10 ** 9
    ids = [lead.add(f"in a segment juliet {i}")[0] for i in range(5)]
    lead.ns.rotate("test")
    gone = lead.add("deleted later kilo")[0]
    lead.ns.rotate("test")
    lead.delete(gone)
    store_holder = {}
    rep = None
    from memd.storage import replica as _replica

    orig_init = _replica.ReadOnlyObjectStore.get

    def get(self, key):
        if "/seg-" in key and not store_holder:
            store_holder["fired"] = True
            lead.compact(force=True)                     # deletes every segment it replaces
        return orig_init(self, key)

    _replica.ReadOnlyObjectStore.get = get
    try:
        rep = be.replica(replica_refresh_s=3600)
    finally:
        _replica.ReadOnlyObjectStore.get = orig_init
    try:
        assert store_holder.get("fired"), "the hook did not run"
        for rid in ids:
            assert rep.get(rid), f"{rid} lost by the replica"
        assert rep.get(gone) is None
    finally:
        rep.close()
        lead.close()


def test_an_event_below_the_horizon_rebuilds_the_replica(be):
    """The horizon assumes the writer numbers events in the order they become
    durable. An append whose response was lost and that the writer then
    numbered again breaks that: an event the replica reads AFTER its horizon
    passed its seq. It must not be skipped (here: a delete) - the replica
    rebuilds from durable data, which orders it as the writer's replay does."""
    import json as _json

    from memd.storage.engine import _frame_encode

    lead = be.leader()
    rep = be.replica(replica_refresh_s=3600)
    try:
        rid = lead.add("deleted by a renumbered op lima")[0]
        lead.add("later mike")
        rep.ns.refresh()
        h = rep.ns.applied_seq
        assert rep.get(rid)
        op = {"op": "tombstone", "id": rid, "at": 1, "seq": h}     # a seq the replica has passed
        payload = _json.dumps(op, separators=(",", ":")).encode()
        env = lead.ns.envelope
        lead.ns.store.append(lead.ns.ops_key, _frame_encode(
            env.encrypt("default", payload) if env.enabled else payload))
        rebuilds = rep.ns.rebuilds
        rep.ns.refresh()
        assert rep.ns.rebuilds == rebuilds + 1
        assert rep.get(rid) is None
        assert _texts(rep, "later mike") == {"later mike"}
    finally:
        rep.close()
        lead.close()


# ------------------------------------------------------- never writes


def test_a_replica_never_writes_any_object(be):
    lead = be.leader()
    for i in range(30):
        lead.add(f"record number {i} golf")
    lead.delete(lead.add("deleted hotel")[0], hard=True)
    lead.close()
    before = be.objects()
    rep = be.replica()
    try:
        assert _texts(rep, "record number golf")
        rep.ns.refresh()
        rep.get("nonexistent")
        rep.find_ids("golf")
        assert len(rep.export_jsonl().splitlines()) == 30
        rep.stats()
        rep.status()
        rep.flush()
    finally:
        rep.close()
    assert be.objects() == before, "the replica changed the bucket or the keys"


def test_a_replica_issues_no_mutating_store_call_while_the_leader_writes(be):
    from memd.storage.objectstore import LocalObjectStore

    lead = be.leader()
    rep = be.replica()
    calls = []
    try:
        if be.kind == "s3":
            # the engine's store, and the view with short timeouts the
            # replica (and its key envelope) reads through
            clients = {id(c): c for st in (rep.engine.store, rep.ns.store.inner)
                       for c in (st._client, st._ctl)}
            assert rep.ns.store.inner is not rep.engine.store
            for client in clients.values():
                client.meta.events.register(
                    "before-call.s3", lambda model, **kw: calls.append(model.name))
            allowed = {"GetObject", "HeadObject", "ListObjectsV2", "ListObjects"}
        else:
            store = rep.engine.store
            assert isinstance(store, LocalObjectStore)
            for name in ("put", "put_hint", "put_if_match", "put_if_absent", "append", "delete",
                         "delete_log", "truncate", "shred", "remove_prefix", "copy", "open_log"):
                orig = getattr(store, name)

                def spy(*a, _n=name, _o=orig, **kw):
                    calls.append(_n)
                    return _o(*a, **kw)
                setattr(store, name, spy)
            allowed = set()
        for i in range(20):
            rid = lead.add(f"busy leader india {i}")[0]
            if i % 3 == 0:
                lead.delete(rid, hard=i % 2 == 0)
        lead.compact(force=True)
        lead.add("after the compaction juliet")
        assert _wait(lambda: "after the compaction juliet" in _texts(rep, "compaction juliet"))
        rep.export_jsonl()
        assert set(calls) <= allowed, f"mutating store calls from a replica: {sorted(set(calls) - allowed)}"
        if be.kind == "s3":
            assert calls, "the instrumentation saw nothing"
    finally:
        rep.close()
        lead.close()
    assert set(calls) <= allowed, f"mutating store calls at close: {sorted(set(calls) - allowed)}"


def test_every_mutating_call_on_a_replica_is_refused(be):
    from memd.storage.engine import ReadOnlyError

    lead = be.leader()
    rid = lead.add("kept kilo")[0]
    lead.close()
    rep = be.replica()
    try:
        for call in (lambda: rep.add("x"), lambda: rep.add_events([{"content": "x"}]),
                     lambda: rep.remember("x"), lambda: rep.observe([{"role": "user", "content": "x"}], "y"),
                     lambda: rep.delete(rid), lambda: rep.delete(rid, hard=True),
                     lambda: rep.delete_many([rid]), lambda: rep.forget("kilo"),
                     lambda: rep.close_session("s1"), lambda: rep.compact(force=True),
                     lambda: rep.destroy_namespace()):
            with pytest.raises(ReadOnlyError):
                call()
        ns = rep.ns
        from memd.core.schema import Kind, MemoryRecord

        for call in (lambda: ns.append([MemoryRecord.create(namespace="default", kind=Kind.FACT,
                                                            content="x")]),
                     lambda: ns.append_ops([{"op": "tombstone", "id": rid, "at": 1}]),
                     lambda: ns.rotate(), lambda: ns.compact(force=True),
                     lambda: ns.write_index_snapshot(), lambda: ns._persist_manifest(),
                     lambda: ns.rebuild_index()):
            with pytest.raises(ReadOnlyError):
                call()
        assert rep.get(rid)["content"] == "kept kilo"
    finally:
        rep.close()


# ------------------------------------------------- folds and tenures


def test_a_compaction_on_the_leader_rebuilds_the_replica_and_never_resurrects(be):
    lead = be.leader()
    rep = be.replica(replica_refresh_s=3600)
    try:
        soft = lead.add("soft deleted lima")[0]
        hard = lead.add("hard deleted mike")[0]
        keep = lead.add("kept november")[0]
        rep.ns.refresh()
        assert rep.get(soft) and rep.get(hard) and rep.get(keep)
        lead.delete(soft)
        lead.delete(hard, hard=True)
        lead.compact(force=True)       # retires both ops: replay cannot redo them
        lead.add("after oscar")
        rebuilds = rep.ns.rebuilds
        rep.ns.refresh()
        assert rep.get(soft) is None and rep.get(hard) is None, "a delete the compaction retired came back"
        assert rep.get(keep)["content"] == "kept november"
        assert "after oscar" in _texts(rep, "after oscar")
        assert b"hard deleted mike" not in _cache_bytes(rep)
        assert rep.ns.rebuilds == rebuilds + 1
    finally:
        rep.close()
        lead.close()


def test_rotations_on_the_leader_are_caught_up_without_a_rebuild(be):
    lead = be.leader()
    # rotate every few frames (object stores) / bytes (local): the namespace
    # is open already, so on its store
    lead.ns.wal_rotate_frames = 4
    lead.ns.wal_rotate_bytes = 2000
    rep = be.replica(replica_refresh_s=3600)
    try:
        lead.add("first papa")
        rep.ns.refresh()
        rebuilds = rep.ns.rebuilds
        ids = [lead.add(f"rotated record quebec {i}")[0] for i in range(11)]   # several rotations
        lead.delete(ids[2])
        lead.delete(ids[5], hard=True)
        assert len(lead.ns.manifest.segments) >= 2
        rep.ns.refresh()
        assert rep.ns.rebuilds == rebuilds, "a rotation must be caught up, not rebuilt"
        for i, rid in enumerate(ids):
            got = rep.get(rid)
            assert (got is None) == (i in (2, 5)), (i, got)
        assert rep.ns.applied_seq == lead.ns.manifest.seq
        # a refresh with nothing new is a no-op
        rep.ns.refresh()
        assert rep.ns.rebuilds == rebuilds
    finally:
        rep.close()
        lead.close()


def test_a_new_tenure_rebuilds_the_replica(be):
    lead = be.leader()
    rep = be.replica(replica_refresh_s=3600)
    try:
        a = lead.add("before the takeover romeo")[0]
        rep.ns.refresh()
        lead.close()
        lead = be.leader()                 # a new tenure: a new lineage
        b = lead.add("after the takeover sierra")[0]
        lead.delete(a)
        rebuilds = rep.ns.rebuilds
        rep.ns.refresh()
        assert rep.ns.rebuilds == rebuilds + 1
        assert rep.get(a) is None and rep.get(b)["content"] == "after the takeover sierra"
    finally:
        rep.close()
        lead.close()


def test_a_replica_bootstraps_from_the_index_snapshot(be, monkeypatch):
    from memd.storage.engine import NamespaceStore

    monkeypatch.setattr(NamespaceStore, "SNAPSHOT_MIN_RECORDS", 5)
    lead = be.leader()
    try:
        ids = [lead.add(f"snapshotted tango {i}")[0] for i in range(12)]
        lead.compact(force=True)          # publishes the snapshot
        assert lead.ns.manifest.snapshot_name
        tail = lead.add("after the snapshot uniform")[0]
        rep = be.replica()
        try:
            assert rep.ns.installed_snapshot, "the replica replayed instead of installing the snapshot"
            assert all(rep.get(i) for i in ids) and rep.get(tail)
        finally:
            rep.close()
    finally:
        lead.close()


# -------------------------------------------------------- hard deletes


def test_a_hard_delete_leaves_the_replicas_local_files(be):
    marker = "ERASE-ME-" + uuid.uuid4().hex
    lead = be.leader()
    rep = be.replica(replica_refresh_s=3600)
    try:
        for i in range(10):
            lead.add(f"filler victor {i}")
        rid = lead.add(f"secret {marker} whiskey")[0]
        rep.ns.refresh()
        assert rep.get(rid) and marker.encode() in _cache_bytes(rep)
        lead.delete(rid, hard=True)
        rep.ns.refresh()
        assert rep.get(rid) is None
        assert _wait(lambda: marker.encode() not in _cache_bytes(rep), timeout=20), \
            "the hard-deleted text is still in the replica's files"
        assert _wait(lambda: rep.ns.scrubbed_through >= rep.ns.applied_seq, timeout=20)
    finally:
        rep.close()
        lead.close()


@pytest.mark.parametrize("how", ["takeover", "bootstrap"])
def test_a_replica_built_from_durable_data_scrubs_the_hard_deletes_it_still_holds(be, how):
    """A hard delete the writer has not purged yet (its deadline is days
    away) is still in durable data: an older segment holds the record, and a
    newer segment's header carries the delete. A replica built from that
    data - rebuilt after a takeover, or bootstrapped - writes the record into
    its index and deletes it again, which leaves its text in the cache files
    (the SQLite WAL): it scrubs them, as it does for a delete it applies
    from the tail - sooner than the writer's own purge. It used to stamp
    such a build scrubbed and never scrub it."""
    marker = "ERASE-ME-" + uuid.uuid4().hex
    lead = be.leader()
    rep = None
    try:
        for i in range(8):
            lead.add(f"filler xray {i}")
        rid = lead.add(f"secret {marker} yankee")[0]
        lead.ns.rotate()                          # the record is in a segment
        if how == "takeover":
            rep = be.replica(replica_refresh_s=3600)
            assert rep.get(rid) and marker.encode() in _cache_bytes(rep)
        lead.delete(rid, hard=True)
        deleted_at = lead.ns.manifest.seq
        lead.ns.rotate()                          # the delete is folded: carried, its purge pending
        assert lead.ns.manifest.scrub_seq < deleted_at and lead.ns.pending_hard_deletes == 1
        if how == "takeover":
            lead.close()
            lead = be.leader()                    # a new tenure: the next refresh rebuilds
            rebuilds = rep.ns.rebuilds
            rep.ns.refresh()
            assert rep.ns.rebuilds == rebuilds + 1
        else:
            rep = be.replica(replica_refresh_s=3600)
        assert rep.get(rid) is None and rep.get(rid, include_deleted=True) is None
        assert _wait(lambda: marker.encode() not in _cache_bytes(rep), timeout=20), \
            "the hard-deleted text is still in the replica's files"
        assert _wait(lambda: rep.ns.scrubbed_through >= deleted_at, timeout=20)
        assert lead.ns.pending_hard_deletes == 1   # the writer has not purged it yet
    finally:
        if rep is not None:
            rep.close()
        lead.close()


def test_a_destroyed_namespace_drops_the_replica_cache(be):
    lead = be.leader()      # (a facade re-creates its own default namespace after a destroy)
    rep = be.replica(namespace="doomed", replica_refresh_s=3600)
    try:
        lead.add("about to be shredded xray", namespace="doomed")
        rep.ns.refresh()
        assert _texts(rep, "shredded xray")
        lead.destroy_namespace("doomed")
        lead.close()
        rep.ns.refresh()
        assert rep.search("shredded xray").items == []
        assert b"about to be shredded xray" not in _cache_bytes(rep)
        assert not rep.ns.exists
    finally:
        rep.close()


@pytest.fixture(scope="module")
def kms_server():
    """A moto KMS server (aws-kms keys: the wrapped data key is an object in
    the store, so a re-created namespace's new key is a new object)."""
    pytest.importorskip("moto")
    boto3 = pytest.importorskip("boto3")
    import socket

    from moto.server import ThreadedMotoServer

    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    srv = ThreadedMotoServer(ip_address="127.0.0.1", port=port, verbose=False)
    srv.start()
    url = f"http://127.0.0.1:{port}"
    arn = boto3.client("kms", endpoint_url=url, region_name="us-east-1", aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET).create_key()["KeyMetadata"]["Arn"]
    yield url, arn
    srv.stop()


@pytest.fixture(params=["local", "kms"])
def keys(request, monkeypatch):
    """Config for the key provider under test."""
    if request.param == "local":
        return {}
    url, arn = request.getfixturevalue("kms_server")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", KEY)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", SECRET)
    return {"key_provider": "aws-kms", "kms_endpoint_url": url, "kms_region": "us-east-1",
            "kms_key_id": arn}


@pytest.mark.parametrize("which", ["default", "other"])
def test_a_namespace_destroyed_and_created_again_between_refreshes_is_followed(be, which, keys):
    """Crypto-shredded and created again under the same name (a new data key,
    a new lineage) before the replica's next refresh saw it gone: the replica
    resolves the key again and follows the new namespace. It used to keep the
    shredded key: every refresh KeyCustodyError, every read unavailable, for
    good. Memory.destroy_namespace() on the facade's own namespace always
    does this - the facade creates it again at once."""
    ns = "default" if which == "default" else "reborn"
    lead = be.leader(**keys)
    rep = be.replica(namespace=ns, replica_refresh_s=3600, **keys)
    try:
        lead.add("the old secret alpha kilo", namespace=ns)
        rep.ns.refresh()
        assert _texts(rep, "secret") == {"the old secret alpha kilo"}
        lead.destroy_namespace(ns)
        lead.add("the new secret bravo lima", namespace=ns)
        rep.ns.refresh()
        assert _texts(rep, "secret") == {"the new secret bravo lima"}
        assert rep.ns._key_retry_at is None, "the new tenure's key was resolved only after a refusal"
        assert b"the old secret alpha kilo" not in _cache_bytes(rep)
        rid = lead.add("written after the rebirth charlie", namespace=ns)[0]
        rep.ns.refresh()
        assert rep.get(rid)["content"] == "written after the rebirth charlie"
        assert rep.ns.failures == 0
    finally:
        rep.close()
        lead.close()


def test_a_replicas_cache_is_deleted_when_it_closes(be):
    lead = be.leader()
    lead.add("transient yankee")
    lead.close()
    rep = be.replica()
    path = rep.ns.index.path
    assert os.path.exists(path)
    rep.close()
    assert not os.path.exists(path)
    assert not os.path.exists(os.path.dirname(path)), "the replica directory outlived its engine"


# ---------------------------------------------------------- key custody


def test_a_replica_with_the_wrong_key_refuses_and_writes_nothing(be, tmp_path):
    lead = be.leader()
    lead.add("only the right key reads this zulu")
    lead.close()
    before = be.objects()
    other = Backend(be.kind, tmp_path / "other")   # another deployment's keys directory
    other_lead = Memory(other.path, config=other.cfg()) if be.kind == "local" else None
    if other_lead is not None:
        other_lead.add("x")
        other_lead.close()
        wrong_keys = os.path.join(other.path, "keys")
        right_keys = os.path.join(be.path, "keys")
        shutil.rmtree(right_keys)
        shutil.copytree(wrong_keys, right_keys)
        before = be.objects()
        with pytest.raises(KeyCustodyError):
            be.replica()
    else:
        # S3 with `local` keys: a replica on a node without the keys
        with pytest.raises(KeyCustodyError):
            Memory(be.path, read_only=True, config=be.cfg(local_dir=str(tmp_path / "elsewhere")))
        os.makedirs(str(tmp_path / "elsewhere2" / "keys"), exist_ok=True)
        other.local = str(tmp_path / "elsewhere2")
        o = Memory(f"s3://{BUCKET}/rr-{uuid.uuid4().hex[:10]}", config=other.cfg())
        o.add("another deployment")
        o.close()
        shutil.rmtree(os.path.join(be.local, "keys"))
        shutil.copytree(os.path.join(other.local, "keys"), os.path.join(be.local, "keys"))
        before = be.objects()
        with pytest.raises(KeyCustodyError):
            be.replica()
    assert be.objects() == before


def _key_file(be, ns):
    keys = os.path.join(be.local if be.kind == "s3" else be.path, "keys")
    return os.path.join(keys, f"ns-{ns}.key")


@pytest.mark.parametrize("wrong", ["another namespace's", "the shredded incarnation's"])
def test_a_wrong_key_for_a_namespace_created_again_is_still_refused(be, wrong):
    """Resolving the key again after a rebirth must not open the door to the
    wrong one: a key that does not match the new namespace's data is
    refused, and nothing - old or new - is served. The shredded
    incarnation's key (an old backup of the keys directory) even unwraps
    fine; it must not stay in hand once refused: when the right key is
    restored, the next refresh resolves it."""
    from memd.storage.replica import ReplicaUnavailableError

    lead = be.leader()
    rep = be.replica(namespace="reborn", replica_refresh_s=3600)
    try:
        lead.add("the old secret delta", namespace="reborn")
        lead.add("another namespace's echo", namespace="bystander")
        rep.ns.refresh()
        with open(_key_file(be, "reborn"), "rb") as f:
            shredded = f.read()
        lead.destroy_namespace("reborn")
        lead.add("the new secret foxtrot", namespace="reborn")
        # the replica's node holds a key for it that is not the data's (the
        # writer keeps the right one in memory)
        with open(_key_file(be, "reborn"), "rb") as f:
            right = f.read()
        if wrong == "another namespace's":
            shutil.copyfile(_key_file(be, "bystander"), _key_file(be, "reborn"))
        else:
            with open(_key_file(be, "reborn"), "wb") as f:
                f.write(shredded)
        for _ in range(2):
            with pytest.raises(KeyCustodyError):
                rep.ns.refresh()
            with pytest.raises(ReplicaUnavailableError):
                rep.search("secret", max_staleness_ms=0)
        assert b"the old secret delta" not in _cache_bytes(rep)
        assert b"the new secret foxtrot" not in _cache_bytes(rep)
        # the right key restored: the next refresh resolves it (a refused key
        # is never kept - not even for the KEY_RETRY_S window)
        with open(_key_file(be, "reborn"), "wb") as f:
            f.write(right)
        rep.ns.refresh()
        assert _texts(rep, "secret") == {"the new secret foxtrot"}
    finally:
        rep.close()
        lead.close()


def test_a_custody_refusal_mid_refresh_resolves_the_key_again_once(be):
    """A key in hand that stopped matching within one lineage (it can only be
    a stale copy) is dropped and resolved again once; the refresh goes on."""
    lead = be.leader()
    rep = be.replica(replica_refresh_s=3600)
    try:
        lead.add("before golf")
        rep.ns.refresh()
        env = rep.ns.envelope
        if not env.enabled:
            pytest.skip("encryption is off")
        with env._mu:
            env._cache["default"] = b"\x01" * 32       # a stale copy of the key
        rid = lead.add("after hotel")[0]
        rep.ns.refresh()
        assert rep.get(rid)["content"] == "after hotel"
        assert rep.ns.failures == 0
    finally:
        rep.close()
        lead.close()


def test_a_rebuild_that_failed_part_way_runs_again_on_the_next_refresh(be, monkeypatch):
    """A rebuild discards the index before it reads durable data. One that
    failed part-way (the bucket stalled, a custody refusal) must run again
    on the next refresh: following only the log tail from there served an
    index missing everything the segments hold."""
    from memd.storage.replica import ReplicaStore

    lead = be.leader()
    rep = be.replica(replica_refresh_s=3600)
    try:
        a = lead.add("folded before the takeover india")[0]
        rep.ns.refresh()
        lead.close()
        lead = be.leader()                 # a new tenure: the replica's next refresh rebuilds
        b = lead.add("folded after the takeover juliet")[0]
        lead.compact(force=True)           # both in a segment now
        c = lead.add("in the tail kilo")[0]
        real = ReplicaStore._load_from
        calls = []

        def flaky(self, *args, **kw):
            calls.append(1)
            if len(calls) == 1:
                raise OSError("the bucket stalled mid-rebuild")
            return real(self, *args, **kw)

        monkeypatch.setattr(ReplicaStore, "_load_from", flaky)
        with pytest.raises(OSError):
            rep.ns.refresh()
        rep.ns.refresh()
        assert len(calls) >= 2, "the failed rebuild was not run again"
        for rid in (a, b, c):
            assert rep.get(rid), f"{rid} missing after the rebuild was retried"
    finally:
        rep.close()
        lead.close()


def test_a_custody_failure_in_a_rebuild_is_retried_on_the_next_refresh(be, keys, monkeypatch):
    """The key cannot be resolved while a rebuild runs (the provider is
    unreachable): that refresh fails and no read is served; the next one
    resolves the key again - it is not latched on the failure - and rebuilds
    from durable data."""
    from memd.storage.replica import ReplicaUnavailableError

    lead = be.leader(**keys)
    rep = be.replica(replica_refresh_s=3600, **keys)
    try:
        a = lead.add("before the takeover oscar")[0]
        rep.ns.refresh()
        lead.close()
        lead = be.leader(**keys)                 # a new tenure: the next refresh rebuilds
        lead.compact(force=True)
        b = lead.add("after the takeover papa")[0]
        env = rep.ns.envelope
        real = env.data_key
        broken = [True]

        def flaky(ns):
            if broken[0]:
                raise KeyCustodyError("the key provider is unreachable (test)")
            return real(ns)

        monkeypatch.setattr(env, "data_key", flaky)
        with pytest.raises(KeyCustodyError):
            rep.ns.refresh()
        with pytest.raises(ReplicaUnavailableError):
            rep.get(a, max_staleness_ms=60_000)
        broken[0] = False
        rep.ns.refresh()
        assert rep.get(a) and rep.get(b), "the retried refresh did not rebuild from durable data"
        assert rep.ns.status()["failures"] == 0
    finally:
        rep.close()
        lead.close()


def test_no_read_is_served_from_a_rebuild_that_failed(be, monkeypatch):
    """A rebuild discards the index first. A read that passed its freshness
    check before the rebuild began waits for it on the serve lock - and must
    not then read the empty (or half-built) index the failed rebuild left:
    the replica is unavailable until a rebuild completes."""
    from memd.storage.replica import ReplicaStore, ReplicaUnavailableError

    lead = be.leader()
    rep = be.replica(replica_refresh_s=3600)
    try:
        a = lead.add("served only when whole lima")[0]
        rep.ns.refresh()
        lead.close()
        lead = be.leader()                       # a new tenure: the next refresh rebuilds

        def stalled(self, *args, **kw):
            raise OSError("the bucket stalled mid-rebuild")

        monkeypatch.setattr(ReplicaStore, "_load_from", stalled)
        with pytest.raises(OSError):
            rep.ns.refresh()
        # (as if the freshness check had passed just before the rebuild began)
        monkeypatch.setattr(ReplicaStore, "ensure_fresh", lambda self, *a, **kw: None)
        with pytest.raises(ReplicaUnavailableError):
            rep.search("whole lima")
        with pytest.raises(ReplicaUnavailableError):
            rep.get(a)
    finally:
        rep.close()
        lead.close()


@pytest.mark.parametrize("tail_in_time", [True, False], ids=["tail-in-time", "tail-late"])
def test_a_read_that_waited_for_a_rebuild_never_sees_it_before_its_tail(be, monkeypatch, tail_in_time):
    """A rebuild builds the index again from durable data; the refresh that
    ran it applies the log tail after it - and the tail may hold a hard
    delete this replica already applied and served. A read that passed its
    freshness check just before the rebuild began waits for it on the serve
    lock: it must not then read the rebuilt index before the tail is applied
    (the deleted record served again, older than what the replica already
    served). It waits for that refresh, at most refresh_wait_s, and is
    refused when it does not complete in time; what it reports (applied_seq,
    age_ms) is the state it was served from."""
    from memd.storage.replica import ReplicaStore, ReplicaUnavailableError

    lead = be.leader()
    rep = be.replica(replica_refresh_s=3600,
                     replica_refresh_wait_ms=5000 if tail_in_time else 1000)
    release = threading.Event()
    try:
        x = lead.add("erased before the takeover yankee")[0]
        lead.add("kept across the takeover zulu")
        lead.ns.rotate()                         # x is in a segment; its delete will be in the tail
        rep.ns.refresh()
        assert rep.get(x)
        lead.delete(x, hard=True)
        rep.ns.refresh()
        assert rep.get(x, include_deleted=True) is None     # served: the delete
        lead.close()
        lead = be.leader()                       # a new tenure: the next refresh rebuilds

        rebuilt = threading.Event()
        real_rebuild, real_tail = ReplicaStore._rebuild, ReplicaStore._read_tail

        def rebuild(self, *a, **kw):
            real_rebuild(self, *a, **kw)
            rebuilt.set()

        def tail(self):
            if rebuilt.is_set():
                release.wait(10)                 # the tail read after the rebuild is slow
            return real_tail(self)

        monkeypatch.setattr(ReplicaStore, "_rebuild", rebuild)
        monkeypatch.setattr(ReplicaStore, "_read_tail", tail)
        refresh = threading.Thread(target=rep.ns.refresh, daemon=True)
        refresh.start()
        assert rebuilt.wait(10)
        # (as if the freshness check had passed just before the rebuild began)
        monkeypatch.setattr(ReplicaStore, "ensure_fresh", lambda self, *a, **kw: None)
        info = {}
        t0 = time.monotonic()
        if tail_in_time:
            threading.Timer(0.4, release.set).start()
            assert rep.get(x, include_deleted=True, read_info=info) is None
            waited = time.monotonic() - t0
            assert waited >= 0.4, "served before the tail was applied"
            refresh.join(10)
            assert info["served_by"] == "replica" and info["applied_seq"] == rep.ns.applied_seq
            assert info["age_ms"] >= 400, info    # its refresh started before the rebuild
        else:
            with pytest.raises(ReplicaUnavailableError):
                rep.get(x, include_deleted=True)
            assert time.monotonic() - t0 < 3.0
            with pytest.raises(ReplicaUnavailableError):
                rep.search("erased before the takeover yankee")
            release.set()
            refresh.join(10)
            assert rep.get(x, include_deleted=True, read_info=info) is None
            assert info["applied_seq"] == rep.ns.applied_seq
        assert _texts(rep, "takeover") == {"kept across the takeover zulu"}
    finally:
        release.set()
        rep.close()
        lead.close()


# -------------------------------------------------------------- bounds


def test_the_replica_table_is_bounded_and_idle_replicas_close(be):
    lead = be.leader()
    for ns in ("n1", "n2", "n3"):
        lead.add(f"in {ns} alpha", namespace=ns)
    rep = be.replica(max_replicas=2, replica_idle_s=0.5)
    try:
        for ns in ("n1", "n2", "n3"):
            assert _texts(rep, "alpha", namespace=ns) == {f"in {ns} alpha"}
        open_now = rep.engine.open_replicas()
        assert len([n for n in open_now if n != "default"]) <= 2
        assert _wait(lambda: set(rep.engine.open_replicas()) <= {"default"}, timeout=10)
    finally:
        rep.close()
        lead.close()


def test_a_stale_replica_refreshes_inline_before_it_serves(be):
    lead = be.leader()
    rep = be.replica(replica_refresh_s=3600)    # the background refresh never runs
    try:
        rid = lead.add("fresh enough bravo")[0]
        got = rep.get(rid, max_staleness_ms=0)    # forces a refresh before the read
        assert got and got["content"] == "fresh enough bravo"
        info = {}
        rep.get(rid, max_staleness_ms=60_000, read_info=info)
        assert info["served_by"] == "replica" and info["applied_seq"] >= 1
    finally:
        rep.close()
        lead.close()


def test_a_read_never_waits_long_on_a_stuck_refresh(be):
    """A refresh stuck on the store (an outage that accepts connections and
    never answers) holds the refresh lock. A read within its staleness bound
    is served at once; a staler one waits at most refresh_wait_s - and never
    longer than the stuck refresh has already run - then is refused
    (ReplicaUnavailableError: the router sends it to the writer). It used to
    queue on the lock behind the stuck refresh for minutes."""
    from memd.storage.replica import ReplicaUnavailableError

    lead = be.leader()
    rep = be.replica(replica_refresh_s=3600, replica_max_staleness_ms=200)
    release = threading.Event()
    try:
        lead.add("served while the bucket hangs tango")
        rep.ns.refresh()
        store = rep.ns.store
        real = store.get_versioned

        def hung(key):
            release.wait(8)
            return real(key)

        store.get_versioned = hung
        def refresh_quietly():
            try:
                rep.ns.refresh()
            except Exception:  # noqa: BLE001 - only its holding the lock matters here
                pass

        stuck = threading.Thread(target=refresh_quietly, daemon=True)
        stuck.start()
        time.sleep(0.4)                      # the replica is now staler than 200 ms
        t0 = time.monotonic()
        with pytest.raises(ReplicaUnavailableError):
            rep.search("bucket hangs")
        assert time.monotonic() - t0 < 2.0, "a read queued behind the stuck refresh"
        t0 = time.monotonic()
        with pytest.raises(ReplicaUnavailableError):
            rep.get("whatever")
        assert time.monotonic() - t0 < 0.5, "the refresh in flight is past the wait: refuse at once"
        t0 = time.monotonic()
        assert _texts(rep, "bucket hangs", max_staleness_ms=600_000) == {"served while the bucket hangs tango"}
        assert time.monotonic() - t0 < 1.0, "a read within its bound must not wait for the refresh"
        release.set()
        store.get_versioned = real
        stuck.join(10)
        assert _wait(lambda: _texts(rep, "bucket hangs") == {"served while the bucket hangs tango"})
    finally:
        release.set()
        rep.close()
        lead.close()


class _Blackhole:
    """A TCP proxy in front of MinIO that can start accepting connections
    and never answering them (an outage that hangs instead of refusing)."""

    def __init__(self, target: str):
        import socket
        from urllib.parse import urlparse

        u = urlparse(target)
        self.target = (u.hostname, u.port)
        self.hang = False
        self.held = []
        self.ls = socket.socket()
        self.ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.ls.bind(("127.0.0.1", 0))
        self.ls.listen(64)
        self.url = f"http://127.0.0.1:{self.ls.getsockname()[1]}"
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        import socket

        while True:
            try:
                c, _ = self.ls.accept()
            except OSError:
                return
            if self.hang:
                self.held.append(c)
                continue
            u = socket.create_connection(self.target)
            for a, b in ((c, u), (u, c)):
                threading.Thread(target=self._pipe, args=(a, b), daemon=True).start()

    def _pipe(self, a, b):
        try:
            while True:
                d = a.recv(65536)
                if not d or self.hang:
                    break
                b.sendall(d)
        except OSError:
            pass
        for x in (a, b):
            try:
                x.close()
            except OSError:
                pass

    def close(self):
        self.ls.close()
        for c in self.held:
            c.close()


def test_a_replicas_store_calls_have_short_timeouts(tmp_path):
    """A replica reads the bucket through a client of its own with short
    connect/read timeouts and few attempts (replica_connect_timeout_s,
    replica_read_timeout_s, replica_max_attempts): a hung bucket fails a
    refresh in seconds, not botocore's 60 s x 5 attempts."""
    be = Backend("s3", tmp_path)
    proxy = _Blackhole(ENDPOINT)
    lead = be.leader()
    rep = Memory(be.path, read_only=True, config=be.cfg(
        s3_endpoint_url=proxy.url, replica_refresh_s=3600, replica_connect_timeout_s=1,
        replica_read_timeout_s=1, replica_max_attempts=1))
    try:
        lead.add("before the bucket hangs uniform")
        rep.ns.refresh()
        proxy.hang = True
        done = threading.Event()
        err = []

        def run():
            try:
                rep.ns.refresh()
            except Exception as ex:  # noqa: BLE001
                err.append(ex)
            done.set()

        t0 = time.monotonic()
        threading.Thread(target=run, daemon=True).start()
        assert done.wait(10), "a refresh against a hung bucket did not time out"
        assert err and time.monotonic() - t0 < 10
        # the leader's own data client keeps its long read timeout (an append
        # is a conditional PUT: a retry after a lost response would conflict)
        assert lead.engine.store._client.meta.config.read_timeout == 60
    finally:
        proxy.hang = False
        proxy.close()
        rep.close()
        lead.close()


def test_a_failed_replica_open_is_not_retried_by_every_read(be, monkeypatch):
    """A replica that cannot open (the key provider unreachable: every open
    spent 12-20 s on KMS retries before the read fell back) is remembered
    per namespace: eventual reads fail over at once for a backoff (5 s,
    doubling to 60 s), then one open is tried again; success forgets it."""
    from memd.storage.crypto import KeyUnavailableError
    from memd.storage.engine import StorageEngine
    from memd.storage.replica import ReplicaStore, ReplicaUnavailableError

    monkeypatch.setattr(StorageEngine, "REPLICA_OPEN_BACKOFF_S", (0.4, 1.6), raising=False)
    writer = be.leader(namespace="elsewhere")
    reader = be.leader(namespace="here")         # an eventual read of "elsewhere" is a replica read
    opens = []
    real = ReplicaStore._open

    def unreachable(self):
        opens.append(time.monotonic())
        if fail[0]:
            raise KeyUnavailableError("aws-kms Decrypt failed (test: unreachable)")
        return real(self)

    fail = [True]
    monkeypatch.setattr(ReplicaStore, "_open", unreachable)
    try:
        writer.add("followed once the key is back victor")
        for _ in range(5):
            with pytest.raises(ReplicaUnavailableError):
                reader.search("victor", namespace="elsewhere", consistency="eventual")
        assert len(opens) == 1, f"{len(opens)} opens: every read retried the bootstrap"
        time.sleep(0.5)                          # the backoff (0.4 s) passed: one more try
        for _ in range(3):
            with pytest.raises(ReplicaUnavailableError):
                reader.search("victor", namespace="elsewhere", consistency="eventual")
        assert len(opens) == 2
        time.sleep(0.5)                          # the backoff doubled (0.8 s): not yet
        with pytest.raises(ReplicaUnavailableError):
            reader.search("victor", namespace="elsewhere", consistency="eventual")
        assert len(opens) == 2
        fail[0] = False
        time.sleep(0.5)
        res = reader.search("victor", namespace="elsewhere", consistency="eventual")
        assert res.served_by == "replica" and len(opens) == 3
        assert {i.content for i in res.items} == {"followed once the key is back victor"}
    finally:
        reader.close()
        writer.close()


def test_a_slow_replica_open_holds_an_eventual_read_no_longer_than_its_wait(be, monkeypatch):
    """An eventual read that can go to the writer waits for the replica's
    open at most replica_refresh_wait_s - as for a refresh - and never past
    that into an open already running: the read goes to the writer, and the
    open goes on in the background; once it completes, the replica serves.
    The read used to wait out the whole open (a KMS that hangs: 10-11 s)."""
    from memd.storage.replica import ReplicaStore, ReplicaUnavailableError

    writer = be.leader(namespace="elsewhere")
    reader = be.leader(namespace="here")         # an eventual read of "elsewhere" is a replica read
    release = threading.Event()
    real = ReplicaStore._open

    def slow(self):
        release.wait(10)
        return real(self)

    monkeypatch.setattr(ReplicaStore, "_open", slow)
    try:
        writer.add("served once the open completes xray")
        t0 = time.monotonic()
        with pytest.raises(ReplicaUnavailableError):
            reader.search("xray", namespace="elsewhere", consistency="eventual")
        assert time.monotonic() - t0 < 2.0, "the read waited out the replica's open"
        t0 = time.monotonic()
        with pytest.raises(ReplicaUnavailableError):
            reader.get("whatever", namespace="elsewhere", consistency="eventual")
        assert time.monotonic() - t0 < 0.5, "the open in flight is past the wait: fail over at once"
        release.set()
        assert _wait(lambda: reader.engine.peek_replica("elsewhere") is not None)
        res = reader.search("xray", namespace="elsewhere", consistency="eventual")
        assert res.served_by == "replica"
        assert {i.content for i in res.items} == {"served once the open completes xray"}
    finally:
        release.set()
        reader.close()
        writer.close()


def test_a_hung_key_provider_holds_an_eventual_read_no_longer_than_its_wait(tmp_path, kms_server,
                                                                          monkeypatch):
    """A KMS that accepts connections and never answers: the replica's open
    times out on its key unwrap (replica_read_timeout_s per attempt), and
    the first eventual read of the namespace used to wait for all of it. It
    goes to the writer after replica_refresh_wait_s (1 s), the next one at
    once while the open is still in flight."""
    from memd.storage.replica import ReplicaUnavailableError

    url, arn = kms_server
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", KEY)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", SECRET)
    keys = {"key_provider": "aws-kms", "kms_region": "us-east-1", "kms_key_id": arn}
    be = Backend("local", tmp_path)
    proxy = _Blackhole(url)
    writer = be.leader(namespace="elsewhere", kms_endpoint_url=url, **keys)
    reader = be.leader(namespace="here", kms_endpoint_url=proxy.url, replica_read_timeout_s=4,
                       replica_max_attempts=1, **keys)
    try:
        writer.add("behind a hung key provider yankee")
        proxy.hang = True
        t0 = time.monotonic()
        with pytest.raises(ReplicaUnavailableError):
            reader.search("yankee", namespace="elsewhere", consistency="eventual")
        assert time.monotonic() - t0 < 2.5, "the read waited out the key provider's timeouts"
        t0 = time.monotonic()
        with pytest.raises(ReplicaUnavailableError):
            reader.search("yankee", namespace="elsewhere", consistency="eventual")
        assert time.monotonic() - t0 < 0.5
    finally:
        proxy.hang = False
        proxy.close()
        reader.close()
        writer.close()


def test_a_replicas_key_provider_calls_have_short_timeouts(tmp_path, kms_server, monkeypatch):
    """A replica unwraps data keys through a KMS client of its own with the
    replica timeouts: a KMS that hangs fails the open in seconds (it used
    to hold it for botocore's 60 s x 5 attempts)."""
    url, arn = kms_server
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", KEY)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", SECRET)
    keys = {"key_provider": "aws-kms", "kms_region": "us-east-1", "kms_key_id": arn}
    be = Backend("local", tmp_path)
    proxy = _Blackhole(url)
    lead = be.leader(kms_endpoint_url=url, **keys)
    rep = be.replica(kms_endpoint_url=proxy.url, replica_refresh_s=3600, replica_connect_timeout_s=1,
                     replica_read_timeout_s=1, replica_max_attempts=1, **keys)
    try:
        lead.add("behind a hung key provider whiskey", namespace="sealed")
        proxy.hang = True
        done = threading.Event()
        err = []

        def run():
            try:
                rep.search("whiskey", namespace="sealed")
            except Exception as ex:  # noqa: BLE001
                err.append(ex)
            done.set()

        threading.Thread(target=run, daemon=True).start()
        assert done.wait(10), "a replica open against a hung KMS did not time out"
        assert err, "served without the key?"
    finally:
        proxy.hang = False
        proxy.close()
        rep.close()
        lead.close()


def test_concurrent_reads_during_refreshes_and_rebuilds(be):
    lead = be.leader()
    rep = be.replica(replica_refresh_s=0.05)
    stop = threading.Event()
    errors = []

    def reader():
        while not stop.is_set():
            try:
                rep.search("concurrent charlie")
            except Exception as ex:  # noqa: BLE001
                errors.append(ex)

    ts = [threading.Thread(target=reader) for _ in range(3)]
    for t in ts:
        t.start()
    try:
        for i in range(30):
            rid = lead.add(f"concurrent charlie {i}")[0]
            if i % 10 == 9:
                lead.delete(rid, hard=True)
                lead.compact(force=True)
    finally:
        stop.set()
        for t in ts:
            t.join()
    try:
        assert not errors, errors[:3]
        assert _wait(lambda: rep.ns.applied_seq == lead.ns.manifest.seq)
    finally:
        rep.close()
        lead.close()
