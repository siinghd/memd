"""Pass 44: a node drops its local copy of a namespace another node took over.

Each node keeps a plaintext copy of every namespace it served - the SQLite
index (and its -wal/-shm), the tantivy copy, the ANN sidecar - in its local
cache directory. A node that lost a namespace (its lease was taken over, or
it released it cleanly and another node took it) kept that copy until it
took the namespace back: records hard-deleted - and purged - on the new
owner stayed in its files (stale_node.py: A's _cache/t.sqlite held the
purged text while B served the namespace).

Now a node drops a namespace's copy:
  - once it is closed after the lease was lost (a fenced store dropped on
    its next use, or closed by the LRU / engine close);
  - when another node has taken the namespace over since this node last
    served it - another tenure's lineage in the manifest - checked at
    startup and every cache_sweep_s for each namespace it does not have open.
A copy this node was the last to serve is kept: a plain reopen with no
other owner in between is still warm.
"""
import os
import time
import uuid

import pytest

from memd.core.schema import Kind, MemoryRecord
from memd.storage.engine import NamespaceBusyError, StorageEngine

ENDPOINT = os.environ.get("MEMD_TEST_S3_ENDPOINT")
BUCKET = os.environ.get("MEMD_TEST_S3_BUCKET", "memd-engine")
KEY = os.environ.get("MEMD_TEST_S3_KEY", "minioadmin")
SECRET = os.environ.get("MEMD_TEST_S3_SECRET", "minioadmin")
MARKER = "ERASE-ME-qx44-stale-node"

pytestmark = pytest.mark.s3


def _s3(prefix, **kw):
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
    return S3ObjectStore(bucket=BUCKET, prefix=prefix, endpoint_url=ENDPOINT, access_key=KEY,
                         secret_key=SECRET, region="us-east-1", **kw)


def _node(tmp_path, name, prefix, ttl=10.0) -> StorageEngine:
    store = _s3(prefix, lease_ttl_s=ttl, lease_holder=f"{name}@{uuid.uuid4().hex[:6]}")
    return StorageEngine(str(tmp_path / name), store=store, cache_dir=str(tmp_path / name / "cache"),
                         max_open_namespaces=1)


def _rec(ns, content):
    return MemoryRecord.create(namespace=ns, kind=Kind.FACT, content=content)


def _files_with(d, needle: bytes) -> list[str]:
    out = []
    for dp, _ds, fs in os.walk(str(d)):
        for f in fs:
            try:
                with open(os.path.join(dp, f), "rb") as fh:
                    if needle in fh.read():
                        out.append(os.path.relpath(os.path.join(dp, f), str(d)))
            except OSError:
                pass
    return sorted(out)


def _write(e: StorageEngine, ns: str, n: int = 5) -> MemoryRecord:
    s = e.namespace(ns)
    victim = _rec(ns, f"victim {MARKER}")
    s.append([victim] + [_rec(ns, f"filler {i}") for i in range(n)])
    s.index.flush()
    return victim


def _purge(e: StorageEngine, ns: str, victim: MemoryRecord) -> None:
    """Hard-delete `victim` and purge it (a compaction, as at its deadline)."""
    s = e.namespace(ns)
    s.append_op({"op": "hard_delete", "id": victim.id, "deadline": 0})
    s.index.hard_delete(victim.id)
    assert s.compact().hard_deleted_purged == 1


def _wait(cond, timeout=15.0) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if cond():
            return True
        time.sleep(0.1)
    return cond()


def test_a_running_node_drops_its_copy_once_another_node_took_the_namespace(tmp_path, monkeypatch):
    monkeypatch.setenv("MEMD_CACHE_SWEEP_S", "0.5")
    prefix = f"sn-{uuid.uuid4().hex[:8]}"
    A, B = _node(tmp_path, "A", prefix), _node(tmp_path, "B", prefix)
    cache_a = tmp_path / "A" / "cache"
    try:
        victim = _write(A, "t")
        A.namespace("u").append([_rec("u", "u1")])   # the LRU closes t: a clean release
        assert "t" not in A.open_namespaces()
        assert _files_with(cache_a, MARKER.encode()) == ["t.sqlite"], "positive control"
        _purge(B, "t", victim)                       # B takes t over and purges the record
        assert _wait(lambda: not _files_with(cache_a, MARKER.encode())), \
            f"A keeps the text B purged: {_files_with(cache_a, MARKER.encode())}"
        assert not os.path.exists(cache_a / "t.sqlite")
        assert os.path.exists(cache_a / "u.sqlite"), "the open namespace's cache was touched"
    finally:
        A.close()
        B.close()


def test_a_copy_nobody_else_took_stays_warm(tmp_path, monkeypatch):
    """Control: the sweep keeps the copy of a namespace this node was the
    last to serve, and its reopen replays nothing."""
    monkeypatch.setenv("MEMD_CACHE_SWEEP_S", "0.2")
    prefix = f"sn-{uuid.uuid4().hex[:8]}"
    A = _node(tmp_path, "A", prefix)
    try:
        _write(A, "t")
        A.namespace("u")                          # t closed (released) here
        path = tmp_path / "A" / "cache" / "t.sqlite"
        ino = os.stat(path).st_ino
        time.sleep(1.0)                           # several sweeps
        t = A.namespace("t")
        assert not t._replayed_at_open, "a warm reopen replayed or rebuilt the cache"
        assert os.stat(path).st_ino == ino
        assert t.index.stats()["records"] == 6
    finally:
        A.close()


def test_a_restarted_node_drops_the_copies_other_nodes_superseded(tmp_path):
    prefix = f"sn-{uuid.uuid4().hex[:8]}"
    A = _node(tmp_path, "A", prefix)
    victim = _write(A, "t")
    _write(A, "u")
    A.close()                                     # a graceful stop: every lease released
    cache_a = tmp_path / "A" / "cache"
    assert _files_with(cache_a, MARKER.encode()) == ["t.sqlite", "u.sqlite"], "positive control"
    ino_u = os.stat(cache_a / "u.sqlite").st_ino
    B = _node(tmp_path, "B", prefix)
    _purge(B, "t", victim)
    B.close()
    A = _node(tmp_path, "A", prefix)              # restarts (default sweep interval)
    try:
        assert _wait(lambda: not os.path.exists(cache_a / "t.sqlite")), \
            "the startup sweep kept a copy another node superseded"
        assert _files_with(cache_a, MARKER.encode()) == ["u.sqlite"]
        u = A.namespace("u")                      # nobody else served u: still warm
        assert not u._replayed_at_open and os.stat(cache_a / "u.sqlite").st_ino == ino_u
    finally:
        A.close()


@pytest.mark.parametrize("notice", ["reopen", "evict"])
def test_a_node_that_lost_the_lease_drops_its_copy(tmp_path, monkeypatch, notice):
    monkeypatch.setenv("MEMD_CACHE_SWEEP_S", "3600")   # only the startup sweep: not what drops it
    prefix = f"sn-{uuid.uuid4().hex[:8]}"
    A = _node(tmp_path, "A", prefix, ttl=2.0)
    cache_a = tmp_path / "A" / "cache"
    B = None
    try:
        victim = _write(A, "t")
        a = A.store
        a._lease_stop.set()                       # A stalls: its lease goes stale
        time.sleep(2.5)
        B = _node(tmp_path, "B", prefix, ttl=2.0)
        _purge(B, "t", victim)                    # B reclaims t and purges the record
        a._renew_one("t", a._leases.get("t") or "x")   # A notices: fenced
        assert "t" in a._fenced
        assert _files_with(cache_a, MARKER.encode()), "positive control"
        if notice == "reopen":
            with pytest.raises(NamespaceBusyError):
                A.namespace("t")                  # the stale store is dropped, B holds t
        else:
            A.namespace("u")                      # the LRU closes the lost store
            assert "t" not in A.open_namespaces()
        assert _files_with(cache_a, MARKER.encode()) == [], "A keeps the text B purged"
        assert not os.path.exists(cache_a / "t.sqlite")
    finally:
        A.close()
        if B is not None:
            B.close()


def test_a_copy_another_process_serves_from_is_kept(tmp_path, monkeypatch):
    """Several processes sharing one cache directory serve from the same
    files: a superseded copy another process still has open is not deleted
    under it (the next sweep drops it once that process let go)."""
    import subprocess
    import sys

    monkeypatch.setenv("MEMD_CACHE_SWEEP_S", "0")   # no sweeper thread: sweeps run here
    prefix = f"sn-{uuid.uuid4().hex[:8]}"
    A = _node(tmp_path, "A", prefix)
    victim = _write(A, "t")
    A.close()
    B = _node(tmp_path, "B", prefix)
    _purge(B, "t", victim)                        # another tenure: A's copy is superseded
    B.close()
    path = tmp_path / "A" / "cache" / "t.sqlite"
    holder = subprocess.Popen(
        [sys.executable, "-c", "import sqlite3, sys; c = sqlite3.connect(sys.argv[1]); "
         "c.execute('SELECT COUNT(*) FROM meta').fetchone(); print('open', flush=True); "
         "sys.stdin.readline()", str(path)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert holder.stdout.readline().strip() == "open"
        A = _node(tmp_path, "A", prefix)
        assert A.sweep_stale_caches() == [], "deleted a cache another process has open"
        assert os.path.exists(path)
    finally:
        holder.stdin.write("\n")
        holder.stdin.flush()
        holder.wait(10)
    try:
        assert A.sweep_stale_caches() == ["t"]
        assert not os.path.exists(path)
    finally:
        A.close()
