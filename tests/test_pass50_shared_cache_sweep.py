"""Pass 50: a cache sweep never deletes files another process serves from.

Several processes may share one local cache directory (local_dir), each
serving its own namespaces from it. The stale-cache sweep (pass 44) dropped a
namespace's copy when it was superseded and "no other process has it open"
- a check, then the delete: a process that opened the namespace in between
lost its live files (fu2-verify/sweep/shared_race.py: A1's sweep checked
x, A2 opened x, A1 deleted x.sqlite, the tantivy copy and the sidecar under
A2). And with the .sqlite missing - briefly, while an open rebuilds it -
the "orphaned" branch deleted the tantivy copy and the sidecar with no
check at all.

Now every process holds a per-namespace cache lock (`<ns>.lock` beside the
cache files) shared for as long as it has the namespace's cache open; a
sweep decides and deletes only under that lock held exclusively, taken
without waiting - one it cannot take is skipped until the next sweep.
These tests stand two engines in for the two processes: flock locks of
separately opened files conflict within one process as across processes.
"""
import os
import threading

import pytest

from memd.core.schema import Kind, MemoryRecord
import memd.storage.engine as E
from memd.storage.engine import StorageEngine

NS = "x"


def _engine(root: str, cache: str) -> StorageEngine:
    return StorageEngine(root, cache_dir=cache, cache_sweep_s=0)


def _rec(content: str) -> MemoryRecord:
    return MemoryRecord.create(namespace=NS, kind=Kind.FACT, content=content)


def _cache_files(cache: str) -> list[str]:
    try:
        names = os.listdir(cache)
    except FileNotFoundError:
        return []
    return sorted(n for n in names if n.startswith(f"{NS}.") and not n.endswith(".lock"))


@pytest.fixture
def shared(tmp_path):
    """A namespace with a closed cache copy, and two engines sharing the
    data root and the cache directory: A sweeps, B serves."""
    root, cache = str(tmp_path / "data"), str(tmp_path / "cache")
    seed = _engine(root, cache)
    seed.namespace(NS).append([_rec("a fact the cache holds")])
    seed.close()
    assert f"{NS}.sqlite" in _cache_files(cache), "precondition: a cache copy"
    a, b = _engine(root, cache), _engine(root, cache)
    yield a, b, cache
    for e in (a, b):
        try:
            e.close()
        except Exception:  # noqa: BLE001 - teardown
            pass


def _serves(b: StorageEngine, cache: str, n: int) -> None:
    ns = b.namespace(NS)
    assert len(ns.index.all_records()) == n
    path = os.path.join(cache, f"{NS}.sqlite")
    assert os.path.exists(path), "B's index file was deleted under it"
    (con_file,) = [r[2] for r in ns.index._con.execute("PRAGMA database_list") if r[1] == "main"]
    assert os.stat(con_file).st_ino == os.stat(path).st_ino


def test_a_namespace_opened_during_the_sweep_keeps_its_files(shared, monkeypatch):
    a, b, cache = shared
    monkeypatch.setattr(StorageEngine, "_cache_superseded", lambda self, ns: "taken_over")
    real = E._unused_elsewhere
    opened = threading.Event()
    out: dict = {}

    def open_b():
        try:
            b.namespace(NS).append([_rec("written by B while A sweeps")])
            out["ok"] = True
        except Exception as ex:  # noqa: BLE001 - reported below
            out["ok"] = ex
        opened.set()

    def checked_then_b_opens(path):
        r = real(path)
        if not path.startswith(cache + os.sep) or "thread" in out:
            return r            # another test's leftover sweeper: not this race
        t = threading.Thread(target=open_b, daemon=True)
        t.start()
        opened.wait(1.0)        # B opens between A's check and A's delete - unless it must wait
        out["thread"] = t
        return r

    monkeypatch.setattr(E, "_unused_elsewhere", checked_then_b_opens)
    dropped = a.sweep_stale_caches()
    out["thread"].join(10)
    assert out.get("ok") is True, out.get("ok")
    # either A dropped the copy before B opened it (B then built a new one
    # from durable data), or it kept it: never deleted under B
    assert dropped in ([], [NS])
    _serves(b, cache, 2)
    assert set(_cache_files(cache)) >= {f"{NS}.sqlite"}


def test_an_open_namespace_without_its_sqlite_file_is_not_orphaned(shared):
    a, b, cache = shared
    b.namespace(NS)                             # B serves x from the shared cache
    side = os.path.join(cache, f"{NS}.usearch")
    os.makedirs(side, exist_ok=True)
    with open(os.path.join(side, "live"), "wb") as f:
        f.write(b"B's sidecar")
    held = os.path.join(cache, f"{NS}.sqlite.held")
    os.rename(os.path.join(cache, f"{NS}.sqlite"), held)   # the moment it is missing
    try:
        assert a.sweep_stale_caches() == []
        assert os.path.exists(os.path.join(side, "live")), "B's sidecar was deleted as orphaned"
    finally:
        os.rename(held, os.path.join(cache, f"{NS}.sqlite"))


def test_a_closed_superseded_copy_is_still_dropped(shared, monkeypatch):
    a, b, cache = shared
    b.namespace(NS)
    b.close()
    monkeypatch.setattr(StorageEngine, "_cache_superseded", lambda self, ns: "taken_over")
    assert a.sweep_stale_caches() == [NS]
    assert _cache_files(cache) == []
    assert not os.path.exists(os.path.join(cache, f"{NS}.lock")), "the dropped copy's lock file was left"
    # and the namespace opens again, from durable data
    c = _engine(os.path.dirname(cache) + "/data", cache)
    try:
        assert len(c.namespace(NS).index.all_records()) == 1
    finally:
        c.close()


def test_a_sweep_skips_a_namespace_open_in_another_process(shared, monkeypatch):
    a, b, cache = shared
    b.namespace(NS)
    monkeypatch.setattr(StorageEngine, "_cache_superseded", lambda self, ns: "taken_over")
    real = E._unused_elsewhere
    monkeypatch.setattr(E, "_unused_elsewhere",           # an older check that cannot tell
                        lambda path: path.startswith(cache + os.sep) or real(path))
    assert a.sweep_stale_caches() == []
    _serves(b, cache, 1)
