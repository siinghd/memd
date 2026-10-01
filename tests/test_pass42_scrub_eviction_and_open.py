"""Pass 42: an LRU eviction no longer interrupts a purge's scrub, and an open
never waits for a reader of its index to finish one.

Pass 41 moved the purge scrub out of the namespace lock and made a close
interrupt it (the next open finishes it). The LRU used to skip a namespace
whose compaction was scrubbing - it could not take the namespace lock - and
now it could: opening another namespace evicted the one whose scrub was
waiting for a reader memd does not control, the close interrupted the scrub,
and the next open of that namespace ran the scrub again INLINE - blocking
for the reader's whole lifetime (a 15 s reader: the reopen took 14.45 s,
0 s before pass 41).

Now the LRU skips a namespace with a scrub in progress, and an open waits
for its scrub only briefly (OPEN_SCRUB_WAIT_S) before finishing it in the
background - still never giving up. Meanwhile the purged records are not
served (their rows went with the purge; only the file still holds bytes),
no index snapshot is published (it could carry those bytes), and the cache
is not stamped scrubbed until the bytes are gone.
"""
import os
import sqlite3
import threading
import time

from memd.core.schema import MemoryRecord, Scope
from memd.index.sqlite_index import IndexFilter
from memd.storage.engine import NamespaceStore, StorageEngine

MARKER = "ERASE-ME-qx42-scrub-open"
HOLD_S = 8.0


def _files_with(root, needle: bytes) -> list[str]:
    out = []
    for dirpath, _dirs, files in os.walk(str(root)):
        for fn in files:
            path = os.path.join(dirpath, fn)
            try:
                with open(path, "rb") as f:
                    if needle in f.read():
                        out.append(os.path.relpath(path, str(root)))
            except OSError:
                pass
    return sorted(out)


def _rec(content: str, ns: str = "n") -> MemoryRecord:
    return MemoryRecord.create(namespace=ns, kind="raw_event", content=content,
                               scope=Scope(user="alice"))


def _foreign_reader(path: str) -> sqlite3.Connection:
    """A connection memd knows nothing about, holding a read snapshot."""
    con = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
    con.execute("BEGIN")
    con.execute("SELECT COUNT(*) FROM records").fetchone()
    return con


def _release(con: sqlite3.Connection) -> None:
    try:
        con.execute("COMMIT")
    finally:
        con.close()


def _purging(e: StorageEngine, monkeypatch):
    """Namespace "n" with a hard-deleted record whose text is in the index
    file, a foreign reader holding a snapshot older than the purge, and the
    purge compaction running in a thread, its scrub started."""
    ns = e.namespace("n")
    idx = ns.index
    victim = _rec(f"victim {MARKER}")
    ns.append([_rec(f"keeper note {i}") for i in range(300)] + [victim])
    ns.compact(force=True)
    assert _files_with(e.cache_dir, MARKER.encode()), "positive control: the text is in the index"
    reader = _foreign_reader(idx.path)
    idx.upsert(_rec("written after the reader's snapshot"))
    idx.flush()
    ns.append_op({"op": "hard_delete", "id": victim.id, "deadline": 0})
    scrubbing = threading.Event()
    real_scrub = idx.scrub
    monkeypatch.setattr(idx, "scrub", lambda: (scrubbing.set(), real_scrub())[1])
    purge = threading.Thread(target=ns.compact, daemon=True)
    purge.start()
    assert scrubbing.wait(30)
    time.sleep(0.3)
    return ns, victim, reader, purge


def _wait_scrubbed(ns, timeout: float) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if int(ns.index.get_meta("scrubbed_seq") or 0) >= ns.manifest.scrub_seq:
            return True
        time.sleep(0.05)
    return False


def test_eviction_skips_a_namespace_whose_purge_scrub_is_waiting(tmp_path, monkeypatch):
    e = StorageEngine(str(tmp_path / "s"), max_open_namespaces=1)
    reader = None
    try:
        ns, victim, reader, purge = _purging(e, monkeypatch)
        t0 = time.monotonic()
        other = e.namespace("b")
        other.append([_rec("b1", ns="b")])
        assert time.monotonic() - t0 < 3.0, "opening another namespace waited for the scrub"
        assert "n" in e.open_namespaces(), \
            "the LRU evicted a namespace whose purge scrub was waiting (the close interrupts it)"
        assert purge.is_alive() and not ns.index.closing, "the scrub was interrupted"
        _release(reader)
        reader = None
        purge.join(30)
        assert not purge.is_alive(), "the purge completes once the reader lets go"
        assert int(ns.index.get_meta("scrubbed_seq") or 0) == ns.manifest.scrub_seq > 0
        t0 = time.monotonic()
        assert e.namespace("n") is ns, "the namespace was reopened"
        assert time.monotonic() - t0 < 1.0
        assert _files_with(e.cache_dir, MARKER.encode()) == []
    finally:
        if reader is not None:
            _release(reader)
        e.close()


def test_an_open_finishes_an_interrupted_scrub_without_waiting_for_a_reader(tmp_path, monkeypatch):
    # an image of any size is publishable: the snapshot rule is what is checked
    monkeypatch.setattr(NamespaceStore, "SNAPSHOT_MIN_RECORDS", 1)
    root = tmp_path / "s"
    e = StorageEngine(str(root), max_open_namespaces=1)
    reader = None
    try:
        ns, victim, reader, purge = _purging(e, monkeypatch)
        e.close()   # interrupts the scrub: the next open finishes it
        purge.join(10)
        assert not purge.is_alive()
        released = threading.Event()

        def release():
            _release(reader)
            released.set()

        timer = threading.Timer(HOLD_S, release)
        timer.start()
        t_open = time.monotonic()
        e = StorageEngine(str(root), max_open_namespaces=1)
        ns = e.namespace("n")
        took = time.monotonic() - t_open
        assert took < 4.0, f"the open waited {took:.1f}s for a reader of its index"
        assert not released.is_set()
        assert _files_with(root, MARKER.encode()), "positive control: the file still holds the text"
        # the scrub goes on in the background: nothing purged is served...
        assert ns.index.get_many([victim.id]) == []
        hits = ns.index.search_bm25(MARKER, IndexFilter(scope=Scope(user="alice")), limit=5)
        assert victim.id not in {h.record.id for h in hits}
        # ...the purge is not reported done...
        assert int(ns.index.get_meta("scrubbed_seq") or 0) < ns.manifest.scrub_seq
        # ...and no image of the unscrubbed file is published
        assert ns.write_index_snapshot() is False
        assert not ns.manifest.snapshot_name
        # the LRU leaves it open while its scrub waits
        e.namespace("b").append([_rec("b1", ns="b")])
        assert "n" in e.open_namespaces(), "the LRU interrupted the open's background scrub"
        # writes go on meanwhile
        ns.append([_rec("written while the scrub waits")])
        timer.join()
        assert _wait_scrubbed(ns, 15.0), "the background scrub never finished"
        assert _files_with(root, MARKER.encode()) == [], "erased text left in the local index"
        assert ns.write_index_snapshot() is True, "a scrubbed index publishes again"
    finally:
        if reader is not None and not released.is_set():
            try:
                _release(reader)
            except sqlite3.Error:
                pass
        e.close()


def test_a_close_interrupts_an_opens_background_scrub(tmp_path, monkeypatch):
    root = tmp_path / "s"
    e = StorageEngine(str(root))
    reader = None
    try:
        ns, victim, reader, purge = _purging(e, monkeypatch)
        e.close()
        purge.join(10)
        e = StorageEngine(str(root))
        got = []
        opener = threading.Thread(target=lambda: got.append(e.namespace("n")), daemon=True)
        opener.start()
        opener.join(5)
        if opener.is_alive():
            _release(reader)
            reader = None
            opener.join(30)
        assert got and reader is not None, "the open waited for a reader of its index"
        ns = got[0]
        assert int(ns.index.get_meta("scrubbed_seq") or 0) < ns.manifest.scrub_seq
        t0 = time.monotonic()
        e.close()
        took = time.monotonic() - t0
        assert took < 3.0, f"close() waited {took:.1f}s for an open's background scrub"
    finally:
        if reader is not None:
            _release(reader)
        e.close()
    # the reader is gone: the next open finishes the scrub (here, within its wait)
    e = StorageEngine(str(root))
    try:
        ns = e.namespace("n")
        assert int(ns.index.get_meta("scrubbed_seq") or 0) == ns.manifest.scrub_seq > 0
        assert _files_with(root, MARKER.encode()) == []
        assert ns.index.get_many([victim.id]) == []
    finally:
        e.close()
