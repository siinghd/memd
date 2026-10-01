"""Pass 41: a foreign SQLite reader no longer stalls a namespace during a purge.

After a hard-delete purge the scrub checkpoints the local index's WAL until
it is truncated, and never gives up (D10). A reader memd does not control -
another sqlite3 connection holding an old snapshot, not a memd image pin -
keeps that from happening for as long as it holds on. Each attempt waited
out the 5 s busy timeout HOLDING the index lock, and the compaction ran the
scrub holding the namespace lock: searches and index writes waited ~5 s at
p99, and appends and close() for the reader's whole lifetime (a 75 s reader:
append p99 75.7 s).

Now each attempt is a checkpoint under a short busy timeout, the index lock
is released between attempts (with a backoff), and the compaction runs the
scrub outside the namespace lock; it still never gives up and warns every
60 s, and a close interrupts it (the next open finishes it).
"""
import logging
import os
import sqlite3
import threading
import time

from memd.core.schema import MemoryRecord, Scope
from memd.engine.memory import Memory
from memd.index import sqlite_index
from memd.index.sqlite_index import IndexFilter
from memd.storage.engine import StorageEngine

MARKER = "ERASE-ME-qx41-foreign-reader"
HOLD_S = 10.0


def _files_with(root, needle: bytes) -> list[str]:
    out = []
    for dirpath, _dirs, files in os.walk(str(root)):
        for fn in files:
            path = os.path.join(dirpath, fn)
            with open(path, "rb") as f:
                if needle in f.read():
                    out.append(os.path.relpath(path, str(root)))
    return sorted(out)


def _rec(content: str) -> MemoryRecord:
    return MemoryRecord.create(namespace="n", kind="raw_event", content=content,
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


def _p99(xs: list[float]) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * 0.99))]


def _purging(e: StorageEngine, monkeypatch):
    """A namespace with a hard-deleted record whose text is in the index
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


def test_a_foreign_reader_does_not_stall_the_namespace_during_a_purge(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(sqlite_index.NamespaceIndex, "SCRUB_WARN_EVERY_S", 2.0, raising=False)
    e = StorageEngine(str(tmp_path / "s"))
    reader = None
    try:
        with caplog.at_level(logging.WARNING, logger="memd.index.sqlite_index"):
            ns, victim, reader, purge = _purging(e, monkeypatch)
            idx = ns.index
            released = threading.Event()
            waiting_at_release = []

            def release():
                waiting_at_release.append(purge.is_alive())
                _release(reader)
                released.set()

            timer = threading.Timer(HOLD_S, release)
            timer.start()
            lat: dict[str, list[float]] = {"append": [], "search": [], "index write": []}
            i = 0
            while not released.is_set():
                t0 = time.monotonic()
                ns.append([_rec(f"appended while the reader holds on {i}")])
                lat["append"].append(time.monotonic() - t0)
                t0 = time.monotonic()
                idx.search_bm25("keeper note", IndexFilter(scope=Scope(user="alice")), limit=5)
                lat["search"].append(time.monotonic() - t0)
                t0 = time.monotonic()
                idx.upsert(_rec(f"index write while the reader holds on {i}"))
                lat["index write"].append(time.monotonic() - t0)
                i += 1
                time.sleep(0.02)
            timer.join()
            assert waiting_at_release == [True], "(the scrub waited for the reader)"
            for kind, xs in lat.items():
                assert _p99(xs) < 1.0, f"{kind} p99 {_p99(xs):.2f}s while a foreign reader held on"
            purge.join(60)
            assert not purge.is_alive(), "the purge completes once the reader lets go"
        assert any("a reader has kept its WAL from being truncated" in r.getMessage()
                   for r in caplog.records), "the scrub still says it is waiting"
        assert int(idx.get_meta("scrubbed_seq") or 0) == ns.manifest.scrub_seq > 0
        assert _files_with(e.cache_dir, MARKER.encode()) == [], "erased text left in the local index"
        assert ns.index.get_many([victim.id]) == []
    finally:
        if reader is not None:
            try:
                _release(reader)
            except sqlite3.Error:
                pass
        e.close()


def test_close_interrupts_a_scrub_waiting_for_a_foreign_reader(tmp_path, monkeypatch):
    root = tmp_path / "s"
    e = StorageEngine(str(root))
    reader = None
    try:
        ns, victim, reader, purge = _purging(e, monkeypatch)
        closer = threading.Thread(target=e.close, daemon=True)
        t0 = time.monotonic()
        closer.start()
        closer.join(10)
        took = time.monotonic() - t0
        assert not closer.is_alive() and took < 3.0, f"close() waited {took:.1f}s for a foreign reader"
        purge.join(10)
        assert not purge.is_alive(), "the interrupted purge returns"
    finally:
        if reader is not None:
            _release(reader)
        e.close()
    # the purge itself is durable; the next open finishes the scrub
    e = StorageEngine(str(root))
    try:
        ns = e.namespace("n")
        assert int(ns.index.get_meta("scrubbed_seq") or 0) == ns.manifest.scrub_seq > 0
        assert _files_with(e.cache_dir, MARKER.encode()) == []
        assert ns.index.get_many([victim.id]) == []
    finally:
        e.close()


def test_memory_close_interrupts_a_background_purge_waiting_for_a_foreign_reader(tmp_path):
    root = str(tmp_path / "m")
    cfg = {"embedder": "hash", "hard_delete_deadline_ms": 0,
           "rate_max_writes": 10**9, "dup_max_repeats": 10**9}
    m = Memory(root, namespace="n", config=cfg)
    reader = closer = None
    try:
        for i in range(50):
            m.remember(f"keeper note {i}")
        victim = m.remember(f"victim {MARKER}")
        m.flush()
        scrubbing = threading.Event()
        real_scrub = m.ns.index.scrub
        m.ns.index.scrub = lambda: (scrubbing.set(), real_scrub())[1]
        reader = _foreign_reader(m.ns.index.path)
        m.remember("written after the reader's snapshot")
        m.ns.index.flush()
        assert m.delete(victim, hard=True)   # due at once: the background purge runs
        assert scrubbing.wait(30)
        time.sleep(0.3)
        closer = threading.Thread(target=m.close, daemon=True)
        t0 = time.monotonic()
        closer.start()
        closer.join(20)
        took = time.monotonic() - t0
        assert not closer.is_alive() and took < 5.0, f"close() waited {took:.1f}s for a foreign reader"
    finally:
        if reader is not None:
            _release(reader)
        if closer is not None:
            closer.join(60)
    m = Memory(root, namespace="n", config=cfg)
    try:
        assert m.get(victim) is None
        assert int(m.ns.index.get_meta("scrubbed_seq") or 0) == m.ns.manifest.scrub_seq > 0
        assert _files_with(m.engine.cache_dir, MARKER.encode()) == []
    finally:
        m.close()
