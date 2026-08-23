"""Pass 20 regression: maintenance and teardown costs, and a segfault of my own.

 1. SEGFAULT (introduced by pass 19, caught by this suite's own destroy-race
    coverage): moving reads off the writer lock left close() free to close a
    per-thread connection while another thread was executing on it. That is a
    process crash, not an exception. Readers are now counted and close()
    drains them.
 2. LOW  destroy_namespace on an absent namespace walked every object in the
         store.
 3. MED  A no-op compaction still read and rewrote the entire live set.
 4. MED  Session-close swept the whole namespace: the composite scope index
         cannot serve a filter on scope_session alone.
 5. LOW  Group commit was unreachable - append() held the namespace lock
         across the fsync, so the sync-owner election could only ever elect
         the sole appender. Measured 200 fsyncs for 200 appends.
 6. MED  Audit ledgers grew without bound (~200 bytes/op, nothing reclaimed).
"""
import os
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from memd.engine.memory import Memory  # noqa: E402
from memd.storage.audit import BufferedAuditLog  # noqa: E402
from memd.storage.crypto import LocalKeyEnvelope  # noqa: E402
from memd.storage.objectstore import LocalObjectStore  # noqa: E402


def test_searching_while_the_index_closes_does_not_crash(tmp_path):
    """The pass-19 segfault. Reads run without the writer lock, so a
    concurrent close() must drain them rather than free the connection
    underneath. A regression here takes the process down."""
    for _ in range(6):
        m = Memory(str(tmp_path / f"d{_}"))
        for i in range(300):
            m.add(f"note {i} about deployment staging", user_id="u")
        m.flush()
        errs = []
        stop = threading.Event()

        def reader():
            while not stop.is_set():
                try:
                    m.search("deployment staging note", user_id="u")
                except Exception as ex:  # lifecycle races are allowed to raise
                    errs.append(type(ex).__name__)
                    return

        ts = [threading.Thread(target=reader, daemon=True) for _ in range(4)]
        [t.start() for t in ts]
        time.sleep(0.05)
        m.ns.index.close()          # yank it while they read
        stop.set()
        [t.join(timeout=5) for t in ts]
        assert all(not t.is_alive() for t in ts)
        # surviving this loop at all is the assertion: a regression segfaults


def test_destroying_an_absent_namespace_does_not_walk_the_store(tmp_path):
    store = LocalObjectStore(str(tmp_path / "s"))
    for i in range(3000):
        store.put(f"ns/live/seg-{i:05d}", b"x" * 32)
    t0 = time.perf_counter()
    assert store.remove_prefix("ns/absent") == 0
    dt = (time.perf_counter() - t0) * 1000
    assert dt < 5.0, f"absent-namespace destroy took {dt:.1f}ms - it is scanning the store"
    assert store.remove_prefix("ns/live") == 3000  # the real path still works


def test_noop_compaction_does_not_rewrite_the_live_set(tmp_path):
    m = Memory(str(tmp_path / "d"), encrypt=False)
    try:
        for i in range(0, 4000, 500):
            m.add_events([{"content": f"note {i + j} about deployment", "user_id": "u"}
                          for j in range(500)])
        m.flush()
        m.compact(force=True)                      # settle to one segment
        t0 = time.monotonic()
        m.compact(force=False)
        noop_ms = (time.monotonic() - t0) * 1000
        assert noop_ms < 60, f"no-op compaction took {noop_ms:.0f}ms - it is folding anyway"
        # and a compaction with real work to do must still run
        rid = m.add("to be removed", user_id="u")[0]
        m.delete(rid, hard=True)
        rep = m.compact(force=True)
        assert rep["records_folded"] >= 4000
    finally:
        m.close()


def test_session_sweep_uses_a_session_index(tmp_path):
    m = Memory(str(tmp_path / "d"), encrypt=False)
    try:
        for i in range(0, 4000, 500):
            m.add_events([{"content": f"note {i + j}", "user_id": "u",
                           "session_id": f"s{(i + j) % 200}"} for j in range(500)])
        m.flush()
        plan = m.ns.index._con.execute(
            "EXPLAIN QUERY PLAN SELECT * FROM records WHERE scope_session = ? "
            "AND kind = 'raw_event' AND deleted = 0", ("s7",)).fetchall()
        text = " ".join(str(r[-1]) for r in plan)
        assert "SCAN" not in text.upper() or "INDEX" in text.upper(), (
            f"session sweep is scanning the namespace: {text}")
        assert len(m.ns.index.records_of_session("s7")) == 20
    finally:
        m.close()


def test_concurrent_appenders_share_fsyncs(tmp_path):
    from memd.storage import objectstore as OS

    real = OS.LocalLogWriter.sync
    calls = {"n": 0}

    def counting(self):
        calls["n"] += 1
        return real(self)

    OS.LocalLogWriter.sync = counting
    try:
        m = Memory(str(tmp_path / "d"), encrypt=False)
        calls["n"] = 0
        barrier = threading.Barrier(8)

        def w(t):
            barrier.wait()
            for i in range(25):
                m.add(f"thread {t} record {i}", user_id="u")

        ts = [threading.Thread(target=w, args=(i,)) for i in range(8)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        m.close()
        assert calls["n"] < 200, (
            f"{calls['n']} fsyncs for 200 concurrent appends - group commit is "
            "unreachable again (the sync-owner election can only elect one writer)")
    finally:
        OS.LocalLogWriter.sync = real


class TestAuditRotation:
    def _log(self, tmp_path, **kw):
        env = LocalKeyEnvelope(str(tmp_path / "k"))
        store = LocalObjectStore(str(tmp_path / "s"))
        log = BufferedAuditLog(store, "ns/t/audit", env, flush_every=50)
        log.rotate_bytes = 40_000
        return store, log

    def test_ledger_stays_bounded_and_readable(self, tmp_path):
        store, log = self._log(tmp_path)
        for i in range(3000):
            log.append(actor="search", action="search", target=f"q{i}")
        log.flush()
        assert log._segments > 0, "ledger never rotated"
        assert store.size("ns/t/audit") <= 40_000
        assert len(log.read()) == 3000, "rotation must not lose entries"
        assert log.verify(), "chain must survive rotation"

    def test_rotation_survives_a_reopen(self, tmp_path):
        store, log = self._log(tmp_path)
        for i in range(2000):
            log.append(actor="search", action="search", target=f"q{i}")
        log.flush()
        again = BufferedAuditLog(store, "ns/t/audit", log.envelope, flush_every=50)
        assert again._segments == log._segments
        assert len(again.read()) == 2000
        assert again.verify()

    def test_crash_between_seal_and_truncate_does_not_duplicate(self, tmp_path):
        """copy-then-truncate is deliberate: losing entries is worse than
        seeing one twice, and read() de-duplicates on the entry hash."""
        store, log = self._log(tmp_path)
        for i in range(200):
            log.append(actor="search", action="search", target=f"q{i}")
        log.flush()
        before = len(log.read())
        # simulate the crash window: sealed, but never truncated
        store.copy("ns/t/audit", log._seg_key(log._segments))
        log._segments += 1
        assert len(log.read()) == before, "duplicate entries leaked through"
        assert log.verify()
