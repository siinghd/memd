"""Pass 23 regression: three defects the final re-audit found in passes 16-22.

All three are in code written EARLIER IN THE SAME SESSION - which is the point.
The log's most repeated lesson is that pass-N's fix carries a bug found at N+1,
and a re-audit aimed squarely at the newest machinery is what surfaced them.

 1. CRIT  Group commit ACKED writes that no fsync ever covered. The sync owner
          published `_synced_pos = max(_synced_pos, _written_pos)` AFTER its
          fsync, and _written_pos is advanced by other appenders under a
          DIFFERENT lock while that fsync is in flight. Measured: 160 acks for
          bytes no fsync had seen, up to 3108 bytes at a time. The machinery
          was unreachable before pass 20 made group commit work, so the bug
          was latent, not new - enabling it is what exposed it.
 2. HIGH  The pass-22 index snapshot published OUTSIDE the namespace lock, so a
          concurrent destroy left it writing an object under a crypto-SHREDDED
          namespace - and envelope.encrypt() minted a FRESH DATA KEY for it,
          resurrecting what the crypto-shred had just destroyed.
 3. MED   The pass-20 audit ring deletes sealed segments past KEEP_SEGMENTS.
          verify() anchored at 0*64, so the moment the ring engaged it went
          permanently False - reading as "tampered" when the truth was "pruned
          on purpose". 4000 entries appended, 800 readable, verify() False.
"""
import os
import sys
import tempfile
import threading
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from memd.engine.memory import Memory  # noqa: E402
from memd.storage import objectstore as OS  # noqa: E402
from memd.storage.audit import BufferedAuditLog  # noqa: E402
from memd.storage.objectstore import LocalObjectStore  # noqa: E402


def test_no_write_is_acked_beyond_a_completed_fsync(tmp_path):
    """The durability contract: an ack means the bytes are on disk.

    Instruments LocalLogWriter.sync to record the file size at fsync ENTRY -
    the only offset POSIX guarantees that call covers - and asserts the
    published watermark never runs ahead of it.
    """
    covered: list[int] = []
    lock = threading.Lock()
    real_sync = OS.LocalLogWriter.sync

    def watching(self):
        entry_size = self.size()
        real_sync(self)
        with lock:
            covered.append(entry_size)

    OS.LocalLogWriter.sync = watching
    try:
        m = Memory(str(tmp_path / "d"), encrypt=False, config={"rate_max_writes": 10 ** 9})
        ns = m.ns
        bad = []
        barrier = threading.Barrier(8)

        def w(t):
            barrier.wait()
            for i in range(20):
                m.add(f"thread {t} record {i} payload padding padding", user_id="u")
                with lock:
                    high = max(covered) if covered else 0
                    if ns._synced_pos > high:
                        bad.append((ns._synced_pos, high))

        ts = [threading.Thread(target=w, args=(i,)) for i in range(8)]
        [x.start() for x in ts]
        [x.join() for x in ts]
        m.close()
        assert not bad, (
            f"{len(bad)} acks published beyond any completed fsync; worst: "
            f"claimed {bad[0][0]} durable, fsync covered {bad[0][1]}")
        assert covered, "expected fsyncs"
    finally:
        OS.LocalLogWriter.sync = real_sync


def test_group_commit_still_coalesces(tmp_path):
    """The durability fix must not silently undo pass 20's coalescing."""
    calls = {"n": 0}
    real_sync = OS.LocalLogWriter.sync

    def counting(self):
        calls["n"] += 1
        return real_sync(self)

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
        [x.start() for x in ts]
        [x.join() for x in ts]
        m.close()
        assert calls["n"] < 200, f"{calls['n']} fsyncs for 200 appends - coalescing is gone"
    finally:
        OS.LocalLogWriter.sync = real_sync


def test_snapshot_cannot_resurrect_a_shredded_namespace(tmp_path, monkeypatch):
    """A crypto-shred must stay shredded even if a snapshot is mid-flight.

    Writing the snapshot object would re-create the namespace prefix, and
    encrypting it would mint a fresh data key - defeating the crypto-shred.
    """
    from memd.storage import engine as E

    real = E.NamespaceStore.write_index_snapshot

    def slow(self):
        time.sleep(0.35)          # widen the window between compact and publish
        return real(self)

    monkeypatch.setattr(E.NamespaceStore, "write_index_snapshot", slow)
    root = str(tmp_path / "d")
    m = Memory(root, namespace="keep")
    try:
        for i in range(2500):
            m.add(f"secret record {i}", user_id="u", namespace="doomed")
        m.flush()
        t = threading.Thread(target=lambda: m.compact(force=True, namespace="doomed"), daemon=True)
        t.start()
        time.sleep(0.12)
        m.destroy_namespace("doomed", actor="admin")
        t.join(timeout=20)

        leftover = [k for k in m.engine.store.list("ns/") if k.startswith("ns/doomed/")]
        assert not leftover, f"objects survived the shred: {leftover[:4]}"
        assert not os.path.exists(os.path.join(root, "keys", "ns-doomed.key")), \
            "the namespace data key was re-minted - crypto-shred defeated"
    finally:
        m.close()


class TestAuditRetentionBound:
    """The pruning branch - the thing that makes the ledger 'bounded' - was
    executed by no test in the suite, and both invariants the pass-20 tests
    assert by name become false once the bound engages."""

    def _log(self, tmp_path):
        store = LocalObjectStore(str(tmp_path / "s"))
        log = BufferedAuditLog(store, "ns/t/audit", None, flush_every=50)
        log.rotate_bytes = 3000
        return store, log

    def test_verify_holds_over_the_retained_window_after_pruning(self, tmp_path):
        store, log = self._log(tmp_path)
        for i in range(4000):
            log.append(actor="search", action="search", target=f"q{i:06d}")
        log.flush()
        assert log._pruned > 0, "the retention bound never engaged - test is vacuous"
        assert len(log.read()) < 4000, "expected old entries to be dropped"
        assert log.verify(), (
            "verify() went False after pruning: bounded retention reads as "
            "tampering unless the chain is re-anchored")

    def test_truncation_is_recorded_not_hidden(self, tmp_path):
        store, log = self._log(tmp_path)
        for i in range(4000):
            log.append(actor="search", action="search", target=f"q{i:06d}")
        log.flush()
        assert log._pruned > 0
        assert log._chain_start != "0" * 64, (
            "the chain anchor must move when history is discarded, so a reader "
            "can tell 'pruned' from 'complete'")

    def test_the_anchor_survives_a_reopen(self, tmp_path):
        store, log = self._log(tmp_path)
        for i in range(4000):
            log.append(actor="search", action="search", target=f"q{i:06d}")
        log.flush()
        again = BufferedAuditLog(store, "ns/t/audit", None, flush_every=50)
        assert again._chain_start == log._chain_start
        assert again._pruned == log._pruned
        assert again.verify()

    def test_tampering_inside_the_retained_window_is_still_caught(self, tmp_path):
        """Re-anchoring must not weaken tamper-evidence for what we DO keep."""
        import json

        store, log = self._log(tmp_path)
        for i in range(4000):
            log.append(actor="search", action="search", target=f"q{i:06d}")
        log.flush()
        for i in range(5):          # a live tail past the last seal
            log.append(actor="search", action="search", target=f"tail{i}")
        log.flush()
        assert log.verify()

        live = store.get("ns/t/audit") or b""
        lines = live.decode().splitlines()
        assert lines, "expected a live tail to tamper with"
        e = json.loads(lines[-1])
        e["actor"] = "mallory"
        lines[-1] = json.dumps(e, separators=(",", ":"))
        store.put("ns/t/audit", ("\n".join(lines) + "\n").encode())
        assert not log.verify(), "a rewritten entry inside the window must break the chain"
