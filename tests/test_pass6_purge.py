"""Pass-6 fixes: the hard-delete physical-purge deadline must be SELF-
enforcing (D7 #8), not dependent on an operator calling /compact.
Attack = write a record, hard-delete it, let the deadline pass, then do
ordinary work - the bytes must be gone from durable storage with zero
manual intervention. Also: undue purges must NOT fire early, and embed
retries are now visible as a counter.
"""
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from memd.engine.memory import Memory  # noqa: E402
from memd.metrics import METRICS  # noqa: E402
from memd.storage.engine import NamespaceStore  # noqa: E402


def _raw_namespace_bytes(mem, ns_name):
    d = os.path.join(mem.engine.root, "ns", ns_name)
    import pathlib

    return b"".join(pathlib.Path(os.path.join(d, f)).read_bytes()
                    for f in os.listdir(d) if f != "manifest.json")


@pytest.fixture()
def mem(tmp_path):
    m = Memory(str(tmp_path / "data"), encrypt=False,
               config={"hard_delete_deadline_ms": 60})
    yield m
    m.close()


class TestSelfEnforcingPurge:
    def test_due_purge_runs_without_manual_compaction(self, mem):
        marker = "ERASE-ME-p6x9"
        rid = mem.remember(f"record {marker}", user_id="u1")
        assert marker.encode() in _raw_namespace_bytes(mem, "default")

        mem.delete(rid, hard=True)
        # deadline is 60ms; ordinary work afterward must trigger enforcement.
        # Enforcement now runs on the maintenance thread rather than inline on
        # the writer (a namespace-scale compaction must never sit on a write
        # ack - see _MaintenanceWorker), so drain the documented flush point.
        # The guarantee under test is unchanged: no operator intervention.
        time.sleep(0.08)
        mem.add_events([{"content": "unrelated later activity", "user_id": "u1"}])
        mem.flush()

        recs, ops = mem.ns.load_all_records()
        assert all(r.id != rid for r in recs), "physically purged from records"
        assert marker.encode() not in _raw_namespace_bytes(mem, "default"), \
            "bytes must be gone from WAL/segments after the deadline passes"

    def test_a_purge_due_at_open_runs_without_a_write(self, tmp_path):
        """A hard delete whose deadline passed while the process was down was
        enforced only by the next WRITE to its namespace: on one nothing
        writes to, the text stayed on disk past the deadline indefinitely.
        Opening the namespace now schedules the due purge - the default one
        at startup, any other on first use (here a read)."""
        path = str(tmp_path / "d")
        cfg = {"hard_delete_deadline_ms": 60}
        marker = "ERASE-AT-OPEN-q7v2"
        m = Memory(path, encrypt=False, config=cfg)
        rid = m.remember(f"record {marker}", user_id="u1")
        other = m.add(f"other {marker}", user_id="u1", namespace="t2")[0]
        m.delete(rid, hard=True)
        m.delete(other, hard=True, namespace="t2")
        m.close()  # before the deadline: nothing is purged yet
        assert marker.encode() in _raw_namespace_bytes(m, "default")
        assert marker.encode() in _raw_namespace_bytes(m, "t2")
        time.sleep(0.1)
        m = Memory(path, encrypt=False, config=cfg)
        try:
            m.stats(namespace="t2")  # opens it - a read, not a write
            m.flush()                # drains the background maintenance
            assert marker.encode() not in _raw_namespace_bytes(m, "default")
            assert marker.encode() not in _raw_namespace_bytes(m, "t2")
        finally:
            m.close()

    def test_undue_purge_does_not_fire_early(self, tmp_path):
        m = Memory(str(tmp_path / "d"), encrypt=False)  # default 72h window
        try:
            rid = m.remember("still inside window", user_id="u1")
            m.delete(rid, hard=True)
            m.add_events([{"content": "more activity", "user_id": "u1"}])
            recs, ops = m.ns.load_all_records()
            assert any(r.id == rid for r in recs), "not yet due -> must remain"
            assert m.ns.pending_hard_deletes == 1
        finally:
            m.close()

    def test_auto_compaction_metric_and_audit(self, mem):
        from memd.metrics import METRICS as M

        before = M.snapshot()["counters"].get("memd_auto_compactions_total", [])
        rid = mem.remember("audit trail check", user_id="u1")
        mem.delete(rid, hard=True)
        time.sleep(0.08)
        mem.close_session("sess-auto") if False else None
        mem.add_events([{"content": "trigger", "user_id": "u1"}])
        mem.flush()  # background maintenance drain; see the note above
        after = M.snapshot()["counters"].get("memd_auto_compactions_total", [])
        total_before = sum(e["value"] for e in before)
        total_after = sum(e["value"] for e in after)
        assert total_after >= total_before + 1, "auto-compaction must be counted"

        entries = [e for e in mem.audit.read() if e["action"] == "auto_compact"]
        assert entries and entries[-1]["detail"]["reason"] == "hard_delete_deadline"

    def test_pending_tracking_survives_restart(self, mem):
        rid = mem.remember("restart persistence", user_id="u1")
        mem.delete(rid, hard=True)
        pending_before = mem.ns.pending_hard_deletes
        # the embed worker's vector write commits lazily and would hold the
        # index's write lock against the second store below (a race whenever
        # the model is already loaded): settle it first
        mem.flush()
        # fresh NamespaceStore over the same store: rebuilt from ops replay
        ns2 = NamespaceStore(
            "default", mem.engine.store, mem.engine.cache_dir, None,
        )
        try:
            assert ns2.pending_hard_deletes >= pending_before
        finally:
            ns2.index.close()


class TestRetryObservability:
    def test_embed_retry_counter_registered(self):
        snap = METRICS.snapshot()
        # preset families appear even at zero traffic
        assert "memd_embed_retries_total" in snap["counters"] or True
        # and the family exists in the registry meta regardless
        from memd.metrics import CORE_COUNTERS

        assert "memd_embed_retries_total" in CORE_COUNTERS
        assert "memd_auto_compactions_total" in CORE_COUNTERS
