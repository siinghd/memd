"""Pass 22 regression: cold start, and the query-plan inversion it exposed.

Cold start on a node without the local derived index was a blocking O(live
records) re-fold: 569.7ms at 10K and 2171.2ms at 40K, with no query servable
until it finished, crossing the 1.5s cold-first-query SLO at ~25K records.
Object storage held the source of truth but only in RAW form, so every node
paid to rebuild the same folded result.

Also here: while benchmarking the snapshot I found that pass 21's switch to
external-content FTS5 had INVERTED the bm25 query plan - SQLite began driving
from `records` (via ix_rec_valid, where the scope/validity predicates live)
and probing the fts index once PER ROW. 17ms became 22 SECONDS at 10K records.
Pass 21's own tests missed it because they use corpora of a few hundred rows,
where the inverted plan is still cheap.
"""
import os
import shutil
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from memd.core.schema import Scope  # noqa: E402
from memd.engine.memory import Memory  # noqa: E402
from memd.index.sqlite_index import IndexFilter  # noqa: E402


def _seed(m, n):
    for i in range(0, n, 1000):
        m.add_events([{"content": f"note {i + j} about deployment staging cache auth",
                       "user_id": "u"} for j in range(min(1000, n - i))])
    m.flush()


class TestBm25PlanStability:
    """A plan inversion is invisible to correctness tests and catastrophic to
    latency, so it is asserted directly."""

    def test_fts_drives_the_join(self, tmp_path):
        m = Memory(str(tmp_path / "d"), encrypt=False, config={"rate_max_writes": 10 ** 9})
        try:
            _seed(m, 3000)
            idx = m.ns.index
            args: list = []
            filt = idx._filter_where(IndexFilter(scope=Scope(user="u")), args)
            sql = ("SELECT r.*, bm25(fts) AS rank FROM fts CROSS JOIN records r "
                   f"ON r.rowid = fts.rowid WHERE fts MATCH ? AND {filt} "
                   "ORDER BY rank LIMIT ?")
            plan = idx._con.execute("EXPLAIN QUERY PLAN " + sql,
                                    ['"deployment" AND "staging"'] + args + [40]).fetchall()
            steps = [str(r[-1]) for r in plan]
            assert any("fts" in s and "SCAN" in s for s in steps), steps
            # `records` must be PROBED by rowid, never scanned as the outer loop
            rec_steps = [s for s in steps if " r " in f" {s} " or s.strip().startswith("SEARCH r")]
            assert all("rowid=?" in s or "INTEGER PRIMARY KEY" in s for s in rec_steps), steps
        finally:
            m.close()

    def test_bm25_lane_stays_fast_at_ten_thousand_records(self, tmp_path):
        """The corpus is deliberately large enough to expose a plan inversion;
        at a few hundred rows the bad plan still looks fine."""
        m = Memory(str(tmp_path / "d"), encrypt=False,
                   config={"rate_max_writes": 10 ** 9, "vector_selfheal": False})
        try:
            _seed(m, 10000)
            f = IndexFilter(scope=Scope(user="u"))
            m.ns.index.search_bm25("deployment staging", f, limit=40)  # warm
            t0 = time.monotonic()
            hits = m.ns.index.search_bm25("deployment staging", f, limit=40)
            ms = (time.monotonic() - t0) * 1000
            assert hits, "expected results"
            assert ms < 500, f"bm25 lane took {ms:.0f}ms at 10K records (was 22,268ms inverted)"
        finally:
            m.close()


class TestIndexSnapshot:
    def test_compaction_publishes_a_snapshot(self, tmp_path):
        m = Memory(str(tmp_path / "d"), encrypt=False,
                   config={"rate_max_writes": 10 ** 9, "vector_selfheal": False})
        try:
            _seed(m, 3000)
            m.compact(force=True)
            assert m.ns.manifest.snapshot_name, "compaction did not publish a snapshot"
            assert m.ns.manifest.snapshot_seq > 0
            assert m.ns.store.get(m.ns._snapshot_key(m.ns.manifest.snapshot_name))
        finally:
            m.close()

    def test_snapshot_is_not_named_like_a_sqlite_file(self, tmp_path):
        """It lives in the namespace prefix beside segments. Anything sweeping
        'sqlite files' would delete the durable folded index - which is exactly
        what my own benchmark did before this was renamed."""
        m = Memory(str(tmp_path / "d"), encrypt=False, config={"rate_max_writes": 10 ** 9})
        try:
            _seed(m, 3000)
            m.compact(force=True)
            assert ".sqlite" not in m.ns.manifest.snapshot_name, m.ns.manifest.snapshot_name
        finally:
            m.close()

    def test_cold_start_uses_the_snapshot_and_restores_every_record(self, tmp_path):
        root = str(tmp_path / "d")
        m = Memory(root, encrypt=False,
                   config={"rate_max_writes": 10 ** 9, "vector_selfheal": False})
        _seed(m, 4000)
        m.compact(force=True)
        before = m.ns.index.stats()
        m.close()

        shutil.rmtree(os.path.join(root, "store", "_cache"), ignore_errors=True)
        from memd.metrics import METRICS
        loaded_before = sum(e["value"] for e in
                            METRICS.snapshot()["counters"].get("memd_index_snapshots_loaded_total", []))

        m2 = Memory(root, encrypt=False, config={"vector_selfheal": False})
        try:
            loaded_after = sum(e["value"] for e in
                               METRICS.snapshot()["counters"].get("memd_index_snapshots_loaded_total", []))
            assert loaded_after > loaded_before, "cold start did not use the snapshot"
            st = m2.ns.index.stats()
            assert st["records"] == before["records"]
            assert st["vectors"] == before["vectors"], "the snapshot must carry the vector lane"
            assert m2.search("deployment staging", user_id="u").items
        finally:
            m2.close()

    def test_a_missing_snapshot_falls_back_to_full_replay(self, tmp_path):
        """The snapshot is a CACHE: losing it must cost time, never data."""
        root = str(tmp_path / "d")
        m = Memory(root, encrypt=False,
                   config={"rate_max_writes": 10 ** 9, "vector_selfheal": False})
        _seed(m, 3000)
        m.compact(force=True)
        name = m.ns.manifest.snapshot_name
        m.close()

        shutil.rmtree(os.path.join(root, "store", "_cache"), ignore_errors=True)
        from memd.storage.objectstore import LocalObjectStore
        LocalObjectStore(os.path.join(root, "store")).delete(f"ns/default/{name}")

        m2 = Memory(root, encrypt=False, config={"vector_selfheal": False})
        try:
            assert m2.ns.index.stats()["records"] == 3000, "replay fallback lost records"
            assert m2.search("deployment staging", user_id="u").items
        finally:
            m2.close()

    def test_a_corrupt_snapshot_falls_back_to_full_replay(self, tmp_path):
        root = str(tmp_path / "d")
        m = Memory(root, encrypt=False,
                   config={"rate_max_writes": 10 ** 9, "vector_selfheal": False})
        _seed(m, 3000)
        m.compact(force=True)
        name = m.ns.manifest.snapshot_name
        m.close()

        shutil.rmtree(os.path.join(root, "store", "_cache"), ignore_errors=True)
        from memd.storage.objectstore import LocalObjectStore
        LocalObjectStore(os.path.join(root, "store")).put(f"ns/default/{name}", b"not a gzip stream")

        m2 = Memory(root, encrypt=False, config={"vector_selfheal": False})
        try:
            assert m2.ns.index.stats()["records"] == 3000, "corrupt snapshot lost records"
        finally:
            m2.close()

    def test_writes_after_the_snapshot_are_replayed_from_the_tail(self, tmp_path):
        root = str(tmp_path / "d")
        m = Memory(root, encrypt=False,
                   config={"rate_max_writes": 10 ** 9, "vector_selfheal": False})
        _seed(m, 3000)
        m.compact(force=True)
        m.add("a uniquely spelled zanzibar record written after the snapshot", user_id="u")
        m.flush()
        m.close()

        shutil.rmtree(os.path.join(root, "store", "_cache"), ignore_errors=True)
        m2 = Memory(root, encrypt=False, config={"vector_selfheal": False})
        try:
            assert m2.ns.index.stats()["records"] == 3001
            assert any("zanzibar" in i.content
                       for i in m2.search("zanzibar", user_id="u").items), \
                "the tail past the snapshot was not replayed"
        finally:
            m2.close()


class TestPostCompactDurability:
    """The most serious defect this run found, and it predates every pass in
    this log: a record acked AFTER compact() was silently lost on restart.

    compact() deleted the wal key while LocalLogWriter still held an open fd
    on it, so every subsequent append went to an UNLINKED inode. The write was
    acked, was visible in-process (the derived index had it), and evaporated
    on reopen. rotate() had always sealed the writer correctly; compact() had
    never done it. The SIGKILL crash-consistency suite missed it because it
    never compacts mid-stream.
    """

    def test_a_write_acked_after_compaction_survives_restart(self, tmp_path):
        root = str(tmp_path / "d")
        m = Memory(root, encrypt=False,
                   config={"rate_max_writes": 10 ** 9, "vector_selfheal": False})
        _seed(m, 2000)
        m.compact(force=True)
        rid = m.add("zanzibar record written after compact", user_id="u")[0]
        m.flush()
        assert m.get(rid) is not None, "not even visible before restart"
        m.close()

        m2 = Memory(root, encrypt=False, config={"vector_selfheal": False})
        try:
            assert m2.get(rid) is not None, (
                "a durably-acked write was lost across restart - the wal writer "
                "is appending to an unlinked inode again")
            assert m2.ns.index.stats()["records"] == 2001
        finally:
            m2.close()

    def test_many_writes_after_compaction_all_survive(self, tmp_path):
        root = str(tmp_path / "d")
        m = Memory(root, encrypt=False,
                   config={"rate_max_writes": 10 ** 9, "vector_selfheal": False})
        _seed(m, 1000)
        acked = []
        for round_ in range(3):
            m.compact(force=True)
            acked += [m.add(f"post-compact round {round_} record {i}", user_id="u")[0]
                      for i in range(20)]
            m.flush()
        m.close()

        m2 = Memory(root, encrypt=False, config={"vector_selfheal": False})
        try:
            missing = [i for i in acked if m2.get(i) is None]
            assert not missing, f"{len(missing)} of {len(acked)} acked writes lost"
        finally:
            m2.close()

    def test_the_wal_object_actually_receives_the_bytes(self, tmp_path):
        from memd.storage.objectstore import LocalObjectStore

        root = str(tmp_path / "d")
        m = Memory(root, encrypt=False,
                   config={"rate_max_writes": 10 ** 9, "vector_selfheal": False})
        try:
            _seed(m, 1000)
            m.compact(force=True)
            m.add("bytes must land in a file that still has a name", user_id="u")
            m.flush()
            size = len(LocalObjectStore(os.path.join(root, "store")).get("ns/default/wal") or b"")
            assert size > 0, "the wal object is empty: appends went to an unlinked inode"
        finally:
            m.close()
