"""Pass 19 regression: retrieval cost and vector-lane observability.

 1. HIGH  Reads did not scale. Every read shared the WRITER's SQLite
          connection under one RLock, so a namespace served one query at a
          time: 26.3 q/s at 1 thread, 52.4 at 2, then DOWN to 49.3 at 4 and
          41.5 at 8, p99 46.9ms -> 288.1ms. SQLite WAL already supports
          concurrent readers; the lock was the only obstacle.
 2. HIGH  The real cost centre, found with the per-stage timings added in
          pass 16: the BM25 lane was 61% of search latency because it
          hydrated and JSON-deserialized every row in the bounded window
          (~320) to rank them, then kept ~40.
 3. MED   Losing the derived index cache replayed all records and ZERO
          vectors. Retrieval then ran with one of four fusion lanes empty and
          looked FASTER, not broken, so nothing flagged it - and the repair
          existed only behind the CLI.
"""
import glob
import os
import shutil
import statistics
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from memd.engine.memory import Memory  # noqa: E402
from memd.metrics import METRICS  # noqa: E402

TOPICS = ["standup", "refactor", "deploy", "cache", "auth", "lint"]


def _seed(m, n=3000):
    for i in range(0, n, 500):
        m.add_events([{"content": f"Notes from {TOPICS[(i + j) % 6]} session {i + j}: discussed "
                                  f"{TOPICS[(i + j) % 6]} and agreed follow ups number {i + j}.",
                       "user_id": "u", "session_id": f"s{(i + j) % 40}"}
                      for j in range(min(500, n - i))])
    m.flush()


def test_reads_are_not_serialized_on_the_writer_lock(tmp_path):
    """Throughput must rise with readers. This asserts a floor well below the
    measured 2.5x so it cannot flake on a loaded box - the defect it guards
    against was throughput going DOWN with concurrency."""
    m = Memory(str(tmp_path / "d"))
    try:
        _seed(m, 3000)

        def run(threads, per_thread=12):
            barrier = threading.Barrier(threads)

            def worker(tid):
                barrier.wait()
                for k in range(per_thread):
                    m.search(f"{TOPICS[k % 6]} session discussed follow ups number "
                             f"{(tid * 997 + k * 13) % 3000}", user_id="u")

            ts = [threading.Thread(target=worker, args=(i,)) for i in range(threads)]
            t0 = time.monotonic()
            [t.start() for t in ts]
            [t.join() for t in ts]
            return (threads * per_thread) / (time.monotonic() - t0)

        one = run(1)
        four = run(4)
        assert four > one * 1.15, (
            f"concurrent reads did not scale: {one:.1f} q/s at 1 thread, "
            f"{four:.1f} q/s at 4 - reads are serialized")
    finally:
        m.close()


def test_bm25_hydrates_only_what_it_returns(tmp_path):
    """Full records are built only for the rows the lane returns.
    Guards against reintroducing an O(window) hydration.

    Mechanism changed in retrieval v2: this used to rank a ~320-row window on
    content and hydrate survivors through get_many. The lane now ranks IN SQL
    (FTS5 bm25, ORDER BY rank LIMIT k), so the LIMIT itself bounds hydration;
    count every record build on either path."""
    from memd.core.schema import Scope
    from memd.index.sqlite_index import IndexFilter

    m = Memory(str(tmp_path / "d"))
    try:
        _seed(m, 2000)
        idx = m.ns.index
        real = idx.get_many
        asked = []
        idx.get_many = lambda ids: (asked.append(len(ids)), real(ids))[1]
        real_row = idx._row_to_record
        built = []
        idx._row_to_record = lambda row: (built.append(1), real_row(row))[1]
        try:
            hits = idx.search_bm25("standup session discussed follow ups",
                                   IndexFilter(scope=Scope(user="u")), limit=40)
        finally:
            idx.get_many = real
            del idx._row_to_record
        assert hits, "expected results"
        hydrated = len(built) + sum(asked)
        assert hydrated <= 40, (
            f"hydrated {hydrated} rows to return {len(hits)} - a candidate "
            "window is being materialized in full again")
    finally:
        m.close()


def test_search_still_finds_what_it_found_before(tmp_path):
    """The hydration change must not cost recall."""
    m = Memory(str(tmp_path / "d"))
    try:
        _seed(m, 1000)
        m.add("the deployment codeword is zanzibar", user_id="u", session_id="s1")
        m.flush()
        res = m.search("deployment codeword zanzibar", user_id="u")
        assert any("zanzibar" in i.content for i in res.items)
    finally:
        m.close()


class TestVectorLaneHealth:
    def test_cache_loss_is_visible_and_self_heals(self, tmp_path):
        root = str(tmp_path / "d")
        m = Memory(root)
        for i in range(120):
            m.add(f"record {i} about deployment", user_id="u")
        m.flush()
        assert m.ns.index.stats()["vectors"] == 120
        m.close()

        # node restore: the derived index cache is gone, segments remain
        for f in glob.glob(os.path.join(root, "**", "*.sqlite*"), recursive=True):
            os.unlink(f)
        for d in (os.path.join(root, "store", ".cache"), os.path.join(root, "cache")):
            if os.path.isdir(d):
                shutil.rmtree(d)

        m2 = Memory(root)
        try:
            gauge = [g for g in METRICS.snapshot()["gauges"].get("memd_vectors_missing", [])
                     if g["labels"].get("ns") == "default"]
            assert gauge and gauge[-1]["value"] > 0, (
                "an empty vector lane must be visible, not silent")
            m2.flush()  # drains the maintenance worker
            st = m2.ns.index.stats()
            assert st["records"] == 120
            assert st["vectors"] == 120, f"vector lane did not heal: {st}"
        finally:
            m2.close()

    def test_selfheal_can_be_disabled(self, tmp_path):
        root = str(tmp_path / "d")
        m = Memory(root, config={"vector_selfheal": False})
        try:
            m.add("one record", user_id="u")
            m.flush()
        finally:
            m.close()


def test_reembed_has_an_operator_door(tmp_path):
    """The heal was CLI-only, which a hosted operator cannot reach on the node
    that needs it."""
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from memd.server.http import create_app

    app = create_app(data_dir=str(tmp_path / "data"), admin_key="adm-p19")
    with TestClient(app) as c:
        h = {"Authorization": "Bearer adm-p19"}
        c.post("/v1/ns/acme/memories", headers=h, json={"content": "hello world", "user_id": "u"})
        r = c.post("/v1/ns/acme/reembed", headers=h)
        assert r.status_code == 200, r.text
        assert "embedded" in r.json() and "missing" in r.json()
        assert c.post("/v1/ns/acme/reembed").status_code == 401


def test_open_does_not_materialize_the_reembed_worklist(tmp_path):
    """Pass 19's vector-health gauge took len() of the WORKLIST, building a
    MemoryRecord (three json.loads each) per missing vector on the namespace
    OPEN path: 1066ms to reopen a 50K namespace with an incomplete lane.
    The gauge needs a COUNT; the worklist belongs on the maintenance thread.
    """
    root = str(tmp_path / "d")
    m = Memory(root, encrypt=False, config={"rate_max_writes": 10 ** 9})
    for i in range(0, 12000, 2000):
        m.add_events([{"content": f"note {i + j} about deployment", "user_id": "u"}
                      for j in range(2000)])
    m.flush()
    m.close()

    idx_calls = []
    from memd.index.sqlite_index import NamespaceIndex
    real = NamespaceIndex.records_missing_embedding

    def spy(self, model, limit=100_000):
        idx_calls.append(limit)
        return real(self, model, limit)

    NamespaceIndex.records_missing_embedding = spy
    try:
        t0 = time.monotonic()
        m2 = Memory(root, encrypt=False, config={"vector_selfheal": False})
        open_ms = (time.monotonic() - t0) * 1000
        try:
            assert not idx_calls, (
                "open built the re-embed worklist; it only needs a count")
            assert open_ms < 400, f"namespace open took {open_ms:.0f}ms"
            from memd.metrics import METRICS
            g = [x for x in METRICS.snapshot()["gauges"].get("memd_vectors_missing", [])
                 if x["labels"].get("ns") == "default"]
            assert g, "the vector-health gauge must still be published"
        finally:
            m2.close()
    finally:
        NamespaceIndex.records_missing_embedding = real
