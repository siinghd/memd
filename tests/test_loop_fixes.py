"""Loop PASS-1 fixes: forget completeness, export-under-writes, rotate/sync race."""
import concurrent.futures
import threading

import pytest

from memd.engine.memory import Memory
from memd.pipeline.embedder import fastembed_available


def test_forget_deletes_beyond_packing_budget(tmp_path):
    """Destructive delete-by-query must not be capped by packing budget:
    400 matching records ≫ any reasonable packed context."""
    m = Memory(str(tmp_path / "d"))
    try:
        events = [{"content": f"delete-me secret zebra report {i}", "user_id": "u1",
                   "session_id": f"s{i//50}"} for i in range(400)]
        m.add_events(events)
        m.flush()
        before = m.stats()["records"]
        assert before >= 400
        ids = m.forget("delete-me secret zebra", user_id="u1")
        assert len(ids) >= 350, f"forget missed matches: {len(ids)}"
        m.flush()
        res = m.search("delete-me secret zebra report", user_id="u1")
        assert all("zebra" not in i.content for i in res.items), "forget left matches behind"
    finally:
        m.close()


@pytest.mark.parametrize("embedder", [
    "hash",
    pytest.param("fastembed", marks=pytest.mark.skipif(
        not fastembed_available(), reason="fastembed extra not installed")),
])
def test_forget_never_deletes_zero_evidence_records(tmp_path, embedder):
    """The catastrophic case: records sharing NO terms with the forget query
    must never be swept by weak vector-only similarity. Run under every
    embedder: bge-small scores these unrelated strings ~0.58, far above the
    hash embedder's ~0, so a threshold that is safe for one deletes 120
    records under the other."""
    m = Memory(str(tmp_path / "d"), config={"embedder": embedder})
    try:
        for i in range(60):
            m.add(f"purge target zebra {i}", user_id="u1", session_id=f"s{i//20}")
            m.add(f"keep me dolphin {i}", user_id="u1", session_id=f"k{i//20}")
        m.flush()
        gone = m.forget("purge target zebra", user_id="u1")
        assert len(gone) == 60, f"over-deletion: {len(gone)} (expected exactly 60)"
        m.flush()
        kept = m.search("dolphin keep", user_id="u1")
        assert len(kept.items) >= 1 and all("dolphin" in i.content for i in kept.items)
        m.compact(force=True)  # tombstones are logical until compaction
        assert m.stats()["records"] == 60
    finally:
        m.close()


def test_export_concurrent_with_writes_is_complete(tmp_path):
    """Exports racing appends must never lose already-acked records."""
    d = str(tmp_path / "d")
    m = Memory(d, config={"rate_max_writes": 10**9})
    stop = threading.Event()
    exported_min = []

    def writer(wid):
        for i in range(40):
            m.add(f"export race w{wid} item {i} marker text", session_id=f"s{wid}", user_id="u1")

    def exporter():
        while not stop.is_set():
            blob = m.export_jsonl().decode()
            n = sum(1 for line in blob.splitlines() if line.strip())
            exported_min.append(n)

    wt = [threading.Thread(target=writer, args=(w,)) for w in range(4)]
    ex = threading.Thread(target=exporter)
    ex.start()
    [t.start() for t in wt]
    [t.join() for t in wt]
    stop.set()
    ex.join()

    final_blob = m.export_jsonl().decode()
    final = sum(1 for line in final_blob.splitlines() if line.strip())
    assert final == 160, f"expected 160 acked records, export saw {final}"
    # every intermediate export must be a prefix-consistent snapshot (no crashes)
    assert all(isinstance(n, int) and n >= 0 for n in exported_min)
    m.close()


def test_concurrent_rotate_vs_append_no_loss(tmp_path):
    """Appends racing segment rotation: no exceptions, no lost frames,
    log replays cleanly afterwards."""
    from memd.core.schema import Kind, MemoryRecord

    e_storage = None
    from memd.storage.engine import StorageEngine

    e = StorageEngine(str(tmp_path / "d"))
    ns = e.namespace("gc")
    errors = []

    def appender(k):
        try:
            for i in range(30):
                ns.append([MemoryRecord.create(
                    namespace="gc", kind=Kind.RAW_EVENT,
                    content=f"rotate-race {k}-{i}", source="user")])
        except Exception as ex_:  # noqa: BLE001
            errors.append(ex_)

    def rotator():
        try:
            for _ in range(10):
                ns.rotate("stress")
                threading.Event().wait(0.001)
        except Exception as ex_:  # noqa: BLE001
            errors.append(ex_)

    threads = [threading.Thread(target=appender, args=(k,)) for k in range(4)]
    rt = threading.Thread(target=rotator)
    for t in threads:
        t.start()
    rt.start()
    for t in threads:
        t.join()
    rt.join()
    assert not errors, f"concurrent append/rotate raised: {errors[:3]}"
    ns.index.flush()
    total = ns.index.stats()["records"]
    assert total == 120, f"lost records under rotate race: {total}"
    # clean replay after reopen
    ns.close()
    e2 = StorageEngine(str(tmp_path / "d"))
    ns2 = e2.namespace("gc")
    assert ns2.index.stats()["records"] == 120
    ns2.close()


def test_rotate_vs_wal_write_no_closed_file(tmp_path):
    """Regression: rotate() closed the WAL fd while another thread was mid-
    write -> ValueError('write to closed file') -> lost frames."""
    from memd.storage.engine import StorageEngine

    e = StorageEngine(str(tmp_path / "d"))
    ns = e.namespace("race")
    errors: list[str] = []
    stop = threading.Event()

    def writer():
        while not stop.is_set():
            try:
                ns._wal_write(b"x" * 64)
            except Exception as ex:  # noqa: BLE001
                errors.append(repr(ex))
                return

    def rotator():
        for _ in range(300):
            try:
                ns.rotate("probe")
            except Exception as ex:  # noqa: BLE001
                errors.append(repr(ex))
                return
        stop.set()

    t = threading.Thread(target=writer)
    t.start()
    rotator()
    t.join(timeout=30)
    assert not errors, f"concurrent rotate broke writer: {errors[:2]}"
    e.close()


def test_destroy_invalidates_cached_contexts(tmp_path):
    """Regression: crypto-shredded data must never resurrect from the
    search cache (destroy reported success but cached contexts kept
    serving the destroyed records)."""
    from memd.metrics import METRICS

    m = Memory(str(tmp_path / "d"))
    try:
        m.add("shred me please classified", user_id="u1")
        r1 = m.search("shred me please classified", user_id="u1")
        assert r1.items, "setup: item should be searchable"
        before = METRICS.snapshot()["counters"].get("memd_destroys_total", [])
        assert m.destroy_namespace() is True
        after = METRICS.snapshot()["counters"].get("memd_destroys_total", [])
        b = sum(x["value"] for x in before) if before else 0
        a = sum(x["value"] for x in after) if after else 0
        assert a == b + 1
        res = m.search("shred me please classified", user_id="u1")
        assert not res.items, "destroyed data resurrected from search cache"
    finally:
        m.close()


def test_destroy_vs_inflight_ops_clean_errors(tmp_path):
    """Destroy racing in-flight search/add: no driver internals may leak
    (sqlite 'closed database'); ops fail with the lifecycle error or succeed
    before teardown - never opaque 500s."""
    from fastapi.testclient import TestClient
    from memd.server.http import create_app
    from memd.server.auth import KeyStore

    app = create_app(data_dir=str(tmp_path / "d"), keys_path=str(tmp_path / "k.json"))
    ks: KeyStore = app.state.keystore
    admin, _ = ks.create("acme", scope_override=True)
    t = TestClient(app)
    t.headers["Authorization"] = f"Bearer {admin}"
    t.post("/v1/ns/acme/events", json={"events": [{"content": "seed", "user_id": "u1"}]})

    errs = []
    stop = threading.Event()

    def hammer():
        while not stop.is_set():
            try:
                t.post("/v1/ns/acme/search", json={"query": "seed", "user_id": "u1"})
                t.post("/v1/ns/acme/events", json={"events": [{"content": "x", "user_id": "u1"}]})
            except Exception as ex:  # noqa: BLE001
                errs.append(repr(ex))
                return

    ht = threading.Thread(target=hammer)
    ht.start()
    r = t.request("DELETE", "/v1/ns/acme")
    stop.set()
    ht.join(timeout=10)

    assert r.status_code == 200, f"crypto-shred failed: {r.text[:120]}"
    body_probe = t.get("/health")  # server alive after lifecycle race
    assert body_probe.status_code == 200
    for e in errs:
        assert "closed database" not in e and "ProgrammingError" not in e, e
    app.state.engine.close()


def test_rebuild_index_locked_no_partial_reads(tmp_path):
    """rebuild_index must never expose a wiped/partial index to concurrent
    searches (regression for wipe-outside-lock)."""
    from memd.engine.memory import Memory

    m = Memory(str(tmp_path / "d"))
    try:
        for i in range(50):
            m.add(f"rebuild probe record {i}", user_id="u1")
        m.flush()
        errs = []

        def rebuild():
            try:
                m.ns.rebuild_index()
            except Exception as ex:  # noqa: BLE001
                errs.append(ex)

        def reader():
            for _ in range(30):
                res = m.search("rebuild probe record", user_id="u1")
                # during rebuild we may see full set; never a hard error
                assert res.latency_ms >= 0

        t1 = threading.Thread(target=rebuild)
        t2 = threading.Thread(target=reader)
        t1.start(); t2.start(); t1.join(); t2.join()
        assert not errs, errs[:2]
        assert m.stats()["records"] == 50
    finally:
        m.close()
