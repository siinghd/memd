"""Group-commit + concurrency tests: parallel writers share fsyncs, every
ack is durable, and the log stays parseable."""
import concurrent.futures

from memd.engine.memory import Memory
from memd.storage.engine import StorageEngine


def test_concurrent_writers_all_durable(tmp_path):
    m = Memory(str(tmp_path / "d"), config={"rate_max_writes": 10**9})
    def worker(wid):
        for i in range(25):
            m.add(f"writer {wid} event {i} payload text", session_id=f"w{wid}", user_id=f"u{wid}")
        return wid

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(worker, range(8)))
    m.flush()
    st = m.stats()
    assert st["records"] == 200, f"lost writes under concurrency: {st}"
    # log must replay cleanly on a fresh process (no torn frames)
    m.close()
    m2 = Memory(str(tmp_path / "d"))
    try:
        assert m2.stats()["records"] == 200
    finally:
        m2.close()


def test_wal_frames_parse_after_group_commit(tmp_path):
    from memd.core.schema import records_from_jsonl

    e = StorageEngine(str(tmp_path / "d2"))
    ns = e.namespace("gc")
    def w(k):
        ns.append([__import__("memd.core.schema", fromlist=["MemoryRecord"]).MemoryRecord.create(
            namespace="gc", kind="raw_event", content=f"gc {k}", source="user")])
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        list(ex.map(w, range(60)))
    wal_bytes = e.store.get("ns/gc/wal")
    n = 0
    i = 0
    from memd.storage.engine import _frame_iter
    for fr in _frame_iter(wal_bytes):
        recs = records_from_jsonl(fr)
        n += len(recs)
    assert n == 60, f"expected 60 records across frames, parsed {n}"
    e.close()
