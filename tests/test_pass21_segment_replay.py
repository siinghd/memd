"""Pass 20 regression: fresh-cache rebuild must recover EVERYTHING from
segments.

_node-loss scenario: the derived index cache (rebuildable by contract,
ADR-5) is destroyed; a new process replays segments + WAL tail. A tuple-
unpacking bug in NamespaceStore._open made every segment replay throw,
get swallowed, and the handler itself NameError - so node loss silently
reduced a namespace to its WAL tail while metrics counted 'parse errors'.
"""
import shutil
import sys

sys.path.insert(0, "/home/deploy/agent-memory/src")

from memd.engine.memory import Memory


def test_fresh_cache_rebuild_recovers_all_segments(tmp_path):
    root = str(tmp_path / "data")
    mem = Memory(root)
    try:
        total = 0
        for b in range(15):
            events = [{"content": f"recovery probe record {b * 40 + j} marker text",
                       "session_id": f"s{(b * 40 + j) % 10}", "user_id": "u1"}
                      for j in range(40)]
            ids = mem.add_events(events)
            total += len(ids)
            if (b + 1) % 3 == 0:
                mem.ns.rotate("rebuild-bench")
        assert len(mem.ns.manifest.segments) >= 4, "test needs multiple segments"
    finally:
        mem.close()

    # node loss: derived index cache is gone; only object-store blobs remain
    cache_dir = root + "/store/_cache"
    assert shutil.rmtree(cache_dir, ignore_errors=True) is None

    mem2 = Memory(root)
    try:
        st = mem2.stats()
        assert st["records"] == total, (
            f"fresh-cache rebuild recovered {st['records']}/{total} records")
        res = mem2.search("recovery probe marker", user_id="u1")
        assert res.items, "rebuilt namespace not searchable"
        # index watermark advanced correctly for the NEXT restart too
        mem2.ns.rotate("post-rebuild")
        assert mem2.stats()["records"] == total
    finally:
        mem2.close()


def test_corrupt_segment_degrades_without_crashing(tmp_path):
    """One poisoned segment must be skipped+counted, never brick the open."""
    root = str(tmp_path / "data")
    mem = Memory(root)
    try:
        n = 0
        for b in range(6):
            ids = mem.add_events([{"content": f"survivor {b * 30 + j}",
                                   "user_id": "u1"} for j in range(30)])
            n += len(ids)
            if (b + 1) % 2 == 0:
                mem.ns.rotate("poison-bench")
        # poison the OLDEST segment blob directly
        segs = sorted(mem.ns.manifest.segments, key=lambda s: s["fold_seq"])
        victim = segs[0]["name"]
        mem.engine.store.put(f"ns/default/{victim}", b"\x00\x01not-a-valid-segment")
    finally:
        mem.close()

    shutil.rmtree(root + "/store/_cache", ignore_errors=True)
    mem2 = Memory(root)  # must not raise
    try:
        st = mem2.stats()
        lost = n - sum(30 for _ in range(1))  # victims: first two batches
        assert st["records"] < n, "poisoned segment rows unexpectedly survived"
        assert st["records"] > 0, "one bad segment wiped the whole namespace"
    finally:
        mem2.close()
