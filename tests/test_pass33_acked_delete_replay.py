"""Pass 33: recovery must reproduce the ACKNOWLEDGED history exactly.

Replay used to apply the whole ops log first and the WAL records after it, so
a record deleted (soft or hard) while still in the WAL came back to life on
the next open after a crash: the later record upsert re-created what the
delete op had removed. A partial fold (rotate) also dropped ops whose target
lived in an older segment, so a cache wipe, a rebuild or a second node
resurrected those records too, and a due hard delete folded by a rotate was
never physically purged.

The invariant these tests pin: every durable event (a WAL frame or an op)
carries a sequence number, and every consumer - open, rebuild, rotate,
compact - folds events in that one total order, with a segment standing in
for every event at or below its fold_seq for the ids it holds.
"""
import os
import shutil
import signal
import subprocess
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from memd.core.schema import MemoryRecord, now_ms  # noqa: E402
from memd.engine.memory import Memory  # noqa: E402
from memd.storage import engine as storage_engine  # noqa: E402
from memd.storage.engine import StorageEngine  # noqa: E402
from memd.storage.objectstore import LocalObjectStore, ObjectStore  # noqa: E402

SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
CFG = {"embedder": "hash", "rate_max_writes": 10**9, "dup_max_repeats": 10**9}

# The child acks (fsync'd file) only AFTER delete()/delete_many() returned,
# keeps writing a little, then waits to be SIGKILLed: nothing after the ack
# may run - no close(), no watermark, no rotate.
CHILD = r"""
import os, sys, time
sys.path.insert(0, {src!r})
from memd.core.schema import MemoryRecord, Scope
from memd.engine.memory import Memory

root, ack, mode, kind = sys.argv[1:5]
m = Memory(root, config={cfg!r})
m.add_events([{{"content": f"filler note {{i}}", "user_id": "u"}} for i in range(20)])
ids = m.add_events([{{"content": f"erasable quokka{{i}} payload", "user_id": "u"}} for i in range(3)])
keep = m.add("survivor quokka keeper", user_id="u")[0]
m.flush()
hard = kind == "hard"
if mode == "single":
    for rid in ids:
        m.delete(rid, hard=hard)
elif mode == "batch":
    m.delete_many(ids, hard=hard)
elif mode == "readd":
    # delete, then re-import the SAME id (a native restore keeps ids)
    for rid in ids:
        m.delete(rid, hard=hard)
    m.ns.append([MemoryRecord.create(namespace="default", kind="raw_event", content=f"reborn quokka {{rid}}",
                                     scope=Scope(user="u"), record_id=rid) for rid in ids])
# later, unrelated writes land in the WAL after the delete op
m.add_events([{{"content": f"after note {{i}}", "user_id": "u"}} for i in range(5)])
with open(ack, "w") as f:
    f.write(" ".join(ids) + "\n" + keep + "\n")
    f.flush()
    os.fsync(f.fileno())
time.sleep(120)
"""


def _crash_after_ack(tmp_path, mode: str, kind: str) -> tuple[str, list[str], str]:
    root = str(tmp_path / "data")
    ack = str(tmp_path / "ack")
    child = subprocess.Popen(
        [sys.executable, "-c", CHILD.format(src=SRC, cfg=CFG), root, ack, mode, kind],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    try:
        deadline = time.monotonic() + 60
        while not os.path.exists(ack) or not open(ack).read().endswith("\n"):
            if child.poll() is not None:
                raise AssertionError(f"child died: {child.stderr.read().decode()[-2000:]}")
            assert time.monotonic() < deadline, "child never acked"
            time.sleep(0.02)
        with open(ack) as f:
            ids_line, keep = f.read().splitlines()[:2]
        os.kill(child.pid, signal.SIGKILL)
        child.wait(timeout=15)
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=15)
    return root, ids_line.split(), keep


class TestAckedDeleteSurvivesSigkill:
    @pytest.mark.parametrize("mode", ["single", "batch"])
    @pytest.mark.parametrize("kind", ["soft", "hard"])
    def test_deleted_records_stay_deleted_after_reopen(self, tmp_path, mode, kind):
        root, ids, keep = _crash_after_ack(tmp_path, mode, kind)
        m = Memory(root, config=CFG)
        try:
            back = [rid for rid in ids if m.get(rid) is not None]
            assert not back, f"{kind} {mode} delete was acked, then resurrected on reopen: {back}"
            hits = [i.content for i in m.search("quokka", user_id="u").items]
            assert not any("erasable" in h for h in hits), hits
            assert any("survivor" in h for h in hits), "unrelated record lost"
            assert m.get(keep) is not None
            for rid in ids:
                row = m.ns.index.get_by_id(rid, include_deleted=True)
                if kind == "hard":
                    assert row is None, "hard-deleted row must not exist in the index at all"
                else:
                    assert row is None or row.deleted
            # a second clean cycle must not bring them back either
            m.close()
            m = Memory(root, config=CFG)
            assert all(m.get(rid) is None for rid in ids)
        finally:
            m.close()

    @pytest.mark.parametrize("kind", ["soft", "hard"])
    def test_readd_of_the_same_id_after_delete_survives(self, tmp_path, kind):
        """The other half of exact history: a record re-imported with the same
        id AFTER its delete must be live after the crash (applying every op
        after every record would wrongly delete it)."""
        root, ids, _keep = _crash_after_ack(tmp_path, "readd", kind)
        m = Memory(root, config=CFG)
        try:
            for rid in ids:
                got = m.get(rid)
                assert got is not None, f"re-added {rid} lost after {kind} delete + crash"
                assert got["content"] == f"reborn quokka {rid}"
        finally:
            m.close()


WATERMARK_CHILD = r"""
import os, sys
sys.path.insert(0, {src!r})
from memd.core.schema import MemoryRecord
from memd.storage.engine import StorageEngine
e = StorageEngine(sys.argv[1])
ns = e.namespace("wm")
ns.append([MemoryRecord.create(namespace="wm", kind="raw_event", content="late arrival zebu")])
os._exit(0)   # crash: the index transaction holding the row never commits
"""


def test_frame_after_interleaved_ops_is_not_hidden_by_the_watermark(tmp_path):
    """Frame seqs used to be inferred from position (wal_base_seq + index),
    ignoring that ops consume seqs too. After [frame, op, frame] + clean close
    the watermark read 3, so the next frame (really seq 4) was computed as 3,
    skipped on replay, and lost from the index after a crash."""
    root = str(tmp_path / "d")
    e = StorageEngine(root)
    ns = e.namespace("wm")
    a = MemoryRecord.create(namespace="wm", kind="raw_event", content="first zebu")
    ns.append([a])
    ns.append_ops([{"op": "tombstone", "id": a.id, "at": now_ms()}])
    ns.append([MemoryRecord.create(namespace="wm", kind="raw_event", content="second zebu")])
    e.close()
    subprocess.run([sys.executable, "-c", WATERMARK_CHILD.format(src=SRC), root], check=True)
    e = StorageEngine(root)
    try:
        live = [r.content for r in e.namespace("wm").index.all_records() if not r.deleted]
        assert "late arrival zebu" in live, f"acked frame invisible after crash: {live}"
    finally:
        e.close()


# --------------------------------------------------------------- full replay


def _wipe_cache(root: str) -> None:
    cache = os.path.join(root, "_cache")
    for name in os.listdir(cache):
        p = os.path.join(cache, name)
        shutil.rmtree(p, ignore_errors=True) if os.path.isdir(p) else os.unlink(p)


def _rec(ns: str, content: str, rid: str | None = None) -> MemoryRecord:
    return MemoryRecord.create(namespace=ns, kind="raw_event", content=content, record_id=rid)


def _live(ns) -> dict[str, str]:
    return {r.id: r.content for r in ns.index.all_records() if not r.deleted}


class _PerFrameStore(LocalObjectStore):
    """Local files, but on the object-store write path: no persistent log
    handle, one durable append per frame, frame-count rotation - the path
    S3ObjectStore takes. Replay reads it through the same store.get()."""
    open_log = ObjectStore.open_log


@pytest.fixture(params=["local", "per-frame"])
def make_engine(request, tmp_path):
    root = str(tmp_path / "d")

    def make():
        store = _PerFrameStore(root) if request.param == "per-frame" else None
        return StorageEngine(root, store=store)
    return root, make


class TestFullReplayIsExact:
    def test_fresh_cache_replay_matches_history(self, make_engine):
        root, make = make_engine
        e = make()
        ns = e.namespace("h")
        x, y, z = _rec("h", "x one"), _rec("h", "y one"), _rec("h", "z one")
        ns.append([x, y, z])
        ns.append_ops([{"op": "tombstone", "id": x.id, "at": 1}])
        ns.append_ops([{"op": "tombstone", "id": y.id, "at": 1},
                       {"op": "hard_delete", "id": y.id, "deadline": now_ms() + 10**8}])
        ns.append([_rec("h", "y reborn", rid=y.id)])        # re-add after hard delete
        ns.append_ops([{"op": "hard_delete", "id": z.id, "deadline": now_ms() + 10**8}])
        expected = _live(ns)
        assert x.id not in expected and z.id not in expected and expected[y.id] == "y reborn"
        e.close()
        _wipe_cache(root)
        e = make()
        try:
            assert _live(e.namespace("h")) == expected
        finally:
            e.close()

    def test_rotate_keeps_ops_that_target_older_segments(self, make_engine):
        root, make = make_engine
        e = make()
        ns = e.namespace("r")
        x, y, a, b = (_rec("r", c) for c in ("x old", "y old", "fact a", "fact b"))
        ns.append([x, y, a, b])
        ns.rotate("probe")                                   # x, y, a, b -> seg1
        ns.append_ops([{"op": "tombstone", "id": x.id, "at": 5}])
        ns.append_ops([{"op": "tombstone", "id": y.id, "at": 5},
                       {"op": "hard_delete", "id": y.id, "deadline": now_ms() + 10**8}])
        ns.append_ops([{"op": "supersede", "old": a.id, "new": b.id, "at": 7}])
        ns.append([_rec("r", "unrelated")])
        ns.rotate("probe")                                   # ops target seg1 only
        expected = _live(ns)
        assert x.id not in expected and y.id not in expected
        e.close()
        _wipe_cache(root)
        e = make()
        try:
            ns = e.namespace("r")
            assert _live(ns) == expected, "rotate dropped ops for records in older segments"
            assert ns.index.get_by_id(a.id).time.superseded_by == b.id
            ns.rebuild_index()
            assert _live(ns) == expected
            assert ns.index.get_by_id(a.id).time.superseded_by == b.id
            ns.compact(force=False)                          # the fold must agree too
            ns.rebuild_index()
            assert _live(ns) == expected
            assert ns.index.get_by_id(a.id).time.superseded_by == b.id
            assert ns.pending_hard_deletes == 1, "undue purge must stay scheduled"
        finally:
            e.close()

    def test_compaction_keeps_a_readd_after_hard_delete(self, make_engine):
        root, make = make_engine
        e = make()
        ns = e.namespace("c")
        x = _rec("c", "x first")
        ns.append([x])
        ns.rotate("probe")
        ns.append_ops([{"op": "tombstone", "id": x.id, "at": 1},
                       {"op": "hard_delete", "id": x.id, "deadline": 0}])   # already due
        ns.rotate("probe")
        ns.append([_rec("c", "x second", rid=x.id)])
        ns.rotate("probe")
        ns.compact(force=False)                              # enforces the due purge
        assert _live(ns) == {x.id: "x second"}
        e.close()
        _wipe_cache(root)
        e = make()
        try:
            ns = e.namespace("c")
            assert _live(ns) == {x.id: "x second"}
            blob = b"".join(e.store.get(f"ns/c/{s['name']}") or b"" for s in ns.manifest.segments)
            assert b"x first" not in blob, "due hard delete must purge the old bytes"
            assert b"x second" in blob
        finally:
            e.close()

    def test_undue_hard_delete_may_keep_bytes_but_never_visibility(self, make_engine):
        """A not-yet-due hard delete may leave the bytes in the folded segment
        until the deadline (D7), but replaying that segment must not serve
        the record - and a later re-add of the id must still win."""
        root, make = make_engine
        e = make()
        ns = e.namespace("u")
        v = _rec("u", "victim bytes")
        ns.append([v])
        ns.append_op({"op": "hard_delete", "id": v.id, "deadline": now_ms() + 10**8})
        ns.compact()
        assert any(r.id == v.id for r in ns.load_all_records()[0]), "deferred purge"
        e.close()
        _wipe_cache(root)
        e = make()
        try:
            ns = e.namespace("u")
            assert ns.index.get_by_id(v.id, include_deleted=True) is None
            ns.append([_rec("u", "victim back", rid=v.id)])
            ns.compact()
            e.close()
            _wipe_cache(root)
            e = make()
            assert _live(e.namespace("u")) == {v.id: "victim back"}
        finally:
            e.close()

    def test_legacy_frames_without_seq_replay_in_history_order(self, tmp_path, monkeypatch):
        """WAL frames written before frames carried their seq still replay in
        the right order relative to the ops log (ops own their seqs, frames
        fill the gaps)."""
        # simulate the pre-0.2.0 writer: frames without a seq stamp
        monkeypatch.setattr(storage_engine, "_wal_stamp", lambda payload, seq: payload, raising=False)
        root = str(tmp_path / "d")
        e = StorageEngine(root)
        ns = e.namespace("lg")
        x, y = _rec("lg", "x v1"), _rec("lg", "y v1")
        ns.append([x, y])
        ns.append_ops([{"op": "tombstone", "id": x.id, "at": 1}])
        ns.append([_rec("lg", "x v2", rid=x.id)])
        ns.append_ops([{"op": "tombstone", "id": y.id, "at": 1}])
        e.close()
        monkeypatch.undo()
        _wipe_cache(root)
        e = StorageEngine(root)
        try:
            ns = e.namespace("lg")
            assert _live(ns) == {x.id: "x v2"}
            z = _rec("lg", "z new")
            ns.append([z])                                   # new-format frame after legacy ones
            e.close()
            _wipe_cache(root)
            e = StorageEngine(root)
            assert _live(e.namespace("lg")) == {x.id: "x v2", z.id: "z new"}
        finally:
            e.close()


def test_due_hard_delete_folded_by_a_rotate_is_still_purged(tmp_path):
    """A rotate that folds a DUE hard delete must keep it scheduled: the
    record's bytes live in an older segment only compaction can rewrite."""
    m = Memory(str(tmp_path / "data"), encrypt=False,
               config=dict(CFG, hard_delete_deadline_ms=60))
    try:
        marker = b"PURGE-ACROSS-ROTATE-q7"
        rid = m.add(f"record {marker.decode()}", user_id="u")[0]
        m.ns.rotate("probe")                                 # bytes now in a segment
        m.delete(rid, hard=True)
        time.sleep(0.08)                                     # deadline passes
        m.ns.rotate("probe")                                 # folds the due op
        assert m.ns.has_due_deletes(), "due purge dropped by the rotate"
        m.add_events([{"content": "later activity", "user_id": "u"}])   # re-checks deadlines
        m.flush()                                            # drains background maintenance
        d = os.path.join(m.engine.root, "ns", "default")
        raw = b"".join(open(os.path.join(d, f), "rb").read() for f in os.listdir(d)
                       if f not in ("manifest.json", ".owner"))
        assert marker not in raw, "hard-deleted bytes survived past the deadline"
    finally:
        m.close()
