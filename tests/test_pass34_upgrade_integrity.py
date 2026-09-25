"""Pass 34: upgrades, downgrades and the bulk read paths keep acked deletes deleted.

A verifier ran randomized crash runs against a model oracle. Durability of
the current format held; compatibility and some older paths did not:

  - format-1 WAL frames carry no seq, and the old seq counter was rebuilt
    from the ops log after a crash, so it REUSED numbers. Gap-filling frame
    seqs then put frames after their own tombstones and resurrected deletes
    the old binary itself hid (also after a downgrade, a crash and an
    upgrade);
  - a format-1 binary ignores the ops a format-2 segment header carries and
    serves (then compacts in) records they deleted;
  - export dumped raw stored copies: acked-deleted and hard-deleted records;
  - a crash inside a compaction whose output was empty re-adopted every
    segment it had just replaced;
  - a torn ops-log tail was never repaired, so a delete acked after it was
    invisible to a cold open and dropped by the next rotate.
"""
import json
import os
import shutil
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from memd.core.schema import MemoryRecord, now_ms, records_from_jsonl, records_to_jsonl  # noqa: E402
from memd.engine.memory import Memory  # noqa: E402
from memd.index.sqlite_index import NamespaceIndex  # noqa: E402
from memd.storage import engine as storage_engine  # noqa: E402
from memd.storage.engine import StorageEngine, StoreFormatError, _frame_encode, _frame_iter  # noqa: E402
from memd.storage.objectstore import LocalObjectStore  # noqa: E402

# Written by the v0.2.0 release binary (f979ea8) through its StorageEngine:
#   session 1: add A, add B, then crash (os._exit) - manifest.seq stays 0
#   session 2: delete B (op seq 1), add D, delete D (op seq 3), close
# Its own warm open serves A only; a gap-filling reader put B and D at 4 and 5.
FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "legacy_f979ea8_reused_seqs")
CFG = {"embedder": "hash", "rate_max_writes": 10**9, "dup_max_repeats": 10**9}


def _rec(ns: str, content: str, rid: str | None = None) -> MemoryRecord:
    return MemoryRecord.create(namespace=ns, kind="raw_event", content=content, record_id=rid)


def _live(ns) -> list[str]:
    ns.index.flush()
    return sorted(r.id for r in ns.index.all_records() if not r.deleted)


def _wipe(cache: str) -> None:
    shutil.rmtree(cache, ignore_errors=True)


def _manifest(root: str, ns: str) -> dict:
    with open(os.path.join(root, "ns", ns, "manifest.json")) as f:
        return json.load(f)


def _fixture_store(tmp_path, seq: int | None = None) -> tuple[str, str]:
    root, cache = str(tmp_path / "store"), str(tmp_path / "cache")
    shutil.copytree(FIXTURE, root)
    if seq is not None:  # what an earlier gap-filling open persisted
        m = _manifest(root, "t")
        m["seq"] = seq
        with open(os.path.join(root, "ns", "t", "manifest.json"), "w") as f:
            json.dump(m, f)
    return root, cache


def _write_format1(root: str, ns: str, *, seq: int, wal_base_seq: int = 0, wal: bytes = b"",
                   ops: bytes = b"", segments: dict[str, tuple[int, list[MemoryRecord]]] | None = None):
    """A format-1 namespace, byte for byte: framed WAL and ops logs, v2
    segment blobs, and a manifest with an integer version and no format."""
    d = os.path.join(root, "ns", ns)
    os.makedirs(d, exist_ok=True)
    entries = []
    for name, (fold, recs) in (segments or {}).items():
        with open(os.path.join(d, name), "wb") as f:
            f.write(json.dumps({"_seg": {"fold_seq": fold}}).encode() + b"\n" + records_to_jsonl(recs))
        entries.append({"name": name, "records": len(recs), "fold_seq": fold, "reason": "size"})
    for key, data in (("wal", wal), ("ops", ops)):
        if data:
            with open(os.path.join(d, key), "wb") as f:
                f.write(data)
    with open(os.path.join(d, "manifest.json"), "w") as f:
        json.dump({"version": 7, "seq": seq, "segments": entries, "wal_size": len(wal),
                   "ops_size": len(ops), "wal_base_seq": wal_base_seq,
                   "snapshot_seq": 0, "snapshot_name": ""}, f)


def _frame(recs: list[MemoryRecord], stamp: int | None = None) -> bytes:
    payload = records_to_jsonl(recs)
    return _frame_encode(payload if stamp is None else storage_engine._wal_stamp(payload, stamp))


def _op(**op) -> bytes:
    return _frame_encode(json.dumps(op, separators=(",", ":")).encode())


# --------------------------------------------------------------- format 1 -> 2


class TestFormat1Upgrade:
    def test_reused_seqs_keep_the_old_binarys_deletes(self, tmp_path):
        root, cache = _fixture_store(tmp_path)
        e = StorageEngine(root, cache_dir=cache)
        try:
            assert _live(e.namespace("t")) == ["rA"], "a gap-filled frame undid its own tombstone"
        finally:
            e.close()
        m = _manifest(root, "t")
        assert m["format"] == 2
        assert not {"wal", "ops"} & set(os.listdir(os.path.join(root, "ns", "t"))), \
            "the format-1 logs must be folded away by the migration"
        _wipe(cache)
        e = StorageEngine(root, cache_dir=cache)
        try:
            ns = e.namespace("t")
            assert _live(ns) == ["rA"]
            ns.rebuild_index()
            assert _live(ns) == ["rA"]
            ns.append([_rec("t", "later write", rid="rZ")])
            ns.rotate("probe")
            ns.compact(force=True)
        finally:
            e.close()
        _wipe(cache)
        e = StorageEngine(root, cache_dir=cache)
        try:
            assert _live(e.namespace("t")) == ["rA", "rZ"]
        finally:
            e.close()

    def test_reused_seqs_over_the_old_binarys_warm_index(self, tmp_path):
        """The old binary's warm cache (every event applied live) is what
        served A only; opening over it must not replay B and D back in."""
        root, cache = _fixture_store(tmp_path)
        os.makedirs(cache)
        with open(os.path.join(FIXTURE, "ns", "t", "wal"), "rb") as f:
            recs = [r for fr in _frame_iter(f.read()) for r in records_from_jsonl(fr)]
        idx = NamespaceIndex(os.path.join(cache, "t.sqlite"))
        idx.upsert_batch([(r, None, "") for r in recs])
        idx.tombstone("rB", now_ms())
        idx.tombstone("rD", now_ms())
        idx.flush()
        idx.set_meta("applied_seq", "3")
        idx.close()
        e = StorageEngine(root, cache_dir=cache)
        try:
            assert _live(e.namespace("t")) == ["rA"]
        finally:
            e.close()

    def test_a_seq_an_earlier_open_inflated_still_keeps_the_deletes(self, tmp_path):
        """An open that gap-filled persisted manifest.seq=5, so the counts now
        reconcile; a copy ingested before its delete still may not follow it."""
        root, cache = _fixture_store(tmp_path, seq=5)
        e = StorageEngine(root, cache_dir=cache)
        try:
            assert _live(e.namespace("t")) == ["rA"]
        finally:
            e.close()

    def test_downgrade_crash_upgrade_keeps_the_deletes(self, tmp_path):
        """A build without the fence stamped X@1 and Y@2 and crashed with the
        manifest at seq 0; an older binary then reused seq 1 for 'delete Y',
        appended Z unstamped and deleted it at seq 3."""
        root = str(tmp_path / "store")
        x, y, z = _rec("dg", "x"), _rec("dg", "y"), _rec("dg", "z")
        at = now_ms() + 1000
        _write_format1(
            root, "dg", seq=3,
            wal=_frame([x], stamp=1) + _frame([y], stamp=2) + _frame([z]),
            ops=_op(op="tombstone", id=y.id, at=at, seq=1) + _op(op="tombstone", id=z.id, at=at, seq=3))
        e = StorageEngine(root)
        try:
            assert _live(e.namespace("dg")) == [x.id]
        finally:
            e.close()

    def test_a_consistent_history_keeps_a_readd_after_delete(self, tmp_path):
        """Nothing reused: gap-filling is exact, so a record re-added with the
        same id after its delete is live."""
        root = str(tmp_path / "store")
        x, y = _rec("ok", "x v1"), _rec("ok", "y v1")
        x2 = _rec("ok", "x v2", rid=x.id)
        _write_format1(root, "ok", seq=4,
                       wal=_frame([x, y]) + _frame([x2]),
                       ops=_op(op="tombstone", id=x.id, at=1, seq=2) + _op(op="tombstone", id=y.id, at=1, seq=4))
        e = StorageEngine(root)
        try:
            ns = e.namespace("ok")
            assert _live(ns) == [x.id]
            assert ns.index.get_by_id(x.id).content == "x v2"
        finally:
            e.close()

    def test_a_delete_a_format1_rotate_dropped_is_kept_from_the_local_index(self, tmp_path):
        """A format-1 rotate folded only the WAL and deleted the ops log, so a
        delete whose target already lived in an older segment was lost from
        durable data; only the old binary's live index still applied it."""
        root, cache = str(tmp_path / "store"), str(tmp_path / "cache")
        a, b, c, d = (_rec("ev", f"record {n}") for n in "abcd")
        _write_format1(root, "ev", seq=5, wal_base_seq=5,
                       segments={"seg-00LEGACY0001": (2, [a, b, c]), "seg-00LEGACY0002": (5, [d])})
        os.makedirs(cache)
        idx = NamespaceIndex(os.path.join(cache, "ev.sqlite"))
        idx.upsert_batch([(r, None, "") for r in (a, b, c, d)])
        idx.tombstone(a.id, now_ms())                  # the op the rotate dropped
        idx.mark_superseded(b.id, d.id, now_ms())      # likewise
        idx.flush()
        idx.set_meta("applied_seq", "5")
        idx.close()
        e = StorageEngine(root, cache_dir=cache)
        try:
            assert _live(e.namespace("ev")) == sorted([b.id, c.id, d.id])
        finally:
            e.close()
        _wipe(cache)  # the durable state must now say so on its own
        e = StorageEngine(root, cache_dir=cache)
        try:
            ns = e.namespace("ev")
            assert _live(ns) == sorted([b.id, c.id, d.id])
            assert ns.index.get_by_id(b.id).time.superseded_by == d.id
            ns.compact(force=True)
            ns.rebuild_index()
            assert _live(ns) == sorted([b.id, c.id, d.id])
            assert ns.index.get_by_id(b.id).time.superseded_by == d.id
        finally:
            e.close()

    @pytest.mark.parametrize("point", ["segment", "manifest", "wal", "ops"])
    def test_a_crash_mid_migration_leaves_the_old_or_the_new_state(self, tmp_path, monkeypatch, point):
        root, cache = _fixture_store(tmp_path)
        mpath = os.path.join(root, "ns", "t", "manifest.json")
        with open(mpath, "rb") as f:
            before = f.read()
        put, delete = LocalObjectStore.put, LocalObjectStore.delete

        def crashing_put(self, key, data):
            if (point == "segment" and "/seg-" in key) or (
                    point == "manifest" and key.endswith("manifest.json") and b'"format": 2' in data):
                raise KeyboardInterrupt("power loss")
            return put(self, key, data)

        def crashing_delete(self, key):
            if key.endswith(f"/{point}"):
                raise KeyboardInterrupt("power loss")
            return delete(self, key)

        monkeypatch.setattr(LocalObjectStore, "put", crashing_put)
        monkeypatch.setattr(LocalObjectStore, "delete", crashing_delete)
        with pytest.raises(KeyboardInterrupt):
            StorageEngine(root, cache_dir=cache).namespace("t")
        monkeypatch.undo()
        if point in ("segment", "manifest"):
            with open(mpath, "rb") as f:
                assert f.read() == before, "a crash before the commit must leave format 1 untouched"
        else:
            assert _manifest(root, "t")["format"] == 2
        e = StorageEngine(root, cache_dir=cache)
        try:
            assert _live(e.namespace("t")) == ["rA"]
        finally:
            e.close()
        assert _manifest(root, "t")["format"] == 2
        assert not {"wal", "ops"} & set(os.listdir(os.path.join(root, "ns", "t")))
        _wipe(cache)
        e = StorageEngine(root, cache_dir=cache)
        try:
            assert _live(e.namespace("t")) == ["rA"]
        finally:
            e.close()


class TestFormatFence:
    def test_a_format1_parser_fails_loudly_on_an_upgraded_manifest(self, tmp_path):
        root = str(tmp_path / "store")
        e = StorageEngine(root)
        e.namespace("f").append([_rec("f", "hello")])
        e.close()
        m = _manifest(root, "f")
        assert m["format"] == 2
        # the first thing every format-1 release does with a manifest
        # (Manifest.from_dict: version=int(d.get("version", 0)))
        with pytest.raises(ValueError, match="downgrades are not supported"):
            int(m.get("version", 0))

    def test_a_newer_format_is_refused(self, tmp_path):
        root = str(tmp_path / "store")
        e = StorageEngine(root)
        e.namespace("f")
        e.close()
        m = _manifest(root, "f")
        m["format"] = 3
        with open(os.path.join(root, "ns", "f", "manifest.json"), "w") as f:
            json.dump(m, f)
        with pytest.raises(StoreFormatError):
            StorageEngine(root).namespace("f")


# --------------------------------------------------------------- export


class TestExportVisibility:
    def test_export_serves_exactly_what_reads_serve(self, tmp_path):
        e = StorageEngine(str(tmp_path / "store"))
        try:
            ns = e.namespace("x")
            a, b, c, d, v = (_rec("x", f"record {n}") for n in "abcdv")
            old, new = _rec("x", "fact v1"), _rec("x", "fact v2")
            far = now_ms() + 10**8
            ns.append([a, b, old])
            ns.append_ops([{"op": "tombstone", "id": a.id, "at": now_ms()}])
            ns.append([c])
            ns.rotate("probe")                                  # c's bytes in a segment
            ns.append_ops([{"op": "tombstone", "id": c.id, "at": now_ms()},
                           {"op": "hard_delete", "id": c.id, "deadline": far}])
            ns.append([new])
            ns.append_ops([{"op": "supersede", "old": old.id, "new": new.id, "at": now_ms()}])
            ns.append([v])
            ns.append_ops([{"op": "hard_delete", "id": v.id, "deadline": far}])   # purge pending
            ns.append([d])
            lines = [json.loads(l) for l in ns.export_jsonl().splitlines() if l.strip()]
            exported = {r["id"]: r for r in lines}
            assert sorted(exported) == _live(ns) == sorted([b.id, d.id, old.id, new.id])
            assert exported[old.id]["time"]["superseded_by"] == new.id
            raw = {r.id for r in ns.load_all_records()[0]}
            assert {c.id, v.id} <= raw, "precondition: the bytes are still stored"
        finally:
            e.close()

    def test_memory_export_leaves_out_soft_and_hard_deleted(self, tmp_path):
        m = Memory(str(tmp_path / "d"), encrypt=False, config=CFG)
        try:
            keep = m.add("keep this note", user_id="u")[0]
            soft = m.add("soft deleted note", user_id="u")[0]
            hard = m.add("hard deleted note", user_id="u")[0]
            m.ns.rotate("probe")
            m.delete(soft)
            m.delete(hard, hard=True)
            blob = m.export_jsonl()
            ids = {json.loads(l)["id"] for l in blob.splitlines() if l.strip()}
            assert ids == {keep}
            assert b"deleted note" not in blob
            assert b"".join(m.export_jsonl_iter()) == blob
        finally:
            m.close()


# --------------------------------------------------------------- orphan segments


def test_a_crash_inside_an_empty_compaction_does_not_readopt_its_input(tmp_path, monkeypatch):
    root = str(tmp_path / "store")
    e = StorageEngine(root, cache_dir=str(tmp_path / "warm"))
    ns = e.namespace("p")
    a, b = _rec("p", "a"), _rec("p", "b")
    ns.append([a, b])
    ns.rotate("probe")
    ns.append_ops([{"op": "tombstone", "id": a.id, "at": now_ms()},
                   {"op": "tombstone", "id": b.id, "at": now_ms()},
                   {"op": "hard_delete", "id": b.id, "deadline": 0}])
    delete = LocalObjectStore.delete

    def crashing_delete(self, key):
        if "/seg-" in key:
            raise KeyboardInterrupt("power loss before the old segment is removed")
        return delete(self, key)

    monkeypatch.setattr(LocalObjectStore, "delete", crashing_delete)
    with pytest.raises(KeyboardInterrupt):
        ns.compact()
    monkeypatch.undo()
    assert any(f.startswith("seg-") for f in os.listdir(os.path.join(root, "ns", "p")))
    e2 = StorageEngine(root, cache_dir=str(tmp_path / "cold"))
    try:
        assert _live(e2.namespace("p")) == [], "residue of the compaction was re-adopted"
    finally:
        e2.close()
    assert not any(f.startswith("seg-") for f in os.listdir(os.path.join(root, "ns", "p"))), \
        "the residue must be collected at open"
    m = _manifest(root, "p")
    assert m["segments"] == [] and m.get("checkpoint") == "" and m.get("checkpoint_seq", 0) > 0, \
        "an empty compaction commits an explicit empty checkpoint"


# --------------------------------------------------------------- ops log tail


def _ops_path(root: str, ns: str) -> str:
    return os.path.join(root, "ns", ns, "ops")


class TestOpsLogRepair:
    def test_an_acked_delete_after_a_torn_tail_survives(self, tmp_path):
        root, cache = str(tmp_path / "store"), str(tmp_path / "cache")
        a, b = _rec("o", "a"), _rec("o", "b")
        e = StorageEngine(root, cache_dir=cache)
        e.namespace("o").append([a, b])
        e.close()
        with open(_ops_path(root, "o"), "ab") as f:   # power loss mid-append
            f.write((500).to_bytes(4, "big") + b'{"op":"tombst')
        e = StorageEngine(root, cache_dir=cache)
        e.namespace("o").append_op({"op": "tombstone", "id": a.id, "at": now_ms()})
        e.close()
        for step in ("cold", "rotate+cold"):
            _wipe(cache)
            e = StorageEngine(root, cache_dir=cache)
            try:
                ns = e.namespace("o")
                assert _live(ns) == [b.id], f"acked delete lost ({step})"
                ns.rotate("probe")
            finally:
                e.close()

    def test_ops_an_older_binary_acked_behind_a_torn_record_are_salvaged(self, tmp_path):
        root, cache = str(tmp_path / "store"), str(tmp_path / "cache")
        a, b, c = _rec("o", "a"), _rec("o", "b"), _rec("o", "c")
        e = StorageEngine(root, cache_dir=cache)
        ns = e.namespace("o")
        ns.append([a, b, c])
        ns.append_op({"op": "tombstone", "id": c.id, "at": now_ms()})
        seq = ns.manifest.seq
        e.close()
        with open(_ops_path(root, "o"), "ab") as f:
            f.write((500).to_bytes(4, "big") + b'{"op":"tombst')   # torn...
            f.write(_op(op="tombstone", id=a.id, at=now_ms(), seq=seq + 1))  # ...and appended behind
        size = os.path.getsize(_ops_path(root, "o"))
        _wipe(cache)
        e = StorageEngine(root, cache_dir=cache)
        try:
            assert _live(e.namespace("o")) == [b.id]
        finally:
            e.close()
        assert os.path.getsize(_ops_path(root, "o")) == size, "a salvaged op must not be cut off"

    def test_a_failed_append_is_cut_back_before_the_next_op(self, tmp_path, monkeypatch):
        root, cache = str(tmp_path / "store"), str(tmp_path / "cache")
        a, b = _rec("o", "a"), _rec("o", "b")
        e = StorageEngine(root, cache_dir=cache)
        ns = e.namespace("o")
        ns.append([a, b])
        append = LocalObjectStore.append

        def torn_append(self, key, data):
            if key.endswith("/ops"):
                with open(self._path(key), "ab") as f:
                    f.write(data[: len(data) // 2])
                raise OSError("no space left on device")
            return append(self, key, data)

        monkeypatch.setattr(LocalObjectStore, "append", torn_append)
        with pytest.raises(OSError):
            ns.append_op({"op": "tombstone", "id": a.id, "at": now_ms()})
        monkeypatch.undo()
        assert os.path.getsize(_ops_path(root, "o")) == 0, "the torn half-frame must be cut back"
        ns.append_op({"op": "tombstone", "id": b.id, "at": now_ms()})  # acked
        e.close()
        _wipe(cache)
        e = StorageEngine(root, cache_dir=cache)
        try:
            assert _live(e.namespace("o")) == [a.id]
        finally:
            e.close()


# --------------------------------------------------------------- segment GC

MARKER = "PURGE-GC-7q3v-zebu"
GC_CHILD = r"""
import os, sys
sys.path.insert(0, {src!r})
from memd.engine.memory import Memory
from memd.storage import objectstore

root, ack = sys.argv[1:3]
m = Memory(root, encrypt=False, config={cfg!r})
victim = m.add("record {marker}", user_id="u")[0]
keep = m.add("unrelated keeper note", user_id="u")[0]
m.ns.rotate("probe")                      # the victim's bytes now sit in a segment
m.delete(victim, hard=True)               # acked hard delete; purge pending
with open(ack, "w") as f:
    f.write(victim + " " + keep)
    f.flush()
    os.fsync(f.fileno())
real = objectstore.LocalObjectStore.delete
def delete(self, key):
    if "/seg-" in key:
        os._exit(9)                       # killed after the commit, before the old segment goes
    return real(self, key)
objectstore.LocalObjectStore.delete = delete
m.ns.compact(force=True)                  # purges the victim from its output
os._exit(3)                               # never reached
"""


def _store_bytes(root: str) -> bytes:
    """Every durable object of the data root (the derived index cache aside)."""
    out = []
    for dirpath, dirs, files in os.walk(os.path.join(root, "store", "ns")):
        for fn in files:
            with open(os.path.join(dirpath, fn), "rb") as f:
                out.append(f.read())
    return b"".join(out)


def test_a_killed_compaction_leaves_no_hard_deleted_bytes_after_reopen(tmp_path):
    root, ack = str(tmp_path / "data"), str(tmp_path / "ack")
    src = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
    child = subprocess.run(
        [sys.executable, "-c", GC_CHILD.format(src=src, cfg=CFG, marker=MARKER), root, ack],
        capture_output=True, timeout=120)
    assert child.returncode == 9, child.stderr.decode()[-2000:]
    victim, keep = open(ack).read().split()
    nsdir = os.path.join(root, "store", "ns", "default")
    m = _manifest(os.path.join(root, "store"), "default")
    orphans = [f for f in os.listdir(nsdir)
               if f.startswith("seg-") and f not in {s["name"] for s in m["segments"]}]
    # positive control: the killed compaction committed, but the segment it
    # replaced - holding the hard-deleted record - is still on disk
    assert orphans, "precondition: the kill left the replaced segment behind"
    assert MARKER.encode() in _store_bytes(root), "precondition: the marker is on disk"

    mem = Memory(root, encrypt=False, config=CFG)
    try:
        assert mem.get(victim) is None and mem.get(keep) is not None
        collected = mem.stats().get("segments_collected")
        mem.audit.flush()
        gc = sorted(e["target"] for e in mem.audit.read() if e["action"] == "segment_gc")
    finally:
        mem.close()
    assert MARKER.encode() not in _store_bytes(root), "hard-deleted bytes survived the reopen"
    left = [f for f in os.listdir(nsdir) if f.startswith("seg-")]
    assert left == [s["name"] for s in _manifest(os.path.join(root, "store"), "default")["segments"]]
    assert collected == len(orphans)
    assert gc == sorted(orphans), "each deletion must be in the audit ledger"


def test_collection_never_touches_a_segment_newer_than_the_checkpoint(tmp_path):
    """An unreferenced segment stamped with the current generation may be a
    writer's uncommitted output (a crashed rotate's, here): it stays."""
    root = str(tmp_path / "store")
    e = StorageEngine(root)
    ns = e.namespace("n")
    ns.append([_rec("n", "a")])
    ns.compact(force=True)                               # checkpoint at gen G
    ns.append([_rec("n", "b")])
    fresh = "seg-ZZZZZZZZZZZZZZZZZZZZZZZZZZ"
    ns._write_segment(fresh, [_rec("n", "b copy")], ns.manifest.seq)  # gen >= G, never committed
    e.close()
    e = StorageEngine(root)
    try:
        ns = e.namespace("n")
        assert fresh in os.listdir(os.path.join(root, "ns", "n"))
        assert ns.stats().get("segments_collected", 0) == 0
    finally:
        e.close()
