"""Pass 35: a stale index snapshot, an interrupted migration and crash orphans.

A review's crash oracle and upgrade kill-matrix found:

  - the index snapshot was installed whenever it was newer than the last
    purge, ignoring the compaction after it. A compaction retires the
    tombstones it applies - the records they deleted are simply absent from
    its output - so an image taken before it still holds them as live rows
    and nothing replayable deletes them again: a cold open served every acked
    soft delete since the snapshot (200/200 at the real snapshot floor, no
    crash needed; 30/30 with a kill after the commit), and reads disagreed
    with export;
  - the format-1 migration wiped the local index BEFORE its commit, and that
    index is the only evidence of the deletes a format-1 rotate dropped: a
    migration killed at its segment or manifest put (or failing on ENOSPC and
    retried) resurrected them and lost supersedes for good;
  - orphans a crash left (a segment or snapshot written but never committed)
    were collected only at the next open, so they kept hard-deleted text
    past the purge compaction that followed;
  - the migration ran under the engine-wide lock, stopping every other
    namespace for its duration.
"""
import errno
import gzip
import json
import os
import shutil
import subprocess
import sys
import threading

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from memd.core.schema import MemoryRecord, now_ms, records_to_jsonl, ulid_new  # noqa: E402
from memd.index.sqlite_index import NamespaceIndex  # noqa: E402
from memd.storage.engine import NamespaceStore, StorageEngine, _frame_encode  # noqa: E402
from memd.storage.objectstore import LocalObjectStore  # noqa: E402

SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
MARKER = "vqzzpass35markerkwbdfhjtr"


def _rec(ns: str, content: str, rid: str | None = None) -> MemoryRecord:
    return MemoryRecord.create(namespace=ns, kind="raw_event", content=content, record_id=rid)


def _live(ns) -> list[str]:
    ns.index.flush()
    return sorted(r.id for r in ns.index.all_records() if not r.deleted)


def _sups(ns) -> dict[str, str]:
    ns.index.flush()
    return {r.id: r.time.superseded_by for r in ns.index.all_records()
            if not r.deleted and r.time.superseded_by}


def _exported(ns) -> list[str]:
    return sorted(json.loads(line)["id"] for line in ns.export_jsonl().splitlines() if line.strip())


def _manifest_path(root: str, ns: str) -> str:
    return os.path.join(root, "ns", ns, "manifest.json")


def _manifest(root: str, ns: str) -> dict:
    with open(_manifest_path(root, ns)) as f:
        return json.load(f)


def _files_with(root: str, needle: bytes) -> list[str]:
    out = []
    for dirpath, _dirs, files in os.walk(root):
        for fn in files:
            path = os.path.join(dirpath, fn)
            with open(path, "rb") as f:
                data = f.read()
            if data[:2] == b"\x1f\x8b":
                try:
                    data += gzip.decompress(data)
                except OSError:
                    pass
            if needle in data:
                out.append(os.path.relpath(path, root))
    return sorted(out)


def _tombstone(ns, rec) -> None:
    ns.append_op({"op": "tombstone", "id": rec.id, "at": now_ms()})


# --------------------------------------------------- the snapshot must cover the compaction


class TestSnapshotCoversTheCompaction:
    def _store(self, tmp_path, monkeypatch, floor: int):
        monkeypatch.setattr(NamespaceStore, "SNAPSHOT_MIN_RECORDS", floor)
        root, cache = str(tmp_path / "store"), str(tmp_path / "cache")
        e = StorageEngine(root, cache_dir=cache)
        ns = e.namespace("s")
        recs = [_rec("s", f"record {i} lorem") for i in range(30)]
        ns.append(recs)
        ns.compact(force=True)
        assert ns.manifest.snapshot_name, "precondition: a snapshot of 30 live records"
        return root, cache, e, ns, recs

    def _cold(self, root, cache):
        shutil.rmtree(cache, ignore_errors=True)
        e = StorageEngine(root, cache_dir=cache)
        return e, e.namespace("s")

    def test_a_compaction_below_the_floor_does_not_leave_the_old_snapshot_referenced(
            self, tmp_path, monkeypatch):
        """No crash: the deletes take the namespace under the snapshot floor,
        so the compaction publishes none - the old image stayed referenced."""
        root, cache, e, ns, recs = self._store(tmp_path, monkeypatch, floor=20)
        for r in recs[:15]:
            _tombstone(ns, r)                     # acked soft deletes; 15 live < 20
        ns.compact(force=True)
        m = ns.manifest
        assert not m.snapshot_name or m.snapshot_seq >= m.checkpoint_seq, \
            "the manifest references a snapshot older than the compaction it names"
        e.close()
        e, ns = self._cold(root, cache)
        try:
            want = sorted(r.id for r in recs[15:])
            assert _live(ns) == want, "a cold open resurrected acked soft deletes"
            assert _exported(ns) == want
        finally:
            e.close()

    def test_a_kill_between_the_compaction_commit_and_the_snapshot_publish(
            self, tmp_path, monkeypatch):
        root, cache, e, ns, recs = self._store(tmp_path, monkeypatch, floor=5)
        for r in recs[:10]:
            _tombstone(ns, r)
        ns.append([_rec("s", "one more")])

        def killed(self):
            raise KeyboardInterrupt("killed after the commit, before the new snapshot")

        monkeypatch.setattr(NamespaceStore, "write_index_snapshot", killed)
        with pytest.raises(KeyboardInterrupt):
            ns.compact(force=True)
        monkeypatch.undo()
        e.close()
        e, ns = self._cold(root, cache)
        try:
            assert not set(_live(ns)) & {r.id for r in recs[:10]}, "acked deletes came back"
            assert _exported(ns) == _live(ns)
        finally:
            e.close()

    def test_an_image_older_than_the_compaction_is_never_installed(self, tmp_path, monkeypatch):
        """A snapshot published by a backup that a compaction overtook: the
        manifest stamps it with a seq past that compaction, the image itself
        reflects an older one. The image's own watermark decides."""
        root, cache, e, ns, recs = self._store(tmp_path, monkeypatch, floor=5)
        old_name = ns.manifest.snapshot_name
        with open(os.path.join(root, "ns", "s", old_name), "rb") as f:
            old_image = f.read()
        for r in recs[:10]:
            _tombstone(ns, r)
        ns.compact(force=True)
        seq = ns.manifest.seq
        e.close()
        with open(os.path.join(root, "ns", "s", old_name), "wb") as f:
            f.write(old_image)
        man = _manifest(root, "s")
        man["snapshot_name"], man["snapshot_seq"] = old_name, seq
        with open(_manifest_path(root, "s"), "w") as f:
            json.dump(man, f)
        e, ns = self._cold(root, cache)
        try:
            assert _live(ns) == sorted(r.id for r in recs[10:]), "a stale image was installed"
        finally:
            e.close()

    def test_a_manifest_from_before_compact_seq_drops_a_snapshot_older_than_its_checkpoint(
            self, tmp_path, monkeypatch):
        """Stores the previous build left in the bad state: no compact_seq,
        a snapshot older than the checkpoint the manifest names."""
        root, cache, e, ns, recs = self._store(tmp_path, monkeypatch, floor=5)
        old_name, old_seq = ns.manifest.snapshot_name, ns.manifest.snapshot_seq
        with open(os.path.join(root, "ns", "s", old_name), "rb") as f:
            old_image = f.read()
        for r in recs[:10]:
            _tombstone(ns, r)
        ns.compact(force=True)
        e.close()
        with open(os.path.join(root, "ns", "s", old_name), "wb") as f:
            f.write(old_image)
        man = _manifest(root, "s")
        man.pop("compact_seq", None)
        man["snapshot_name"], man["snapshot_seq"] = old_name, old_seq
        with open(_manifest_path(root, "s"), "w") as f:
            json.dump(man, f)
        e, ns = self._cold(root, cache)
        try:
            assert _live(ns) == sorted(r.id for r in recs[10:])
            assert ns.manifest.snapshot_name != old_name, "the stale snapshot is still referenced"
        finally:
            e.close()

    def test_publish_refuses_an_image_that_predates_the_compaction(self, tmp_path, monkeypatch):
        root, cache, e, ns, recs = self._store(tmp_path, monkeypatch, floor=5)
        before = ns.manifest.snapshot_name
        # the image a backup took before the compaction committed
        ns.index.set_meta("applied_seq", str(ns.manifest.checkpoint_seq - 1))
        try:
            assert ns.write_index_snapshot() is False
            assert ns.manifest.snapshot_name == before
        finally:
            e.close()


# ------------------------------------------------------ an interrupted migration


def _format1_store_with_evidence(tmp_path):
    """A format-1 namespace whose rotates dropped a delete (a) and a
    supersede (b -> d) that only the old binary's live index still applies,
    with a WAL (e, f) and an ops log (delete f) left to fold."""
    root, cache = str(tmp_path / "store"), str(tmp_path / "cache")
    a, b, c, d, e_, f_ = (_rec("ev", f"record {n}", rid=f"r{n}") for n in "abcdef")
    nsdir = os.path.join(root, "ns", "ev")
    os.makedirs(nsdir)
    entries = []
    for name, fold, recs in (("seg-00LEGACY0001", 2, [a, b, c]), ("seg-00LEGACY0002", 5, [d])):
        with open(os.path.join(nsdir, name), "wb") as fh:
            fh.write(json.dumps({"_seg": {"fold_seq": fold}}).encode() + b"\n" + records_to_jsonl(recs))
        entries.append({"name": name, "records": len(recs), "fold_seq": fold, "reason": "size"})
    wal = _frame_encode(records_to_jsonl([e_, f_]))  # unstamped: seq 6
    ops = _frame_encode(json.dumps({"op": "tombstone", "id": f_.id, "at": now_ms(), "seq": 7},
                                   separators=(",", ":")).encode())
    for key, data in (("wal", wal), ("ops", ops)):
        with open(os.path.join(nsdir, key), "wb") as fh:
            fh.write(data)
    with open(os.path.join(nsdir, "manifest.json"), "w") as fh:
        json.dump({"version": 7, "seq": 7, "segments": entries, "wal_size": len(wal),
                   "ops_size": len(ops), "wal_base_seq": 5, "snapshot_seq": 0,
                   "snapshot_name": ""}, fh)
    os.makedirs(cache)
    idx = NamespaceIndex(os.path.join(cache, "ev.sqlite"))
    idx.upsert_batch([(r, None, "") for r in (a, b, c, d, e_, f_)])
    idx.tombstone(a.id, now_ms())               # the op a format-1 rotate dropped
    idx.mark_superseded(b.id, d.id, now_ms())   # likewise
    idx.tombstone(f_.id, now_ms())
    idx.flush()
    idx.set_meta("applied_seq", "7")
    idx.close()
    return root, cache


WANT_LIVE = ["rb", "rc", "rd", "re"]
WANT_SUP = {"rb": "rd"}


def _check_migrated(root, cache, label=""):
    e = StorageEngine(root, cache_dir=cache)
    try:
        ns = e.namespace("ev")
        assert (_live(ns), _sups(ns)) == (WANT_LIVE, WANT_SUP), f"{label}: warm"
    finally:
        e.close()
    assert _manifest(root, "ev")["format"] == 2
    shutil.rmtree(cache)
    e = StorageEngine(root, cache_dir=cache)
    try:
        ns = e.namespace("ev")
        assert (_live(ns), _sups(ns)) == (WANT_LIVE, WANT_SUP), f"{label}: cold after the migration"
        assert _exported(ns) == WANT_LIVE
    finally:
        e.close()


def _index_rows(cache):
    import sqlite3

    con = sqlite3.connect(os.path.join(cache, "ev.sqlite"))
    try:
        return (con.execute("SELECT id, deleted, superseded_by FROM records ORDER BY id").fetchall(),
                con.execute("SELECT v FROM meta WHERE k='applied_seq'").fetchone())
    finally:
        con.close()


class TestInterruptedMigration:
    def test_an_uninterrupted_migration_keeps_what_the_old_index_proves(self, tmp_path):
        root, cache = _format1_store_with_evidence(tmp_path)
        _check_migrated(root, cache)

    @pytest.mark.parametrize("point", ["segment", "segment_landed", "manifest", "enospc"])
    def test_a_migration_interrupted_before_its_commit_keeps_its_evidence(
            self, tmp_path, monkeypatch, point):
        root, cache = _format1_store_with_evidence(tmp_path)
        with open(_manifest_path(root, "ev"), "rb") as f:
            manifest_before = f.read()
        rows_before = _index_rows(cache)
        put = LocalObjectStore.put

        def failing_put(self, key, data):
            if point == "segment" and "/seg-" in key:
                raise KeyboardInterrupt("killed before the segment landed")
            if point == "enospc" and "/seg-" in key:
                raise OSError(errno.ENOSPC, "No space left on device")
            if point == "manifest" and key.endswith("manifest.json") and b'"format": 2' in data:
                raise KeyboardInterrupt("killed before the commit")
            put(self, key, data)
            if point == "segment_landed" and "/seg-" in key:
                raise KeyboardInterrupt("killed after the segment landed")

        monkeypatch.setattr(LocalObjectStore, "put", failing_put)
        with pytest.raises((KeyboardInterrupt, OSError)):
            StorageEngine(root, cache_dir=cache).namespace("ev")
        monkeypatch.undo()
        with open(_manifest_path(root, "ev"), "rb") as f:
            assert f.read() == manifest_before, "the format-1 namespace must be untouched"
        assert _index_rows(cache) == rows_before, "the old index is the migration's evidence"
        _check_migrated(root, cache, point)          # the retry ends where one run does
        nsdir = os.path.join(root, "ns", "ev")
        assert sorted(f for f in os.listdir(nsdir) if f.startswith("seg-")) == \
            sorted(s["name"] for s in _manifest(root, "ev")["segments"]), \
            "the uncommitted segment must be collected once the retry commits"


KILL_CHILD = r"""
import os, sys
sys.path.insert(0, {src!r})
from memd.storage import objectstore as osm
from memd.storage.engine import StorageEngine

root, cache, K, how = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]
calls = [0]

def wrap(cls, meth):
    orig = getattr(cls, meth)
    def w(self, *a, **kw):
        calls[0] += 1
        hit = calls[0] == K
        if hit and how == "before":
            os._exit(9)
        r = orig(self, *a, **kw)
        if hit:
            os._exit(9)
        return r
    setattr(cls, meth, w)

for m_ in ("put", "append", "delete", "truncate", "remove_prefix"):
    wrap(osm.LocalObjectStore, m_)
wrap(osm.LocalLogWriter, "write")
wrap(osm.LocalLogWriter, "sync")
e = StorageEngine(root, cache_dir=cache)
e.namespace("ev")
e.close()
os._exit(0)
"""


def test_a_kill_at_every_write_of_the_migrating_open_ends_where_one_run_does(tmp_path):
    """The review's kill matrix: SIGKILL-equivalent at each durable
    mutation of the migrating open (before and after it lands), then a
    restart on the node's cache, then a cold node - always the uninterrupted
    result, which keeps what the old index proves."""
    pristine = tmp_path / "pristine"
    root0, cache0 = _format1_store_with_evidence(pristine)
    child = KILL_CHILD.format(src=SRC)
    runs = 0
    for k in range(1, 40):
        done = False
        for how in ("before", "after"):
            run = tmp_path / f"k{k}{how}"
            root, cache = str(run / "store"), str(run / "cache")
            shutil.copytree(root0, root)
            shutil.copytree(cache0, cache)
            p = subprocess.run([sys.executable, "-c", child, root, cache, str(k), how],
                               capture_output=True, timeout=120)
            if p.returncode == 0:
                done = True
                break
            assert p.returncode == 9, p.stderr.decode()[-2000:]
            runs += 1
            committed = _manifest(root, "ev").get("format") == 2
            if committed:  # a cold node right after a kill past the commit
                cold = str(run / "cold-cache")
                e = StorageEngine(root, cache_dir=cold)
                try:
                    ns = e.namespace("ev")
                    assert (_live(ns), _sups(ns)) == (WANT_LIVE, WANT_SUP), f"K={k}/{how}: cold first"
                finally:
                    e.close()
            _check_migrated(root, cache, f"K={k}/{how}")
            shutil.rmtree(run)
        if done:
            break
    assert runs >= 6, f"the migrating open made only {runs // 2} writes?"


# ------------------------------------------------ orphans are collected after a purge


def _orphan_store(tmp_path, kind: str):
    """A namespace holding a victim record, and an orphan (written, never
    committed - a crash) holding its text, stamped with the generation of
    the open that follows."""
    root = str(tmp_path / "store")
    e = StorageEngine(root)
    ns = e.namespace("g")
    victim, keep = _rec("g", f"secret {MARKER} end"), _rec("g", "keeper control text")
    ns.append([victim, keep] + [_rec("g", f"filler {i}") for i in range(5)])
    ns.compact(force=True)
    name = f"seg-{ulid_new()}" if kind == "seg" else f"index-{ulid_new()}.g{ns.manifest.version}.snap"
    if kind == "seg":
        ns._write_segment(name, [victim, keep], ns.manifest.seq)
    else:
        ns.store.put(ns._snapshot_key(name), gzip.compress(f"image with {MARKER}".encode()))
    return root, e, ns, victim, name


@pytest.mark.parametrize("kind", ["seg", "snap"])
def test_a_purge_compaction_collects_the_orphans_that_hold_its_text(tmp_path, monkeypatch, kind):
    root, e, ns, victim, orphan = _orphan_store(tmp_path, kind)
    # the process dies: nothing collects the orphan until the next open,
    # where it is as new as the checkpoint (a writer may still own it)
    monkeypatch.setattr(NamespaceStore, "_collect_garbage_now", lambda self, defer_audit: None, raising=False)
    e.close()
    monkeypatch.undo()
    e = StorageEngine(root)
    try:
        ns = e.namespace("g")
        assert orphan in os.listdir(os.path.join(root, "ns", "g")), "precondition: kept at open"
        ns.append_op({"op": "hard_delete", "id": victim.id, "deadline": now_ms()})
        rep = ns.compact()
        assert rep.hard_deleted_purged == 1
        assert orphan not in os.listdir(os.path.join(root, "ns", "g")), \
            "the purge compaction left an orphan holding the purged text"
        assert _files_with(root, MARKER.encode()) == []
        assert (ns.stats()["segments_collected"], ns.stats()["snapshots_collected"]) == \
            ((1, 0) if kind == "seg" else (0, 1))
    finally:
        e.close()


def test_a_clean_close_collects_what_a_newer_checkpoint_replaced(tmp_path, monkeypatch):
    root, e, ns, _victim, orphan = _orphan_store(tmp_path, "seg")
    monkeypatch.setattr(NamespaceStore, "_collect_garbage_now", lambda self, defer_audit: None, raising=False)
    e.close()
    monkeypatch.undo()
    e = StorageEngine(root)
    ns = e.namespace("g")
    assert orphan in os.listdir(os.path.join(root, "ns", "g"))
    ns.append([_rec("g", "later")])
    ns.compact(force=True)            # no purge: collection used to wait for the next open
    assert orphan in os.listdir(os.path.join(root, "ns", "g"))
    e.close()
    assert orphan not in os.listdir(os.path.join(root, "ns", "g")), "a clean close left it"
    from memd.storage.audit import AuditLog

    entries = AuditLog(LocalObjectStore(root), "ns/g/audit", None).read()
    assert ("segment_gc", orphan) in {(x["action"], x["target"]) for x in entries}


def test_a_put_killed_before_its_rename_leaves_no_text_behind(tmp_path):
    """A put writes a temp file and renames it; SIGKILL in between left the
    temp file - a segment's records - in the namespace directory for good."""
    from memd.storage import objectstore

    root = str(tmp_path / "store")
    e = StorageEngine(root)
    ns = e.namespace("t")
    ns.append([_rec("t", "a"), _rec("t", "b")])
    ns.compact(force=True)
    nsdir = os.path.join(root, "ns", "t")
    dead = os.path.join(nsdir, ".tmp-4242xdeadbeef-k2j3h4")     # another process's, killed
    with open(dead, "wb") as f:
        f.write(f"seg records {MARKER}".encode())
    # this process's own: it may still be writing it
    mine = os.path.join(nsdir, getattr(objectstore, "_TMP_TAG", ".tmp-") + "inflight")
    with open(mine, "wb") as f:
        f.write(b"in flight")
    e.close()
    e = StorageEngine(root)
    try:
        e.namespace("t")
        assert not os.path.exists(dead), "an interrupted put's temp file survived the owner's open"
        assert os.path.exists(mine), "a temp file this process may still be writing was deleted"
        assert _files_with(root, MARKER.encode()) == []
    finally:
        e.close()


# ------------------------------------------------ one namespace's open does not stall the rest


def test_opening_one_namespace_does_not_block_the_others(tmp_path, monkeypatch):
    e = StorageEngine(str(tmp_path / "store"))
    entered, gate = threading.Event(), threading.Event()
    real_open = NamespaceStore._open

    def slow_open(self):
        if self.namespace == "slow":   # e.g. a first open that migrates 120K records
            entered.set()
            gate.wait(30)
        return real_open(self)

    monkeypatch.setattr(NamespaceStore, "_open", slow_open)
    got: dict[str, list] = {"slow": [], "fast": [], "destroyed": []}
    t1 = threading.Thread(target=lambda: got["slow"].append(e.namespace("slow")))
    t2 = threading.Thread(target=lambda: got["slow"].append(e.namespace("slow")))
    t3 = threading.Thread(target=lambda: got["fast"].append(e.namespace("fast")))
    t4 = threading.Thread(target=lambda: got["destroyed"].append(e.destroy_namespace("fast2")))
    try:
        t1.start()
        assert entered.wait(10)
        t2.start()
        t3.start()
        t4.start()
        t3.join(10)
        t4.join(10)
        assert got["fast"], "another namespace's open waited for this one"
        assert got["destroyed"] == [False]
        assert not got["slow"]
    finally:
        gate.set()
        for t in (t1, t2, t3, t4):
            t.join(30)
    try:
        assert len(got["slow"]) == 2 and got["slow"][0] is got["slow"][1], \
            "two opens of one namespace must share one store"
    finally:
        e.close()
