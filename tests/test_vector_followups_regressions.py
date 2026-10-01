"""Regressions found by an independent review of the vector follow-ups.

1. The sidecar's post-build repair pass counted a node whose self-search was
   answered by a node holding the very same vector (a duplicate record) as
   found. When that node was in fact unreachable, a search could only ever
   return its twin - and a scoped search that drops the twin (another
   user's copy, or a copy quarantined or deleted since) never saw the node:
   14-19 misses in 9000 at two copies per vector, 0 before.
2. A snapshot publish copies the SQLite image from a pinned read snapshot
   with no lock held. A hard-delete purge that ran meanwhile could not
   truncate the WAL past that reader within the busy timeout, gave up, and
   left the erased text and vector bytes in the local SQLite file until the
   next open.
"""
import os
import threading
import time

import numpy as np
import pytest

from memd.core.schema import MemoryRecord, Scope
from memd.index.sqlite_index import IndexFilter
from memd.storage import engine as storage_engine
from memd.storage.engine import StorageEngine

DIM = 48


def _vectors(n: int, seed: int, dim: int = DIM) -> np.ndarray:
    rng = np.random.default_rng(seed)
    centers = rng.standard_normal((32, dim))
    x = centers[rng.integers(0, 32, n)] + 0.45 * rng.standard_normal((n, dim))
    return (x / np.linalg.norm(x, axis=1, keepdims=True)).astype(np.float32)


def _files_with(root, needle: bytes) -> list[str]:
    out = []
    for dirpath, _dirs, files in os.walk(str(root)):
        for fn in files:
            path = os.path.join(dirpath, fn)
            try:
                with open(path, "rb") as f:
                    if needle in f.read():
                        out.append(os.path.relpath(path, str(root)))
            except FileNotFoundError:
                continue   # deleted mid-walk (a sidecar rebuild, a scrub, a replica closing)
    return sorted(out)


# ------------------------------------------------------------ 1: twins

def test_every_copy_of_a_duplicated_vector_is_found_by_its_scoped_search(tmp_path):
    """Every vector held by exactly two records: both in the index (alice
    and bob), or one quarantined, deleted or hard-deleted after the build
    (its twin in carol's scope). A parallel build leaves a few nodes poorly
    linked; a search for each eligible record's own vector, scoped to its
    user, must still return it."""
    pytest.importorskip("usearch")
    n = 4500
    third = n // 3
    e = StorageEngine(str(tmp_path / "s"), vector_index={
        "mode": "usearch", "min_vectors": 0, "overfetch": 4, "exact_max": 0, "dtype": "f16",
        "build_threads": 8})
    try:
        ns = e.namespace("n")
        idx, ann = ns.index, ns.index.ann
        x = _vectors(n, seed=61)
        # first copy of every vector: alice; second: bob (both stay), then
        # carol (her twin in alice's scope is quarantined, deleted or purged)
        first = [MemoryRecord.create(namespace="n", kind="raw_event", content=f"first {i}",
                                     scope=Scope(user="alice"), t_event=1_700_000_000_000 + i)
                 for i in range(n)]
        second = [MemoryRecord.create(namespace="n", kind="raw_event", content=f"second {i}",
                                      scope=Scope(user="bob" if i < third else "carol"),
                                      t_event=1_700_000_000_000 + i)
                  for i in range(n)]
        for recs in (first, second):
            ns.append(recs)
            for i in range(0, n, 256):
                idx.set_vectors([r.id for r in recs[i:i + 256]], x[i:i + 256], "test-model")
        assert ann.drain(120)
        assert ann.wait_built(ann.request_build("test"), 300)  # the parallel build + repair pass
        assert ann.size() == 2 * n
        for i in range(third, n):
            rid = first[i].id
            kind = (i - third) % 3
            if kind == 0:
                idx.mark_quarantined(rid, True)
            elif kind == 1:
                idx.tombstone(rid, 1)
            else:
                idx.hard_delete(rid)
        assert ann.drain(120) and ann.size() == n + third
        missed = []
        checks = [(r, i) for i, r in enumerate(first[:third])] + [(r, i) for i, r in enumerate(second)]
        for r, i in checks:
            hits = idx.search_vector(x[i], IndexFilter(scope=r.scope), limit=10)
            if r.id not in [h.record.id for h in hits]:
                missed.append((r.scope.user, i))
        assert missed == [], f"{len(missed)} of {len(checks)} eligible records not found: {missed[:10]}"
    finally:
        e.close()


@pytest.mark.parametrize("dtype", ["f16", "i8"])
def test_the_repair_pass_reinserts_a_twin_no_search_reaches(tmp_path, dtype):
    """A self-search answered by the node's identical twin proves only that
    the twin is reachable. Nodes 701-705 (twins of 1-5) are never returned
    by any search: they must be re-inserted, not counted as found."""
    pytest.importorskip("usearch")
    e = StorageEngine(str(tmp_path / "s"), vector_index={
        "mode": "usearch", "min_vectors": 0, "overfetch": 4, "exact_max": 0, "dtype": dtype,
        "build_threads": 1})
    try:
        ann = e.namespace("n").index.ann
        x = _vectors(1000, seed=62)
        x[700:] = x[:300]
        ix = ann._new_index(DIM)
        ix.add(np.arange(1, 1001, dtype=np.uint64), ann._cast(x), threads=1)
        real = ix.search
        hidden = np.arange(701, 706, dtype=np.uint64)

        def search(q, k, **kw):
            m = real(q, k, **kw)
            keys = np.asarray(m.keys, dtype=np.uint64).reshape(len(q), -1).copy()
            gone = np.isin(keys, hidden)
            keys[gone] = keys[gone] - 700  # the tied twin answers instead
            return type("M", (), {"keys": keys})()
        ix.search = search
        added = []
        real_add = ix.add

        def add(keys, vecs, **kw):
            added.extend(np.asarray(keys).ravel().tolist())
            return real_add(keys, vecs, **kw)
        ix.add = add
        ann._repair(ix)
        assert set(hidden.tolist()) <= set(added), "an unreachable twin was counted as found"
        assert len(ix) == 1000
    finally:
        e.close()


# ------------------------------------------------------------ 2: purge vs a pinned image

class _SlowCopy:
    """A pinned image whose backup starts only after a while."""

    def __init__(self, con, started: threading.Event, delay: float):
        self.con, self.started, self.delay = con, started, delay

    def backup(self, dst, **kw):
        self.started.set()
        time.sleep(self.delay)
        return self.con.backup(dst, **kw)

    def close(self):
        self.con.close()


def test_a_purge_scrubs_the_index_while_a_snapshot_copy_holds_its_image(tmp_path, monkeypatch):
    """The copy's read snapshot keeps the WAL from being checkpointed; the
    purge used to give up after the busy timeout and leave the erased text
    and vector in the SQLite file (and WAL) until the next open. It must
    not report the purge scrubbed until those bytes are gone, and the copy's
    image (taken before the purge) must not be published."""
    monkeypatch.setattr(storage_engine.NamespaceStore, "SNAPSHOT_MIN_RECORDS", 1)
    root = tmp_path / "s"
    e = StorageEngine(str(root))
    try:
        ns = e.namespace("n")
        idx = ns.index
        marker = "ERASE-ME-zq7v-pinned-image"
        recs = [MemoryRecord.create(namespace="n", kind="raw_event", content=f"keeper note {i}",
                                    scope=Scope(user="alice")) for i in range(300)]
        victim = MemoryRecord.create(namespace="n", kind="raw_event", content=f"victim {marker}",
                                     scope=Scope(user="alice"))
        ns.append(recs + [victim])
        x = _vectors(301, seed=63)
        idx.set_vectors([r.id for r in recs + [victim]], x, "test-model")
        ns.compact(force=True)  # folded, stamped and published once
        needle = bytes(idx._con.execute("SELECT vec FROM vectors WHERE id=?", (victim.id,)).fetchone()[0])
        assert _files_with(e.cache_dir, marker.encode()), "positive control: the text is in the index"
        assert _files_with(e.cache_dir, needle), "positive control: the vector is in the index"
        # a checkpoint blocked by a reader gives up after 0.1 s; the copy holds its image for 2 s
        idx._con.execute("PRAGMA busy_timeout=100")
        started = threading.Event()
        real_pin = idx.pin_image
        monkeypatch.setattr(idx, "pin_image", lambda: _SlowCopy(real_pin(), started, 2.0))
        published = []
        t = threading.Thread(target=lambda: published.append(ns.write_index_snapshot()))
        t.start()
        assert started.wait(30)
        before = ns.manifest.snapshot_name
        ns.append_op({"op": "hard_delete", "id": victim.id, "deadline": 0})
        idx.hard_delete(victim.id)
        scrubbing = threading.Event()
        real_scrub = idx.scrub
        monkeypatch.setattr(idx, "scrub", lambda: (scrubbing.set(), real_scrub())[1])
        purge = threading.Thread(target=ns.compact)  # the due purge, while the copy holds its image
        purge.start()
        assert scrubbing.wait(30)
        time.sleep(0.3)
        # the scrub waits for the copy without the index lock: index writes
        # and searches go on meanwhile
        t0 = time.monotonic()
        idx.upsert(MemoryRecord.create(namespace="n", kind="raw_event", content="written during the scrub",
                                       scope=Scope(user="alice")))
        hits = idx.search_vector(x[0], IndexFilter(scope=Scope(user="alice")), limit=3)
        assert time.monotonic() - t0 < 1.0 and hits
        assert t.is_alive(), "(the copy still holds its image)"
        purge.join(60)
        assert not purge.is_alive()
        # the purge is reported scrubbed only once it is
        assert int(idx.get_meta("scrubbed_seq") or 0) == ns.manifest.scrub_seq > 0
        assert _files_with(e.cache_dir, marker.encode()) == [], "erased text left in the local index"
        assert _files_with(e.cache_dir, needle) == [], "erased vector left in the local index"
        t.join(60)
        assert published == [False], "an image taken before the purge was published"
        assert ns.manifest.snapshot_name and ns.manifest.snapshot_name != before
        assert _files_with(root, marker.encode()) == []
        assert _files_with(root, needle) == []
    finally:
        e.close()
