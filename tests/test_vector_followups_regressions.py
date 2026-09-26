"""Regressions found by an independent review of the vector follow-ups.

The sidecar's post-build repair pass counted a node whose self-search was
answered by a node holding the very same vector (a duplicate record) as
found. When that node was in fact unreachable, a search could only ever
return its twin - and a scoped search that drops the twin (another user's
copy, or a copy quarantined or deleted since) never saw the node: 14-19
misses in 9000 at two copies per vector, 0 before.
"""

import numpy as np
import pytest

from memd.core.schema import MemoryRecord, Scope
from memd.index.sqlite_index import IndexFilter
from memd.storage.engine import StorageEngine

DIM = 48


def _vectors(n: int, seed: int, dim: int = DIM) -> np.ndarray:
    rng = np.random.default_rng(seed)
    centers = rng.standard_normal((32, dim))
    x = centers[rng.integers(0, 32, n)] + 0.45 * rng.standard_normal((n, dim))
    return (x / np.linalg.norm(x, axis=1, keepdims=True)).astype(np.float32)


# ------------------------------------------------------------ twins

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
