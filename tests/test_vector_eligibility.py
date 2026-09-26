"""Vector-lane parity across eligibility changes made AFTER the flat scan's
matrix is loaded.

The matrix holds the vectors of the rows that were live when it loaded. A
record that became live afterwards (unquarantined - by hand or by the
rotate-time quarantine expiry - or restored by a re-add of its id) was
missing from it, so the default (flat) vector lane and every flat sweep
could not see it until a compaction reloaded the matrix; a re-embedded
record kept its old vector beside the new one; and rows that stopped being
live stayed in it, so enough of them near a query crowded the eligible rows
out of its windows. After each kind of change the flat scan, the usearch
sidecar (when installed) and the exact streamed scan must return the same
rows, for ranked queries and sweeps.
"""
import numpy as np
import pytest

from memd.core.schema import MemoryRecord, Scope, now_ms
from memd.index.ann_usearch import usearch_available
from memd.index.sqlite_index import IndexFilter
from memd.storage.engine import StorageEngine

DIM = 32
N = 1200
NEAR = 300          # the records the change applies to: the probe's nearest
USERS = ("alice", "bob")
MODES = ["flat"] + (["usearch"] if usearch_available() else [])
CHANGES = ["unquarantine", "quarantine", "expiry", "supersede", "tombstone", "hard_delete",
           "restore", "batch", "reembed"]
LIVE_AFTER = {"unquarantine", "expiry", "restore", "reembed"}


def _vectors(n: int, seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    centers = rng.standard_normal((16, DIM))
    x = centers[rng.integers(0, 16, n)] + 0.45 * rng.standard_normal((n, DIM))
    return (x / np.linalg.norm(x, axis=1, keepdims=True)).astype(np.float32)


def _engine(root, mode: str) -> StorageEngine:
    if mode == "flat":
        return StorageEngine(str(root), vector_index={"mode": "flat"})
    return StorageEngine(str(root), vector_index={
        "mode": "usearch", "min_vectors": 0, "overfetch": 4, "exact_max": 0, "dtype": "f16",
        "build_threads": 1})


def _paths(idx, q: np.ndarray, f: IndexFilter) -> dict[str, list[str]]:
    q = (q / np.linalg.norm(q)).astype(np.float32)
    out = {
        "lane": idx.search_vector(q, f, limit=10),
        "flat": idx._search_vector_flat(q, f, 10),
        "exact": idx._exact_vector(q, f, 10),
        "flat_sweep": idx._search_vector_flat(q, f, 2000),
        "exact_sweep": idx._exact_vector(q, f, 2000),
    }
    if idx.ann is not None:
        out["ann"] = idx._search_vector_ann(idx.ann, q, f, 10) or []
    return {k: [h.record.id for h in v] for k, v in out.items()}


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("change", CHANGES)
def test_every_vector_path_agrees_after_an_eligibility_change(tmp_path, mode, change):
    x = _vectors(N, seed=31)
    probe = x[0] + 0.05 * np.random.default_rng(32).standard_normal(DIM).astype(np.float32)
    near = [int(i) for i in np.argsort(-(x @ probe))[:NEAR]]
    near_set = set(near)
    others = [i for i in range(N) if i not in near_set]
    e = _engine(tmp_path / "s", mode)
    try:
        ns = e.namespace("n")
        idx = ns.index
        past = now_ms() - 1000
        recs = []
        for i in range(N):
            meta = {}
            if change == "expiry" and i in near_set:
                meta = {"quarantined": True, "quarantine_expires": past}
            recs.append(MemoryRecord.create(namespace="n", kind="raw_event", content=f"doc {i}",
                                            scope=Scope(user=USERS[i % 2]), meta=meta,
                                            t_event=1_700_000_000_000 + i))
        ns.append(recs)
        for s in range(0, N, 256):
            idx.set_vectors([r.id for r in recs[s:s + 256]], x[s:s + 256], "m")
        # the state before the matrix loads
        if change == "unquarantine":
            ns.append_ops([{"op": "quarantine", "id": recs[i].id, "flag": True} for i in near])
        elif change == "restore":
            ns.append_ops([{"op": "tombstone", "id": recs[i].id, "at": past} for i in near])
        if idx.ann is not None:
            assert idx.ann.drain(60) and idx.ann.ready()
        idx._search_vector_flat(x[1], IndexFilter(), 10)
        assert idx._vec_loaded, "the flat matrix loads before the change"
        at = now_ms()
        new_vecs = None
        if change == "unquarantine":
            ns.append_ops([{"op": "quarantine", "id": recs[i].id, "flag": False} for i in near])
        elif change == "quarantine":
            ns.append_ops([{"op": "quarantine", "id": recs[i].id, "flag": True} for i in near])
        elif change == "expiry":
            ns.rotate()  # the fold finds the expired quarantines and clears them in the index
        elif change == "supersede":
            ns.append_ops([{"op": "supersede", "old": recs[i].id, "new": recs[others[0]].id, "at": at}
                           for i in near])
        elif change == "tombstone":
            ns.append_ops([{"op": "tombstone", "id": recs[i].id, "at": at} for i in near])
        elif change == "hard_delete":
            ns.append_ops([{"op": "hard_delete", "id": recs[i].id, "deadline": at + 10 ** 9}
                           for i in near])
        elif change == "restore":
            ns.append([recs[i] for i in near])  # the same ids, not deleted
        elif change == "batch":
            ops = []
            for j, i in enumerate(near):
                kind = ("tombstone", "supersede", "quarantine", "hard_delete")[j % 4]
                op = {"op": kind, "id": recs[i].id, "at": at}
                if kind == "supersede":
                    op = {"op": kind, "old": recs[i].id, "new": recs[others[0]].id, "at": at}
                ops.append(op)
            idx.apply_ops_batch(ops)
        elif change == "reembed":
            new_vecs = _vectors(NEAR, seed=33)
            idx.set_vectors([recs[i].id for i in near], new_vecs, "m")
        if idx.ann is not None:
            assert idx.ann.drain(60) and idx.ann.ready()
        live = change in LIVE_AFTER
        queries = [("probe", probe, None)]
        for j, i in enumerate(near[:15]):
            v = new_vecs[j] if new_vecs is not None else x[i]
            queries.append((f"own {i}", v, recs[i]))
        if new_vecs is not None:
            queries += [(f"old {i}", x[i], None) for i in near[:5]]
        bad = []
        for name, q, own in queries:
            for f in (IndexFilter(), IndexFilter(scope=Scope(user="alice"))):
                got = _paths(idx, q, f)
                want = set(got["exact"])
                for path, ids in got.items():
                    ref = want if not path.endswith("sweep") else set(got["exact_sweep"])
                    if set(ids) != ref:
                        bad.append((name, f.scope, path, "set", len(ids), len(ref)))
                if own is None:
                    continue
                expect = live and (f.scope is None or f.scope.contains(own.scope))
                for path, ids in got.items():
                    if (own.id in ids) != expect:
                        bad.append((name, f.scope, path, "own", own.id in ids, expect))
        assert not bad, f"{len(bad)} disagreements, first: {bad[:6]}"
    finally:
        e.close()


def test_the_flat_matrix_holds_exactly_the_live_rows_through_folds(tmp_path):
    """Random eligibility changes with a tiny overflow bound, so the overflow
    folds and dead rows compact out many times: after every step the matrix
    holds each live row's stored vector exactly once, and nothing else."""
    x = _vectors(400, seed=34)
    e = _engine(tmp_path / "s", "flat")
    try:
        ns = e.namespace("n")
        idx = ns.index
        idx.OVERFLOW_MAX = 16
        recs = [MemoryRecord.create(namespace="n", kind="raw_event", content=f"doc {i}",
                                    scope=Scope(user=USERS[i % 2])) for i in range(400)]
        ns.append(recs)
        idx.set_vectors([r.id for r in recs[:200]], x[:200], "m")
        idx._search_vector_flat(x[0], IndexFilter(), 10)
        assert idx._vec_loaded
        rng = np.random.default_rng(35)
        for step in range(300):
            i = int(rng.integers(0, 400))
            rid = recs[i].id
            kind = rng.choice(["vec", "quar", "unquar", "tomb", "restore", "hard", "super"])
            if kind == "vec":
                idx.set_vectors([rid], [_vectors(1, seed=1000 + step)[0]], "m")
            elif kind in ("quar", "unquar"):
                idx.mark_quarantined(rid, kind == "quar")
            elif kind == "tomb":
                idx.tombstone(rid, now_ms())
            elif kind == "restore":
                ns.append([recs[i]])
            elif kind == "hard":
                idx.hard_delete(rid)
            else:
                idx.mark_superseded(rid, recs[(i + 1) % 400].id, now_ms())
            live_rows = {r[0]: np.frombuffer(r[1], dtype=np.float16).astype(np.float32)
                         for r in idx._con.execute(
                             "SELECT v.id, v.vec FROM vectors v JOIN records r ON r.id = v.id "
                             "WHERE r.deleted=0 AND r.quarantined=0 AND r.invalidated_at IS NULL "
                             "AND r.superseded_by IS NULL")}
            if not idx._vec_loaded:
                continue
            held = [rid_ for j, rid_ in enumerate(idx._main_ids) if not idx._main_dead[j]] + idx._ovf_ids
            assert sorted(held) == sorted(live_rows), f"step {step} ({kind})"
            assert len(idx._main_pos) + len(idx._ovf_pos) == len(held)
            for j, rid_ in enumerate(idx._main_ids):
                if not idx._main_dead[j]:
                    v = live_rows[rid_] / np.linalg.norm(live_rows[rid_])
                    assert np.allclose(idx._main_mat[j], v, atol=1e-6)
            for rid_, v in zip(idx._ovf_ids, idx._ovf_vecs):
                assert np.allclose(v, live_rows[rid_] / np.linalg.norm(live_rows[rid_]), atol=1e-6)
            assert idx._n_dead < max(idx.OVERFLOW_MAX, len(idx._main_ids) // 8) + 1
        q = x[5] / np.linalg.norm(x[5])
        for f in (IndexFilter(), IndexFilter(scope=Scope(user="bob"))):
            assert ({h.record.id for h in idx._search_vector_flat(q, f, 10)}
                    == {h.record.id for h in idx._exact_vector(q, f, 10)})
    finally:
        e.close()
