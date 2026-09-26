"""The optional usearch ANN sidecar for the vector lane (memd[ann]).

SQLite's vectors table stays the source of truth; the sidecar is a derived
HNSW index keyed by record rowid. These tests hold it to that contract:
recall against the exact scan (parity), the same filters and isolation as
every other lane (even with its SQL prefilter sabotaged), deletes,
supersession, quarantine and hard deletes honoured at once, a missing,
corrupt, foreign, stale or half-written file rebuilt (never served), no
purged vector left in any file (D7), a published snapshot a cold node can
install, and the auto threshold switching flat <-> usearch.
"""
import json
import os
import signal
import subprocess
import sys
import threading
import time

import numpy as np
import pytest

pytest.importorskip("usearch")

import memd.index.ann_usearch as au  # noqa: E402
from memd.core.schema import MemoryRecord, Scope  # noqa: E402
from memd.engine.memory import Memory  # noqa: E402
from memd.index.ann_usearch import UsearchSidecar, resolve_vector_index, vector_index_config  # noqa: E402
from memd.index.sqlite_index import IndexFilter, NamespaceIndex  # noqa: E402
from memd.metrics import METRICS  # noqa: E402
from memd.storage import engine as storage_engine  # noqa: E402
from memd.storage.engine import StorageEngine  # noqa: E402

SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
USERS = ("alice", "bob", "carol")
DIM = 48


def _counter(name: str, **labels) -> float:
    return sum(x["value"] for x in METRICS.snapshot()["counters"].get(name, [])
               if all(x["labels"].get(k) == v for k, v in labels.items()))


def _vcfg(**over) -> dict:
    # one build thread: HNSW construction is then deterministic (parallel
    # insertion leaves a few nodes poorly linked - ~0.3% on these small
    # fixtures, 0.04% at 30K x 384 - which no search depth reaches, so a
    # test asserting an exact top-1 would be flaky, not wrong)
    cfg = {"mode": "usearch", "min_vectors": 0, "overfetch": 4, "exact_max": 0,
           "dtype": "f16", "build_threads": 1}
    cfg.update(over)
    return cfg


def _engine(root, **over) -> StorageEngine:
    return StorageEngine(str(root), vector_index=_vcfg(**over))


def _vectors(n: int, seed: int = 0, dim: int = DIM, draw: int | None = None) -> np.ndarray:
    """Unit vectors with clustered structure (what real embeddings look like
    to an HNSW graph, unlike the hash embedder's sparse n-gram vectors).
    `draw` samples new points around the same centers (queries)."""
    rng = np.random.default_rng(seed)
    centers = rng.standard_normal((32, dim))
    if draw is not None:
        rng = np.random.default_rng(draw)
    x = centers[rng.integers(0, 32, n)] + 0.45 * rng.standard_normal((n, dim))
    return (x / np.linalg.norm(x, axis=1, keepdims=True)).astype(np.float32)


def _fill(ns, n: int, seed: int = 0, start: int = 0) -> tuple[list[MemoryRecord], np.ndarray]:
    x = _vectors(n, seed)
    recs = [MemoryRecord.create(namespace=ns.namespace, kind="raw_event",
                                content=f"doc {start + i} seed {seed}",
                                scope=Scope(user=USERS[(start + i) % len(USERS)]),
                                t_event=1_700_000_000_000 + start + i)
            for i in range(n)]
    ns.append(recs)
    for i in range(0, n, 256):
        ns.index.set_vectors([r.id for r in recs[i:i + 256]], x[i:i + 256], "test-model")
    if ns.index.ann is not None:
        assert ns.index.ann.drain(120)
    return recs, x


def _ids(hits) -> list[str]:
    return [h.record.id for h in hits]


def _flat(idx: NamespaceIndex, q, f, limit):
    q = np.asarray(q, dtype=np.float32)
    return idx._search_vector_flat(q / np.linalg.norm(q), f, limit)


def _sidecar_dir(root) -> str:
    hits = [os.path.join(dp, d) for dp, ds, _ in os.walk(str(root)) for d in ds if d.endswith(".usearch")]
    assert len(hits) == 1, hits
    return hits[0]


def _state(root) -> dict:
    with open(os.path.join(_sidecar_dir(root), au.STATE_FILE)) as f:
        return json.load(f)


def _files_with(root, needle: bytes) -> list[str]:
    out = []
    for dirpath, _dirs, files in os.walk(str(root)):
        for fn in files:
            path = os.path.join(dirpath, fn)
            with open(path, "rb") as f:
                if needle in f.read():
                    out.append(os.path.relpath(path, str(root)))
    return sorted(out)


# ------------------------------------------------------------ selection

def test_selection(monkeypatch):
    monkeypatch.delenv("MEMD_VECTOR_INDEX", raising=False)
    monkeypatch.setattr(au, "usearch_available", lambda: True)
    assert resolve_vector_index({}) == "auto"
    assert resolve_vector_index({"vector_index": "flat"}) == "flat"
    assert resolve_vector_index({"vector_index": "usearch"}) == "usearch"
    monkeypatch.setenv("MEMD_VECTOR_INDEX", "usearch")
    assert resolve_vector_index({}) == "usearch"
    assert resolve_vector_index({"vector_index": "flat"}) == "flat"
    monkeypatch.setattr(au, "usearch_available", lambda: False)
    monkeypatch.delenv("MEMD_VECTOR_INDEX")
    assert resolve_vector_index({}) == "flat", "auto without usearch is the exact scan"
    with pytest.raises(ImportError):
        resolve_vector_index({"vector_index": "usearch"})
    with pytest.raises(ValueError):
        resolve_vector_index({"vector_index": "faiss"})
    with pytest.raises(ValueError):
        vector_index_config({"ann_dtype": "f64"}, "auto")
    cfg = vector_index_config({}, "auto")
    assert (cfg["min_vectors"], cfg["overfetch"], cfg["exact_max"], cfg["dtype"]) == (20000, 4, 2000, "f16")


def test_expansion_search_floor_is_applied_to_built_and_loaded_indexes(tmp_path):
    root = tmp_path / "s"
    e = _engine(root, expansion_search=300)
    _fill(e.namespace("n"), 200)
    assert e.namespace("n").index.ann._ix.expansion_search == 300
    e.close()
    e = _engine(root, expansion_search=300)
    try:
        ann = e.namespace("n").index.ann
        assert ann.loaded_from == "file" and ann._ix.expansion_search == 300
    finally:
        e.close()
    with pytest.raises(ValueError):
        vector_index_config({"ann_expansion_search": -1}, "auto")


def test_flat_mode_attaches_nothing_and_drops_the_sidecar(tmp_path):
    root = tmp_path / "s"
    e = _engine(root)
    _fill(e.namespace("n"), 300)
    e.close()
    sdir = _sidecar_dir(root)
    e = StorageEngine(str(root), vector_index={"mode": "flat"})
    try:
        ns = e.namespace("n")
        assert ns.index.ann is None
        assert not os.path.exists(sdir), "a sidecar kept while flat would fall behind"
        q = _vectors(1, seed=5)[0]
        assert ns.index.search_vector(q, IndexFilter(), limit=5)
    finally:
        e.close()


# ------------------------------------------------------------ parity

def test_recall_at_10_against_the_exact_scan(tmp_path):
    e = _engine(tmp_path / "s")
    try:
        ns = e.namespace("n")
        _fill(ns, 6000, seed=1)
        ann = ns.index.ann
        assert ann.ready() and ann.size() == 6000
        qs = _vectors(60, seed=1, draw=99)
        recalls, served = [], ann.searches
        for i, q in enumerate(qs):
            f = IndexFilter(scope=Scope(user=USERS[i % 3])) if i % 2 else IndexFilter()
            got = ns.index.search_vector(q, f, limit=10)
            want = _flat(ns.index, q, f, 10)
            assert len(want) == 10
            recalls.append(len(set(_ids(got)) & set(_ids(want))) / 10)
            # survivors are re-scored exactly: a shared hit has the flat score
            wscore = {h.record.id: h.score for h in want}
            for h in got:
                if h.record.id in wscore:
                    assert h.score == pytest.approx(wscore[h.record.id], abs=1e-5)
        assert ann.searches - served == 60, "the ANN path must have answered"
        assert float(np.mean(recalls)) >= 0.95, recalls
    finally:
        e.close()


def test_tied_scores_order_the_same_on_both_paths(tmp_path):
    e = _engine(tmp_path / "s")
    try:
        ns = e.namespace("n")
        _fill(ns, 1500, seed=2)
        v = _vectors(1, seed=77)[0]
        twins = [MemoryRecord.create(namespace="n", kind="raw_event", content=f"twin {c}",
                                     scope=Scope(user="alice"), t_event=1_800_000_000_000 + (i % 3))
                 for i, c in enumerate("qwertyuiopasdfgh")]
        ns.append(twins)
        ns.index.set_vectors([t.id for t in twins], [v] * len(twins), "test-model")
        ns.index.ann.drain(60)
        f = IndexFilter(scope=Scope(user="alice"))
        got = ns.index.search_vector(v, f, limit=10)
        assert _ids(got) == _ids(_flat(ns.index, v, f, 10))
        assert all(h.record.content.startswith("twin") for h in got)
        # (score, -t_event, content hash, id): newest first among equal scores
        assert [h.record.time.t_event for h in got] == sorted((h.record.time.t_event for h in got),
                                                              reverse=True)
    finally:
        e.close()


# ------------------------------------------------------------ filters, isolation

@pytest.mark.parametrize("sabotage", [False, True])
@pytest.mark.parametrize("exact_max", [0, 100_000])
def test_another_users_vectors_never_come_back(tmp_path, monkeypatch, sabotage, exact_max):
    e = _engine(tmp_path / "s", exact_max=exact_max)
    try:
        ns = e.namespace("n")
        _fill(ns, 3000, seed=3)
        # session-private rows of bob: visible to bob's user-level view, never
        # to another user nor to bob's other sessions
        priv = [MemoryRecord.create(namespace="n", kind="raw_event", content=f"private {i}",
                                    scope=Scope(user="bob", session="s1")) for i in range(50)]
        ns.append(priv)
        ns.index.set_vectors([r.id for r in priv], _vectors(50, seed=3), "test-model")
        ns.index.ann.drain(60)
        if sabotage:
            # defence in depth: with NO SQL filter at all, _passes_filter (the
            # Python twin) must still hold the boundary
            monkeypatch.setattr(NamespaceIndex, "_ann_where", lambda self, f, args: "1=1")
        for scope in (Scope(user="alice"), Scope(user="bob"), Scope(user="bob", session="s2"),
                      Scope(user="carol", session="s1")):
            f = IndexFilter(scope=scope)
            for q in _vectors(8, seed=3):  # queries sit right on the fill's clusters
                for h in ns.index.search_vector(q, f, limit=40):
                    assert scope.contains(h.record.scope), (scope, h.record.scope)
                    assert h.record.scope.user == scope.user
                    if scope.session is not None:
                        assert not h.record.content.startswith("private"), (scope, h.record.scope)
        mine = ns.index.search_vector(_vectors(1, seed=3)[0], IndexFilter(scope=Scope(user="bob", session="s1")),
                                      limit=100)
        assert any(h.record.content.startswith("private") for h in mine)
    finally:
        e.close()


def test_filters_match_the_exact_scan(tmp_path):
    """kinds, time bounds, exclude_ids and quarantine are the SQL filter's
    and _passes_filter's, whichever path answers."""
    e = _engine(tmp_path / "s")
    try:
        ns = e.namespace("n")
        recs, x = _fill(ns, 2500, seed=4)
        facts = [MemoryRecord.create(namespace="n", kind="fact", content=f"fact {i}",
                                     scope=Scope(user="alice"), t_event=1_700_000_000_000 + i)
                 for i in range(40)]
        ns.append(facts)
        ns.index.set_vectors([r.id for r in facts], x[:40], "test-model")
        ns.index.ann.drain(60)
        q = x[7]
        cases = [
            IndexFilter(kinds=("fact",)),
            IndexFilter(t_event_min=1_700_000_000_500, t_event_max=1_700_000_001_000),
            IndexFilter(scope=Scope(user="alice"), exclude_ids=frozenset(r.id for r in recs[:300])),
        ]
        for f in cases:
            got = ns.index.search_vector(q, f, limit=20)
            want = _flat(ns.index, q, f, 20)
            assert set(_ids(got)) <= {r.id for r in recs + facts}
            for h in got:
                assert ns.index._passes_filter(h.record, f)
            assert len(set(_ids(got)) & set(_ids(want))) >= 18, f
        before = _counter("memd_vector_index_fallback_total", reason="selective")
        ns.index.ann.exact_max = 100
        assert ns.index.search_vector(q, IndexFilter(kinds=("fact",)), limit=5)
        assert _counter("memd_vector_index_fallback_total", reason="selective") == before + 1
        assert ns.index.ann.stats()["fallback_exact_total"] >= 1
    finally:
        e.close()


# ------------------------------------------------------------ mutations

def test_delete_supersede_quarantine_and_hard_delete_are_honoured(tmp_path):
    e = _engine(tmp_path / "s")
    try:
        ns = e.namespace("n")
        recs, x = _fill(ns, 2000, seed=5)
        idx, ann = ns.index, ns.index.ann
        f = IndexFilter()

        def top(i):
            return _ids(idx.search_vector(x[i], f, limit=5))

        assert recs[10].id in top(10)
        size = ann.size()
        idx.tombstone(recs[10].id, 1)
        assert recs[10].id not in top(10) and ann.size() == size - 1
        idx.mark_superseded(recs[11].id, recs[12].id, 1)
        assert recs[11].id not in top(11) and ann.size() == size - 2
        idx.mark_quarantined(recs[13].id, True)
        assert recs[13].id not in top(13) and ann.size() == size - 3
        idx.mark_quarantined(recs[13].id, False)
        assert recs[13].id in top(13) and ann.size() == size - 2, "unquarantined: back in the sidecar"
        idx.hard_delete(recs[14].id)
        assert recs[14].id not in top(14) and ann.size() == size - 3
        idx.apply_ops_batch([{"op": "tombstone", "id": recs[15].id, "at": 1},
                             {"op": "hard_delete", "id": recs[16].id},
                             {"op": "supersede", "old": recs[17].id, "new": recs[18].id, "at": 1}])
        for i in (15, 16, 17):
            assert recs[i].id not in top(i)
        assert ann.size() == size - 6
        # a re-embedded record replaces its entry (no duplicate key)
        idx.set_vectors([recs[20].id], [x[21]], "test-model")
        ann.drain(30)
        assert ann.size() == size - 6
        assert recs[20].id in top(21)
        # a wipe (rebuild_index) empties it; vectors come back with reembed
        ns.rebuild_index()
        ann.drain(30)
        assert ann.size() == 0 and idx.search_vector(x[0], f, limit=5) == []
    finally:
        e.close()


def test_memory_facade_deletes_and_forget(tmp_path):
    m = Memory(str(tmp_path / "d"), encrypt=False, config={
        "embedder": "hash", "reranker": "none", "vector_index": "usearch", "fuse_vector": True,
        "rate_max_writes": 10 ** 9, "dup_max_repeats": 10 ** 9})
    try:
        ids = m.add_events([{"content": f"kumquat orchard note {i} ledger", "user_id": "u"}
                            for i in range(60)])
        m.flush()
        st = m.stats()["vector_index"]
        assert st["kind"] == "usearch" and st["ready"] and st["size"] == 60
        m.delete(ids[0])
        m.delete_many(ids[1:5], hard=True)
        m.flush()
        assert m.stats()["vector_index"]["size"] == 55
        res = m.search("kumquat orchard", user_id="u", budget_tokens=20000)
        assert not {i.id for i in res.items} & set(ids[:5])
    finally:
        m.close()


# ------------------------------------------------------------ lifecycle

def test_clean_reopen_loads_the_file(tmp_path):
    root = tmp_path / "s"
    e = _engine(root)
    _fill(e.namespace("n"), 1200, seed=6)
    q = _vectors(1, seed=6, draw=60)[0]
    want = _ids(e.namespace("n").index.search_vector(q, IndexFilter(), limit=10))
    e.close()
    e = _engine(root)
    try:
        ann = e.namespace("n").index.ann
        assert ann.ready() and ann.rebuilds == 0 and ann.loaded_from == "file"
        assert _ids(e.namespace("n").index.search_vector(q, IndexFilter(), limit=10)) == want
    finally:
        e.close()


@pytest.mark.parametrize("damage", ["corrupt", "missing", "mismatch", "behind", "dtype"])
def test_a_damaged_or_stale_file_is_rebuilt(tmp_path, damage):
    root = tmp_path / "s"
    e = _engine(root)
    recs, x = _fill(e.namespace("n"), 1500, seed=7)
    e.close()
    sdir = _sidecar_dir(root)
    st = _state(root)
    fpath = os.path.join(sdir, st["file"])
    over = {}
    if damage == "corrupt":
        with open(fpath, "r+b") as f:
            f.truncate(os.path.getsize(fpath) // 2)
    elif damage == "missing":
        os.unlink(fpath)
    elif damage == "mismatch":
        st["uid"] = "0" * 32  # built from another SQLite file
        json.dump(st, open(os.path.join(sdir, au.STATE_FILE), "w"))
    elif damage == "behind":
        st["wm"] -= 1  # saved before the last change
        json.dump(st, open(os.path.join(sdir, au.STATE_FILE), "w"))
    else:
        over = {"dtype": "i8"}
    before = _counter("memd_vector_index_rebuilds_total", reason=damage if damage != "dtype" else "mismatch")
    e = _engine(root, **over)
    try:
        ns = e.namespace("n")
        ann = ns.index.ann
        # until the rebuild is done the exact scan serves, and is right
        assert _ids(ns.index.search_vector(x[3], IndexFilter(), limit=3))[0] == recs[3].id
        assert ann.drain(120) and ann.ready()
        assert ann.rebuilds == 1 and ann.size() == 1500
        assert _counter("memd_vector_index_rebuilds_total",
                        reason=damage if damage != "dtype" else "mismatch") == before + 1
        assert _ids(ns.index.search_vector(x[3], IndexFilter(), limit=3))[0] == recs[3].id
        assert _state(root)["dtype"] == over.get("dtype", "f16")
    finally:
        e.close()


CRASH_CHILD = r"""
import os, sys
sys.path.insert(0, {src!r})
import numpy as np
import usearch.index
import memd.index.ann_usearch as au
from memd.core.schema import MemoryRecord, Scope
from memd.storage.engine import StorageEngine
root, point, more = sys.argv[1], sys.argv[2], sys.argv[3] == "1"
cfg = {{"mode": "usearch", "min_vectors": 0, "overfetch": 4, "exact_max": 0, "dtype": "f16",
       "build_threads": 1}}
e = StorageEngine(root, vector_index=cfg)
ns = e.namespace("n")
ann = ns.index.ann
assert ann.loaded_from == "file", ann.loaded_from
if more:
    rng = np.random.default_rng(1)
    recs = [MemoryRecord.create(namespace="n", kind="raw_event", content=f"late {{i}}",
                                scope=Scope(user="alice")) for i in range(40)]
    ns.append(recs)
    ns.index.set_vectors([r.id for r in recs], rng.standard_normal((40, {dim})), "test-model")
else:
    ann._saved_wm = None  # the rebuild below must write a new file
if point == "save":
    real = usearch.index.Index.save
    def save(self, path_or_buffer=None, progress=None):
        data = bytes(real(self))
        with open(path_or_buffer, "wb") as f:
            f.write(data[:len(data) // 2])
            f.flush()
            os.fsync(f.fileno())
        os._exit(9)  # killed mid-write of the new file
    usearch.index.Index.save = save
else:
    def write_state(self, *a, **k):
        os._exit(9)  # killed after the new file was renamed in, before the state names it
    au.UsearchSidecar._write_state = write_state
ann.wait_built(ann.request_build("test"), 60)
os._exit(3)
"""


@pytest.mark.parametrize("point", ["save", "state"])
@pytest.mark.parametrize("more", [False, True])
def test_killed_mid_rebuild_serves_the_old_file_or_rebuilds(tmp_path, point, more):
    root = tmp_path / "s"
    e = _engine(root)
    recs, x = _fill(e.namespace("n"), 1500, seed=8)
    e.close()
    old = _state(root)
    child = subprocess.run([sys.executable, "-c", CRASH_CHILD.format(src=SRC, dim=DIM), str(root),
                            point, "1" if more else "0"], capture_output=True, timeout=180)
    assert child.returncode == 9, child.stderr.decode()[-3000:]
    sdir = _sidecar_dir(root)
    leftovers = sorted(os.listdir(sdir))
    assert len(leftovers) > 2, f"precondition: the kill left a partial file behind: {leftovers}"
    e = _engine(root)
    try:
        ns = e.namespace("n")
        ann = ns.index.ann
        if more:
            assert ann.loaded_from != "file", "behind: rebuilt"
            assert ann.drain(120) and ann.rebuilds == 1 and ann.size() == 1540
        else:
            assert ann.loaded_from == "file" and ann.rebuilds == 0, "the old file, complete"
            assert _state(root) == old
        # temp files and files no state names are gone
        assert sorted(os.listdir(sdir)) == sorted([au.STATE_FILE, _state(root)["file"]])
        assert ann.ready()
        assert _ids(ns.index.search_vector(x[5], IndexFilter(), limit=3))[0] == recs[5].id
    finally:
        e.close()


SIGKILL_CHILD = r"""
import os, sys, time
sys.path.insert(0, {src!r})
import numpy as np
from memd.core.schema import MemoryRecord, Scope
from memd.storage.engine import StorageEngine
root = sys.argv[1]
cfg = {{"mode": "usearch", "min_vectors": 0, "overfetch": 4, "exact_max": 0, "dtype": "f16",
       "build_threads": 1}}
e = StorageEngine(root, vector_index=cfg)
ns = e.namespace("n")
rec = MemoryRecord.create(namespace="n", kind="raw_event", content="written after the last save",
                          scope=Scope(user="alice"))
ns.append([rec])
v = np.zeros({dim}); v[0] = 1.0
ns.index.set_vectors([rec.id], [v], "test-model")
ns.index.flush()
print(rec.id, flush=True)
time.sleep(60)
"""


def test_sigkill_after_a_change_rebuilds_behind(tmp_path):
    root = tmp_path / "s"
    e = _engine(root)
    _fill(e.namespace("n"), 1200, seed=9)
    e.close()
    child = subprocess.Popen([sys.executable, "-c", SIGKILL_CHILD.format(src=SRC, dim=DIM), str(root)],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        rid = child.stdout.readline().strip()
        assert rid, child.stderr.read()[-2000:]
        os.kill(child.pid, signal.SIGKILL)
        child.wait(timeout=15)
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)
    before = _counter("memd_vector_index_rebuilds_total", reason="behind")
    e = _engine(root)
    try:
        ns = e.namespace("n")
        ann = ns.index.ann
        assert ann.drain(120) and ann.rebuilds == 1
        assert _counter("memd_vector_index_rebuilds_total", reason="behind") == before + 1
        v = np.zeros(DIM)
        v[0] = 1.0
        assert _ids(ns.index.search_vector(v, IndexFilter(), limit=1)) == [rid]
    finally:
        e.close()


def test_threshold_switches_flat_and_usearch(tmp_path):
    root = tmp_path / "d"
    cfg = {"embedder": "hash", "reranker": "none", "vector_index": "auto", "ann_min_vectors": 300,
           "rate_max_writes": 10 ** 9, "dup_max_repeats": 10 ** 9}
    m = Memory(str(root), encrypt=False, config=cfg)
    try:
        m.add_events([{"content": f"note {i} about the harbor ferry", "user_id": "u"} for i in range(250)])
        m.flush()
        st = m.stats()["vector_index"]
        assert st["kind"] == "flat" and st["size"] == 0
        assert os.listdir(_sidecar_dir(root)) == []
        m.add_events([{"content": f"late note {i} about the harbor ferry", "user_id": "u"} for i in range(100)])
        m.flush()
        st = m.stats()["vector_index"]
        assert st["kind"] == "usearch" and st["ready"] and st["size"] == 350
        assert m.search("harbor ferry", user_id="u").items
    finally:
        m.close()
    m = Memory(str(root), encrypt=False, config=cfg)
    try:
        st = m.stats()["vector_index"]
        assert st["kind"] == "usearch" and st["loaded_from"] == "file" and st["rebuilds"] == 0
    finally:
        m.close()
    m = Memory(str(root), encrypt=False, config=dict(cfg, ann_min_vectors=1000))
    try:
        assert m.stats()["vector_index"]["kind"] == "flat"
        assert os.listdir(_sidecar_dir(root)) == [], "below the threshold the file is not kept"
        assert m.search("harbor ferry", user_id="u").items
    finally:
        m.close()


def test_destroy_namespace_removes_the_sidecar(tmp_path):
    e = _engine(tmp_path / "s")
    try:
        _fill(e.namespace("gone"), 400, seed=10)
        e.namespace("gone").index.ann.drain(30)
        e.close()
        e = _engine(tmp_path / "s")
        sdir = _sidecar_dir(tmp_path / "s")
        assert os.listdir(sdir)
        e.namespace("gone")
        assert e.destroy_namespace("gone")
        assert not os.path.exists(sdir)
    finally:
        e.close()


def test_searches_during_writes_and_a_rebuild_never_see_a_torn_index(tmp_path):
    e = _engine(tmp_path / "s")
    try:
        ns = e.namespace("n")
        recs, x = _fill(ns, 3000, seed=11)
        idx, ann = ns.index, ns.index.ann
        stop = threading.Event()
        errors: list[BaseException] = []
        served = [0]

        def searcher(k):
            f = IndexFilter(scope=Scope(user=USERS[k % 3]))
            i = 0
            while not stop.is_set():
                try:
                    for h in idx.search_vector(x[i % 3000], f, limit=10):
                        assert f.scope.contains(h.record.scope)
                        assert not h.record.deleted
                    served[0] += 1
                except BaseException as ex:  # noqa: BLE001
                    errors.append(ex)
                    return
                i += 7

        def writer():
            n = 0
            while not stop.is_set() and n < 30:
                more = [MemoryRecord.create(namespace="n", kind="raw_event", content=f"w {n} {j}",
                                            scope=Scope(user=USERS[j % 3])) for j in range(20)]
                ns.append(more)
                idx.set_vectors([r.id for r in more], _vectors(20, seed=1000 + n), "test-model")
                idx.tombstone(recs[n].id, 1)
                n += 1

        threads = [threading.Thread(target=searcher, args=(k,)) for k in range(3)]
        threads.append(threading.Thread(target=writer))
        for t in threads:
            t.start()
        ticket = ann.request_build("test")
        assert ann.wait_built(ticket, 120)
        threads[-1].join(60)
        stop.set()
        for t in threads:
            t.join(60)
        assert not errors, errors[:3]
        assert served[0] > 0
        assert ann.drain(60) and ann.rebuilds == 1
        live = idx._con.execute(
            "SELECT COUNT(*) FROM vectors v JOIN records r ON r.id = v.id WHERE r.deleted = 0").fetchone()[0]
        assert ann.size() == live, "every change made during the build reached the new index"
    finally:
        e.close()


# ------------------------------------------------------------ D7

def test_a_hard_delete_purge_leaves_no_vector_bytes_in_the_sidecar(tmp_path):
    root = str(tmp_path / "d")
    cfg = {"embedder": "hash", "reranker": "none", "vector_index": "usearch",
           "hard_delete_deadline_ms": 0, "rate_max_writes": 10 ** 9, "dup_max_repeats": 10 ** 9}
    m = Memory(root, encrypt=False, config=cfg)
    victim = m.add("zqxj victim record vyrkt for erasure plmoq", user_id="u")[0]
    m.add_events([{"content": f"unrelated keeper note {i}", "user_id": "u"} for i in range(80)])
    m.flush()
    m.close()                       # a clean close saves the sidecar file
    m = Memory(root, encrypt=False, config=cfg)
    try:
        assert m.stats()["vector_index"]["loaded_from"] == "file"
        needle = bytes(m.ns.index._con.execute("SELECT vec FROM vectors WHERE id=?", (victim,)).fetchone()[0])
        # positive control: the victim's f16 vector is in the sidecar file
        where = _files_with(root, needle)
        assert any(".usearch" in w for w in where), where
        m.delete(victim, hard=True)  # deadline 0: the purge runs at once
        m.flush()
        assert m.get(victim) is None
        st = m.stats()["vector_index"]
        assert st["ready"] and st["size"] == 80 and st["rebuilds"] == 1
        assert _counter("memd_vector_index_rebuilds_total", reason="purge") >= 1
        assert _files_with(root, needle) == [], "purged vector bytes survived in a file"
        assert os.listdir(_sidecar_dir(root)), "the rebuilt sidecar is saved again"
    finally:
        m.close()
    assert _files_with(root, needle) == [], "the clean close saved purged bytes"


# ------------------------------------------------------------ snapshots

def _snapshot_names(root, prefix: str) -> list[str]:
    d = os.path.join(str(root), "ns", "n")
    return sorted(f for f in os.listdir(d) if f.startswith(prefix) and f.endswith(".snap"))


def test_snapshot_is_published_and_installed_on_a_cold_node(tmp_path, monkeypatch):
    monkeypatch.setattr(storage_engine.NamespaceStore, "SNAPSHOT_MIN_RECORDS", 1)
    monkeypatch.setattr(storage_engine.NamespaceStore, "VECTOR_SNAPSHOT_MIN_VECTORS", 1)
    root, cache = tmp_path / "s", tmp_path / "cache"
    e = StorageEngine(str(root), cache_dir=str(cache), vector_index=_vcfg())
    ns = e.namespace("n")
    recs, x = _fill(ns, 2000, seed=12)
    ns.compact(force=True)
    vs = ns.manifest.vector_snapshot
    assert vs.get("name") and _snapshot_names(root, "vector-") == [vs["name"]]
    assert vs["seq"] == ns.manifest.snapshot_seq and vs["wm"] == ns.index._vec_wm
    q = x[40]
    want = _ids(ns.index.search_vector(q, IndexFilter(), limit=10))
    e.close()
    import shutil

    shutil.rmtree(cache)             # a cold node: no local index, no sidecar
    loaded = _counter("memd_vector_index_snapshots_loaded_total")
    e = StorageEngine(str(root), cache_dir=str(cache), vector_index=_vcfg())
    try:
        ns = e.namespace("n")
        ann = ns.index.ann
        assert ann.loaded_from == "snapshot" and ann.rebuilds == 0 and ann.ready()
        assert _counter("memd_vector_index_snapshots_loaded_total") == loaded + 1
        assert _ids(ns.index.search_vector(q, IndexFilter(), limit=10)) == want
        # a second publish replaces (and deletes) the first image
        more = [MemoryRecord.create(namespace="n", kind="raw_event", content="later",
                                    scope=Scope(user="alice"))]
        ns.append(more)
        ns.index.set_vectors([more[0].id], [x[0]], "test-model")
        ns.compact(force=True)
        assert _snapshot_names(root, "vector-") == [ns.manifest.vector_snapshot["name"]] != [vs["name"]]
    finally:
        e.close()


@pytest.mark.parametrize("tamper", ["wm", "floor"])
def test_a_snapshot_that_does_not_match_is_not_installed(tmp_path, monkeypatch, tamper):
    monkeypatch.setattr(storage_engine.NamespaceStore, "SNAPSHOT_MIN_RECORDS", 1)
    monkeypatch.setattr(storage_engine.NamespaceStore, "VECTOR_SNAPSHOT_MIN_VECTORS", 1)
    root, cache = tmp_path / "s", tmp_path / "cache"
    e = StorageEngine(str(root), cache_dir=str(cache), vector_index=_vcfg())
    ns = e.namespace("n")
    _fill(ns, 1500, seed=13)
    ns.compact(force=True)
    e.close()
    mpath = os.path.join(str(root), "ns", "n", "manifest.json")
    m = json.load(open(mpath))
    if tamper == "wm":
        m["vector_snapshot"]["wm"] += 1   # not the image it claims to match
    else:
        m["vector_snapshot"]["seq"] = 0   # older than the newest compaction
    json.dump(m, open(mpath, "w"))
    import shutil

    shutil.rmtree(cache)
    e = StorageEngine(str(root), cache_dir=str(cache), vector_index=_vcfg())
    try:
        ann = e.namespace("n").index.ann
        assert ann.loaded_from != "snapshot"
        assert ann.drain(120) and ann.rebuilds == 1 and ann.size() == 1500
    finally:
        e.close()


def test_a_purge_replaces_the_published_snapshot(tmp_path, monkeypatch):
    monkeypatch.setattr(storage_engine.NamespaceStore, "SNAPSHOT_MIN_RECORDS", 1)
    monkeypatch.setattr(storage_engine.NamespaceStore, "VECTOR_SNAPSHOT_MIN_VECTORS", 1)
    root = tmp_path / "s"
    e = _engine(root)
    try:
        ns = e.namespace("n")
        recs, x = _fill(ns, 1200, seed=14)
        ns.compact(force=True)
        first = ns.manifest.vector_snapshot["name"]
        needle = bytes(ns.index._con.execute("SELECT vec FROM vectors WHERE id=?",
                                             (recs[3].id,)).fetchone()[0])
        assert any(first in w for w in _files_with(root, needle)), "positive control"
        ns.append_op({"op": "hard_delete", "id": recs[3].id, "deadline": 0})
        ns.index.hard_delete(recs[3].id)
        ns.compact(force=False)      # the due purge
        vs = ns.manifest.vector_snapshot
        assert vs.get("name") and vs["name"] != first
        assert _snapshot_names(root, "vector-") == [vs["name"]]
        assert _files_with(root, needle) == []
    finally:
        e.close()


def test_an_orphaned_vector_snapshot_is_collected(tmp_path, monkeypatch):
    monkeypatch.setattr(storage_engine.NamespaceStore, "SNAPSHOT_MIN_RECORDS", 1)
    monkeypatch.setattr(storage_engine.NamespaceStore, "VECTOR_SNAPSHOT_MIN_VECTORS", 1)
    root = tmp_path / "s"
    e = _engine(root)
    ns = e.namespace("n")
    _fill(ns, 1000, seed=15)
    ns.compact(force=True)
    first = ns.manifest.vector_snapshot["name"]
    from memd.storage.objectstore import LocalObjectStore

    delete = LocalObjectStore.delete
    monkeypatch.setattr(LocalObjectStore, "delete",
                        lambda self, key: None if "/vector-" in key else delete(self, key))
    _fill(ns, 10, seed=16, start=1000)
    ns.compact(force=True)          # replaced; the kill "missed" the delete
    second = ns.manifest.vector_snapshot["name"]
    monkeypatch.setattr(LocalObjectStore, "delete", delete)
    monkeypatch.setattr(storage_engine.NamespaceStore, "_collect_garbage_now",
                        lambda self, defer_audit: None)
    e.close()
    monkeypatch.undo()
    assert first != second and {first, second} <= set(_snapshot_names(root, "vector-"))
    e = _engine(root)
    try:
        e.namespace("n")
        assert _snapshot_names(root, "vector-") == [second]
    finally:
        e.close()
