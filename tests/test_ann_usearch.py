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
        assert ann.drain(60) and ann.loaded_from == "file" and ann._ix.expansion_search == 300
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
        # a query far from the facts: the window comes back without them
        assert ns.index.search_vector(x[2000], IndexFilter(kinds=("fact",)), limit=5)
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
        assert ann.drain(60) and ann.ready() and ann.rebuilds == 0 and ann.loaded_from == "file"
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
assert ann.drain(60) and ann.loaded_from == "file", ann.loaded_from
if more:
    rng = np.random.default_rng(1)
    recs = [MemoryRecord.create(namespace="n", kind="raw_event", content=f"late {{i}}",
                                scope=Scope(user="alice")) for i in range(40)]
    ns.append(recs)
    ns.index.set_vectors([r.id for r in recs], rng.standard_normal((40, {dim})), "test-model")
else:
    ann._saved_wm = None  # the rebuild below must write a new file
if point == "save":
    real_fsync = os.fsync
    def fsync(fd):
        if os.readlink(f"/proc/self/fd/{{fd}}").endswith(".usearch.tmp"):
            os.ftruncate(fd, os.fstat(fd).st_size // 2)
            real_fsync(fd)
            os._exit(9)  # killed mid-write of the new file
        return real_fsync(fd)
    os.fsync = fsync
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
        assert ann.drain(120)
        if more:
            assert ann.loaded_from == "build", "behind: rebuilt"
            assert ann.rebuilds == 1 and ann.size() == 1540
        else:
            assert ann.loaded_from == "file" and ann.rebuilds == 0, "the old file, complete"
            assert _state(root) == old
        # temp files and files no state names are gone (a graph loaded from
        # its file keeps the loading marker until it has served cleanly)
        probation = [au.LOADING_MARKER] if ann.loaded_from == "file" else []
        assert sorted(os.listdir(sdir)) == sorted([au.STATE_FILE, _state(root)["file"]] + probation)
        assert ann.ready()
        assert _ids(ns.index.search_vector(x[5], IndexFilter(), limit=3))[0] == recs[5].id
    finally:
        e.close()


SIGKILL_CHILD = r"""
import os, sys, time
sys.path.insert(0, {src!r})
import numpy as np
import memd.index.ann_usearch as au
from memd.core.schema import MemoryRecord, Scope
from memd.storage.engine import StorageEngine
au.MARKER_SECONDS = 0  # the loaded file is trusted at once: the kill below is not a crash on load
root = sys.argv[1]
cfg = {{"mode": "usearch", "min_vectors": 0, "overfetch": 4, "exact_max": 0, "dtype": "f16",
       "build_threads": 1}}
e = StorageEngine(root, vector_index=cfg)
ns = e.namespace("n")
assert ns.index.ann.drain(60) and ns.index.ann.loaded_from == "file"
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
        m.flush()  # (the sidecar loads in the background)
        st = m.stats()["vector_index"]
        assert st["kind"] == "usearch" and st["loaded_from"] == "file" and st["rebuilds"] == 0
    finally:
        m.close()
    m = Memory(str(root), encrypt=False, config=dict(cfg, ann_min_vectors=1000))
    try:
        m.flush()
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
        m.flush()
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
        assert ann.drain(60)
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
        assert ann.drain(120) and ann.loaded_from == "build"
        assert ann.rebuilds == 1 and ann.size() == 1500
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


# ------------------------------------------------------------ memory safety: the flat cap

def _block_builds(monkeypatch) -> threading.Event:
    """Hold every sidecar build until the returned event is set."""
    go = threading.Event()
    real = UsearchSidecar._build

    def build(self, reason):
        go.wait(60)
        return real(self, reason)

    monkeypatch.setattr(UsearchSidecar, "_build", build)
    return go


def _wait_loaded(ann, timeout=30.0) -> None:
    """Until the open step (the load attempt) is over."""
    deadline = time.monotonic() + timeout
    while (ann._loading or ann._open_job is not None) and time.monotonic() < deadline:
        time.sleep(0.02)


def test_a_big_namespace_never_loads_the_flat_matrix_while_the_sidecar_rebuilds(tmp_path, monkeypatch):
    root = tmp_path / "s"
    e = _engine(root, flat_max=500, exact_max=100)
    recs, x = _fill(e.namespace("n"), 1500, seed=17)
    facts = [MemoryRecord.create(namespace="n", kind="fact", content=f"fact {i}", scope=Scope(user="alice"))
             for i in range(40)]
    e.namespace("n").append(facts)
    e.namespace("n").index.set_vectors([r.id for r in facts], x[:40], "test-model")
    e.close()
    os.unlink(os.path.join(_sidecar_dir(root), au.STATE_FILE))  # missing: rebuilt at open
    go = _block_builds(monkeypatch)
    e = _engine(root, flat_max=500, exact_max=100)
    try:
        ns = e.namespace("n")
        idx, ann = ns.index, ns.index.ann
        _wait_loaded(ann)
        assert not ann.ready() and idx.vector_lane_degraded()
        skipped = _counter("memd_vector_lane_skipped_total", reason="ann_rebuilding")
        # an unselective query skips the lane (bm25 and the rest serve it)...
        assert idx.search_vector(x[3], IndexFilter(scope=Scope(user="alice")), limit=40) == []
        assert _counter("memd_vector_lane_skipped_total", reason="ann_rebuilding") == skipped + 1
        assert idx.vector_lane_skipped == 1
        # ...a selective one is answered exactly, from just its rows...
        q = x[5]
        f = IndexFilter(kinds=("fact",))
        got = idx.search_vector(q, f, limit=10)
        assert got and _ids(got) == _ids(idx._exact_vector(q / np.linalg.norm(q), f, 10))
        assert all(h.record.kind == "fact" for h in got)
        # ...and so is a sweep (a destructive one must see every match)
        sweep = idx.search_vector(q, IndexFilter(), limit=5000)
        live = idx._con.execute("SELECT COUNT(*) FROM vectors").fetchone()[0]
        assert sweep and len(sweep) <= live
        assert {h.record.id for h in sweep} == {h.record.id for h in idx._exact_vector(
            q / np.linalg.norm(q), IndexFilter(), 5000)}
        assert not idx._vec_loaded and idx._main_mat.size == 0, "the flat matrix was loaded"
        go.set()
        assert ann.drain(60) and ann.ready()
        assert not idx.vector_lane_degraded()
        assert len(idx.search_vector(x[3], IndexFilter(scope=Scope(user="alice")), limit=40)) == 40
        assert not idx._vec_loaded
    finally:
        go.set()
        e.close()


def test_below_the_cap_the_exact_scan_still_serves_while_the_sidecar_rebuilds(tmp_path, monkeypatch):
    root = tmp_path / "s"
    e = _engine(root, flat_max=100_000)
    _, x = _fill(e.namespace("n"), 800, seed=18)
    e.close()
    os.unlink(os.path.join(_sidecar_dir(root), au.STATE_FILE))
    go = _block_builds(monkeypatch)
    e = _engine(root, flat_max=100_000)
    try:
        idx = e.namespace("n").index
        _wait_loaded(idx.ann)
        assert not idx.ann.ready() and not idx.vector_lane_degraded()
        assert len(idx.search_vector(x[1], IndexFilter(), limit=20)) == 20
        assert idx._vec_loaded and idx._main_mat.size, "below the cap the matrix is the fallback"
        go.set()
        deadline = time.monotonic() + 60
        while not idx.ann.ready() and time.monotonic() - deadline < 0:
            time.sleep(0.001)
        # freed when the sidecar takes over - not after its (seconds-long) save
        assert not idx._vec_loaded and idx._main_mat.size == 0
    finally:
        go.set()
        e.close()


def test_memory_search_serves_other_lanes_and_is_not_cached_while_the_lane_is_skipped(tmp_path, monkeypatch):
    root = str(tmp_path / "d")
    cfg = {"embedder": "hash", "reranker": "none", "vector_index": "usearch", "fuse_vector": True,
           "flat_max_vectors": 50, "ann_exact_max": 10, "lexical_backend": "fts5",
           "rate_max_writes": 10 ** 9, "dup_max_repeats": 10 ** 9, "vector_selfheal": False}
    m = Memory(root, encrypt=False, config=cfg)
    m.add_events([{"content": f"harbor ferry timetable note {i}", "user_id": "u"} for i in range(120)])
    m.flush()
    m.close()
    os.unlink(os.path.join(_sidecar_dir(root), au.STATE_FILE))
    go = _block_builds(monkeypatch)
    m = Memory(root, encrypt=False, config=cfg)
    try:
        _wait_loaded(m.ns.index.ann)
        res = m.search("harbor ferry timetable", user_id="u")
        assert res.items and not any("vector" in i.lanes for i in res.items)
        st = m.stats()["vector_index"]
        assert st["skipped_total"] >= 1 and st["flat_max_vectors"] == 50 and not st["ready"]
        assert not m.ns.index._vec_loaded
        go.set()
        m.flush()
        res = m.search("harbor ferry timetable", user_id="u")  # not the cached degraded result
        assert any("vector" in i.lanes for i in res.items)
    finally:
        go.set()
        m.close()


# ------------------------------------------------------------ save / load off the request path

def test_open_and_close_do_not_wait_for_the_sidecar_files(tmp_path, monkeypatch):
    root = tmp_path / "s"
    e = StorageEngine(str(root), vector_index=_vcfg(), max_open_namespaces=1)
    _, x = _fill(e.namespace("n"), 1200, seed=19)
    e.close()
    real_restore, real_persist = UsearchSidecar._restore, UsearchSidecar._persist

    def slow_restore(self, src, *a, **k):
        time.sleep(1.5)
        return real_restore(self, src, *a, **k)

    def slow_persist(self, cap, *, final=False):
        if final:
            time.sleep(1.5)
        return real_persist(self, cap, final=final)

    monkeypatch.setattr(UsearchSidecar, "_restore", slow_restore)
    monkeypatch.setattr(UsearchSidecar, "_persist", slow_persist)
    e = StorageEngine(str(root), vector_index=_vcfg(), max_open_namespaces=1)
    try:
        t0 = time.monotonic()
        ns = e.namespace("n")
        assert time.monotonic() - t0 < 1.0, "the open waited for the sidecar load"
        ann = ns.index.ann
        assert not ann.ready()
        # a write while it loads is journaled and reaches the loaded index
        late = MemoryRecord.create(namespace="n", kind="raw_event", content="late", scope=Scope(user="bob"))
        ns.append([late])
        ns.index.set_vectors([late.id], [x[0]], "test-model")
        assert ann.drain(30) and ann.loaded_from == "file" and ann.rebuilds == 0 and ann.size() == 1201
        wm = ns.index._vec_wm
        state = os.path.join(ann.path, au.STATE_FILE)
        t0 = time.monotonic()
        e.namespace("other")  # max_open_namespaces=1: "n" is evicted (closed) on this call
        assert time.monotonic() - t0 < 1.0, "the eviction waited for the final save"
        assert json.load(open(state)).get("wm") != wm, "precondition: the final save is in flight"
        ns = e.namespace("n")  # the reopen's load waits for it (settle), then loads it
        ann = ns.index.ann
        assert ann.drain(30) and ann.loaded_from == "file" and ann.rebuilds == 0
        assert json.load(open(state))["wm"] == wm and ann.size() == 1201
    finally:
        e.close()


def test_a_destroy_cancels_a_pending_final_save(tmp_path, monkeypatch):
    root = tmp_path / "s"
    e = _engine(root)
    try:
        ns = e.namespace("n")
        _, x = _fill(ns, 600, seed=20)
        sdir = _sidecar_dir(root)
        real_persist = UsearchSidecar._persist

        def slow_persist(self, cap, *, final=False):
            if final:
                time.sleep(1.0)
            return real_persist(self, cap, final=final)

        monkeypatch.setattr(UsearchSidecar, "_persist", slow_persist)
        more = [MemoryRecord.create(namespace="n", kind="raw_event", content="more", scope=Scope(user="a"))]
        ns.append(more)
        ns.index.set_vectors([more[0].id], [x[0]], "test-model")
        assert e.destroy_namespace("n")
        assert not os.path.exists(sdir)
        au.settle_all()
        time.sleep(1.5)
        assert not os.path.exists(sdir), "a cancelled final save recreated the destroyed sidecar"
    finally:
        e.close()


# ------------------------------------------------------------ verification round (D1-D5)

CORRUPT_CHILD = r"""
import os, sys, json
sys.path.insert(0, {src!r})
import numpy as np
from memd.index.sqlite_index import IndexFilter
from memd.storage.engine import StorageEngine
from memd.metrics import METRICS
root = sys.argv[1]
cfg = {{"mode": "usearch", "min_vectors": 0, "overfetch": 4, "exact_max": 0, "dtype": "f16",
       "build_threads": 1}}
e = StorageEngine(root, vector_index=cfg)
ns = e.namespace("n")
ann = ns.index.ann
ok = ann.drain(60)
qs = np.random.default_rng(5).standard_normal((50, {dim})).astype(np.float32)
hits = sum(len(ns.index.search_vector(q, IndexFilter(), limit=20)) for q in qs)
corrupt = sum(x["value"] for x in METRICS.snapshot()["counters"].get("memd_vector_index_corrupt_total", []))
print(json.dumps({{"ok": ok, "loaded_from": ann.loaded_from, "rebuilds": ann.rebuilds, "size": ann.size(),
                  "hits": hits, "corrupt": corrupt}}), flush=True)
e.close()
"""


@pytest.mark.parametrize("where", ["q1", "mid", "q3"])
def test_a_same_size_corruption_of_the_sidecar_file_is_detected_and_rebuilt(tmp_path, where):
    """64 bytes overwritten in place (size unchanged): usearch loads such a
    file and can crash in search, so the content checksum must reject it
    before usearch ever sees it."""
    root = tmp_path / "s"
    e = _engine(root)
    _fill(e.namespace("n"), 3000, seed=21)
    e.close()
    st = _state(root)
    fp = os.path.join(_sidecar_dir(root), st["file"])
    size = os.path.getsize(fp)
    pos = {"q1": size // 4, "mid": size // 2, "q3": 3 * size // 4}[where]
    with open(fp, "r+b") as f:
        f.seek(pos)
        f.write(np.random.default_rng(pos).integers(0, 256, 64, dtype=np.uint8).tobytes())
    assert os.path.getsize(fp) == size
    child = subprocess.run([sys.executable, "-c", CORRUPT_CHILD.format(src=SRC, dim=DIM), str(root)],
                           capture_output=True, text=True, timeout=180)
    assert child.returncode == 0, f"rc={child.returncode} {child.stderr[-2000:]}"
    out = json.loads(child.stdout.strip().splitlines()[-1])
    assert out["ok"] and out["loaded_from"] == "build" and out["rebuilds"] == 1, out
    assert out["size"] == 3000 and out["hits"] > 0 and out["corrupt"] == 1, out


def test_a_crash_inside_usearch_while_loading_does_not_loop(tmp_path):
    """A process that dies inside usearch's load leaves the loading marker:
    the next open neither loads that file nor the snapshot again, it rebuilds."""
    root = tmp_path / "s"
    e = _engine(root)
    recs, x = _fill(e.namespace("n"), 1500, seed=22)
    e.close()
    crash = CRASH_ON_LOAD.format(src=SRC)
    child = subprocess.run([sys.executable, "-c", crash, str(root)], capture_output=True, text=True, timeout=120)
    assert child.returncode == 11, child.stderr[-2000:]
    e = _engine(root)
    try:
        ns = e.namespace("n")
        ann = ns.index.ann
        assert ann.drain(60) and ann.loaded_from == "build" and ann.rebuilds == 1
        assert _ids(ns.index.search_vector(x[3], IndexFilter(), limit=3))[0] == recs[3].id
        assert not os.path.exists(os.path.join(ann.path, au.LOADING_MARKER))
    finally:
        e.close()


CRASH_ON_LOAD = r"""
import os, sys
sys.path.insert(0, {src!r})
import usearch.index
from memd.storage.engine import StorageEngine
def restore(*a, **k):
    os._exit(11)   # what a segfault inside usearch does to the process
usearch.index.Index.restore = staticmethod(restore)
cfg = {{"mode": "usearch", "min_vectors": 0, "overfetch": 4, "exact_max": 0, "dtype": "f16",
       "build_threads": 1}}
e = StorageEngine(sys.argv[1], vector_index=cfg)
e.namespace("n").index.ann.drain(60)
os._exit(3)
"""


def test_a_corrupted_published_sidecar_snapshot_is_not_installed(tmp_path, monkeypatch):
    monkeypatch.setattr(storage_engine.NamespaceStore, "SNAPSHOT_MIN_RECORDS", 1)
    monkeypatch.setattr(storage_engine.NamespaceStore, "VECTOR_SNAPSHOT_MIN_VECTORS", 1)
    root, cache = tmp_path / "s", tmp_path / "cache"
    e = StorageEngine(str(root), cache_dir=str(cache), vector_index=_vcfg())
    ns = e.namespace("n")
    _fill(ns, 2000, seed=23)
    ns.compact(force=True)
    vname = ns.manifest.vector_snapshot["name"]
    e.close()
    vp = os.path.join(str(root), "ns", "n", vname)
    size = os.path.getsize(vp)
    with open(vp, "r+b") as f:  # same size, inside the usearch image
        f.seek(size // 2)
        f.write(b"\xa5" * 64)
    import shutil

    shutil.rmtree(cache)
    before = _counter("memd_vector_index_corrupt_total", source="snapshot")
    e = StorageEngine(str(root), cache_dir=str(cache), vector_index=_vcfg())
    try:
        ann = e.namespace("n").index.ann
        assert ann.drain(120) and ann.loaded_from == "build" and ann.size() == 2000
        assert _counter("memd_vector_index_corrupt_total", source="snapshot") == before + 1
    finally:
        e.close()


def test_writes_are_searchable_while_the_apply_lock_is_held(tmp_path):
    """Read-your-writes: a snapshot publish holds the apply lock (freeze), so
    the writer cannot apply its own vectors; the queue is searched exactly."""
    e = _engine(tmp_path / "s")
    try:
        ns = e.namespace("n")
        _fill(ns, 3000, seed=24)
        ann = ns.index.ann
        held, release = threading.Event(), threading.Event()

        def holder():
            with ann.frozen():
                held.set()
                release.wait(60)

        t = threading.Thread(target=holder)
        t.start()
        try:
            assert held.wait(10)
            new = _vectors(50, seed=24, draw=77)
            served, found = ann.searches, 0
            for i, v in enumerate(new):
                r = MemoryRecord.create(namespace="n", kind="raw_event", content=f"fresh {i}",
                                        scope=Scope(user="alice"))
                ns.append([r])
                ns.index.set_vectors([r.id], [v], "test-model")
                hits = ns.index.search_vector(v, IndexFilter(scope=Scope(user="alice")), limit=10)
                found += bool(hits) and hits[0].record.id == r.id
            assert ann.searches - served == 50, "the ANN path must answer (not a fallback)"
            assert found == 50
        finally:
            release.set()
            t.join()
    finally:
        e.close()


def test_a_sweep_never_loads_the_flat_matrix_while_the_sidecar_serves(tmp_path):
    e = _engine(tmp_path / "s", flat_max=1_000_000)
    try:
        ns = e.namespace("n")
        _, x = _fill(ns, 2000, seed=25)
        idx = ns.index
        q = x[9] / np.linalg.norm(x[9])
        got = idx.search_vector(x[9], IndexFilter(), limit=5000)  # find_ids sweep size
        assert got and {h.record.id for h in got} == {h.record.id for h in idx._exact_vector(q, IndexFilter(), 5000)}
        assert not idx._vec_loaded and idx._main_mat.size == 0, "the sweep loaded the flat matrix"
    finally:
        e.close()


def test_independent_rebuilds_give_identical_top10(tmp_path):
    """Parallel HNSW construction is not deterministic; the lane's top-10 is,
    because candidates are over-fetched and re-ranked by exact cosine with
    fusion's tie-break."""
    e = _engine(tmp_path / "s", build_threads=4)
    try:
        ns = e.namespace("n")
        dim = 384
        rng = np.random.default_rng(26)
        centers = rng.standard_normal((64, dim))
        x = centers[rng.integers(0, 64, 8000)] + 0.6 * rng.standard_normal((8000, dim))
        recs = [MemoryRecord.create(namespace="n", kind="raw_event", content=f"doc {i}", scope=Scope(user="u"),
                                    t_event=1_700_000_000_000 + i) for i in range(8000)]
        ns.append(recs)
        for i in range(0, 8000, 1000):
            ns.index.set_vectors([r.id for r in recs[i:i + 1000]], x[i:i + 1000], "test-model")
        ann = ns.index.ann
        qs = centers[rng.integers(0, 64, 200)] + 0.6 * rng.standard_normal((200, dim))
        tops = []
        for _ in range(2):
            assert ann.wait_built(ann.request_build("test"), 120) and ann.drain(60)
            served = ann.searches
            tops.append([_ids(ns.index.search_vector(q, IndexFilter(scope=Scope(user="u")), limit=10))
                         for q in qs])
            assert ann.searches - served == 200
        assert sum(a != b for a, b in zip(*tops)) == 0
    finally:
        e.close()


@pytest.mark.parametrize("dtype", ["f16", "i8"])
def test_the_repair_pass_counts_an_identical_twin_as_found(tmp_path, dtype):
    """Records holding the very same vector (the same text embedded twice):
    a self-search for one returns one of the tied twins, which is as good as
    the node itself. Counting those as poorly linked re-inserted every
    duplicate - 30% duplicates cost 2x the build time."""
    e = _engine(tmp_path / "s", dtype=dtype)
    try:
        ann = e.namespace("n").index.ann
        x = _vectors(1000, seed=41)
        x[700:] = x[:300]
        ix = ann._new_index(DIM)
        ix.add(np.arange(1, 1001, dtype=np.uint64), ann._cast(x), threads=1)
        assert ann._repair(ix) <= 10
        assert len(ix) == 1000
        # a self-search answered by a node with ANOTHER vector still counts
        real = ix.search

        def search(q, k, **kw):
            m = real(q, k, **kw)
            keys = np.asarray(m.keys).reshape(len(q), -1)
            if len(q) == 1000:
                keys[10:15, 0] = 400  # nodes 11-15 "unreachable"
            return type("M", (), {"keys": keys})()
        ix.search = search
        assert ann._repair(ix) >= 5
        assert len(ix) == 1000
    finally:
        e.close()


@pytest.mark.parametrize("flag", ["include_quarantined", "include_invalid"])
def test_every_vector_path_applies_the_same_eligibility(tmp_path, flag):
    """The exact scan's matrix never holds deleted, quarantined, invalidated
    or superseded rows, whatever the filter asks; the sidecar's exact paths
    (selective filter, sweep, rescoring) must agree with it."""
    results = {}
    for mode, over in (("flat", {"mode": "flat"}), ("usearch", {}), ("usearch_exact", {"exact_max": 100_000})):
        e = StorageEngine(str(tmp_path / mode), vector_index=_vcfg(**over) if mode != "flat" else over)
        try:
            ns = e.namespace("n")
            recs, x = _fill(ns, 1200, seed=27) if mode != "flat" else _fill_flat(ns, 1200, seed=27)
            for i in range(10):
                ns.index.mark_quarantined(recs[i].id, True)
            for i in range(10, 20):
                ns.index.tombstone(recs[i].id, 1)
            if ns.index.ann is not None:
                ns.index.ann.drain(30)
            f = IndexFilter(scope=Scope(user=USERS[0]), **{flag: True})
            found = 0
            for i in range(20):
                for limit in (10, 2000):
                    found += recs[i].id in {h.record.id for h in ns.index.search_vector(x[i], f, limit=limit)}
            results[mode] = found
        finally:
            e.close()
    assert results["flat"] == 0 and results == {"flat": 0, "usearch": 0, "usearch_exact": 0}, results


def _fill_flat(ns, n, seed):
    x = _vectors(n, seed)
    recs = [MemoryRecord.create(namespace=ns.namespace, kind="raw_event", content=f"doc {i} seed {seed}",
                                scope=Scope(user=USERS[i % len(USERS)]), t_event=1_700_000_000_000 + i)
            for i in range(n)]
    ns.append(recs)
    ns.index.set_vectors([r.id for r in recs], x, "test-model")
    return recs, x


def test_a_stale_local_file_of_the_same_uid_and_watermark_is_not_trusted_after_a_snapshot_install(
        tmp_path, monkeypatch):
    """A snapshot install gives the local SQLite image a new vector lineage:
    a sidecar file left from another lineage that happens to carry the
    publisher's uid and watermark is rebuilt, not trusted."""
    monkeypatch.setattr(storage_engine.NamespaceStore, "SNAPSHOT_MIN_RECORDS", 1)
    monkeypatch.setattr(storage_engine.NamespaceStore, "VECTOR_SNAPSHOT_MIN_VECTORS", 1)
    root, cache = tmp_path / "s", tmp_path / "cache"
    e = StorageEngine(str(root), cache_dir=str(cache), vector_index=_vcfg())
    ns = e.namespace("n")
    recs, x = _fill(ns, 1500, seed=28)
    ns.compact(force=True)                     # publishes (uid U, wm W)
    e.close()
    sdir = _sidecar_dir(cache)
    st = _state(cache)
    # a divergent lineage's file under the same (uid, wm): other vectors, a
    # valid usearch image, a matching checksum
    from usearch.index import Index

    other = Index(ndim=DIM, metric="cos", dtype="f16", connectivity=16)
    rowids = [r[0] for r in sqlite3_rows(os.path.join(str(cache), "n.sqlite"))]
    other.add(np.asarray(rowids, dtype=np.uint64), _vectors(len(rowids), seed=999).astype(np.float16))
    buf = other.save()
    with open(os.path.join(sdir, st["file"]), "wb") as f:
        f.write(buf)
    st.update(size=len(buf), blake2b=__import__("hashlib").blake2b(buf, digest_size=32).hexdigest())
    json.dump(st, open(os.path.join(sdir, au.STATE_FILE), "w"))
    os.unlink(os.path.join(str(cache), "n.sqlite"))   # the SQLite cache is lost; the snapshot is installed
    for suf in ("-wal", "-shm"):
        try:
            os.unlink(os.path.join(str(cache), "n.sqlite" + suf))
        except OSError:
            pass
    e = StorageEngine(str(root), cache_dir=str(cache), vector_index=_vcfg())
    try:
        ns = e.namespace("n")
        ann = ns.index.ann
        assert ns.index.vec_uid != st["uid"], "a snapshot install starts a new vector lineage"
        assert ann.drain(60) and ann.loaded_from == "snapshot"
        assert _ids(ns.index.search_vector(x[7], IndexFilter(), limit=3))[0] == recs[7].id
    finally:
        e.close()


def sqlite3_rows(path):
    import sqlite3

    con = sqlite3.connect(path)
    try:
        return con.execute("SELECT r.rowid FROM records r JOIN vectors v ON v.id = r.id").fetchall()
    finally:
        con.close()


# ------------------------------------------------------------ a loaded graph on probation

REAL_QUERY_CRASH = r"""
import os, sys
sys.path.insert(0, {src!r})
import numpy as np
from memd.index.sqlite_index import IndexFilter
from memd.storage.engine import StorageEngine
cfg = {{"mode": "usearch", "min_vectors": 0, "overfetch": 4, "exact_max": 0, "dtype": "f16",
       "build_threads": 1}}
e = StorageEngine(sys.argv[1], vector_index=cfg)
ns = e.namespace("n")
ann = ns.index.ann
assert ann.drain(60) and ann.loaded_from == "file", ann.loaded_from
for q in np.random.default_rng(3).standard_normal((20, {dim})):
    ns.index.search_vector(q, IndexFilter(), limit=10)
assert ann.searches >= 20
os._exit(11)   # what a segfault inside a real usearch search does to the process
"""


def _replace_graph(root, build) -> dict:
    """Swap the sidecar file for `build(rowids)` (a valid usearch image),
    with a matching checksum and size in the state: checksum-valid."""
    import hashlib

    sdir = _sidecar_dir(root)
    st = _state(root)
    ix = build([r[0] for r in sqlite3_rows(os.path.join(str(root), "_cache", "n.sqlite"))])
    buf = ix.save()
    with open(os.path.join(sdir, st["file"]), "wb") as f:
        f.write(buf)
    st.update(size=len(buf), blake2b=hashlib.blake2b(buf, digest_size=32).hexdigest())
    json.dump(st, open(os.path.join(sdir, au.STATE_FILE), "w"))
    return st


def test_a_crash_in_a_real_query_after_loading_does_not_loop(tmp_path):
    """A checksum-valid graph usearch loads and smoke-searches fine can still
    crash the process in a real query. The marker stays until the loaded
    graph has served cleanly, so the next open rebuilds instead of loading
    the same file into the same crash."""
    root = tmp_path / "s"
    e = _engine(root)
    recs, x = _fill(e.namespace("n"), 1500, seed=42)
    e.close()
    child = subprocess.run([sys.executable, "-c", REAL_QUERY_CRASH.format(src=SRC, dim=DIM), str(root)],
                           capture_output=True, text=True, timeout=120)
    assert child.returncode == 11, child.stderr[-2000:]
    before = _counter("memd_vector_index_corrupt_total", source="crash_on_load")
    e = _engine(root)
    try:
        ns = e.namespace("n")
        ann = ns.index.ann
        assert ann.drain(60) and ann.loaded_from == "build" and ann.rebuilds == 1
        assert _counter("memd_vector_index_corrupt_total", source="crash_on_load") == before + 1
        assert _ids(ns.index.search_vector(x[3], IndexFilter(), limit=3))[0] == recs[3].id
        assert not os.path.exists(os.path.join(ann.path, au.LOADING_MARKER)), "a built graph is ours"
    finally:
        e.close()


def test_the_loading_marker_stays_until_the_loaded_graph_has_served(tmp_path, monkeypatch):
    monkeypatch.setattr(au, "MARKER_SEARCHES", 30)
    root = tmp_path / "s"
    e = _engine(root)
    recs, x = _fill(e.namespace("n"), 1500, seed=43)
    e.close()
    e = _engine(root)
    try:
        ns = e.namespace("n")
        ann = ns.index.ann
        marker = os.path.join(ann.path, au.LOADING_MARKER)
        assert ann.drain(60) and ann.loaded_from == "file"
        assert os.path.exists(marker), "a loaded graph is on probation"
        for i in range(29):
            assert _ids(ns.index.search_vector(x[i], IndexFilter(), limit=3))[0] == recs[i].id
        assert os.path.exists(marker), "29 of 30 searches"
        ns.index.search_vector(x[29], IndexFilter(), limit=3)
        deadline = time.monotonic() + 20
        while os.path.exists(marker) and time.monotonic() < deadline:
            time.sleep(0.05)  # (after the background recall check of its first answers)
        assert not os.path.exists(marker)
        assert ann.rebuilds == 0 and ann.loaded_from == "file", "a healthy graph passes its recall check"
    finally:
        e.close()
    # ...or after MARKER_SECONDS of serving without damage; and a clean close
    # removes it (the process did not crash)
    monkeypatch.setattr(au, "MARKER_SECONDS", 0.3)
    e = _engine(root)
    try:
        ns = e.namespace("n")
        ann = ns.index.ann
        marker = os.path.join(ann.path, au.LOADING_MARKER)
        assert ann.drain(60) and ann.loaded_from == "file" and os.path.exists(marker)
        time.sleep(0.4)
        r = MemoryRecord.create(namespace="n", kind="raw_event", content="late", scope=Scope(user="alice"))
        ns.append([r])
        ns.index.set_vectors([r.id], [x[0]], "test-model")  # applies: the time is checked
        assert not os.path.exists(marker)
    finally:
        e.close()
    monkeypatch.setattr(au, "MARKER_SECONDS", 300.0)
    e = _engine(root)
    ann = e.namespace("n").index.ann
    assert ann.drain(60) and ann.loaded_from == "file" and os.path.exists(os.path.join(ann.path, au.LOADING_MARKER))
    e.close()
    assert not os.path.exists(os.path.join(ann.path, au.LOADING_MARKER)), "a clean close clears it"
    e = _engine(root)
    try:
        ann = e.namespace("n").index.ann
        assert ann.drain(60) and ann.loaded_from == "file" and ann.rebuilds == 0
    finally:
        e.close()


def test_a_checksum_valid_graph_of_another_shape_is_rebuilt(tmp_path):
    """The checksum proves the bytes are the ones written, not that memd
    wrote them. A graph whose own bookkeeping disagrees with what the state
    says was saved (here: connectivity 8) is rebuilt, never searched."""
    root = tmp_path / "s"
    e = _engine(root)
    recs, x = _fill(e.namespace("n"), 1500, seed=44)
    e.close()
    from usearch.index import Index

    def build(rowids):
        ix = Index(ndim=DIM, metric="cos", dtype="f16", connectivity=8)
        ix.add(np.asarray(rowids, dtype=np.uint64), x.astype(np.float16))
        return ix
    _replace_graph(root, build)
    before = _counter("memd_vector_index_corrupt_total", source="structure")
    e = _engine(root)
    try:
        ns = e.namespace("n")
        ann = ns.index.ann
        assert ann.drain(60) and ann.loaded_from == "build" and ann.rebuilds == 1
        assert _counter("memd_vector_index_corrupt_total", source="structure") == before + 1
        assert _ids(ns.index.search_vector(x[3], IndexFilter(), limit=3))[0] == recs[3].id
        assert not os.path.exists(os.path.join(ann.path, au.LOADING_MARKER))
    finally:
        e.close()


def test_the_structure_check_rejects_impossible_bookkeeping(tmp_path):
    from types import SimpleNamespace as NS

    e = _engine(tmp_path / "s")
    try:
        ann = e.namespace("n").index.ann

        def graph(**over):
            g = dict(ndim=DIM, size=20000, connectivity=16, dtype=NS(name="F16"), metric_kind=NS(name="Cos"),
                     multi=False, capacity=20000, max_level=4, nlevels=5,
                     levels_stats=[NS(nodes=20000, edges=502311, max_edges=640000),
                                   NS(nodes=1196, edges=18848, max_edges=19136),
                                   NS(nodes=80, edges=841, max_edges=1280),
                                   NS(nodes=10, edges=90, max_edges=160),
                                   NS(nodes=4, edges=12, max_edges=64)])
            g.update(over)
            return type("G", (), {**g, "__len__": lambda self: g["size"]})()

        expect = {"ndim": DIM, "count": 20000}
        assert ann._structure(graph(), expect) == ""
        assert ann._structure(graph(size=19999), expect) == "count"
        assert ann._structure(graph(dtype=NS(name="I8")), expect) == "kind"
        assert ann._structure(graph(capacity=100), expect) == "size"
        lv = graph().levels_stats
        assert ann._structure(graph(max_level=40, nlevels=41, levels_stats=lv + [NS(nodes=1, edges=0, max_edges=32)] * 36),
                              expect) == "levels", "far more levels than 20000 nodes can have"
        bad = list(lv)
        bad[1] = NS(nodes=1196, edges=10 ** 9, max_edges=19136)
        assert ann._structure(graph(levels_stats=bad), expect) == "levels"
        bad = list(lv)
        bad[2] = NS(nodes=5000, edges=10, max_edges=80000)
        assert ann._structure(graph(levels_stats=bad), expect) == "levels", "an upper level larger than the one below"
        bad = list(lv)
        bad[0] = NS(nodes=20000, edges=0, max_edges=640000)
        assert ann._structure(graph(levels_stats=bad), expect) == "edges"
    finally:
        e.close()


def test_a_loaded_graph_that_does_not_answer_is_rebuilt(tmp_path):
    """Checksum-valid and well-formed, but not a graph of these vectors (an
    adversarial local file, or damage its bookkeeping does not show): its
    first answers are checked against the exact scan in the background, and
    a recall below the threshold rebuilds it from SQLite."""
    root = tmp_path / "s"
    e = _engine(root)
    recs, x = _fill(e.namespace("n"), 3000, seed=45)
    e.close()
    from usearch.index import Index

    def build(rowids):
        ix = Index(ndim=DIM, metric="cos", dtype="f16", connectivity=16)
        ix.add(np.asarray(rowids, dtype=np.uint64), _vectors(len(rowids), seed=999).astype(np.float16))
        return ix
    _replace_graph(root, build)
    before = _counter("memd_vector_index_corrupt_total", source="recall")
    e = _engine(root)
    try:
        ns = e.namespace("n")
        ann = ns.index.ann
        assert ann.drain(60) and ann.loaded_from == "file"
        for i in range(au.VERIFY_QUERIES):
            ns.index.search_vector(x[i], IndexFilter(), limit=10)
        deadline = time.monotonic() + 60
        while ann.loaded_from != "build" and time.monotonic() < deadline:
            time.sleep(0.05)
        assert ann.drain(60) and ann.loaded_from == "build" and ann.rebuilds == 1
        assert _counter("memd_vector_index_corrupt_total", source="recall") == before + 1
        for i in range(20):
            assert _ids(ns.index.search_vector(x[i], IndexFilter(), limit=3))[0] == recs[i].id
        assert not os.path.exists(os.path.join(ann.path, au.LOADING_MARKER))
    finally:
        e.close()


# ------------------------------------------------------------ searches vs publishes and backlogs

def test_a_stream_of_write_slices_does_not_starve_searches():
    """An applier draining a backlog takes the write side slice after slice;
    writer-preference alone let it re-take the lock before any waiting
    search got in (p99 12-16 s behind a snapshot publish and a bulk writer).
    Readers waiting at a release now enter before the next slice - and a
    stream of searches still cannot starve a writer."""
    rw = au._RWLock()
    stop = threading.Event()

    def slices():
        while not stop.is_set():
            with rw.write():
                time.sleep(0.005)

    def reads():
        while not stop.is_set():
            with rw.read():
                time.sleep(0.005)

    for body, other in ((slices, rw.read), (reads, rw.write)):
        stop.clear()
        ths = [threading.Thread(target=body, daemon=True) for _ in range(2)]
        for t in ths:
            t.start()
        time.sleep(0.05)
        waits = []
        for _ in range(10):
            got = threading.Event()

            def take():
                t0 = time.monotonic()
                with other():
                    waits.append(time.monotonic() - t0)
                    got.set()
            threading.Thread(target=take, daemon=True).start()
            assert got.wait(5), f"{other.__name__} starved by a stream of {body.__name__}"
        stop.set()
        for t in ths:
            t.join(5)
        assert max(waits) < 0.5, waits


class _SlowCopy:
    """A pinned image whose backup takes a while."""

    def __init__(self, con, started: threading.Event):
        self.con, self.started = con, started

    def backup(self, dst, **kw):
        self.started.set()
        time.sleep(1.5)
        return self.con.backup(dst, **kw)

    def close(self):
        self.con.close()


def test_a_snapshot_publish_copies_the_index_without_holding_it(tmp_path, monkeypatch):
    """The SQLite image used to be copied on the writer connection inside the
    index lock and the sidecar freeze: every write, and every search that
    had to publish a lazy commit, waited for the whole backup. It now reads
    a snapshot pinned under the lock; the copy runs with nothing held, and
    the image is still exactly the one the sidecar image was taken at."""
    import gzip
    import sqlite3

    monkeypatch.setattr(storage_engine.NamespaceStore, "SNAPSHOT_MIN_RECORDS", 1)
    monkeypatch.setattr(storage_engine.NamespaceStore, "VECTOR_SNAPSHOT_MIN_VECTORS", 1)
    e = _engine(tmp_path / "s")
    try:
        ns = e.namespace("n")
        recs, x = _fill(ns, 2000, seed=46)
        ns.compact(force=True)  # (stamps the index with a seq a snapshot may be published at)
        idx = ns.index
        started, done = threading.Event(), threading.Event()
        real_pin = idx.pin_image
        monkeypatch.setattr(idx, "pin_image", lambda: _SlowCopy(real_pin(), started))
        wm0 = idx._vec_wm
        ok = []
        t = threading.Thread(target=lambda: (ok.append(ns.write_index_snapshot()), done.set()))
        t.start()
        assert started.wait(30)
        t0 = time.monotonic()
        late = MemoryRecord.create(namespace="n", kind="raw_event", content="written during the copy",
                                   scope=Scope(user="alice"))
        ns.append([late])
        v = _vectors(1, seed=46, draw=3)[0]
        idx.set_vectors([late.id], [v], "test-model")
        hits = idx.search_vector(v, IndexFilter(), limit=3)
        assert time.monotonic() - t0 < 1.0 and not done.is_set(), "the write or search waited for the copy"
        assert hits[0].record.id == late.id
        t.join(60)
        assert ok == [True]
        vs = ns.manifest.vector_snapshot
        assert vs["wm"] == wm0
        blob = ns.store.get(ns._snapshot_key(ns.manifest.snapshot_name))
        if ns.envelope.enabled:
            blob = ns.envelope.decrypt(ns.namespace, blob)
        img = tmp_path / "image.sqlite"
        img.write_bytes(gzip.decompress(blob))
        con = sqlite3.connect(str(img))
        try:
            assert int(con.execute("SELECT v FROM meta WHERE k='vec_wm'").fetchone()[0]) == wm0
            assert con.execute("SELECT COUNT(*) FROM records WHERE id=?", (late.id,)).fetchone()[0] == 0
            assert con.execute("SELECT COUNT(*) FROM vectors").fetchone()[0] == 2000
        finally:
            con.close()
    finally:
        e.close()


def test_a_write_applied_between_the_pending_snapshot_and_the_index_search_is_found(tmp_path, monkeypatch):
    """Read-your-writes across the applier's hand-off: the queued rows are
    taken before the index is searched, so a row applied in between is in
    one or the other - never in neither."""
    e = _engine(tmp_path / "s")
    try:
        ns = e.namespace("n")
        _fill(ns, 2000, seed=47)
        ann = ns.index.ann
        real_knn = ann.knn

        def knn_then_apply(q, k):
            got = real_knn(q, k)
            ann.apply_pending()  # the applier finishes right after the index was searched
            return got
        monkeypatch.setattr(ann, "knn", knn_then_apply)
        found = 0
        for i, v in enumerate(_vectors(20, seed=47, draw=5)):
            r = MemoryRecord.create(namespace="n", kind="raw_event", content=f"fresh {i}", scope=Scope(user="bob"))
            ns.append([r])
            with ann._apply_lock:  # the writer cannot apply its own vector: it stays queued
                ns.index.set_vectors([r.id], [v], "test-model")
            assert ann.pending_rowids()
            hits = ns.index.search_vector(v, IndexFilter(), limit=10)
            found += bool(hits) and hits[0].record.id == r.id
        assert found == 20
    finally:
        e.close()


def test_the_pending_pass_filters_only_rows_that_can_make_the_page(tmp_path, monkeypatch):
    """Behind a bulk writer the queue holds thousands of rows. They are
    scored from their queued vectors in one pass; only those that can still
    make the page go through SQL (every one of them did, on every search)."""
    e = _engine(tmp_path / "s")
    try:
        ns = e.namespace("n")
        _fill(ns, 2000, seed=48)
        ann, idx = ns.index.ann, ns.index
        recs = [MemoryRecord.create(namespace="n", kind="raw_event", content=f"bulk {i}", scope=Scope(user="carol"))
                for i in range(3000)]
        ns.append(recs)
        xs = _vectors(3000, seed=48, draw=9)
        counted: list[int] = []
        real = idx._scored_rowids
        with ann._apply_lock:  # nothing is applied: all 3000 stay queued
            for i in range(0, 3000, 500):
                idx.set_vectors([r.id for r in recs[i:i + 500]], xs[i:i + 500], "test-model")
            assert len(ann.pending_rowids()) == 3000
            monkeypatch.setattr(idx, "_scored_rowids",
                                lambda q, f, rowids: (counted.append(len(rowids)), real(q, f, rowids))[1])
            for i in (7, 1500, 2999):
                counted.clear()
                hits = idx.search_vector(xs[i], IndexFilter(), limit=10)
                assert hits[0].record.id == recs[i].id
                assert sum(counted) < 1000, counted
                q = xs[i] / np.linalg.norm(xs[i])
                assert {h.record.id for h in hits} == {h.record.id for h in idx._exact_vector(q, IndexFilter(), 10)}
        ann.apply_pending()
        assert ann.drain(60) and not ann.pending_rowids()
    finally:
        e.close()
