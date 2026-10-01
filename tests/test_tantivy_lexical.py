"""The optional tantivy lexical accelerator (memd[fast]).

FTS5 stays the synchronous source of truth; tantivy is a derived index fed
by a background batcher. These tests hold it to that contract: the same
answers as FTS5 (parity), a write is searchable the moment it is acked
(tail), deletes/supersession/quarantine are honoured before the batcher
runs, another user's rows never come back through it (even with the
prefilter sabotaged), and a crashed, corrupt or foreign index is rebuilt.
"""
import json
import os
import random
import signal
import subprocess
import sys
import time
import uuid

import pytest

pytest.importorskip("tantivy")

import memd.index.tantivy_lexical as tl  # noqa: E402
from memd.core.schema import Scope  # noqa: E402
from memd.engine.memory import Memory  # noqa: E402
from memd.index.sqlite_index import IndexFilter, _fts_escape  # noqa: E402
from memd.index.tantivy_lexical import TantivyLexical, resolve_lexical_backend  # noqa: E402
from memd.metrics import METRICS  # noqa: E402

SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
T0 = 1_684_540_800_000


def _counter(name: str, **labels) -> float:
    return sum(x["value"] for x in METRICS.snapshot()["counters"].get(name, [])
               if all(x["labels"].get(k) == v for k, v in labels.items()))


def _mem(root, **cfg) -> Memory:
    base = {"embedder": "hash", "reranker": "none", "lexical_backend": "tantivy",
            "rate_max_writes": 10 ** 9, "dup_max_repeats": 10 ** 9}
    base.update(cfg)
    return Memory(str(root), encrypt=False, config=base)


def _lex(m: Memory) -> TantivyLexical:
    lex = m.ns.index.lexical
    assert lex is not None, "tantivy accelerator not attached"
    return lex


def _ready(m: Memory) -> TantivyLexical:
    m.flush()
    lex = _lex(m)
    assert lex.drain(30)
    assert lex.ready()
    return lex


VOCAB = ("kumquat mango orchard ferry harbor violin rehearsal passport visa dentist "
         "invoice mortgage marathon blister recipe sourdough starter telescope nebula "
         "keyboard firmware bicycle derailleur gardening compost tomato basil espresso "
         "grinder thermostat boiler insurance premium flight layover museum sculpture").split()
FILLER = "the a of and to in we i it was is that for on with this my".split()


def _corpus(n=400, seed=7):
    rng = random.Random(seed)
    docs = []
    for i in range(n):
        k = rng.randint(3, 9)
        words = [rng.choice(VOCAB) for _ in range(k)] + [rng.choice(FILLER) for _ in range(k)]
        rng.shuffle(words)
        docs.append(f"note {i} " + " ".join(words))
    return docs


def _queries(seed=11, n=25):
    rng = random.Random(seed)
    return [" ".join(rng.sample(VOCAB, rng.randint(1, 4))) for _ in range(n)]


# ------------------------------------------------------------ selection

def test_backend_selection(monkeypatch):
    monkeypatch.delenv("MEMD_LEXICAL_BACKEND", raising=False)
    monkeypatch.setattr(tl, "tantivy_available", lambda: True)
    assert resolve_lexical_backend({}) == "tantivy"
    assert resolve_lexical_backend({"lexical_backend": "fts5"}) == "fts5"
    monkeypatch.setenv("MEMD_LEXICAL_BACKEND", "fts5")
    assert resolve_lexical_backend({}) == "fts5"
    assert resolve_lexical_backend({"lexical_backend": "tantivy"}) == "tantivy"
    monkeypatch.setattr(tl, "tantivy_available", lambda: False)
    monkeypatch.delenv("MEMD_LEXICAL_BACKEND")
    assert resolve_lexical_backend({}) == "fts5"
    with pytest.raises(ImportError):
        resolve_lexical_backend({"lexical_backend": "tantivy"})
    with pytest.raises(ValueError):
        resolve_lexical_backend({"lexical_backend": "lucene"})


def test_fts5_backend_attaches_nothing(tmp_path):
    m = _mem(tmp_path / "d", lexical_backend="fts5")
    try:
        assert m.ns.index.lexical is None
        assert m.stats()["lexical"] == {"backend": "fts5"}
    finally:
        m.close()


def test_opening_without_the_accelerator_drops_its_index(tmp_path):
    """Rows changed while it is not attached would be missing from it, and an
    operator who switched it off should not keep a second copy of the text."""
    root = tmp_path / "d"
    m = _mem(root)
    m.add("kumquat orchard", user_id="u")
    _ready(m)
    m.close()
    tdir = _tantivy_dir(root)
    m = _mem(root, lexical_backend="fts5")
    try:
        assert not os.path.exists(tdir)
        assert m.search("kumquat", user_id="u").items
    finally:
        m.close()


# ------------------------------------------------------------ parity

def test_parity_with_fts5_top10(tmp_path):
    m = _mem(tmp_path / "d")
    try:
        docs = _corpus()
        users = ["alice", "bob"]
        m.add_events([{"content": d, "user_id": users[i % 2], "session_id": f"s{i % 7}",
                       "t_event": T0 + i} for i, d in enumerate(docs)])
        lex = _ready(m)
        idx = m.ns.index
        overlaps = []
        for q in _queries():
            for scope in (Scope(user="alice"), Scope()):
                f = IndexFilter(scope=scope)
                expr = " OR ".join(f'"{w}"' for w in _fts_escape(q).split())
                want = [h.record.id for h in idx._fts5_bm25(expr, f, 10)]
                before = _counter("memd_lexical_searches_total")
                got = [h.record.id for h in idx.search_bm25(q, f, limit=10)]
                assert _counter("memd_lexical_searches_total") == before + 1, \
                    "the query was not served by tantivy"
                assert len(got) == len(want)
                overlaps.append(len(set(got) & set(want)) / max(1, len(want)))
        mean = sum(overlaps) / len(overlaps)
        assert mean >= 0.8, f"top-10 overlap with FTS5 {mean:.3f} < 0.8"
        assert lex.stats()["pending"] == 0
    finally:
        m.close()


# ------------------------------------------------------------ freshness

def test_a_write_is_searchable_before_the_batcher_runs(tmp_path):
    # a long batch window: nothing but the tail can serve the new record
    m = _mem(tmp_path / "d", lexical_commit_ms=600_000, lexical_commit_docs=10 ** 6)
    try:
        m.add_events([{"content": d, "user_id": "u"} for d in _corpus(50)])
        lex = _ready(m)
        w = lex.stats()["watermark"]
        rid = m.add("the zeppelin hangar keycode is 4471", user_id="u")[0]
        tail0 = _counter("memd_lexical_tail_hits_total")
        served0 = _counter("memd_lexical_searches_total")
        res = m.search("zeppelin hangar keycode", user_id="u")
        assert [i.id for i in res.items][:1] == [rid]
        assert _counter("memd_lexical_searches_total") > served0
        assert _counter("memd_lexical_tail_hits_total") > tail0
        assert lex.stats()["watermark"] == w, "the batcher ran; the tail was not exercised"
        m.flush()
        assert lex.stats()["watermark"] > w
        m._bump_epoch()
        tail1 = _counter("memd_lexical_tail_hits_total")
        assert [i.id for i in m.search("zeppelin hangar keycode", user_id="u").items][:1] == [rid]
        assert _counter("memd_lexical_tail_hits_total") == tail1, "committed doc still served by the tail"
    finally:
        m.close()


def test_a_just_written_rare_term_outranks_committed_common_matches(tmp_path):
    """The tail used to be interleaved by rank: tantivy's best common-term
    match came first even when the only record holding the rare, decisive
    query term had just been written."""
    m = _mem(tmp_path / "d", lexical_commit_ms=600_000, lexical_commit_docs=10 ** 6)
    try:
        m.add_events([{"content": f"weekly meeting schedule calendar time slot {i}", "user_id": "u"}
                      for i in range(60)])
        m.add_events([{"content": f"grocery list item {i} bread milk", "user_id": "u"}
                      for i in range(120)])
        lex = _ready(m)
        gold = m.add("the vault password is zanzibar", user_id="u")[0]
        tail0 = _counter("memd_lexical_tail_hits_total")
        hits = m.ns.index.search_bm25("meeting schedule calendar time zanzibar",
                                      IndexFilter(scope=Scope(user="u")), limit=10)
        assert _counter("memd_lexical_tail_hits_total") > tail0, "not served from the tail"
        assert hits[0].record.id == gold, hits[0].record.content
        assert lex.stats()["pending"] >= 1
    finally:
        m.close()


def test_pending_rows_are_served_only_while_eligible_and_few(tmp_path):
    m = _mem(tmp_path / "d", lexical_commit_ms=600_000, lexical_commit_docs=10 ** 6)
    try:
        ids = m.add_events([{"content": f"harbor ferry timetable {i}", "user_id": "u"} for i in range(40)])
        _ready(m)
        f = IndexFilter(scope=Scope(user="u"))
        # a bulk delete leaves 30 pending rows, none of them eligible: the
        # query stays on tantivy (no per-row FTS5 lookups)
        m.delete_many(ids[:30])
        served = _counter("memd_lexical_searches_total")
        hits = m.ns.index.search_bm25("harbor ferry timetable", f, limit=50)
        assert {h.record.id for h in hits} == set(ids[30:])
        assert _counter("memd_lexical_searches_total") == served + 1
        # many rows that BECAME eligible since the last commit: FTS5 answers
        for rid in ids[30:]:
            m.ns.index.mark_quarantined(rid, True)
        m.flush()
        for rid in ids[30:]:
            m.ns.index.mark_quarantined(rid, False)
        before = _counter("memd_lexical_fallback_total", reason="pending")
        hits = m.ns.index.search_bm25("harbor ferry timetable", f, limit=50)
        assert {h.record.id for h in hits} == set(ids[30:])
        assert _counter("memd_lexical_fallback_total", reason="pending") == before + 1
    finally:
        m.close()


def test_delete_supersede_and_quarantine_are_visible_immediately(tmp_path):
    m = _mem(tmp_path / "d", lexical_commit_ms=600_000, lexical_commit_docs=10 ** 6)
    try:
        doomed = m.add("the zeppelin hangar keycode is 4471", user_id="u")[0]
        forgotten = m.add("my zeppelin pilot licence number is 99", user_id="u")[0]
        old = m.remember("the zeppelin is parked in hangar 7", entity_keys=["zeppelin.location"],
                         user_id="u")
        held = m.add("zeppelin maintenance log entry", user_id="u")[0]
        m.ns.index.mark_quarantined(held, True)
        lex = _ready(m)

        m.delete(doomed)
        new = m.remember("the zeppelin is parked in hangar 9", entity_keys=["zeppelin.location"],
                         user_id="u")
        m.ns.index.mark_quarantined(held, False)
        assert lex.stats()["pending"] >= 3
        ids = {i.id for i in m.search("zeppelin hangar keycode parked maintenance",
                                      user_id="u", budget_tokens=4000).items}
        assert doomed not in ids, "a deleted record came back through tantivy"
        assert old not in ids, "a superseded fact came back through tantivy"
        assert new in ids and held in ids, "a newly visible record was hidden"
        m.forget("pilot licence", user_id="u")
        m._bump_epoch()
        ids = {i.id for i in m.search("zeppelin pilot licence", user_id="u").items}
        assert forgotten not in ids

        m.flush()  # committed: the same answers from the index itself
        assert lex.stats()["pending"] == 0
        m._bump_epoch()
        ids = {i.id for i in m.search("zeppelin hangar keycode parked maintenance pilot licence",
                                      user_id="u", budget_tokens=4000).items}
        assert ids == {new, held}
    finally:
        m.close()


def test_hard_delete_of_the_newest_row_then_a_write_reusing_its_rowid(tmp_path):
    m = _mem(tmp_path / "d", lexical_commit_ms=600_000, lexical_commit_docs=10 ** 6)
    try:
        m.add_events([{"content": d, "user_id": "u"} for d in _corpus(30)])
        top = m.add("obsolete zeppelin memo", user_id="u")[0]
        _ready(m)
        m.delete(top, hard=True)
        # SQLite hands the freed max rowid to the next insert - below the
        # watermark, where only the lowered floor makes the tail see it
        fresh = m.add("the dirigible mooring fee is 40 dollars", user_id="u")[0]
        ids = [i.id for i in m.search("dirigible mooring fee", user_id="u").items]
        assert ids[:1] == [fresh]
        m.flush()
        m._bump_epoch()
        assert [i.id for i in m.search("dirigible mooring fee", user_id="u").items][:1] == [fresh]
        assert top not in {i.id for i in m.search("obsolete zeppelin memo", user_id="u").items}
    finally:
        m.close()


# ------------------------------------------------------------ isolation

SCOPES = [
    Scope(user="alice"),
    Scope(user="alice", session="a1"),
    Scope(user="bob"),
    Scope(user="bob", session="shared"),
    Scope(agent="agent-1"),
    Scope(org="acme", user="alice"),
    Scope(session="shared"),
    Scope(),
]


def _isolation_corpus(m: Memory) -> None:
    rows = []
    for i in range(300):  # bob drowns the index in matching rows
        rows.append({"content": f"quarterly roadmap secret {i}", "user_id": "bob",
                     "session_id": "shared" if i % 3 == 0 else f"b{i % 5}"})
    for i in range(4):
        rows.append({"content": f"alice quarterly roadmap draft {i}", "user_id": "alice",
                     "session_id": "a1" if i % 2 else "shared"})
    rows += [
        {"content": "org-wide quarterly roadmap memo", "org_id": "acme"},
        {"content": "agent quarterly roadmap scratchpad", "agent_id": "agent-1"},
        {"content": "session-only quarterly roadmap note", "session_id": "shared"},
        {"content": "unscoped quarterly roadmap reminder"},
        {"content": "hostile quarterly roadmap", "user_id": "alice\" OR user:\"bob"},
    ]
    m.add_events(rows)


@pytest.mark.parametrize("sabotage", [False, True])
def test_another_users_rows_never_come_back_through_tantivy(tmp_path, monkeypatch, sabotage):
    m = _mem(tmp_path / "d")
    try:
        _isolation_corpus(m)
        _ready(m)
        if sabotage:
            # defence in depth: with NO prefilter at all, the post-check
            # (and the FTS5 fallback) must still hold the boundary
            monkeypatch.setattr(TantivyLexical, "_filter_query", lambda self, f, now: [])
        idx = m.ns.index
        for scope in SCOPES:
            f = IndexFilter(scope=scope)
            expr = " OR ".join(f'"{w}"' for w in _fts_escape("quarterly roadmap").split())
            oracle = {h.record.id for h in idx._fts5_bm25(expr, f, 1000)}
            got_hits = idx.search_bm25("quarterly roadmap", f, limit=400)
            got = {h.record.id for h in got_hits}
            for h in got_hits:
                assert scope.contains(h.record.scope), (scope, h.record.scope)
            if len(oracle) < 400:
                assert got == oracle, f"{scope}: tantivy {len(got)} vs FTS5 {len(oracle)}"
        res = m.search("quarterly roadmap", user_id="alice", budget_tokens=20000)
        assert res.items and not any("secret" in i.content for i in res.items)
    finally:
        m.close()


# ------------------------------------------------------------ lifecycle

def test_clean_reopen_reuses_the_index(tmp_path):
    root = tmp_path / "d"
    m = _mem(root)
    m.add_events([{"content": d, "user_id": "u"} for d in _corpus(60)])
    _ready(m)
    m.close()
    m = _mem(root)
    try:
        lex = _lex(m)
        assert lex.ready(), "a cleanly closed index must be served at once"
        assert lex.rebuilds == 0
        assert m.search("kumquat mango", user_id="u").items
    finally:
        m.close()


def _tantivy_dir(root) -> str:
    hits = [os.path.join(dp, d) for dp, ds, _ in os.walk(str(root)) for d in ds if d.endswith(".tantivy")]
    assert len(hits) == 1, hits
    return hits[0]


@pytest.mark.parametrize("damage", ["corrupt", "missing", "foreign"])
def test_damaged_index_is_rebuilt_in_the_background(tmp_path, damage):
    root = tmp_path / "d"
    m = _mem(root)
    m.add_events([{"content": d, "user_id": "u"} for d in _corpus(80)])
    _ready(m)
    m.close()
    tdir = _tantivy_dir(root)
    if damage == "corrupt":
        with open(os.path.join(tdir, "meta.json"), "w") as f:
            f.write("{not json")
    elif damage == "missing":
        import shutil

        shutil.rmtree(tdir)
    else:
        st = json.load(open(os.path.join(tdir, tl.STATE_FILE)))
        st["uid"] = uuid.uuid4().hex  # an index built from another SQLite file
        json.dump(st, open(os.path.join(tdir, tl.STATE_FILE), "w"))
    before = _counter("memd_lexical_rebuilds_total", reason=damage)
    m = _mem(root)
    try:
        lex = _lex(m)
        assert lex.rebuilds == 1
        assert _counter("memd_lexical_rebuilds_total", reason=damage) == before + 1
        # until the rebuild catches up the lane is FTS5; afterwards tantivy
        assert m.search("kumquat mango orchard", user_id="u").items
        _ready(m)
        idx = m.ns.index
        f = IndexFilter(scope=Scope(user="u"))
        served = _counter("memd_lexical_searches_total")
        assert idx.search_bm25("kumquat mango orchard", f, limit=10)
        assert _counter("memd_lexical_searches_total") == served + 1
    finally:
        m.close()


CHILD = r"""
import os, sys, time
sys.path.insert(0, {src!r})
from memd.engine.memory import Memory
root = sys.argv[1]
m = Memory(root, encrypt=False, config={{"embedder": "hash", "reranker": "none",
           "lexical_backend": "tantivy", "rate_max_writes": 10**9}})
m.add_events([{{"content": f"crash corpus {{i}} kumquat orchard ledger", "user_id": "u"}}
              for i in range(200)])
m.flush()
m.add("written after the last commit: zeppelin", user_id="u")
print("ready", flush=True)
time.sleep(60)
"""


def test_sigkill_then_reopen_rebuilds(tmp_path):
    root = str(tmp_path / "d")
    child = subprocess.Popen([sys.executable, "-c", CHILD.format(src=SRC), root],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        line = child.stdout.readline()
        assert line.strip() == "ready", child.stderr.read()[-2000:]
        os.kill(child.pid, signal.SIGKILL)
        child.wait(timeout=15)
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)
    before = _counter("memd_lexical_rebuilds_total")
    m = _mem(root)
    try:
        lex = _lex(m)
        assert lex.rebuilds == 1, "an uncleanly closed index was trusted"
        assert _counter("memd_lexical_rebuilds_total") == before + 1
        _ready(m)
        f = IndexFilter(scope=Scope(user="u"))
        hits = m.ns.index.search_bm25("zeppelin", f, limit=10)
        assert [h.record.content for h in hits] == ["written after the last commit: zeppelin"]
        assert len(m.ns.index.search_bm25("kumquat orchard ledger", f, limit=500)) == 200
    finally:
        m.close()


def test_rebuild_index_resets_the_accelerator(tmp_path):
    m = _mem(tmp_path / "d")
    try:
        m.add_events([{"content": d, "user_id": "u"} for d in _corpus(60)])
        _ready(m)
        m.ns.rebuild_index()  # wipes SQLite: rowids start again from 1
        lex = _ready(m)
        assert lex.rebuilds == 1
        f = IndexFilter(scope=Scope(user="u"))
        expr = " OR ".join(f'"{w}"' for w in _fts_escape("kumquat mango").split())
        want = {h.record.id for h in m.ns.index._fts5_bm25(expr, f, 500)}
        got = {h.record.id for h in m.ns.index.search_bm25("kumquat mango", f, limit=500)}
        assert got == want
    finally:
        m.close()


def test_unsupported_filters_are_served_by_fts5(tmp_path):
    m = _mem(tmp_path / "d")
    try:
        rid = m.add("the kumquat orchard ledger", user_id="u")[0]
        _ready(m)
        before = _counter("memd_lexical_fallback_total", reason="unsupported_filter")
        res = m.search("kumquat orchard", user_id="u", as_of=int(time.time() * 1000) + 1000)
        assert [i.id for i in res.items] == [rid]
        assert _counter("memd_lexical_fallback_total", reason="unsupported_filter") > before
    finally:
        m.close()


def test_destroy_namespace_removes_the_tantivy_index(tmp_path):
    root = tmp_path / "d"
    m = _mem(root)
    try:
        m.add("shred me: kumquat", user_id="u", namespace="tenant")
        m.engine.namespace("tenant").index.lexical.drain(10)
        tdirs = [os.path.join(dp, d) for dp, ds, _ in os.walk(str(root)) for d in ds
                 if d == "tenant.tantivy"]
        assert tdirs
        m.destroy_namespace("tenant")
        assert not os.path.exists(tdirs[0]), "the derived text index survived a crypto-shred"
    finally:
        m.close()


def test_write_path_is_not_blocked_by_the_indexer(tmp_path):
    m = _mem(tmp_path / "d")
    try:
        _ready(m)
        lex = _lex(m)
        with lex._step_lock:  # the indexer is mid-batch
            t0 = time.monotonic()
            for i in range(20):
                m.add(f"ack while indexing {i}", user_id="u")
            assert time.monotonic() - t0 < 5
        m.flush()
        assert len(m.ns.index.search_bm25("ack indexing", IndexFilter(scope=Scope(user="u")),
                                          limit=50)) == 20
    finally:
        m.close()


# ------------------------------------------------------------ v0.2.0 review regressions

def _rowid(m: Memory, rid: str) -> int:
    with m.ns.index._read() as c:
        return int(c.execute("SELECT rowid FROM records WHERE id=?", (rid,)).fetchone()[0])


def _served_by_tantivy(m: Memory, query: str, f: IndexFilter, limit: int = 10) -> list[str]:
    """The lane's answer, asserting tantivy (not the FTS5 fallback) gave it."""
    before = _counter("memd_lexical_searches_total")
    got = [h.record.content for h in m.ns.index.search_bm25(query, f, limit=limit)]
    assert _counter("memd_lexical_searches_total") == before + 1, "served by the FTS5 fallback"
    return got


@pytest.mark.parametrize("mode", ["single", "batch"])
def test_hard_deleting_the_two_newest_rows_never_hides_the_next_write(tmp_path, mode):
    """Only a delete of THE max rowid lowered the scan floor, and only to
    rowid-1: deleting the two newest rows (lower one first) left the floor
    above the rowid SQLite handed out next, so that record was missing from
    the bm25 lane for good - after flush and after reopen too."""
    root = tmp_path / "d"
    m = _mem(root, lexical_commit_ms=50)
    f = IndexFilter(scope=Scope(user="u"))
    try:
        m.add_events([{"content": f"filler note {i} about groceries", "user_id": "u"} for i in range(20)])
        a = m.add("second newest walrusine", user_id="u")[0]
        b = m.add("newest walrusine", user_id="u")[0]
        top = _rowid(m, b)
        _ready(m)
        if mode == "single":
            m.delete(a, hard=True)
            m.delete(b, hard=True)
        else:
            m.delete_many([a, b], hard=True)
        c = m.add("fresh narwhalic record", user_id="u")[0]
        assert _rowid(m, c) > top, "a rowid was handed out twice"
        assert _served_by_tantivy(m, "narwhalic", f) == ["fresh narwhalic record"]  # tail
        _ready(m)
        assert _served_by_tantivy(m, "narwhalic", f) == ["fresh narwhalic record"]  # committed
        assert not _served_by_tantivy(m, "walrusine", f)
    finally:
        m.close()
    m = _mem(root, lexical_commit_ms=50)
    try:
        assert _lex(m).ready(), "a cleanly closed index must be reused"
        assert _served_by_tantivy(m, "narwhalic", f) == ["fresh narwhalic record"]
        d = m.add("after reopen: dugongish", user_id="u")[0]
        assert _rowid(m, d) > top
        _ready(m)
        assert _served_by_tantivy(m, "dugongish", f) == ["after reopen: dugongish"]
    finally:
        m.close()


def test_an_older_index_version_is_rebuilt(tmp_path):
    """v0.2.0 indexes may be missing rows a reused rowid hid, and were built
    with the 40-byte token cut: an index of an older STATE_VERSION is rebuilt."""
    root = tmp_path / "d"
    m = _mem(root)
    m.add("kumquat orchard", user_id="u")
    _ready(m)
    m.close()
    state_path = os.path.join(_tantivy_dir(root), tl.STATE_FILE)
    st = json.load(open(state_path))
    st["version"] = tl.STATE_VERSION - 1
    json.dump(st, open(state_path, "w"))
    m = _mem(root)
    try:
        assert _lex(m).rebuilds == 1
        _ready(m)
        assert _served_by_tantivy(m, "kumquat", IndexFilter(scope=Scope(user="u"))) == ["kumquat orchard"]
    finally:
        m.close()


SHA1 = "3f786850e387550fdab836ed7e6dc881de23001b"
SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
CJK = "東京タワーに行きました昨日の夜とても楽しかったです"


def test_tokens_of_40_bytes_or_more_are_indexed(tmp_path):
    """`en_stem` drops every token of 40+ bytes: a commit hash, a digest or an
    unspaced CJK sentence could not be found through tantivy at all."""
    m = _mem(tmp_path / "d")
    try:
        m.add_events([{"content": f"filler {i} text", "user_id": "u"} for i in range(50)])
        m.add(f"deployed commit {SHA1} to prod", user_id="u")
        m.add(f"artifact digest {SHA256}", user_id="u")
        m.add(CJK, user_id="u")
        _ready(m)
        f = IndexFilter(scope=Scope(user="u"))
        for q in (SHA1, SHA256, CJK):
            assert len(q.encode()) >= 40
            got = _served_by_tantivy(m, q, f)
            assert len(got) == 1 and q in got[0], (q, got)
            m._bump_epoch()
            assert any(q in i.content for i in m.search(q, user_id="u").items)
    finally:
        m.close()


def test_a_query_term_too_long_for_tantivy_goes_to_fts5(tmp_path):
    # tantivy drops a token over 65530 bytes from the doc itself
    m = _mem(tmp_path / "d")
    try:
        blob = "q" * 70_000
        m.add(f"payload {blob}", user_id="u")
        _ready(m)
        before = _counter("memd_lexical_fallback_total", reason="long_term")
        hits = m.ns.index.search_bm25(blob, IndexFilter(scope=Scope(user="u")), limit=5)
        assert [h.record.content for h in hits] == [f"payload {blob}"]
        assert _counter("memd_lexical_fallback_total", reason="long_term") == before + 1
    finally:
        m.close()


def test_an_overwritten_id_is_reindexed(tmp_path):
    """An upsert of an existing id (`memd import --native` keeps ids) never
    reached the indexer: the OLD text kept matching and the new text did not,
    even after flush and reopen."""
    import dataclasses

    root = tmp_path / "d"
    m = _mem(root)
    f = IndexFilter(scope=Scope(user="u"))
    try:
        m.add_events([{"content": f"filler {i}", "user_id": "u"} for i in range(20)])
        rid = m.add("original wombatrix sensitive", user_id="u")[0]
        _ready(m)
        rec = m.ns.index.get_by_id(rid)
        new = dataclasses.replace(rec, content="redacted placeholder kiwiberry")
        new.namespace = "default"
        m._ns_for("default").append([new])  # what cli._import_native does
        # before the batcher runs: the pending row is served from FTS5
        assert not _served_by_tantivy(m, "wombatrix", f)
        assert _served_by_tantivy(m, "kiwiberry", f) == ["redacted placeholder kiwiberry"]
        _ready(m)
        assert not _served_by_tantivy(m, "wombatrix", f)
        assert _served_by_tantivy(m, "kiwiberry", f) == ["redacted placeholder kiwiberry"]
    finally:
        m.close()
    m = _mem(root)
    try:
        assert _lex(m).ready()
        assert not _served_by_tantivy(m, "wombatrix", f)
        assert _served_by_tantivy(m, "kiwiberry", f) == ["redacted placeholder kiwiberry"]
        assert not [i for i in m.search("wombatrix", user_id="u").items]
    finally:
        m.close()


def test_a_rescoped_id_moves_to_its_new_owner(tmp_path):
    import dataclasses

    m = _mem(tmp_path / "d")
    try:
        m.add_events([{"content": f"filler {i}", "user_id": "alice"} for i in range(20)])
        rid = m.add("transferred ocelotine ledger", user_id="alice")[0]
        _ready(m)
        rec = m.ns.index.get_by_id(rid)
        moved = dataclasses.replace(rec, scope=Scope(user="bob"))
        moved.namespace = "default"
        m._ns_for("default").append([moved])
        m._bump_epoch()
        for committed in (False, True):
            if committed:
                _ready(m)
                m._bump_epoch()
            assert not _served_by_tantivy(m, "ocelotine", IndexFilter(scope=Scope(user="alice")))
            assert _served_by_tantivy(m, "ocelotine", IndexFilter(scope=Scope(user="bob"))) == \
                ["transferred ocelotine ledger"]
            assert not m.search("ocelotine", user_id="alice").items
            assert [i.id for i in m.search("ocelotine", user_id="bob").items] == [rid]
    finally:
        m.close()


def _tied_corpus():
    # three texts, 50 exact copies each: bm25 ties by the dozen; distinct
    # t_event per record so only the lane's choice among ties can differ.
    # (A tie group larger than the once-widened window is answered by FTS5
    # by design - test_a_short_window_widens_once_then_fts5_answers - so
    # larger groups would not exercise tantivy's order at all.)
    return [{"content": f"project {['alpha', 'beta', 'gamma'][i % 3]} shipped item", "user_id": "u",
             "session_id": f"s{i % 7}", "t_event": T0 + i * 60_000} for i in range(150)]


def _ingest_ordered(root, commits: int):
    """Ingest the same data, committed to tantivy in `commits` batches (one
    segment each: tantivy merges only at 8+), and return what search shows."""
    m = _mem(root, lexical_commit_ms=600_000, lexical_commit_docs=10 ** 6)
    try:
        ev = _tied_corpus()
        step = len(ev) // commits
        for i in range(0, len(ev), step):
            m.add_events(ev[i:i + step])
            m.flush()
            assert _lex(m).drain(30)
        f = IndexFilter(scope=Scope(user="u"))
        out = []
        for q in ("project alpha shipped", "beta item", "what shipped in gamma"):
            out.append(_served_by_tantivy(m, q, f, limit=10))
            lane = [(h.record.content, h.record.time.t_event)
                    for h in m.ns.index.search_bm25(q, f, limit=10)]
            m._bump_epoch()
            res = [(it.content, it.t_event) for it in m.search(q, user_id="u").items[:5]]
            out.append((lane, res))
        return out
    finally:
        m.close()


def test_tied_scores_order_the_same_across_segment_layouts(tmp_path):
    """Among equal bm25 scores tantivy returned docs in segment order, and
    segment order follows commit timing (and hashing): re-ingesting the same
    data gave a different top-5 (FTS5 did not)."""
    runs = [_ingest_ordered(tmp_path / f"r{k}", commits) for k, commits in enumerate((1, 6, 6, 6))]
    for r in runs[1:]:
        assert r == runs[0]
    # ties go to the newest (fusion's order: score, then -t_event)
    lane, _ = runs[0][1]
    assert [t for _, t in lane] == sorted((t for _, t in lane), reverse=True)


class _FailingIndex:
    """Wraps the tantivy Index: parse_query raises `exc` while armed."""

    def __init__(self, inner, exc):
        self.inner, self.exc, self.armed = inner, exc, True

    def parse_query(self, *a, **k):
        if self.armed:
            raise self.exc
        return self.inner.parse_query(*a, **k)

    def __getattr__(self, name):
        return getattr(self.inner, name)


def test_a_query_error_is_not_a_rebuild(tmp_path):
    """Any search-time error used to wipe and rebuild the whole index, and
    five of them disabled it until restart."""
    m = _mem(tmp_path / "d")
    try:
        m.add_events([{"content": d, "user_id": "u"} for d in _corpus(80)])
        lex = _ready(m)
        f = IndexFilter(scope=Scope(user="u"))
        want = [h.record.id for h in m.ns.index.search_bm25("kumquat mango", f, limit=10)]
        real = lex._index
        lex._index = _FailingIndex(real, ValueError("Syntax Error: injected"))
        before = _counter("memd_lexical_fallback_total", reason="error")
        for _ in range(8):
            got = [h.record.id for h in m.ns.index.search_bm25("kumquat mango", f, limit=10)]
            assert set(got) == set(want)  # FTS5 served it
        assert _counter("memd_lexical_fallback_total", reason="error") == before + 8
        lex._index = real
        st = lex.stats()
        assert st["ready"] and st["rebuilds"] == 0 and st["failures"] == 0 and not st["disabled"]
        assert _served_by_tantivy(m, "kumquat mango", f)
    finally:
        m.close()


def test_damage_rebuilds_with_backoff_and_is_counted(tmp_path, monkeypatch):
    m = _mem(tmp_path / "d")
    try:
        m.add_events([{"content": d, "user_id": "u"} for d in _corpus(80)])
        lex = _ready(m)
        f = IndexFilter(scope=Scope(user="u"))
        damage = ValueError("Failed to open file for read: 'FileDoesNotExist(\"x.store\")'")
        rebuilds0 = _counter("memd_lexical_rebuilds_total", reason="damaged")
        waits = []
        for k in range(1, 8):  # more than the old limit of 5
            lex._index = _FailingIndex(lex._index, damage)
            with lex._lock:
                lex._ready = True  # (as if the rebuild had finished)
            assert m.ns.index.search_bm25("kumquat mango", f, limit=10), "FTS5 must serve"
            st = lex.stats()
            assert not st["ready"] and st["disabled"] and st["failures"] == k
            waits.append(st["retry_in_s"])
            assert lex.drain(1) is False, "drain must respect the backoff"
            lex._index = lex._index.inner
        assert lex.rebuilds == 1, "one rebuild in flight is one rebuild"
        assert _counter("memd_lexical_rebuilds_total", reason="damaged") == rebuilds0 + 1
        assert waits[1] > waits[0] and waits[3] > waits[2], f"not exponential: {waits}"
        assert waits[-1] <= tl.BACKOFF_MAX_S
        with lex._lock:
            lex._retry_at = 0.0  # the backoff has passed
        assert lex.drain(30), "never disabled for good: the rebuild runs"
        st = lex.stats()
        # recovered; the failure history outlives the recovery (it decays
        # after FAILURE_DECAY_S without a failure, not on the first success)
        assert st["ready"] and not st["disabled"] and st["failures"] == 7
        assert _served_by_tantivy(m, "kumquat mango", f)
    finally:
        m.close()


def test_a_rebuild_does_not_reset_the_failure_history(tmp_path, monkeypatch):
    """A successful rebuild reset the failure count, so damage that only
    shows at search time rebuilt every ~2s forever (6 rebuilds in 12s): the
    backoff never grew."""
    m = _mem(tmp_path / "d")
    try:
        m.add_events([{"content": d, "user_id": "u"} for d in _corpus(80)])
        lex = _ready(m)
        f = IndexFilter(scope=Scope(user="u"))
        damage = ValueError("IoError: failed to open file for read: 'abc.idx'")
        waits = []
        for k in (1, 2, 3):
            lex._index = _FailingIndex(lex._index, damage)
            assert m.ns.index.search_bm25("kumquat mango", f, limit=10), "FTS5 must serve"
            st = lex.stats()
            assert st["failures"] == k and st["failures_total"] == k and st["disabled"]
            waits.append(st["retry_in_s"])
            with lex._lock:
                lex._retry_at = 0.0  # let the rebuild run now...
            assert lex.drain(30), "the rebuild succeeds"  # ...into a fresh, healthy index
            st = lex.stats()
            assert st["ready"] and not st["disabled"] and st["failures"] == k
        assert waits[0] < waits[1] < waits[2], f"the backoff must grow across rebuilds: {waits}"
        monkeypatch.setattr(tl, "FAILURE_DECAY_S", 0.0)  # a quiet period passes
        st = lex.stats()
        assert st["failures"] == 0 and st["failures_total"] == 3
    finally:
        m.close()


def test_a_short_window_widens_once_then_fts5_answers(tmp_path):
    """On templated data one tie group fills any window: the lane widened
    60 -> 240 -> 960 -> 3840 -> 4096 (the last step redundant) and then fell
    back to FTS5 anyway, 47-54ms against FTS5's 8-16ms."""
    m = _mem(tmp_path / "d")
    try:
        m.add_events([{"content": "common widget report", "user_id": "u", "t_event": T0 + i}
                      for i in range(400)])
        lex = _ready(m)
        fetches = []
        real = lex._top

        def top(searcher, terms, f, now, fetch):
            fetches.append(fetch)
            return real(searcher, terms, f, now, fetch)
        lex._top = top
        before = _counter("memd_lexical_fallback_total", reason="short")
        hits = m.ns.index.search_bm25("widget", IndexFilter(scope=Scope(user="u")), limit=10)
        assert fetches == [20, 80], f"one widening, then FTS5: {fetches}"
        assert _counter("memd_lexical_fallback_total", reason="short") == before + 1
        # FTS5's answer: the newest ten (ties go to the newest)
        assert [h.record.time.t_event for h in hits] == [T0 + i for i in range(399, 389, -1)]
    finally:
        m.close()


def test_tied_rows_order_the_same_on_both_backends(tmp_path):
    """FTS5 returned equal scores oldest-first (rowid order) and tantivy
    newest-first, so the backend - or whether a row was committed to tantivy
    yet - changed the order of tied rows. Both now use fusion's key: score,
    then -t_event, then content hash, then id - also when the LIMIT cuts a
    group of equal (score, t_event)."""
    from memd.query.fusion import content_sha

    words = ["fig", "kiwi", "lime", "pear", "plum", "date", "yam", "okra"]
    ev = [{"content": f"zebu {w}{i}", "user_id": "u", "t_event": T0 + i * 1000} for i, w in enumerate(words)]
    ev += [{"content": f"zebu {w}", "user_id": "u", "t_event": T0 + 4500} for w in words[:6]]  # one t_event
    random.Random(3).shuffle(ev)  # insertion (rowid) order is neither of the above
    want = [e["content"] for e in sorted(ev, key=lambda e: (-e["t_event"], content_sha(e["content"])))]
    f = IndexFilter(scope=Scope(user="u"))
    for backend in ("fts5", "tantivy"):
        m = _mem(tmp_path / backend, lexical_backend=backend,
                 lexical_commit_ms=600_000, lexical_commit_docs=10 ** 6)
        try:
            m.add_events(ev)
            phases = ["as written"] + (["committed"] if backend == "tantivy" else [])
            for phase in phases:
                if phase == "committed":
                    assert _ready(m)
                for limit in (len(ev), 6, 4, 3):  # 6 and 4 cut inside the equal-t_event group
                    got = [h.record.content for h in m.ns.index.search_bm25("zebu", f, limit=limit)]
                    assert got == want[:limit], f"{backend} {phase} limit={limit}: {got}"
        finally:
            m.close()
