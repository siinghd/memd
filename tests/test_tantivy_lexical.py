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


# ------------------------------------------------------------ v0.2.0 verifier regressions

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
