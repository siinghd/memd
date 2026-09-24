"""Retrieval v2: rank by FTS5 bm25, gate the time lane, deterministic order.

Measured on real LongMemEval data (not the synthetic suite):
 1. The bm25 lane was AND-first; on natural-language questions the AND tier
    never fired and an UNORDERED OR window was re-ranked by distinct
    query-term coverage - no IDF, no TF. A record sharing several common
    query words beat the one holding the rare, decisive term.
 2. The time lane (newest rows, regardless of the query) ran on EVERY query.
 3. Ties fell to ULID ids / ingestion time, so re-ingesting identical data
    changed the top-10 for ~23% of queries; packing measured recency against
    the WALL CLOCK and quantized, so the ranking depended on the run date.
 4. load_real() did not read the real LongMemEval format and silently
    produced zero events.
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from memd.core.schema import Kind, MemoryRecord, Scope, now_ms  # noqa: E402
from memd.engine.memory import Memory  # noqa: E402
from memd.index.sqlite_index import IndexFilter  # noqa: E402
from memd.query.fusion import FusedItem  # noqa: E402
from memd.query.packing import pack_context  # noqa: E402
from memd.query.planner import QueryPlan, plan_query  # noqa: E402

DAY = 86_400_000
T_2023 = 1_684_540_800_000  # 2023-05-20T00:00Z


# ------------------------------------------------------------------ bm25 IDF

def test_rare_term_outranks_many_common_term_matches(tmp_path):
    m = Memory(str(tmp_path / "d"), encrypt=False)
    try:
        # the common query words sit in a third of the corpus
        for i in range(60):
            m.add(f"weekly meeting schedule calendar time slot {i}", user_id="u")
        for i in range(120):
            m.add(f"grocery list item {i} bread milk", user_id="u")
        gold = m.add("the vault password is zanzibar", user_id="u")[0]
        hits = m.ns.index.search_bm25(
            "meeting schedule calendar time zanzibar",
            IndexFilter(scope=Scope(user="u")), limit=10)
        assert hits, "expected bm25 hits"
        # distinct-term coverage ranked all 60 four-term matches above it
        assert hits[0].record.id == gold, (
            "the only record holding the rare term was outranked by records "
            f"matching common terms: top={hits[0].record.content!r}")
    finally:
        m.close()


def test_bm25_is_one_ranked_query_and_counts_it(tmp_path):
    from memd.metrics import METRICS

    def count() -> float:
        return sum(x["value"] for x in
                   METRICS.snapshot()["counters"].get("memd_bm25_queries_total", []))

    m = Memory(str(tmp_path / "d"), encrypt=False)
    try:
        m.add("mango allergy critical", user_id="u")
        before = count()
        hits = m.ns.index.search_bm25("mango allergy", IndexFilter(scope=Scope(user="u")),
                                      limit=5)
        assert [h.lane for h in hits] == ["bm25"]
        assert count() == before + 1
        text = METRICS.render_prometheus()
        for gone in ("memd_bm25_and_hits_total", "memd_bm25_or_bounded_total",
                     "memd_bm25_window_saturated_total"):
            assert gone not in text, f"{gone} still emitted"
    finally:
        m.close()


def test_generator_fitted_words_are_not_stopwords(tmp_path):
    """"session", "notes", "later" ... are real query words on real data."""
    m = Memory(str(tmp_path / "d"), encrypt=False)
    try:
        gold = m.add("my therapy session notes are in the blue folder", user_id="u")[0]
        m.add("the blue whale is large", user_id="u")
        hits = m.ns.index.search_bm25("session notes", IndexFilter(scope=Scope(user="u")),
                                      limit=5)
        assert [h.record.id for h in hits] == [gold]
    finally:
        m.close()


# ------------------------------------------------------------------ time lane

def test_planner_gates_time_lane_on_recency_intent():
    assert plan_query("what did I do recently").use_time_lane
    assert plan_query("what's the latest on the migration?").use_time_lane
    assert not plan_query("what editor do I use?").use_time_lane
    # temporal CLASS (weights) without recency intent: no time lane
    p = plan_query("when is my review meeting?")
    assert p.qclass == "temporal" and not p.use_time_lane
    # an explicit time bound also invokes it
    assert QueryPlan(query="editor", qclass="factual", weights={}, candidate_k=40,
                     t_event_min=0).use_time_lane


def test_time_lane_invoked_only_for_recency_queries(tmp_path):
    m = Memory(str(tmp_path / "d"), encrypt=False)
    try:
        m.add("I use Neovim as my editor.", user_id="u")
        m.add("I went hiking on Saturday.", user_id="u")
        m.flush()
        idx = m.ns.index
        real = idx.search_time_lane
        calls = []
        idx.search_time_lane = lambda f, limit=50: (calls.append(1), real(f, limit=limit))[1]
        try:
            res = m.search("what editor do I use?", user_id="u")
            assert not calls, "time lane ran for a query with no recency intent"
            assert all("time" not in i.lanes for i in res.items)
            res = m.search("what did I do recently", user_id="u")
            assert calls, "time lane skipped for a recency query"
            assert any("time" in i.lanes for i in res.items)
        finally:
            idx.search_time_lane = real
    finally:
        m.close()


# ---------------------------------------------------------------- determinism

def _events():
    topics = ["alpha", "beta", "gamma", "delta"]
    out = []
    for i in range(80):
        t = topics[i % len(topics)]
        # one shared t_event per topic: ties must be broken by content
        out.append((f"project {t} update: item {i} shipped", T_2023 + (i % 4) * DAY))
    return out


def _ordered(path, queries):
    # bulk-load limits off, as for any historical import (harness does the same)
    m = Memory(path, encrypt=False,
               config={"rate_max_writes": 10**9, "dup_max_repeats": 10**9})
    try:
        # ONE batch: the records share an ingestion millisecond, so their ULIDs
        # order randomly - exactly how a re-import reshuffled the old tie-breaks
        m.add_events([{"content": c, "user_id": "u", "session_id": "s1", "t_event": t}
                      for c, t in _events()])
        m.flush()
        return [[(i.content, i.score) for i in m.search(q, user_id="u").items]
                for q in queries]
    finally:
        m.close()


def test_reingesting_identical_data_gives_identical_order(tmp_path):
    queries = ["project alpha update", "what shipped", "item shipped update",
               "tell me about gamma", "delta project item"]
    a = _ordered(str(tmp_path / "a"), queries)
    time.sleep(0.01)  # different ingestion instants, different ULIDs
    b = _ordered(str(tmp_path / "b"), queries)
    for q, ra, rb in zip(queries, a, b):
        assert ra, f"no results for {q!r}"
        assert ra == rb, f"order changed on re-ingest for {q!r}"


def _rec(content, t_event, t_ingested, rid):
    r = MemoryRecord.create(namespace="default", kind=Kind.RAW_EVENT, content=content,
                            scope=Scope(user="u"), t_event=t_event, record_id=rid)
    r.time.t_ingested = t_ingested
    return r


def test_pack_order_ignores_the_wall_clock(monkeypatch):
    # year-old data, ingestion order REVERSED vs relevance. Under the old
    # wall-clock decay + int(s*10000) tiers these were distinct tiers today
    # and collapsed together once "now" moved five years on - order then fell
    # to t_ingested, i.e. flipped
    base = now_ms() - 365 * DAY
    fused = []
    for k in range(12):
        rec = _rec(f"memory number {k} about the trip", base + k * 3_600_000,
                   t_ingested=base + (100 - k), rid=f"01ID{k:04d}")
        fused.append(FusedItem(record=rec, score=0.030 - k * 0.0009,
                               lanes=["bm25"], ranks={"bm25": k + 1}))

    def order():
        return [i.content for i in pack_context(list(fused), budget_tokens=10_000).items]

    now_order = order()
    real_time = time.time
    monkeypatch.setattr(time, "time", lambda: real_time() + 5 * 365 * 86_400)
    later_order = order()
    assert now_order == later_order, "pack order depends on the wall clock"


def test_pack_uses_as_of_as_recency_reference():
    old = _rec("old but relevant", T_2023, T_2023, "01A")
    new = _rec("new and weaker", T_2023 + 700 * DAY, T_2023, "01B")
    fused = [FusedItem(record=old, score=0.030, lanes=["bm25"], ranks={"bm25": 1}),
             FusedItem(record=new, score=0.025, lanes=["bm25"], ranks={"bm25": 2})]
    # data-relative default: "now" is the newest candidate, so the two-year-old
    # record decays and the newer one leads
    assert [i.content for i in pack_context(fused).items][0] == "new and weaker"
    # as of the old record's date, the newer one is not yet in the past
    assert [i.content for i in pack_context(fused, now=T_2023).items][0] == "old but relevant"


# ------------------------------------------------------------ real LongMemEval

def test_load_real_reads_the_real_longmemeval_format(tmp_path):
    from memd.harness.suites import longmemeval_synthetic as L

    rows = [
        {
            "question_id": "q1", "question_type": "single-session-user",
            "question": "Where did I go hiking?", "question_date": "2023/06/01 (Thu) 10:00",
            "answer": "Mount Tam", "answer_session_ids": ["answer_1"],
            "haystack_dates": ["2023/05/20 (Sat) 02:21", "2023/05/21 (Sun) 14:05"],
            "haystack_session_ids": ["noise_1", "answer_1"],
            "haystack_sessions": [
                [{"role": "user", "content": "Recommend a pasta recipe."},
                 {"role": "assistant", "content": "Try cacio e pepe."}],
                [{"role": "user", "content": "I hiked Mount Tam today.", "has_answer": True}],
            ],
        },
        {
            "question_id": "q2_abs", "question_type": "single-session-user",
            "question": "Where did I go skiing?", "question_date": "2023/06/01 (Thu) 10:00",
            "answer": "not mentioned", "answer_session_ids": [],
            "haystack_dates": ["2023/05/20 (Sat) 02:21"],
            "haystack_session_ids": ["noise_1"],
            "haystack_sessions": [[{"role": "user", "content": "hello"}]],
        },
        {
            "question_id": "q3", "question_type": "temporal-reasoning",
            "question": "How many days ago?", "question_date": "2023/06/01 (Thu) 10:00",
            "answer": 3, "answer_session_ids": ["noise_1"],
            "haystack_dates": ["2023/05/20 (Sat) 02:21"],
            "haystack_session_ids": ["noise_1"],
            "haystack_sessions": [[{"role": "user", "content": "It is my birthday."}]],
        },
    ]
    p = tmp_path / "longmemeval_s.json"
    p.write_text(json.dumps(rows))
    events, cases = L.load_real(str(p))

    assert [c["id"] for c in cases] == ["q1", "q3"], "_abs questions must be skipped"
    c1 = cases[0]
    assert c1["qclass"] == "single-session-user" and c1["user_id"] == "q1"
    assert c1["query"] == "Where did I go hiking?" and c1["expected"] == "Mount Tam"
    assert cases[1]["expected"] == "3" and cases[1]["qclass"] == "temporal-reasoning"

    q1 = [e for e in events if e["user_id"] == "q1"]
    assert [e["content"] for e in q1] == [
        "Recommend a pasta recipe.", "Try cacio e pepe.", "I hiked Mount Tam today."]
    assert [e["role"] for e in q1] == ["user", "assistant", "user"]
    assert [e["session_id"] for e in q1] == ["q1:noise_1", "q1:noise_1", "q1:answer_1"]
    assert q1[0]["t_event"] == 1_684_549_260_000  # 2023-05-20T02:21Z
    assert q1[2]["t_event"] == 1_684_677_900_000  # 2023-05-21T14:05Z
    assert not [e for e in events if e["user_id"] == "q2_abs"]
    assert len(events) == 4

    # the events feed the engine end to end
    m = Memory(str(tmp_path / "d"), encrypt=False)
    try:
        for e in q1:
            m.add(e["content"], user_id=e["user_id"], session_id=e["session_id"],
                  role=e["role"], t_event=e["t_event"])
        res = m.search(c1["query"], user_id="q1")
        assert "Mount Tam" in res.packed_context
    finally:
        m.close()
