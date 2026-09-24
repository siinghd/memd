"""Patch 3 retrieval pipeline: hash-vector fusion off, reranker plug-in,
gated evidence packing, shared model cache.

- With the HashEmbedder, fusing its "vector" lane into RRF cost ~0.07
  ndcg@5 on LongMemEval once bm25 was right: it is no longer fused (hash
  vectors are still computed; forget() uses them).
- A reranker re-orders the lexical top-30. It must never take search down:
  failures, timeouts and malformed answers keep the unreranked order and
  are counted by reason.
- Jev (TypeSafe System One) is the default only when TYPESAFE_API_KEY is set
  and the SDK is importable. These tests never touch the network: the SDK is
  replaced by a fake module.
- Packing stays ranked with every reranker by default; gated evidence
  packing is an explicit, experimental opt-in (over a top-30 shortlist it
  dropped second evidence sessions: recall_all@5 0.803 vs 0.928 ranked).
"""
import logging
import sys
import threading
import time
import types
import uuid

import pytest

import memd.pipeline.embedder as embedder_mod
import memd.query.rerank as rerank_mod
from memd.core.schema import MemoryRecord, Scope, Source
from memd.engine.memory import Memory
from memd.metrics import METRICS
from memd.pipeline.embedder import FastEmbedEmbedder, HashEmbedder
from memd.query.fusion import FusedItem
from memd.query.packing import gate_candidates, pack_gated
from memd.query.rerank import (
    JEV_INSTRUCTIONS,
    JevReranker,
    LocalCrossEncoderReranker,
    RerankStage,
    candidate_from_record,
    resolve_reranker,
)

T0 = 1_684_540_800_000  # 2023-05-20T00:00Z
DAY = 86_400_000


def _counter(name: str, **labels) -> float:
    return sum(x["value"] for x in METRICS.snapshot()["counters"].get(name, [])
               if all(x["labels"].get(k) == v for k, v in labels.items()))


@pytest.fixture(autouse=True)
def _no_ambient_reranker(monkeypatch):
    # a developer's TYPESAFE_API_KEY must never make these tests call out
    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    monkeypatch.delenv("MEMD_RERANKER", raising=False)
    monkeypatch.delenv("MEMD_PACK_MODE", raising=False)


def _mem(tmp_path, **cfg) -> Memory:
    base = {"embedder": "hash", "reranker": "none", "rate_max_writes": 10 ** 9,
            "dup_max_repeats": 10 ** 9}
    base.update(cfg)
    return Memory(str(tmp_path / f"d{uuid.uuid4().hex[:6]}"), encrypt=False, config=base)


def _seed(m: Memory, n: int = 8) -> list[str]:
    # distinct bm25 strength: record i repeats the query term i+1 times
    ids = []
    for i in range(n):
        ids += m.add(f"note {i} " + "kumquat " * (i + 1) + f"filler{i}", user_id="u",
                     session_id="s1", t_event=T0)
    m.flush()
    return ids


class _Fixed:
    """A reranker whose scores are given by a function of the candidates."""
    name = "fake"
    model = "fake-1"
    calibrated = False
    timeout_s = 1.0

    def __init__(self, fn):
        self.fn = fn
        self.calls = 0
        self.seen: list = []

    def scores(self, query, cands):
        self.calls += 1
        self.seen.append(cands)
        return self.fn(cands)


def _set_reranker(m: Memory, r, timeout_s=None) -> None:
    m.rerank = RerankStage(r, timeout_s=timeout_s)


# ------------------------------------------------------------ fuse_vector

def test_hash_vector_lane_is_not_fused_by_default(tmp_path):
    m = _mem(tmp_path)
    try:
        assert m.fuse_vector is False
        rid = m.add("the kumquat harvest is in june", user_id="u")[0]
        m.flush()
        res = m.search("kumquat harvest", user_id="u")
        assert [i.id for i in res.items] == [rid]
        assert all("vector" not in i.lanes for i in res.items)
        # the hash vectors are still computed (forget/dedupe use them)
        assert m.ns.index.stats()["vectors"] == 1
        assert rid in m.find_ids("kumquat harvest", user_id="u")
        assert m.stats()["fuse_vector"] is False
    finally:
        m.close()


def test_fuse_vector_can_be_forced(tmp_path):
    m = _mem(tmp_path, fuse_vector=True)
    try:
        assert m.fuse_vector is True
        rid = m.add("the kumquat harvest is in june", user_id="u")[0]
        m.flush()
        res = m.search("kumquat harvest", user_id="u")
        assert any("vector" in i.lanes for i in res.items if i.id == rid)
    finally:
        m.close()


def test_fuse_vector_auto_follows_the_embedder(monkeypatch):
    from memd.engine.memory import resolve_fuse_vector

    assert resolve_fuse_vector({}, HashEmbedder()) is False
    monkeypatch.setattr(embedder_mod, "fastembed_available", lambda: True)
    assert resolve_fuse_vector({}, FastEmbedEmbedder(f"test/{uuid.uuid4().hex}")) is True
    assert resolve_fuse_vector({"fuse_vector": "false"}, FastEmbedEmbedder("x")) is False
    assert resolve_fuse_vector({"fuse_vector": True}, HashEmbedder()) is True
    with pytest.raises(ValueError):
        resolve_fuse_vector({"fuse_vector": "sometimes"}, HashEmbedder())


# ------------------------------------------------------------ order applied

def test_fake_reranker_order_is_applied(tmp_path):
    m = _mem(tmp_path)
    try:
        _seed(m)
        base = [i.id for i in m.search("kumquat", user_id="u", budget_tokens=4000).items]
        assert len(base) == 8
        fake = _Fixed(lambda cands: [float(i) for i in range(len(cands))])  # reverse
        _set_reranker(m, fake)
        m._bump_epoch()
        res = m.search("kumquat", user_id="u", budget_tokens=4000)
        got = [i.id for i in res.items]
        assert fake.calls == 1
        assert got == list(reversed(base)), "the reranker's order was not applied"
        # the candidates are the reranker's view only: date, role, text
        assert set(fake.seen[0][0]) == {"date", "role", "text"}
        assert fake.seen[0][0]["date"].startswith("2023-05-20T00:00")
        assert fake.seen[0][0]["role"] == "user"
        st = m.stats()["reranker"]
        assert st["name"] == "fake" and st["calls"] == 1 and st["fallbacks"] == 0
        assert st["p50_ms"] is not None
    finally:
        m.close()


def test_rerank_shortlist_is_bm25_first_and_capped(tmp_path):
    m = _mem(tmp_path)
    try:
        for i in range(45):
            m.add(f"entry {i} about kumquat trees", user_id="u", t_event=T0 + i)
        m.flush()
        fake = _Fixed(lambda cands: [0.5] * len(cands))
        _set_reranker(m, fake)
        m.search("kumquat trees", user_id="u", budget_tokens=20000)
        assert len(fake.seen[0]) == 30
    finally:
        m.close()


# ------------------------------------------------------------ fallback

@pytest.mark.parametrize("behaviour,reason", [
    ("raise", "error"),
    ("none", "unavailable"),
    ("short", "bad_response"),
    ("nan", "bad_response"),
])
def test_failing_reranker_keeps_the_unreranked_order(tmp_path, behaviour, reason):
    m = _mem(tmp_path)
    try:
        _seed(m)
        base = [i.id for i in m.search("kumquat", user_id="u", budget_tokens=4000).items]

        def bad(cands):
            if behaviour == "raise":
                raise RuntimeError("judge exploded")
            if behaviour == "none":
                return None
            if behaviour == "short":
                return [0.5]
            return [float("nan")] * len(cands)

        _set_reranker(m, _Fixed(bad))
        m._bump_epoch()
        before = _counter("memd_rerank_fallback_total", reason=reason)
        res = m.search("kumquat", user_id="u", budget_tokens=4000)
        assert [i.id for i in res.items] == base
        assert _counter("memd_rerank_fallback_total", reason=reason) == before + 1
        assert m.stats()["reranker"]["fallbacks"] == 1
    finally:
        m.close()


def test_slow_reranker_times_out_and_falls_back(tmp_path):
    m = _mem(tmp_path)
    release = threading.Event()
    try:
        _seed(m)
        base = [i.id for i in m.search("kumquat", user_id="u", budget_tokens=4000).items]

        def slow(cands):
            release.wait(timeout=10)
            return [float(i) for i in range(len(cands))]

        _set_reranker(m, _Fixed(slow), timeout_s=0.2)
        m._bump_epoch()
        before = _counter("memd_rerank_fallback_total", reason="timeout")
        t0 = time.monotonic()
        res = m.search("kumquat", user_id="u", budget_tokens=4000)
        took = time.monotonic() - t0
        assert took < 2.0, f"search waited {took:.2f}s on a slow reranker"
        assert [i.id for i in res.items] == base
        assert _counter("memd_rerank_fallback_total", reason="timeout") == before + 1
    finally:
        release.set()
        m.close()


def test_a_fallback_result_is_not_cached(tmp_path):
    m = _mem(tmp_path)
    try:
        _seed(m)
        state = {"fail": True}

        def flaky(cands):
            if state["fail"]:
                raise RuntimeError("down")
            return [float(i) for i in range(len(cands))]

        _set_reranker(m, _Fixed(flaky))
        first = [i.id for i in m.search("kumquat", user_id="u", budget_tokens=4000).items]
        state["fail"] = False
        second = [i.id for i in m.search("kumquat", user_id="u", budget_tokens=4000).items]
        assert second == list(reversed(first)), "a degraded result outlived the outage"
    finally:
        m.close()


# ------------------------------------------------------------ selection

def test_auto_selects_jev_only_with_a_key_and_the_sdk(monkeypatch):
    monkeypatch.setattr(rerank_mod, "typesafe_available", lambda: True)
    assert resolve_reranker({}) is None, "no key: nothing may leave the machine"
    monkeypatch.setenv("TYPESAFE_API_KEY", "ts-test")
    r = resolve_reranker({})
    assert isinstance(r, JevReranker) and r.model == "jev-latest" and r.timeout_s == 1.5
    r.close()
    r = resolve_reranker({"jev_model": "jev-2026-09", "rerank_timeout_s": 0.7})
    assert r.model == "jev-2026-09" and r.timeout_s == 0.7
    r.close()
    monkeypatch.setattr(rerank_mod, "typesafe_available", lambda: False)
    assert resolve_reranker({}) is None, "key but no SDK: auto must not raise"


def test_explicit_choices(monkeypatch):
    monkeypatch.setattr(rerank_mod, "typesafe_available", lambda: True)
    monkeypatch.setenv("TYPESAFE_API_KEY", "ts-test")
    assert resolve_reranker({"reranker": "none"}) is None
    monkeypatch.setenv("MEMD_RERANKER", "none")
    assert resolve_reranker({}) is None
    r = resolve_reranker({"reranker": "jev"})  # config overrides env
    assert isinstance(r, JevReranker)
    r.close()
    monkeypatch.delenv("MEMD_RERANKER")
    with pytest.raises(ValueError):
        resolve_reranker({"reranker": "cohere"})
    monkeypatch.delenv("TYPESAFE_API_KEY")
    with pytest.raises(ValueError):
        resolve_reranker({"reranker": "jev"})
    monkeypatch.setenv("TYPESAFE_API_KEY", "ts-test")
    monkeypatch.setattr(rerank_mod, "typesafe_available", lambda: False)
    with pytest.raises(ImportError):
        resolve_reranker({"reranker": "jev"})
    monkeypatch.setattr(rerank_mod, "fastembed_available", lambda: False)
    with pytest.raises(ImportError):
        resolve_reranker({"reranker": "local"})
    monkeypatch.setattr(rerank_mod, "fastembed_available", lambda: True)
    r = resolve_reranker({"reranker": "local"})
    assert isinstance(r, LocalCrossEncoderReranker) and r.model == "BAAI/bge-reranker-base"
    r = resolve_reranker({"reranker": "local", "local_rerank_model": "Xenova/ms-marco-MiniLM-L-6-v2"})
    assert r.model == "Xenova/ms-marco-MiniLM-L-6-v2"


def test_open_logs_the_active_reranker_once(tmp_path, caplog):
    with caplog.at_level(logging.INFO, logger="memd.engine.memory"):
        m = _mem(tmp_path)
        m.close()
    lines = [r.getMessage() for r in caplog.records
             if r.name == "memd.engine.memory" and "reranker" in r.getMessage()
             and r.levelno == logging.INFO]
    assert len(lines) == 1, lines
    assert "reranker none" in lines[0] and "requested=none" in lines[0]


def test_stats_without_a_reranker(tmp_path):
    m = _mem(tmp_path)
    try:
        st = m.stats()
        assert st["reranker"] == {"name": "none"}
        assert st["pack_mode"] == "auto"
    finally:
        m.close()


# ------------------------------------------------------------ Jev (mocked SDK)

class _FakeSDK:
    """Stands in for typesafe_sdk: records requests, answers from a function."""

    def __init__(self, answer=None, delay_s=0.0, error=None):
        self.requests: list[dict] = []
        self.answer = answer or (lambda state, i: 0.9 if "kumquat" in
                                 state["candidates"][f"c{i}"]["text"] else 0.1)
        self.delay_s = delay_s
        self.error = error
        self.lock = threading.Lock()
        self.module = self._module()

    def _module(self):
        sdk = self
        mod = types.ModuleType("typesafe_sdk")

        class Noul:
            def __init__(self, *, instructions=None, criteria=None, type="noul"):
                self.instructions, self.criteria = instructions, criteria

        class RetryPolicy:
            def __init__(self, **kw):
                self.kw = kw

        class _Answer:
            def __init__(self, p):
                self.noul = p

        class _Resp:
            def __init__(self, nouls):
                self.nouls = nouls

        class TypeSafeClient:
            def __init__(self, **kw):
                self.kw = kw

            def system_one(self, state, questions, **kw):
                with sdk.lock:
                    sdk.requests.append({"state": state, "questions": questions, "kw": kw,
                                         "t": time.monotonic()})
                if sdk.delay_s:
                    time.sleep(sdk.delay_s)
                if sdk.error is not None:
                    raise sdk.error
                return _Resp({k: _Answer(sdk.answer(state, int(k[1:]))) for k in questions})

        mod.Noul, mod.RetryPolicy, mod.TypeSafeClient = Noul, RetryPolicy, TypeSafeClient
        return mod


def _cands(n):
    return [{"date": "2023-05-20T00:00+00:00", "role": "user", "text": f"turn {i} kumquat" if i % 2 else f"turn {i}"}
            for i in range(n)]


def test_jev_fans_out_in_chunks_of_25_with_the_validated_wording(monkeypatch):
    sdk = _FakeSDK(delay_s=0.05)
    monkeypatch.setitem(sys.modules, "typesafe_sdk", sdk.module)
    jev = JevReranker(api_key="ts-test", timeout_s=1.5)
    try:
        cands = _cands(30)
        out = jev.scores("where are the kumquats?", cands)
        assert out == [0.9 if i % 2 else 0.1 for i in range(30)]
        assert sorted(len(r["questions"]) for r in sdk.requests) == [5, 25]
        # issued concurrently, not one after the other
        ts = sorted(r["t"] for r in sdk.requests)
        assert ts[1] - ts[0] < 0.04
        req = next(r for r in sdk.requests if len(r["questions"]) == 25)
        assert req["state"]["query"] == "where are the kumquats?"
        assert set(req["state"]["candidates"]) == {f"c{i}" for i in range(25)}
        q = req["questions"]["c7"]
        assert q.instructions == ("Does `candidates.c7` contain information that helps answer the "
                                  "user's `query` about their own past conversations?")
        assert JEV_INSTRUCTIONS.format(cid="c7") == q.instructions
        assert q.criteria["true"].startswith("The candidate states a fact, event, preference")
        assert q.criteria["false"].startswith("The candidate is unrelated")
        assert req["kw"]["model"] == "jev-latest"
    finally:
        jev.close()


def test_jev_timeout_and_errors_return_none_with_a_reason(monkeypatch):
    sdk = _FakeSDK(delay_s=1.0)
    monkeypatch.setitem(sys.modules, "typesafe_sdk", sdk.module)
    jev = JevReranker(api_key="ts-test", timeout_s=0.2)
    try:
        t0 = time.monotonic()
        assert jev.scores("q", _cands(30)) is None
        assert time.monotonic() - t0 < 0.8
        assert jev.last_failure_reason() == "timeout"
    finally:
        jev.close()

    class TypeSafeRateLimitError(Exception):
        pass

    sdk2 = _FakeSDK(error=TypeSafeRateLimitError("429"))
    monkeypatch.setitem(sys.modules, "typesafe_sdk", sdk2.module)
    jev = JevReranker(api_key="ts-test")
    stage = RerankStage(jev)
    try:
        before = _counter("memd_rerank_fallback_total", reason="rate_limited")
        assert stage.run("q", _cands(3)) is None
        assert _counter("memd_rerank_fallback_total", reason="rate_limited") == before + 1
    finally:
        stage.close()


def _gardening(m: Memory) -> None:
    for s in range(3):
        turns = [f"session {s} turn {t} about gardening plans" for t in range(5)]
        if s == 1:
            turns[2] = "session 1 turn 2 the kumquat tree needs repotting in spring gardening"
        m.add_events([{"content": c, "user_id": "u", "session_id": f"s{s}",
                       "t_event": T0 + s * DAY} for c in turns])
    m.flush()


def test_memory_with_jev_reranks_and_packs_ranked_by_default(tmp_path, monkeypatch):
    sdk = _FakeSDK()
    monkeypatch.setitem(sys.modules, "typesafe_sdk", sdk.module)
    monkeypatch.setattr(rerank_mod, "typesafe_available", lambda: True)
    monkeypatch.setenv("TYPESAFE_API_KEY", "ts-test")
    m = _mem(tmp_path, reranker="auto")
    try:
        assert m.stats()["reranker"]["name"] == "jev"
        assert m.stats()["pack_mode"] == "auto"
        _gardening(m)
        res = m.search("when should the kumquat tree be repotted? gardening", user_id="u")
        assert sdk.requests, "Jev was not asked"
        # nothing but date/role/text leaves the engine
        sent = next(iter(sdk.requests[0]["state"]["candidates"].values()))
        assert set(sent) == {"date", "role", "text"}
        assert res.items[0].content.startswith("session 1 turn 2 the kumquat")
        # ranked (auto): every candidate that fits, in the reranker's order,
        # not just the gated few - second evidence sessions stay in context
        assert len(res.items) == 15
        assert "<session" not in res.packed_context
        assert m.stats()["reranker"]["calls"] == 1
    finally:
        m.close()


def test_memory_with_jev_packs_gated_on_opt_in(tmp_path, monkeypatch):
    sdk = _FakeSDK()
    monkeypatch.setitem(sys.modules, "typesafe_sdk", sdk.module)
    monkeypatch.setattr(rerank_mod, "typesafe_available", lambda: True)
    monkeypatch.setenv("TYPESAFE_API_KEY", "ts-test")
    monkeypatch.setenv("MEMD_PACK_MODE", "gated")
    m = _mem(tmp_path, reranker="auto")
    try:
        assert m.pack_mode == "gated"
        _gardening(m)
        res = m.search("when should the kumquat tree be repotted? gardening", user_id="u")
        assert res.items[0].content.startswith("session 1 turn 2 the kumquat")
        # gated: the one relevant turn plus its neighbours, nothing else
        assert [i.content.split(" about")[0] for i in res.items[1:]] == [
            "session 1 turn 1", "session 1 turn 3"]
        assert '<session id="s1" date="2023-05-21">' in res.packed_context
        assert res.packed_context.index("turn 1") < res.packed_context.index("turn 2") \
            < res.packed_context.index("turn 3")
        assert m.stats()["reranker"]["calls"] == 1
    finally:
        m.close()


def test_memory_warms_the_jev_client_at_open(tmp_path, monkeypatch):
    """Importing the real SDK takes ~1s; inside the first search it used up
    the whole 1.5s deadline, so the first rerank after open always fell back."""
    sdk = _FakeSDK()
    monkeypatch.setitem(sys.modules, "typesafe_sdk", sdk.module)
    monkeypatch.setattr(rerank_mod, "typesafe_available", lambda: True)
    monkeypatch.setenv("TYPESAFE_API_KEY", "ts-test")
    m = _mem(tmp_path, reranker="jev")
    try:
        deadline = time.monotonic() + 5
        while not m.rerank.reranker.ready() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert m.rerank.reranker.ready(), "the client was not built in the background"
        assert not sdk.requests, "warming must not send anything"
    finally:
        m.close()


# ------------------------------------------------------------ gated packing

def _rec(content, session, t_event, source=Source.USER, user="u"):
    return MemoryRecord.create(namespace="n", kind="raw_event", content=content,
                               scope=Scope(user=user, session=session), source=source,
                               actor_id="user:u", session_id=session, t_event=t_event)


def _item(r):
    return FusedItem(record=r, score=0.0, lanes=["bm25"], ranks={})


def test_gate_keeps_passing_candidates_in_reranker_order_else_the_top_3():
    rs = [_item(_rec(f"t{i}", "s", T0)) for i in range(6)]
    probs = [0.2, 0.7, 0.1, 0.55, 0.9, 0.3]
    ranked = sorted(zip(rs, probs), key=lambda x: -x[1])
    kept = gate_candidates(ranked, 0.5)
    assert [p for _, p in kept] == [0.9, 0.7, 0.55]
    low = [(it, p / 10) for it, p in ranked]
    assert [round(p, 3) for _, p in gate_candidates(low, 0.5)] == [0.09, 0.07, 0.055]


def test_pack_gated_groups_by_session_with_neighbours_and_fences():
    # contents are whole lines, never substrings of the (random) record ids
    a = [_rec(f"alpha turn {i}", "sA", T0 + DAY) for i in range(5)]
    b = [_rec(f"bravo turn {i}", "sB", T0, source=Source.WEB if i == 1 else Source.USER) for i in range(3)]
    pos = {r.id: n for n, r in enumerate(b + a)}
    kept = [(_item(a[2]), 0.9), (_item(b[1]), 0.6)]
    nbrs = {a[2].id: [a[1], a[3]], b[1].id: [b[0], b[2]]}
    out = pack_gated(kept, nbrs, pos, budget_tokens=4000)
    assert [i.content for i in out.items] == ["alpha turn 2", "alpha turn 1", "alpha turn 3",
                                              "bravo turn 1", "bravo turn 0", "bravo turn 2"]
    assert [i.lanes for i in out.items][:2] == [["bm25"], ["neighbour"]]
    text = out.text

    def at(content):
        return text.index(f"\n{content}\n")

    # sessions by date (sB is a day older), turns in session order
    assert text.index('<session id="sB" date="2023-05-20">') < text.index('<session id="sA" date="2023-05-21">')
    assert at("bravo turn 0") < at("bravo turn 1") < at("bravo turn 2") < at("alpha turn 1") \
        < at("alpha turn 2") < at("alpha turn 3")
    assert "alpha turn 0" not in text and "alpha turn 4" not in text
    # provenance fencing is preserved inside the session block
    assert text.count("<untrusted-data") == 1
    fenced = text[text.index("<untrusted-data"):text.index("</untrusted-data>")]
    assert "bravo turn 1" in fenced
    assert out.tokens_used <= 4000 and not out.truncated


def test_pack_gated_respects_the_budget():
    a = [_rec(("A%d " % i) + "word " * 60, "sA", T0) for i in range(5)]
    kept = [(_item(a[2]), 0.9)]
    out = pack_gated(kept, {a[2].id: [a[1], a[3]]}, {r.id: i for i, r in enumerate(a)},
                     budget_tokens=200)
    assert [i.id for i in out.items] == [a[2].id], "neighbours must be dropped before the hit"
    assert out.truncated and out.tokens_used <= 200
    out = pack_gated(kept, {}, {}, budget_tokens=20)
    assert out.items == [] and out.truncated


class _Calibrated(_Fixed):
    calibrated = True


def test_gated_packing_through_memory_and_its_modes(tmp_path):
    m = _mem(tmp_path)
    try:
        for s in range(3):
            m.add_events([{"content": f"s{s} t{t} planning notes kumquat" if (s, t) == (2, 3)
                           else f"s{s} t{t} planning notes", "user_id": "u", "session_id": f"s{s}",
                           "t_event": T0 + s * DAY} for t in range(6)])
        m.flush()

        def judge(cands):
            return [0.95 if "kumquat" in c["text"] else 0.05 for c in cands]

        # auto = ranked, even with a calibrated reranker
        assert m.pack_mode == "auto"
        _set_reranker(m, _Calibrated(judge))
        res = m.search("planning notes kumquat", user_id="u")
        assert len(res.items) == 18 and res.items[0].content.endswith("kumquat")
        assert "<session" not in res.packed_context
        # gated is an explicit opt-in
        m.pack_mode = "gated"
        m._bump_epoch()
        res = m.search("planning notes kumquat", user_id="u")
        assert [i.content for i in res.items] == [
            "s2 t3 planning notes kumquat", "s2 t2 planning notes", "s2 t4 planning notes"]
        assert "<session id=\"s2\"" in res.packed_context
        # the opt-in applies to an uncalibrated reranker too; ranked is ranked
        _set_reranker(m, _Fixed(judge))
        m._bump_epoch()
        assert "<session" in m.search("planning notes kumquat", user_id="u").packed_context
        m.pack_mode = "ranked"
        _set_reranker(m, _Calibrated(judge))
        m._bump_epoch()
        assert "<session" not in m.search("planning notes kumquat", user_id="u").packed_context
    finally:
        m.close()


def test_gated_neighbours_never_cross_users_sharing_a_session_id(tmp_path):
    m = _mem(tmp_path)
    try:
        m.add_events([{"content": f"mine {t} kumquat" if t == 1 else f"mine {t}", "user_id": "alice",
                       "session_id": "shared", "t_event": T0} for t in range(3)])
        # bob's turns in the SAME session id, interleaved in time and rowid
        m.add_events([{"content": f"bob secret {t}", "user_id": "bob", "session_id": "shared",
                       "t_event": T0} for t in range(3)])
        m.flush()
        _set_reranker(m, _Calibrated(lambda cands: [0.9 if "kumquat" in c["text"] else 0.0
                                                    for c in cands]))
        m.pack_mode = "gated"
        res = m.search("kumquat", user_id="alice", budget_tokens=4000)
        assert res.items and all("bob" not in i.content for i in res.items)
        assert "bob secret" not in res.packed_context
        assert {i.content for i in res.items} == {"mine 1 kumquat", "mine 0", "mine 2"}
    finally:
        m.close()


def test_candidate_from_record():
    r = _rec("x" * 3000, "s", T0 + 3_600_000 * 5 + 60_000 * 7)
    c = candidate_from_record(r)
    assert c == {"date": "2023-05-20T05:07+00:00", "role": "user", "text": "x" * 2000}
    r2 = MemoryRecord.create(namespace="n", kind="raw_event", content="hi", scope=Scope(),
                             source=Source.AGENT, actor_id="assistant:u", t_event=T0)
    assert candidate_from_record(r2)["role"] == "assistant"
    r3 = MemoryRecord.create(namespace="n", kind="fact", content="hi", scope=Scope(),
                             source=Source.WEB, actor_id=None, t_event=T0)
    assert candidate_from_record(r3)["role"] == "tool"


# ------------------------------------------------------------ shared model cache

class _FakeCrossEncoder:
    def rerank(self, query, docs, batch_size=32):
        return [float(len(d)) for d in docs]


def test_two_memories_share_one_cross_encoder(tmp_path, monkeypatch):
    loads = []

    def load(self):
        loads.append(self.model)
        return _FakeCrossEncoder()

    monkeypatch.setattr(rerank_mod, "fastembed_available", lambda: True)
    monkeypatch.setattr(LocalCrossEncoderReranker, "_load_model", load)
    name = f"test/xenc-{uuid.uuid4().hex}"
    a = _mem(tmp_path, reranker="local", local_rerank_model=name)
    b = _mem(tmp_path, reranker="local", local_rerank_model=name)
    try:
        a.rerank.reranker.load()
        b.rerank.reranker.load()
        assert loads == [name], "the cross-encoder was loaded more than once"
        assert a.rerank.reranker._shared.model is b.rerank.reranker._shared.model
        # same model name, different kind: its own slot in the cache
        assert embedder_mod._shared_model(name) is not a.rerank.reranker._shared
        assert a.rerank.reranker.scores("q", [{"date": "d", "role": "user", "text": "abc"}])[0] > 0.5
    finally:
        a.close()
        b.close()


class _FakeEmbedModel:
    def embed(self, texts):
        import numpy as np

        for _ in texts:
            yield np.ones(8, dtype="float32")


def test_two_memories_share_one_embedding_model(tmp_path, monkeypatch):
    loads = []

    def load(self):
        loads.append(self.model)
        return _FakeEmbedModel()

    monkeypatch.setattr(embedder_mod, "fastembed_available", lambda: True)
    monkeypatch.setattr(FastEmbedEmbedder, "_load_model", load)
    name = f"test/emb-{uuid.uuid4().hex}"
    a = _mem(tmp_path, embedder="fastembed", local_embedding_model=name)
    b = _mem(tmp_path, embedder="fastembed", local_embedding_model=name)
    try:
        a.flush()
        b.flush()
        assert loads == [name]
        assert a.embedder._shared.model is b.embedder._shared.model
    finally:
        a.close()
        b.close()


def test_cross_encoder_caps_onnx_threads(monkeypatch):
    pytest.importorskip("fastembed")
    import fastembed.rerank.cross_encoder as xenc

    seen = {}

    class _FakeTextCrossEncoder:
        def __init__(self, model_name, threads=None, **kw):
            seen["threads"] = threads

    monkeypatch.setattr(xenc, "TextCrossEncoder", _FakeTextCrossEncoder)
    monkeypatch.setattr(rerank_mod, "fastembed_available", lambda: True)
    monkeypatch.setenv("MEMD_EMBED_THREADS", "2")
    resolve_reranker({"reranker": "local"})._load_model()
    assert seen["threads"] == 2
    resolve_reranker({"reranker": "local", "embed_threads": 1})._load_model()
    assert seen["threads"] == 1


def test_local_reranker_serves_nothing_until_loaded(tmp_path, monkeypatch):
    gate = threading.Event()

    def load(self):
        gate.wait(timeout=10)
        return _FakeCrossEncoder()

    monkeypatch.setattr(rerank_mod, "fastembed_available", lambda: True)
    monkeypatch.setattr(LocalCrossEncoderReranker, "_load_model", load)
    m = _mem(tmp_path, reranker="local", local_rerank_model=f"test/slow-{uuid.uuid4().hex}")
    try:
        _seed(m, 3)
        before = _counter("memd_rerank_fallback_total", reason="not_ready")
        res = m.search("kumquat", user_id="u")
        assert res.items
        assert _counter("memd_rerank_fallback_total", reason="not_ready") == before + 1
    finally:
        gate.set()
        m.close()
