"""Embedder selection, lazy model load, and flush honesty.

- explicit selection (config["embedder"] / MEMD_EMBEDDER); an explicit choice
  that cannot be honoured raises instead of silently falling back
- the local model loads in the embed worker: Memory() and add() never wait on
  it, and search serves from the vector-free lanes until it is up
- flush() says so (log + metric + stats) when the embed drain times out
- the forget() vector-only threshold is a property of the embedder
"""
import logging
import threading
import time
import uuid

import numpy as np
import pytest

import memd.pipeline.embedder as embedder_mod
from memd.engine.memory import Memory
from memd.metrics import METRICS
from memd.pipeline.embedder import (
    Embedder,
    FastEmbedEmbedder,
    HashEmbedder,
    OpenAICompatibleEmbedder,
    resolve_embedder,
)


def _counter(name: str) -> float:
    return sum(x["value"] for x in METRICS.snapshot()["counters"].get(name, []))


# ------------------------------------------------------------------ selection

def test_explicit_hash_is_reported_by_stats(tmp_path):
    m = Memory(str(tmp_path / "d"), config={"embedder": "hash"})
    try:
        st = m.stats()
        assert st["embedder"] == "hash-ngram-384-v2"
        assert st["embedder_kind"] == "hash"
        assert st["embedder_ready"] is True
        assert st["embed_pending"] == 0
    finally:
        m.close()


def test_env_selects_and_config_overrides_env(monkeypatch):
    monkeypatch.setenv("MEMD_EMBEDDER", "hash")
    assert resolve_embedder({}).kind == "hash"
    monkeypatch.setattr(embedder_mod, "fastembed_available", lambda: True)
    assert resolve_embedder({"embedder": "fastembed"}).kind == "fastembed"


def test_auto_keeps_the_old_order(monkeypatch):
    monkeypatch.delenv("MEMD_EMBEDDER", raising=False)
    monkeypatch.setattr(embedder_mod, "fastembed_available", lambda: False)
    assert resolve_embedder({}).kind == "hash"
    monkeypatch.setattr(embedder_mod, "fastembed_available", lambda: True)
    assert resolve_embedder({}).kind == "fastembed"
    assert resolve_embedder({"embedding_api_key": "k"}).kind == "openai"


def test_unknown_embedder_raises():
    with pytest.raises(ValueError, match="unknown embedder"):
        resolve_embedder({"embedder": "word2vec"})


def test_explicit_fastembed_that_cannot_import_raises(monkeypatch, tmp_path):
    monkeypatch.setattr(embedder_mod, "fastembed_available", lambda: False)
    with pytest.raises(ImportError, match="fastembed"):
        resolve_embedder({"embedder": "fastembed"})
    with pytest.raises(ImportError, match="fastembed"):
        Memory(str(tmp_path / "d"), config={"embedder": "fastembed"})


def test_explicit_openai_requires_a_key():
    with pytest.raises(ValueError, match="embedding_api_key"):
        resolve_embedder({"embedder": "openai"})


def test_open_logs_the_active_embedder_once(tmp_path, caplog):
    with caplog.at_level(logging.INFO, logger="memd.engine.memory"):
        m = Memory(str(tmp_path / "d"), config={"embedder": "hash"})
        m.close()
    lines = [r.getMessage() for r in caplog.records
             if r.name == "memd.engine.memory" and "embedder" in r.getMessage()
             and r.levelno == logging.INFO]
    assert len(lines) == 1, lines
    assert "hash-ngram-384-v2" in lines[0] and "requested=hash" in lines[0]


# ------------------------------------------------- strong-match thresholds

def test_strong_match_threshold_is_per_embedder(monkeypatch):
    assert HashEmbedder.strong_match_cosine == 0.35
    assert FastEmbedEmbedder.strong_match_cosine == 0.85
    assert OpenAICompatibleEmbedder.strong_match_cosine == 0.6
    emb = resolve_embedder({"embedder": "hash", "strong_match_cosine": 0.5})
    assert emb.strong_match_cosine == 0.5


def test_vector_only_floor_rises_above_the_namespace_median(tmp_path):
    m = Memory(str(tmp_path / "d"), config={"embedder": "hash"})
    try:
        # too few points to trust a median: the embedder's floor alone
        assert m._vector_only_floor([0.9, 0.9]) == pytest.approx(0.35)
        # a homogeneous sample (median 0.7) must not be swept on vector evidence
        assert m._vector_only_floor([0.7] * 9 + [0.95]) == pytest.approx(0.8)
        # a spread-out sample: the embedder's floor governs
        assert m._vector_only_floor([0.05, 0.1, 0.1, 0.2, 0.9]) == pytest.approx(0.35)
    finally:
        m.close()


def _vector_models(m: Memory) -> set[str]:
    m.ns.index.flush()
    return {r[0] for r in m.ns.index._con.execute("SELECT model FROM vectors").fetchall()}


def _missing_gauge(ns: str = "default") -> float:
    g = [x for x in METRICS.snapshot()["gauges"].get("memd_vectors_missing", [])
         if x["labels"].get("ns") == ns]
    return g[-1]["value"] if g else -1.0


def test_vectors_from_the_pre_v2_hash_embedder_are_re_embedded_on_open(tmp_path):
    """Patch 2 changed the HashEmbedder's features (stopwords) but kept its
    name, and the name is the embedding version stamped on every vector - so
    stores embedded before it were never re-embedded and mixed two feature
    spaces in one lane. The version is now part of the name, and the existing
    open-time vector-health check flags the old vectors and heals them."""
    root = str(tmp_path / "d")
    m = Memory(root, config={"embedder": "hash"})
    m.embedder.name = "hash-ngram-384"  # what patch-2 builds stamped
    for i in range(6):
        m.add(f"record {i} about kumquat orchards", user_id="u")
    m.flush()
    assert _vector_models(m) == {"hash-ngram-384"}
    m.close()

    # flagged: with self-heal off the mismatch is visible and left alone
    m = Memory(root, config={"embedder": "hash", "vector_selfheal": False})
    try:
        assert m.embedder.name == "hash-ngram-384-v2"
        assert _missing_gauge() == 6.0
        m.flush()
        assert _vector_models(m) == {"hash-ngram-384"}
    finally:
        m.close()

    # healed: a normal open schedules the re-embed on the maintenance thread
    m = Memory(root, config={"embedder": "hash"})
    try:
        m.flush()  # drains the maintenance worker
        assert _vector_models(m) == {"hash-ngram-384-v2"}
        assert m.stats()["embedding_model"] == "hash-ngram-384-v2"
        assert _missing_gauge() == 0.0
        assert m.find_ids("kumquat orchards", user_id="u")
    finally:
        m.close()


def test_local_models_cap_onnx_threads(monkeypatch):
    """onnxruntime defaults to one intra-op thread per core; several
    processes (or other load) then oversubscribe the CPU and throughput
    collapses (~4 vectors/s measured at load 35)."""
    import os

    fastembed = pytest.importorskip("fastembed")
    seen = {}

    class _FakeTextEmbedding:
        def __init__(self, model_name, threads=None, **kw):
            seen["threads"] = threads

    monkeypatch.setattr(fastembed, "TextEmbedding", _FakeTextEmbedding)
    monkeypatch.delenv("MEMD_EMBED_THREADS", raising=False)
    for cores, want in ((16, 4), (8, 4), (6, 3), (2, 1), (1, 1), (None, 1)):
        monkeypatch.setattr(os, "cpu_count", lambda c=cores: c)
        assert embedder_mod.default_embed_threads() == want, cores
    monkeypatch.setattr(os, "cpu_count", lambda: 64)
    monkeypatch.setattr(embedder_mod, "fastembed_available", lambda: True)
    name = f"test/threads-{uuid.uuid4().hex}"
    resolve_embedder({"embedder": "fastembed", "local_embedding_model": name})._load_model()
    assert seen["threads"] == 4
    resolve_embedder({"embedder": "fastembed", "local_embedding_model": name,
                      "embed_threads": 3})._load_model()
    assert seen["threads"] == 3
    monkeypatch.setenv("MEMD_EMBED_THREADS", "2")
    resolve_embedder({"embedder": "fastembed", "local_embedding_model": name})._load_model()
    assert seen["threads"] == 2
    with pytest.raises(ValueError):
        embedder_mod.resolve_embed_threads({"embed_threads": 0})


class _RecordingEmbedder(HashEmbedder):
    def __init__(self):
        super().__init__(384)
        self.calls: list[list[str]] = []

    def embed(self, texts):
        self.calls.append(list(texts))
        return super().embed(texts)


def test_embed_batches_are_length_sorted_and_truncated(tmp_path):
    """A local ONNX model pads a batch to its longest member: raw bge-small
    managed 1.2 texts/s batched vs 3.4 one at a time on LongMemEval turns."""
    from memd.engine.memory import embed_order

    ids, texts = embed_order({"a": "x" * 50, "b": "y" * 5, "c": "z" * 12}, 20)
    assert ids == ["b", "c", "a"] and texts == ["y" * 5, "z" * 12, "x" * 20]
    assert embed_order({"a": "x" * 50}, 0)[1] == ["x" * 50], "0 disables the cap"

    m = Memory(str(tmp_path / "d"), config={"embedder": "hash", "embed_max_chars": 40})
    rec = _RecordingEmbedder()
    m.embedder = rec
    m._embed_worker.embedder = rec
    try:
        lengths = [300, 12, 90, 45, 7, 200]
        ids = m.add_events([{"content": f"{n:03d} " + "w" * (n - 4), "user_id": "u"} for n in lengths])
        m.flush()
        sent = [t for call in rec.calls for t in call]
        assert sorted(len(t) for t in sent) == sorted(min(40, n) for n in lengths)
        for call in rec.calls:
            assert [len(t) for t in call] == sorted(len(t) for t in call), call
        # each vector landed on its own record despite the reordering
        h = HashEmbedder(384)
        m.ns.index.flush()
        for rid, n in zip(ids, lengths):
            blob, dim = m.ns.index._con.execute("SELECT vec, dim FROM vectors WHERE id=?", (rid,)).fetchone()
            want = h.embed([(f"{n:03d} " + "w" * (n - 4))[:40]])[0]
            got = np.frombuffer(blob, dtype=np.float16).astype(np.float32)
            assert np.allclose(got, want, atol=1e-2), rid
        # the re-embed job prepares text the same way
        rec.calls.clear()
        m.ns.index._con.execute("DELETE FROM vectors")
        m.ns.index._con.commit()
        m.reembed()
        assert rec.calls and all(len(t) <= 40 for call in rec.calls for t in call)
        assert [len(t) for t in rec.calls[0]] == sorted(len(t) for t in rec.calls[0])
    finally:
        m.close()


def test_hash_embedder_keeps_generator_fitted_words():
    feats = HashEmbedder()._feats("later follow up agreed discussed session number notes")
    for w in ("later", "follow", "up", "agreed", "discussed", "session", "number", "notes"):
        assert w in feats, f"{w!r} is still dropped as a stopword"


# ------------------------------------------------------------ lazy model load

class _FakeModel:
    """Stands in for fastembed.TextEmbedding: hash vectors, no ONNX."""

    def __init__(self):
        self._h = HashEmbedder(384)

    def embed(self, texts):
        return iter(self._h.embed(list(texts)))


def test_memory_returns_quickly_and_add_acks_before_the_model_loads(tmp_path, monkeypatch):
    gate = threading.Event()
    loaded_on: list[str] = []

    def slow_load(self):
        loaded_on.append(threading.current_thread().name)
        assert gate.wait(timeout=30), "test never released the model load"
        return _FakeModel()

    monkeypatch.setattr(embedder_mod, "fastembed_available", lambda: True)
    monkeypatch.setattr(FastEmbedEmbedder, "_load_model", slow_load)

    t0 = time.monotonic()
    # a model name of its own: loaded models are shared per process, and a
    # real bge session loaded by an earlier test must not satisfy this one
    m = Memory(str(tmp_path / "d"), config={"embedder": "fastembed",
                                            "local_embedding_model": f"test/slow-{uuid.uuid4().hex}"})
    open_s = time.monotonic() - t0
    try:
        assert not m.embedder.ready(), "Memory() waited for the model load"
        assert open_s < 5, f"Memory() took {open_s:.2f}s"
        # acks never wait on the model
        t1 = time.monotonic()
        rid = m.add("the deploy command is make ship", user_id="u1")[0]
        m.add_events([{"content": f"release note {i} about canaries", "user_id": "u1"}
                      for i in range(5)])
        assert time.monotonic() - t1 < 5
        assert not m.embedder.ready()
        assert m.get(rid) is not None
        # search before the model is up: served by the vector-free lanes
        before = _counter("memd_embed_query_not_ready_total")
        res = m.search("deploy command", user_id="u1")
        assert any(i.id == rid for i in res.items)
        assert all("vector" not in i.lanes for i in res.items)
        assert _counter("memd_embed_query_not_ready_total") == before + 1
        assert m.stats()["embedder_ready"] is False
        # a destructive sweep resolves on lexical evidence alone meanwhile
        assert rid in m.find_ids("deploy command", user_id="u1")

        gate.set()
        m.flush()
        assert loaded_on == ["memd-embed"], f"model loaded on {loaded_on}"
        assert m.embedder.ready()
        assert m.ns.index.stats()["vectors"] == 6
        res = m.search("deploy command", user_id="u1")
        assert any("vector" in i.lanes for i in res.items if i.id == rid), \
            "a degraded pre-load result outlived the load (search cache)"
        assert m.stats()["embed_pending"] == 0
    finally:
        gate.set()
        m.close()


def test_the_local_model_loads_once_per_process(monkeypatch):
    """Every Memory used to build its own ONNX session; loaded on a worker
    thread and freed from another, each open/close cycle grew RSS ~67MB."""
    calls = []

    def load(self):
        calls.append(self.model)
        return _FakeModel()

    monkeypatch.setattr(FastEmbedEmbedder, "_load_model", load)
    name = f"test/shared-{uuid.uuid4().hex}"
    a, b = FastEmbedEmbedder(name), FastEmbedEmbedder(name)
    assert not a.ready() and not b.ready()
    a.load()
    assert b.ready(), "a second embedder for the same model reloaded it"
    b.load()
    assert calls == [name]
    assert np.allclose(a.embed(["x y"]), b.embed(["x y"]))


# ------------------------------------------------------------ flush honesty

class _GatedEmbedder(Embedder):
    name = HashEmbedder(384).name
    kind = "hash"
    dim = 384

    def __init__(self):
        self.gate = threading.Event()
        self._h = HashEmbedder(384)

    def embed(self, texts):
        self.gate.wait(timeout=30)
        return self._h.embed(texts)


def test_flush_warns_and_counts_when_the_embed_drain_times_out(tmp_path, caplog):
    m = Memory(str(tmp_path / "d"), config={"embedder": "hash", "embed_flush_drain_s": 0.3})
    gated = _GatedEmbedder()
    m.embedder = gated
    m._embed_worker.embedder = gated
    try:
        for i in range(5):
            m.add(f"stuck embedding {i}", user_id="u1")
        before = _counter("memd_flush_embed_pending_total")
        with caplog.at_level(logging.WARNING, logger="memd.engine.memory"):
            m.flush()
        assert _counter("memd_flush_embed_pending_total") == before + 1
        assert any("still pending" in r.getMessage() for r in caplog.records
                   if r.levelno == logging.WARNING)
        assert m.stats()["embed_pending"] > 0

        gated.gate.set()
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger="memd.engine.memory"):
            m.flush()
        assert not [r for r in caplog.records
                    if r.name == "memd.engine.memory" and r.levelno == logging.WARNING]
        assert _counter("memd_flush_embed_pending_total") == before + 1
        assert m.stats()["embed_pending"] == 0
        assert m.ns.index.stats()["vectors"] == 5
    finally:
        gated.gate.set()
        m.close()


def test_the_embedding_key_model_and_base_url_come_from_env(monkeypatch):
    monkeypatch.delenv("MEMD_EMBEDDER", raising=False)
    monkeypatch.setenv("MEMD_EMBEDDING_API_KEY", "sk-env")
    monkeypatch.setenv("MEMD_EMBEDDING_MODEL", "env-embed")
    monkeypatch.setenv("MEMD_EMBEDDING_BASE_URL", "http://embed.local/v1")
    emb = resolve_embedder({})
    assert (emb.kind, emb.api_key, emb.model, emb.base_url) == ("openai", "sk-env", "env-embed", "http://embed.local/v1")
    assert resolve_embedder({"embedder": "openai"}).api_key == "sk-env"
    # config wins; an empty config key turns the env key off
    emb = resolve_embedder({"embedding_api_key": "sk-cfg", "embedding_model": "cfg-embed"})
    assert (emb.api_key, emb.model) == ("sk-cfg", "cfg-embed")
    assert resolve_embedder({"embedding_api_key": "", "embedder": "hash"}).kind == "hash"
    monkeypatch.setattr(embedder_mod, "fastembed_available", lambda: False)
    assert resolve_embedder({"embedding_api_key": ""}).kind == "hash"
