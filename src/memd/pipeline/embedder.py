"""Embedding providers (ADR-8).

Policy: the binary ships with no model. Selection is explicit when asked for:
config["embedder"] (or env MEMD_EMBEDDER) is one of "auto", "hash",
"fastembed", "openai". An explicit choice that cannot be honoured raises; it
never falls back. "auto" (the default) resolves in this order:
  1. BYO API key  -> OpenAICompatibleEmbedder (OpenAI / Voyage / Ollama / LM Studio)
  2. local ONNX   -> FastEmbedEmbedder (optional extra; model license must be
                     re-verified from the model card before bundling - ADR-8)
  3. fallback     -> HashEmbedder: deterministic feature-hashing embedding.
                     Fully functional offline; degraded *semantic* recall
                     (BM25 + time/entity lanes carry retrieval). Honest mode,
                     clearly labeled in stats.
Retrieval and forget() semantics depend on which one is active, so the
choice is reported by stats() and logged when a Memory opens.

Embeddings are versioned (`model` string); re-embedding is a batch job over
raw because raw is retained (design constraint #2).
"""
from __future__ import annotations

import hashlib
import importlib.util
import math
import os
import re
import threading
from abc import ABC, abstractmethod

import numpy as np

_WORD_RE = re.compile(r"[a-z0-9]+")


EMBEDDER_CHOICES = ("auto", "hash", "fastembed", "openai")


class Embedder(ABC):
    name: str = "base"
    kind: str = "base"
    dim: int = 0
    # Minimum cosine at which a VECTOR-ONLY match (no bm25/entity evidence)
    # may drive a destructive sweep (find_ids -> forget). It is a property of
    # the embedding space, not of the engine: unrelated short strings score
    # ~0 under the hash embedder but ~0.5 under bge-small, so one global
    # threshold either deletes unrelated records or never fires. Subclasses
    # document how their value was chosen. Deletion is irreversible: when in
    # doubt, err high (a missed vector-only match leaves a record behind; a
    # false one destroys it).
    strong_match_cosine: float = 0.35

    @abstractmethod
    def embed(self, texts: list[str]) -> np.ndarray: ...

    def embed_one(self, text: str) -> np.ndarray:
        return self.embed([text])[0]

    def ready(self) -> bool:
        """True when embed() will not block on a model load. Latency-critical
        callers (search) check this and serve from the other lanes instead."""
        return True

    def load(self) -> None:
        """Block until the embedder is usable; raise if it cannot be. The
        embed worker calls this on its own thread at open, so a slow model
        load never sits on Memory() or the write path."""
        return None


# Standard IR stopwords only. Words fitted to the synthetic test generator
# ("later follow up agreed discussed session number notes") used to live here
# too, as they did in sqlite_index._FTS_STOPWORDS: they silently erased real
# content words from the hash embedding on real data.
_STOPWORDS = frozenset(
    "a an and are as at be but by for from has have i in is it its of on or that the "
    "this to we was were will with you your do does did not no yes about".split()
)


class HashEmbedder(Embedder):
    """Feature-hashing bag-of-ngrams. Deterministic, zero-dependency.

    Not semantically deep, but stable, fast, and gives the vector lane real
    lexical overlap signal so hybrid fusion works end-to-end offline.
    Stopwords are dropped so boilerplate-heavy distractors don't dominate.

    strong_match_cosine 0.35: under feature hashing, strings that share no
    content n-gram score ~0 (random-sign bucket collisions only: over an
    18-string mixed-topic sample, unrelated pairs had median 0.00 and max
    0.21), so 0.35 means a real share of the weighted n-grams overlaps."""

    kind = "hash"
    strong_match_cosine = 0.35
    # Bumped whenever the features change. The name is the embedding version
    # stamped on every stored vector, and a namespace re-embeds (open-time
    # vector-health check -> reembed()) whatever carries another version.
    # v2: the synthetic-generator words are no longer stopwords; vectors
    # stored as plain "hash-ngram-384" were hashed from different features.
    VERSION = "v2"

    def __init__(self, dim: int = 384):
        self.dim = dim
        self.name = f"hash-ngram-{dim}-{self.VERSION}"

    def _feats(self, text: str) -> list[str]:
        words = [w for w in _WORD_RE.findall(text.lower()) if w not in _STOPWORDS]
        feats = list(words)
        feats += [f"{a}_{b}" for a, b in zip(words, words[1:])]
        return feats

    def embed(self, texts: list[str]) -> np.ndarray:
        out = np.zeros((len(texts), self.dim), dtype=np.float32)
        for i, t in enumerate(texts):
            for f in self._feats(t):
                h = int.from_bytes(hashlib.md5(f.encode(), usedforsecurity=False).digest()[:8], "big")
                idx = h % self.dim
                sign = 1.0 if (h >> 63) & 1 else -1.0
                w = 1.0 + math.log(1 + len(f) / 6)
                out[i, idx] += sign * w
        norms = np.linalg.norm(out, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        return out / norms


class OpenAICompatibleEmbedder(Embedder):
    """POST {base_url}/embeddings. Works with OpenAI, Voyage (via base_url),
    Ollama (/v1), LM Studio, and any OpenAI-shaped gateway.

    strong_match_cosine 0.6: an ESTIMATE, not measured in this repo (no API
    key in CI). It follows the commonly reported range of
    text-embedding-3-small, the default model: unrelated text around
    0.1-0.3, close paraphrases around 0.5-0.7, so 0.6 admits only
    near-paraphrases. Any other model behind this door has its own cosine
    range: measure it and set config["strong_match_cosine"]."""

    kind = "openai"
    strong_match_cosine = 0.6

    def __init__(self, model: str, api_key: str, base_url: str = "https://api.openai.com/v1", dim: int | None = None):
        import httpx

        self.model = model
        self.name = model
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self._client = httpx.Client(timeout=30)
        self._dim = dim

    @property
    def dim(self) -> int:
        if self._dim is None:
            v = self.embed(["dim probe"])
            self._dim = int(v.shape[1])
        return self._dim

    def embed(self, texts: list[str]) -> np.ndarray:
        resp = self._client.post(
            f"{self.base_url}/embeddings",
            headers={"Authorization": f"Bearer {self.api_key}"},
            json={"model": self.model, "input": texts},
        )
        resp.raise_for_status()
        data = sorted(resp.json()["data"], key=lambda d: d["index"])
        return np.array([d["embedding"] for d in data], dtype=np.float32)


def fastembed_available() -> bool:
    """Whether the optional `fastembed` extra is installed, WITHOUT importing
    it (the import alone costs ~1s: it pulls in onnxruntime)."""
    return importlib.util.find_spec("fastembed") is not None


class _SharedModel:
    """One loaded ONNX model per (kind, model name) per PROCESS, shared by
    every FastEmbedEmbedder (kind "embed") and every local cross-encoder
    reranker (kind "rerank", memd.query.rerank). Loading one per embed-worker
    thread and freeing it on close from another thread grew RSS ~67MB per
    Memory open/close cycle (glibc per-thread malloc arenas are not trimmed
    back to the OS), and the test suite was OOM-killed; it also paid the load
    on every open."""

    def __init__(self) -> None:
        self.load_lock = threading.Lock()
        self.embed_lock = threading.Lock()
        self.model = None


_SHARED_MODELS: dict[tuple[str, str], _SharedModel] = {}
_SHARED_MODELS_LOCK = threading.Lock()


def _shared_model(model_name: str, kind: str = "embed") -> _SharedModel:
    """The process-wide slot for one model. Keyed by kind as well as name:
    an embedding model and a cross-encoder are different objects even if a
    registry ever gave them the same name."""
    with _SHARED_MODELS_LOCK:
        entry = _SHARED_MODELS.get((kind, model_name))
        if entry is None:
            entry = _SHARED_MODELS[(kind, model_name)] = _SharedModel()
        return entry


class FastEmbedEmbedder(Embedder):
    """Local ONNX embeddings via the optional `fastembed` extra.

    The model is loaded LAZILY by load() - importing fastembed/onnxruntime and
    building the session takes ~1.3s warm and far longer on a first download,
    and constructing it in Memory() put all of that ahead of the first write
    ack. The embed worker calls load() on its own thread; search checks
    ready() and serves from the other lanes until the model is up. The
    loaded model is shared process-wide (see _SharedModel).

    ONNX runtimes are not guaranteed thread-safe: a lock (shared with the
    model) serializes embed() calls that can arrive concurrently (search
    threads + background workers, across Memory instances).

    strong_match_cosine 0.85 (measured on BAAI/bge-small-en-v1.5): bge's
    cosine space is anisotropic - over an 18-string mixed-topic sample,
    UNRELATED pairs scored median 0.49, p95 0.59, max 0.66, and the test
    corpus pair "purge target zebra" / "keep me dolphin 5" scores 0.58.
    Paraphrases landed 0.64-0.93 and topical neighbours up to 0.80
    ("project alpha deadline" / "project beta launch date"); near-duplicates
    0.95+. 0.85 clears every unrelated pair by ~0.2 and excludes topical
    neighbours, so only near-paraphrases are deleted without lexical
    evidence. The old global 0.35 sat below bge's unrelated median."""

    kind = "fastembed"
    strong_match_cosine = 0.85

    def __init__(self, model: str = "BAAI/bge-small-en-v1.5"):
        self.model = model
        self.name = model
        self._shared = _shared_model(model, kind="embed")
        self._load_error: Exception | None = None
        self._dim: int | None = None

    def _load_model(self):
        from fastembed import TextEmbedding

        return TextEmbedding(model_name=self.model)

    def ready(self) -> bool:
        return self._shared.model is not None

    def load(self) -> None:
        shared = self._shared
        if shared.model is not None:
            return
        with shared.load_lock:
            if shared.model is not None:
                return
            if self._load_error is not None:
                # a failed load is not retried per call: a missing download
                # would otherwise re-hit the network on every batch
                raise RuntimeError(f"fastembed model {self.model!r} failed to load") from self._load_error
            try:
                shared.model = self._load_model()
            except Exception as e:
                self._load_error = e
                raise

    @property
    def dim(self) -> int:
        if self._dim is None:
            self._dim = int(self.embed(["dim probe"]).shape[1])
        return self._dim

    def embed(self, texts: list[str]) -> np.ndarray:
        self.load()
        with self._shared.embed_lock:
            vecs = list(self._shared.model.embed(texts))
        return np.array(vecs, dtype=np.float32)


def requested_embedder(config: dict | None = None) -> str:
    """The embedder asked for: config["embedder"], else env MEMD_EMBEDDER,
    else "auto". Unknown values raise rather than silently meaning "auto"."""
    cfg = config or {}
    choice = str(cfg.get("embedder") or os.environ.get("MEMD_EMBEDDER") or "auto").strip().lower()
    if choice not in EMBEDDER_CHOICES:
        raise ValueError(f"unknown embedder {choice!r}; expected one of {list(EMBEDDER_CHOICES)}")
    return choice


def resolve_embedder(config: dict | None = None) -> Embedder:
    cfg = config or {}
    choice = requested_embedder(cfg)
    emb: Embedder
    if choice == "openai" or (choice == "auto" and cfg.get("embedding_api_key")):
        if not cfg.get("embedding_api_key"):
            raise ValueError("embedder 'openai' requires config['embedding_api_key']")
        emb = OpenAICompatibleEmbedder(
            model=cfg.get("embedding_model", "text-embedding-3-small"),
            api_key=cfg["embedding_api_key"],
            base_url=cfg.get("embedding_base_url", "https://api.openai.com/v1"),
        )
    elif choice == "fastembed" or (choice == "auto" and fastembed_available()):
        if not fastembed_available():
            raise ImportError(
                "embedder 'fastembed' was requested but the `fastembed` package is not "
                "importable; install the optional extra or choose embedder='hash'")
        # the model itself loads lazily (see FastEmbedEmbedder)
        emb = FastEmbedEmbedder(cfg.get("local_embedding_model", "BAAI/bge-small-en-v1.5"))
    else:
        emb = HashEmbedder(cfg.get("hash_dim", 384))
    if cfg.get("strong_match_cosine") is not None:
        # operator override, for a model whose cosine range was measured
        emb.strong_match_cosine = float(cfg["strong_match_cosine"])
    return emb
