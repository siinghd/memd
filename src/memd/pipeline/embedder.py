"""Embedding providers (ADR-8).

Policy: the binary ships with no model. Resolution order on first use:
  1. BYO API key  -> OpenAICompatibleEmbedder (OpenAI / Voyage / Ollama / LM Studio)
  2. local ONNX   -> FastEmbedEmbedder (optional extra; model license must be
                     re-verified from the model card before bundling - ADR-8)
  3. fallback     -> HashEmbedder: deterministic feature-hashing embedding.
                     Fully functional offline; degraded *semantic* recall
                     (BM25 + time/entity lanes carry retrieval). Honest mode,
                     clearly labeled in stats.

Embeddings are versioned (`model` string); re-embedding is a batch job over
raw because raw is retained (design constraint #2).
"""
from __future__ import annotations

import hashlib
import math
import re
from abc import ABC, abstractmethod

import numpy as np

_WORD_RE = re.compile(r"[a-z0-9]+")


class Embedder(ABC):
    name: str = "base"
    dim: int = 0

    @abstractmethod
    def embed(self, texts: list[str]) -> np.ndarray: ...

    def embed_one(self, text: str) -> np.ndarray:
        return self.embed([text])[0]


_STOPWORDS = frozenset(
    "a an and are as at be but by for from has have i in is it its of on or that the "
    "this to we was were will with you your do does did not no yes later about follow "
    "up agreed discussed session number notes from".split()
)


class HashEmbedder(Embedder):
    """Feature-hashing bag-of-ngrams. Deterministic, zero-dependency.

    Not semantically deep, but stable, fast, and gives the vector lane real
    lexical overlap signal so hybrid fusion works end-to-end offline.
    Stopwords are dropped so boilerplate-heavy distractors don't dominate."""

    def __init__(self, dim: int = 384):
        self.dim = dim
        self.name = f"hash-ngram-{dim}"

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
    Ollama (/v1), LM Studio, and any OpenAI-shaped gateway."""

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


class FastEmbedEmbedder(Embedder):
    """Local ONNX embeddings via the optional `fastembed` extra.

    ONNX runtimes are not guaranteed thread-safe: a lock serializes embed()
    calls that can arrive concurrently (search thread + background worker)."""

    def __init__(self, model: str = "BAAI/bge-small-en-v1.5"):
        import threading

        from fastembed import TextEmbedding

        self.model = model
        self.name = model
        self._embed_lock = threading.Lock()
        self._model = TextEmbedding(model_name=model)
        self._dim: int | None = None

    @property
    def dim(self) -> int:
        if self._dim is None:
            v = next(iter(self._model.embed(["dim probe"])))
            self._dim = int(len(v))
        return self._dim

    def embed(self, texts: list[str]) -> np.ndarray:
        with self._embed_lock:
            vecs = list(self._model.embed(texts))
        return np.array(vecs, dtype=np.float32)


def resolve_embedder(config: dict | None = None) -> Embedder:
    cfg = config or {}
    if cfg.get("embedding_api_key"):
        return OpenAICompatibleEmbedder(
            model=cfg.get("embedding_model", "text-embedding-3-small"),
            api_key=cfg["embedding_api_key"],
            base_url=cfg.get("embedding_base_url", "https://api.openai.com/v1"),
        )
    try:
        return FastEmbedEmbedder(cfg.get("local_embedding_model", "BAAI/bge-small-en-v1.5"))
    except ImportError:
        pass
    except Exception:
        pass
    return HashEmbedder(cfg.get("hash_dim", 384))
