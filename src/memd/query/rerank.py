"""Reranker plug-in: re-order the lexical shortlist with a relevance judge.

Evidence (LongMemEval_S dev, session ndcg_any@5): a Jev fan-out rerank of the
BM25 top-30 lifted the BM25 lane from 0.891 to 0.954, and memd's fused order
from 0.810 to 0.936. A control that reranks against a DIFFERENT question
collapses to 0.557, so the gain is the query-candidate judgement, not the
reshuffle.

Selection: config["reranker"] (or env MEMD_RERANKER) is one of
  "auto" (default) - "jev" when a TypeSafe key is configured (env
                     TYPESAFE_API_KEY, or config["typesafe_api_key"]) AND
                     typesafe-sdk is importable, otherwise "none". No key
                     means no data leaves the machine.
  "none"           - no reranking.
  "jev"            - TypeSafe System One (Jev). PRIVACY: the top-30 candidate
                     texts of every search are sent to TypeSafe's API.
  "local"          - a local cross-encoder (fastembed TextCrossEncoder,
                     default BAAI/bge-reranker-base). Nothing leaves the
                     machine; its scores are not calibrated probabilities.
An explicit choice that cannot be honoured raises; it never falls back.

A reranker must never take search down: RerankStage runs it under a hard
deadline, and on any error, timeout or malformed answer the search keeps the
unreranked order and counts memd_rerank_fallback_total{reason}. Only
KeyboardInterrupt and SystemExit raised inside a reranker propagate.

A call that misses its deadline cannot be cancelled once it runs (for Jev,
its request is in flight), so the stage bounds how many calls may be in
flight at once: past `max_inflight` a search skips reranking at once
(reason "busy") instead of queueing more work behind the stuck calls. A call
still queued when its deadline passes never starts, and a Jev request checks
its deadline again immediately before it is sent, so candidate texts never
leave after their search has returned. Until the Jev client is built (SDK
import + construction, ~1-1.5s, started in the background) searches skip
reranking (reason "warming") rather than build it on their own deadline.
"""
from __future__ import annotations

import datetime as _dt
import importlib.util
import logging
import math
import os
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as _FutureTimeout
from concurrent.futures import wait as _wait_futures
from typing import Any, Protocol, TypedDict, runtime_checkable

from memd.metrics import METRICS
from memd.pipeline.embedder import (
    _shared_model,
    default_embed_threads,
    fastembed_available,
    resolve_embed_threads,
)

_log = logging.getLogger(__name__)

RERANKER_CHOICES = ("auto", "none", "jev", "local")
DEFAULT_JEV_MODEL = "jev-latest"
DEFAULT_LOCAL_RERANK_MODEL = "BAAI/bge-reranker-base"
# shortlist size: the reranker sees the top RERANK_K lexical candidates
DEFAULT_RERANK_K = 30
DEFAULT_JEV_TIMEOUT_S = 1.5
# a CPU cross-encoder over 30 pairs is slower than one Jev round trip
DEFAULT_LOCAL_TIMEOUT_S = 5.0
CANDIDATE_TEXT_CHARS = 2000
# Jev fan-out: one Noul per candidate, at most this many per request; the
# chunks of one query are issued concurrently
JEV_CHUNK = 25
# a failed background client build is retried at most this often
JEV_WARM_RETRY_S = 30.0

# Validated wording (experiments 015/016). `{cid}` is the candidate's key in
# the request state, e.g. `candidates.c7`.
JEV_INSTRUCTIONS = ("Does `candidates.{cid}` contain information that helps answer "
                    "the user's `query` about their own past conversations?")
JEV_CRITERIA = {
    "true": "The candidate states a fact, event, preference, date, or detail that the query asks about or needs.",
    "false": "The candidate is unrelated, or only shares topic words without the needed information.",
}


class Candidate(TypedDict):
    date: str   # ISO 8601, minute precision, UTC
    role: str   # user | assistant | tool | ...
    text: str   # record content, truncated to CANDIDATE_TEXT_CHARS


@runtime_checkable
class Reranker(Protocol):
    """One relevance score per candidate, higher = more relevant, or None
    when no judgement is available (the search then keeps its own order).

    `calibrated` rerankers return probabilities in [0, 1] that mean the same
    thing across queries (what the experimental gated packing thresholds)."""

    name: str
    model: str
    calibrated: bool
    timeout_s: float

    def scores(self, query: str, cands: list[Candidate]) -> list[float] | None: ...


_ROLES = ("user", "assistant", "agent", "system", "tool")


def candidate_from_record(rec: Any) -> Candidate:
    """The reranker's view of a record: when, who, what - nothing else (no
    ids, scopes or metadata leave the engine)."""
    t = _dt.datetime.fromtimestamp(rec.time.t_event / 1000, _dt.timezone.utc)
    role = (rec.provenance.actor_id or "").split(":", 1)[0].lower()
    if role not in _ROLES:
        role = {"USER": "user", "AGENT": "assistant"}.get(rec.provenance.source.name, "tool")
    return Candidate(date=t.isoformat(timespec="minutes"), role=role,
                     text=rec.content[:CANDIDATE_TEXT_CHARS])


def _failure_reason(exc: BaseException) -> str:
    """Bounded label set for memd_rerank_fallback_total{reason}."""
    name = type(exc).__name__
    if isinstance(exc, TimeoutError) or "Timeout" in name:
        return "timeout"
    if "RateLimit" in name:
        return "rate_limited"
    if "Authentication" in name or "PermissionDenied" in name:
        return "auth"
    if isinstance(exc, ConnectionError) or "Connection" in name:
        return "connection"
    if name.startswith("TypeSafe"):
        return "api_error"
    return "error"


class _Failure(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


# ---------------------------------------------------------------- Jev

class JevReranker:
    """TypeSafe System One (Jev): calibrated relevance probabilities.

    ONE request per query carries every candidate in its state and one Noul
    per candidate (fan-out; 1.6x faster than a request per candidate and no
    worse, experiment 015). Requests hold at most JEV_CHUNK candidates; a
    30-candidate shortlist is two requests, issued concurrently. The whole
    call is bounded by timeout_s (SDK retries off: a retry cannot fit).
    Any failure returns None and leaves its reason for RerankStage."""

    name = "jev"
    calibrated = True

    def __init__(self, model: str = DEFAULT_JEV_MODEL, timeout_s: float = DEFAULT_JEV_TIMEOUT_S,
                 api_key: str | None = None, client: Any = None, max_workers: int = 8):
        self.model = model
        self.timeout_s = float(timeout_s)
        self._api_key = api_key
        self._client = client
        self._client_lock = threading.Lock()
        self._warm_lock = threading.Lock()
        self._warming = False
        self._warm_after = 0.0  # monotonic: no new background build before this
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="memd-jev")
        self._local = threading.local()

    def load(self) -> None:
        """Import the SDK and build the client. Importing typesafe_sdk costs
        ~1-1.5s: done inside the first search it consumed the whole 1.5s
        deadline, so the first rerank after open always fell back (seen in
        the live smoke check). Memory calls this on a background thread at
        open; until it is done scores() skips ("warming") and, if no build is
        running, starts one in the background."""
        self._get_client()

    def ready(self) -> bool:
        return self._client is not None

    def _warm(self) -> None:
        """Build the client on a background thread, one build at a time."""
        with self._warm_lock:
            if self._warming or self._client is not None or time.monotonic() < self._warm_after:
                return
            self._warming = True

        def build() -> None:
            try:
                self._get_client()
            except Exception as e:  # noqa: BLE001 - searches keep their own order
                _log.warning("memd: jev client build failed (%s); retrying in %.0fs",
                             type(e).__name__, JEV_WARM_RETRY_S)
                with self._warm_lock:
                    self._warm_after = time.monotonic() + JEV_WARM_RETRY_S
            finally:
                with self._warm_lock:
                    self._warming = False

        threading.Thread(target=build, daemon=True, name="memd-jev-warm").start()

    def _get_client(self) -> Any:
        if self._client is None:
            with self._client_lock:
                if self._client is None:
                    from typesafe_sdk import RetryPolicy, TypeSafeClient

                    self._client = TypeSafeClient(
                        api_key=self._api_key, model=self.model,
                        retry=RetryPolicy(max_retries=0), timeout=self.timeout_s)
        return self._client

    def _chunk(self, query: str, cands: list[Candidate], deadline: float) -> list[float]:
        if time.monotonic() >= deadline:
            # queued behind other requests until scores() gave up: never
            # send the texts for a search that has already returned
            raise _Failure("timeout")
        from typesafe_sdk import Noul

        state = {"query": query, "candidates": {f"c{i}": c for i, c in enumerate(cands)}}
        questions = {
            f"c{i}": Noul(instructions=JEV_INSTRUCTIONS.format(cid=f"c{i}"), criteria=JEV_CRITERIA)
            for i in range(len(cands))
        }
        client = self._get_client()
        if time.monotonic() >= deadline:
            # checked again AFTER everything that can take time and right
            # before the send: checked only before the client build (~1-1.5s
            # when cold), texts left the machine 0.7s after their search
            # had returned
            raise _Failure("timeout")
        resp = client.system_one(state=state, questions=questions, model=self.model,
                                 timeout=self.timeout_s)
        nouls = resp.nouls
        return [float(nouls[f"c{i}"].noul) for i in range(len(cands))]

    def scores(self, query: str, cands: list[Candidate]) -> list[float] | None:
        self._local.reason = None
        if not cands:
            return []
        if self._client is None:
            # never build it on a search's deadline (see load())
            self._warm()
            self._local.reason = "warming"
            return None
        try:
            deadline = time.monotonic() + self.timeout_s
            futs = [self._pool.submit(self._chunk, query, cands[i:i + JEV_CHUNK], deadline)
                    for i in range(0, len(cands), JEV_CHUNK)]
            done, pending = _wait_futures(futs, timeout=self.timeout_s)
            if pending:
                for f in pending:
                    f.cancel()
                raise _Failure("timeout")
            out: list[float] = []
            for f in futs:
                out.extend(f.result())
            return out
        except _Failure as e:
            self._local.reason = e.reason
        except Exception as e:  # noqa: BLE001 - a remote judge must never fail search
            self._local.reason = _failure_reason(e)
            _log.debug("memd: jev rerank failed: %s", type(e).__name__)
        return None

    def last_failure_reason(self) -> str | None:
        """Why the last scores() call on THIS thread returned None."""
        return getattr(self._local, "reason", None)

    def close(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)
        closer = getattr(self._client, "close", None)
        if callable(closer):
            try:
                closer()  # its HTTP connection pool
            except Exception:
                pass


# ---------------------------------------------------------------- local cross-encoder

class LocalCrossEncoderReranker:
    """fastembed TextCrossEncoder, loaded lazily and shared per process.

    Scores are sigmoid(logit): monotone in the model's logit, so the order is
    the model's, and the scale is positive (the recency tilt multiplies it).
    They are NOT calibrated probabilities, so gated packing does not use them
    by default."""

    name = "local"
    calibrated = False

    def __init__(self, model: str = DEFAULT_LOCAL_RERANK_MODEL,
                 timeout_s: float = DEFAULT_LOCAL_TIMEOUT_S, threads: int | None = None):
        self.model = model
        self.timeout_s = float(timeout_s)
        self.threads = int(threads) if threads else default_embed_threads()
        # the same process-wide cache the local embedder uses, in its own
        # key space: every Memory instance shares one loaded cross-encoder
        self._shared = _shared_model(model, kind="rerank")
        self._load_error: Exception | None = None
        self._local = threading.local()

    def _load_model(self) -> Any:
        from fastembed.rerank.cross_encoder import TextCrossEncoder

        return TextCrossEncoder(model_name=self.model, threads=self.threads)

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
                raise RuntimeError(f"cross-encoder {self.model!r} failed to load") from self._load_error
            try:
                shared.model = self._load_model()
            except Exception as e:
                self._load_error = e
                raise

    def scores(self, query: str, cands: list[Candidate]) -> list[float] | None:
        self._local.reason = None
        if not self.ready():
            # the model loads in the background (see Memory); until then the
            # search keeps its own order rather than waiting seconds on it
            self._local.reason = "not_ready"
            return None
        docs = [f"[{c['date']}] {c['role']}: {c['text']}" for c in cands]
        with self._shared.embed_lock:
            logits = list(self._shared.model.rerank(query, docs, batch_size=32))
        return [1.0 / (1.0 + math.exp(-max(-60.0, min(60.0, float(x))))) for x in logits]

    def last_failure_reason(self) -> str | None:
        return getattr(self._local, "reason", None)

    def close(self) -> None:
        return None


# ---------------------------------------------------------------- selection

def typesafe_available() -> bool:
    """Whether typesafe-sdk is installed, without importing it."""
    return importlib.util.find_spec("typesafe_sdk") is not None


def _typesafe_key(cfg: dict) -> str | None:
    return cfg.get("typesafe_api_key") or os.environ.get("TYPESAFE_API_KEY") or None


def requested_reranker(config: dict | None = None) -> str:
    """config["reranker"], else env MEMD_RERANKER, else "auto". Unknown
    values raise rather than silently meaning "auto"."""
    cfg = config or {}
    choice = str(cfg.get("reranker") or os.environ.get("MEMD_RERANKER") or "auto").strip().lower()
    if choice not in RERANKER_CHOICES:
        raise ValueError(f"unknown reranker {choice!r}; expected one of {list(RERANKER_CHOICES)}")
    return choice


def resolve_reranker(config: dict | None = None) -> Reranker | None:
    cfg = config or {}
    choice = requested_reranker(cfg)
    if choice == "auto":
        choice = "jev" if (_typesafe_key(cfg) and typesafe_available()) else "none"
    if choice == "none":
        return None
    if choice == "jev":
        if not typesafe_available():
            raise ImportError("reranker 'jev' was requested but `typesafe-sdk` is not importable; "
                              "install the optional extra (memd[jev]) or choose reranker='none'")
        key = _typesafe_key(cfg)
        if not key:
            raise ValueError("reranker 'jev' requires TYPESAFE_API_KEY in the environment")
        return JevReranker(model=str(cfg.get("jev_model") or DEFAULT_JEV_MODEL),
                           timeout_s=float(cfg.get("rerank_timeout_s") or DEFAULT_JEV_TIMEOUT_S),
                           api_key=key)
    # "local"
    if not fastembed_available():
        raise ImportError("reranker 'local' was requested but the `fastembed` package is not "
                          "importable; install memd[local-embeddings] or choose reranker='none'")
    return LocalCrossEncoderReranker(
        model=str(cfg.get("local_rerank_model") or DEFAULT_LOCAL_RERANK_MODEL),
        timeout_s=float(cfg.get("rerank_timeout_s") or DEFAULT_LOCAL_TIMEOUT_S),
        threads=resolve_embed_threads(cfg))


# ---------------------------------------------------------------- the stage

class RerankStage:
    """Runs a reranker for search under a hard deadline. Never raises
    (except KeyboardInterrupt / SystemExit from inside the reranker).

    The call runs on a small pool so a reranker that ignores its own timeout
    (or blocks in C) cannot hold the search past the deadline; the late
    answer is discarded. At most `max_inflight` calls (default: the pool
    size) run at once, abandoned ones included: past that, reranking is
    skipped at once ("busy"), so a stuck reranker costs a bounded number of
    threads and requests, never a growing backlog. Every fallback is
    counted by reason."""

    def __init__(self, reranker: Reranker, *, timeout_s: float | None = None,
                 k: int = DEFAULT_RERANK_K, max_workers: int = 8, max_inflight: int | None = None):
        self.reranker = reranker
        self.timeout_s = float(timeout_s if timeout_s is not None
                               else getattr(reranker, "timeout_s", DEFAULT_JEV_TIMEOUT_S))
        self.k = int(k)
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="memd-rerank")
        # a slot is held from submit until the call RETURNS, not until its
        # search stops waiting: abandoned calls still count
        self.max_inflight = max(1, int(max_inflight if max_inflight is not None else max_workers))
        self._slots = threading.BoundedSemaphore(self.max_inflight)
        self._lock = threading.Lock()
        self._calls = 0
        self._fallbacks = 0
        self._lat_ms: deque[float] = deque(maxlen=512)

    @property
    def name(self) -> str:
        return str(getattr(self.reranker, "name", type(self.reranker).__name__))

    @property
    def model(self) -> str:
        return str(getattr(self.reranker, "model", ""))

    @property
    def calibrated(self) -> bool:
        return bool(getattr(self.reranker, "calibrated", False))

    def _invoke(self, query: str, cands: list[Candidate],
                deadline: float) -> tuple[list[float] | None, str | None]:
        """Runs on the pool thread: scores() and its failure reason are read
        on the same thread (the reason is thread-local on the reranker)."""
        try:
            if time.monotonic() >= deadline:
                # its search already returned (queued behind abandoned calls):
                # starting now would only send the texts for nothing
                return None, "timeout"
            try:
                out = self.reranker.scores(query, cands)
            except (KeyboardInterrupt, SystemExit):
                raise  # deliberately not contained: the process is being stopped
            except BaseException as e:  # noqa: BLE001 - a reranker must never fail search
                # (BaseException: a native panic or a stray GeneratorExit too)
                return None, _failure_reason(e)
            return self._validate(out, cands)
        finally:
            self._slots.release()

    def _validate(self, out: Any, cands: list[Candidate]) -> tuple[list[float] | None, str | None]:
        if out is None:
            why = getattr(self.reranker, "last_failure_reason", None)
            return None, (why() if callable(why) else None) or "unavailable"
        try:
            vals = [float(x) for x in out]
        except (TypeError, ValueError):
            return None, "bad_response"
        if len(vals) != len(cands) or not all(math.isfinite(v) for v in vals):
            return None, "bad_response"
        return vals, None

    def run(self, query: str, cands: list[Candidate], *, ns: str = "") -> list[float] | None:
        """Scores aligned with `cands`, or None: keep the unreranked order."""
        if not cands:
            return []
        t0 = time.monotonic()
        reason: str | None
        if not self._slots.acquire(blocking=False):
            # max_inflight calls are still running (timed out, most likely):
            # skip at once rather than queue behind them
            vals, reason = None, "busy"
        else:
            try:
                fut = self._pool.submit(self._invoke, query, cands, t0 + self.timeout_s)
            except BaseException as e:  # noqa: BLE001 - pool shut down, etc.
                self._slots.release()  # never submitted: _invoke will not release it
                if isinstance(e, (KeyboardInterrupt, SystemExit)):
                    raise
                vals, reason = None, _failure_reason(e)
            else:
                try:
                    vals, reason = fut.result(timeout=self.timeout_s)
                except _FutureTimeout:
                    vals, reason = None, "timeout"
                except (KeyboardInterrupt, SystemExit):
                    raise  # raised inside the reranker, re-raised on purpose (see _invoke)
                except BaseException as e:  # noqa: BLE001 - cancelled, etc.
                    vals, reason = None, _failure_reason(e)
        ms = (time.monotonic() - t0) * 1000
        with self._lock:
            self._calls += 1
            if vals is None:
                self._fallbacks += 1
            else:
                self._lat_ms.append(ms)
        METRICS.inc("memd_rerank_calls_total", help="searches that asked the reranker",
                    ns=ns, reranker=self.name)
        METRICS.observe("memd_rerank_ms", ms, help="reranker call duration (ms), including fallbacks",
                        ns=ns, reranker=self.name)
        if vals is None:
            METRICS.inc("memd_rerank_fallback_total",
                        help="searches that kept the unreranked order: reranker failed, timed out or was not ready",
                        ns=ns, reranker=self.name, reason=reason or "unavailable")
        return vals

    def stats(self) -> dict:
        with self._lock:
            lat = sorted(self._lat_ms)
            calls, fallbacks = self._calls, self._fallbacks
        p50 = round(lat[len(lat) // 2], 3) if lat else None
        return {"name": self.name, "model": self.model, "calibrated": self.calibrated,
                "calls": calls, "fallbacks": fallbacks, "p50_ms": p50,
                "timeout_s": self.timeout_s, "k": self.k, "max_inflight": self.max_inflight}

    def close(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)
        closer = getattr(self.reranker, "close", None)
        if callable(closer):
            try:
                closer()
            except Exception:
                pass
