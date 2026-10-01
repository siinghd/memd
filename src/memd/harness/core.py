"""Harness core: frozen configs, version hash, adapters, metrics.

Every result embeds HARNESS_VERSION - the content hash of the harness
package. Quality and cost are emitted together; the gate fails on accuracy
regressions or >20% cost regressions vs baseline.
"""
from __future__ import annotations

import hashlib
import json
import os
import statistics
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Callable

HARNESS_VERSION = hashlib.sha256(
    b"\n".join(sorted(p.read_bytes() for p in Path(__file__).parent.rglob("*.py")))
).hexdigest()[:16]

# Frozen judge config (offline heuristic judge by default; swap via env for a
# pinned LLM judge - same prompt across runs is what matters).
JUDGE_CONFIG = {
    "judge": os.environ.get("MEMD_JUDGE", "heuristic-v1"),
    "answer_model": os.environ.get("MEMD_ANSWER_MODEL", "heuristic-extract"),
    "temperature": 0,
}

COST_PER_1M_INPUT_TOKENS = 0.02  # text-embedding-3-small class, for COGS math


@dataclass
class CaseResult:
    case_id: str
    qclass: str
    correct: bool
    score: float
    tokens_injected: int
    latency_ms: float
    detail: dict = field(default_factory=dict)


@dataclass
class SuiteReport:
    suite: str
    system: str
    harness_version: str
    results: list[CaseResult] = field(default_factory=list)
    started: float = field(default_factory=time.time)
    duration_s: float = 0.0

    def add(self, r: CaseResult) -> None:
        self.results.append(r)

    def summary(self) -> dict:
        n = len(self.results) or 1
        acc = sum(r.correct for r in self.results) / n
        lat = [r.latency_ms for r in self.results]
        toks = [r.tokens_injected for r in self.results]
        return {
            "suite": self.suite,
            "system": self.system,
            "harness_version": self.harness_version,
            "cases": len(self.results),
            "accuracy": round(acc, 4),
            "tokens_per_query_avg": round(statistics.mean(toks), 1) if toks else 0,
            "latency_p50_ms": round(statistics.median(lat), 2) if lat else 0,
            "latency_p95_ms": round(sorted(lat)[int(0.95 * (len(lat) - 1))], 2) if lat else 0,
            "duration_s": round(self.duration_s, 3),
        }


class SystemAdapter:
    """One adapter interface: add_events / search / answer."""

    name: str = "base"

    def setup(self) -> None: ...

    def teardown(self) -> None: ...

    def add_events(self, events: list[dict]) -> None: ...

    def search(self, query: str, budget_tokens: int = 2000) -> str:
        raise NotImplementedError

    def answer(self, query: str, context: str) -> str:
        """Offline extractive 'answerer': TF-IDF-weighted query coverage over
        context sentences (light-stemmed). Deterministic; the *same* function
        grades every system, so comparisons are fair."""
        import math
        import re

        def stems(s: str) -> set[str]:
            out: set[str] = set()
            for w in re.findall(r"[a-z0-9]+", s.lower()):
                out.add(w)
                for suf in ("ing", "es", "ed", "s"):
                    if len(w) > len(suf) + 1 and w.endswith(suf):
                        out.add(w[: -len(suf)])
                out.add(w)
            return out

        sents = [s for s in re.split(r"(?<=[.!?])\s+|\n", context) if s.strip()]
        if not sents:
            return ""
        sent_tokens = [set(stems(s)) for s in sents]
        n = len(sents)
        df: dict[str, int] = {}
        for toks in sent_tokens:
            for t in toks:
                df[t] = df.get(t, 0) + 1
        q = set(stems(query))
        if not q:
            return sents[0].strip()

        def idf(t: str) -> float:
            return math.log((n + 1) / (df.get(t, 0) + 1)) + 0.1

        best, best_score = sents[0], -1.0
        for s, toks in zip(sents, sent_tokens):
            score = sum(idf(t) for t in q if t in toks) / sum(idf(t) for t in q)
            if score > best_score:
                best, best_score = s, score
        return best.strip()


class MemdAdapter(SystemAdapter):
    name = "memd"

    def __init__(self, data_dir: str, config: dict | None = None):
        self.data_dir = data_dir
        # bulk historical loads are not an attack pattern; the adversarial
        # suite runs its own strictly-limited instance
        self.config = {"rate_max_writes": 10**9, "dup_max_repeats": 10**9, **(config or {})}
        self._mem = None

    def setup(self) -> None:
        from memd.engine.memory import Memory

        self._mem = Memory(self.data_dir, config=self.config)

    def teardown(self) -> None:
        if self._mem:
            self._mem.close()

    def add_events(self, events: list[dict]) -> None:
        assert self._mem
        for e in events:
            self._mem.add(
                e["content"], session_id=e.get("session_id"), user_id=e.get("user_id"),
                role=e.get("role", "user"), t_event=e.get("t_event"), source=e.get("source"),
                actor_id=e.get("actor_id"),
            )

    def search(self, query: str, budget_tokens: int = 2000) -> str:
        assert self._mem
        res = self._mem.search(query, user_id=self.user_id, budget_tokens=budget_tokens)
        return res.packed_context

    user_id: str = "u1"


class FullContextAdapter(MemdAdapter):
    """The baseline to beat: stuff everything into context.
    Accuracy ceiling at ~100x the token cost."""

    name = "full-context"

    def __init__(self, data_dir: str, token_budget: int = 10**9):
        super().__init__(data_dir)
        self.token_budget = token_budget

    def search(self, query: str, budget_tokens: int = 2000) -> str:
        assert self._mem
        from memd.core.schema import Scope
        from memd.index.sqlite_index import IndexFilter

        recs = self._mem.ns.index.query_records(
            IndexFilter(scope=Scope(user=self.user_id)),
            limit=10_000,
        )
        lines = [r.content for r in recs]
        out: list[str] = []
        used = 0
        for ln in lines:
            c = max(1, (len(ln) + 3) // 4)
            if used + c > min(budget_tokens, self.token_budget):
                break
            out.append(ln)
            used += c
        return "\n".join(out)


class PlainRagAdapter(MemdAdapter):
    """Plain RAG baseline: single-lane lexical retrieval, no fusion/validity/
    packing structure. This is the thing raw+hybrid must beat."""

    name = "plain-rag"

    def search(self, query: str, budget_tokens: int = 2000) -> str:
        assert self._mem
        from memd.core.schema import Scope
        from memd.index.sqlite_index import IndexFilter

        hits = self._mem.ns.index.search_bm25(
            query, IndexFilter(scope=Scope(user=self.user_id)), limit=20
        )
        out, used = [], 0
        for h in hits:
            c = max(1, (len(h.record.content) + 3) // 4)
            if used + c > budget_tokens:
                break
            out.append(h.record.content)
            used += c
        return "\n".join(out)


def run_case(
    report: SuiteReport,
    adapter: SystemAdapter,
    case: dict,
    check_fn: Callable[[str, dict], tuple[bool, float]],
    budget_tokens: int = 2000,
) -> CaseResult:
    t0 = time.monotonic()
    ctx = adapter.search(case["query"], budget_tokens=budget_tokens)
    ans = adapter.answer(case["query"], ctx)
    latency = (time.monotonic() - t0) * 1000
    ok, score = check_fn(ans, case)
    r = CaseResult(
        case_id=case["id"],
        qclass=case.get("qclass", "unknown"),
        correct=ok,
        score=score,
        tokens_injected=max(1, (len(ctx) + 3) // 4),
        latency_ms=round(latency, 3),
        detail={"answer": ans[:160], "expected_hint": case.get("expected")},
    )
    report.add(r)
    return r


def write_results(reports: list[SuiteReport], out_dir: str) -> str:
    os.makedirs(out_dir, exist_ok=True)
    stamp = time.strftime("%Y%m%dT%H%M%S")
    path = os.path.join(out_dir, f"results-{stamp}.json")
    payload = {
        "harness_version": HARNESS_VERSION,
        "judge_config": JUDGE_CONFIG,
        "suites": [r.summary() for r in reports],
        "cases": [asdict(x) for r in reports for x in r.results],
    }
    with open(path, "w") as f:
        json.dump(payload, f, indent=1)
    return path
