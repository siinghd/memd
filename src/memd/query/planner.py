"""Query planner (D3 §3.5): rules + optional tiny classifier -> strategy.

Explicitly NO reflection loop (survey finding: planning helps, reflection
doesn't). Output: query class, lane weights, filter hints.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

TEMPORAL_RE = re.compile(
    r"\b(when|yesterday|today|tomorrow|last (?:week|month|year)|next (?:week|month|year)|"
    r"\d{4}-\d{2}-\d{2}|before|after|ago|recently|since|until)\b",
    re.I,
)
PROCEDURAL_RE = re.compile(
    r"\b(how (?:do|to|did|can)|steps?|procedure|command|deploy|build|run|install|"
    r"configure|setup|set up|workflow|instructions)\b",
    re.I,
)
FACTUAL_RE = re.compile(r"\b(what|who|where|which|whose|why)\b", re.I)


@dataclass
class QueryPlan:
    query: str
    qclass: str  # factual | temporal | procedural | exploratory
    weights: dict[str, float]  # lane -> weight
    candidate_k: int
    t_event_min: int | None = None
    t_event_max: int | None = None
    kinds: tuple[str, ...] | None = None
    notes: list[str] = field(default_factory=list)


def classify(query: str) -> str:
    if TEMPORAL_RE.search(query):
        return "temporal"
    if PROCEDURAL_RE.search(query):
        return "procedural"
    if FACTUAL_RE.search(query):
        return "factual"
    return "exploratory"


def plan_query(query: str, candidate_k: int = 40) -> QueryPlan:
    qc = classify(query)
    base = {"vector": 1.0, "bm25": 1.0, "time": 0.4, "entity": 0.6}
    kinds = None
    if qc == "temporal":
        base.update(time=1.4, bm25=1.2, vector=0.8)
    elif qc == "procedural":
        base.update(vector=0.9, bm25=1.3, time=0.5)
        kinds = ("raw_event", "procedure", "fact", "summary")
    elif qc == "exploratory":
        base.update(vector=1.2, bm25=0.8, time=0.6, entity=0.8)
        candidate_k = int(candidate_k * 1.5)
    return QueryPlan(query=query, qclass=qc, weights=base, candidate_k=candidate_k, kinds=kinds)
