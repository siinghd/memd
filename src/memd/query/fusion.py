"""RRF fusion + trust-aware scoring (D3 §3.5 step 3, D7 control #5)."""
from __future__ import annotations

from dataclasses import dataclass

from memd.core.schema import MemoryRecord
from memd.index.sqlite_index import Hit

RRF_K = 60


@dataclass
class FusedItem:
    record: MemoryRecord
    score: float
    lanes: list[str]
    ranks: dict[str, int]


def rrf_fuse(
    lane_hits: dict[str, list[Hit]],
    weights: dict[str, float] | None = None,
    limit: int = 50,
    trust_bonus: float = 0.01,
) -> list[FusedItem]:
    """Reciprocal-rank fusion with per-lane weights, a small monotone trust
    bonus, and a mild consolidated-knowledge bonus (facts outrank their own
    sources on ties - never enough to beat strong relevance evidence)."""
    weights = weights or {}
    kind_bonus = {"fact": 1.15, "summary": 1.05, "pin": 1.2}
    scores: dict[str, float] = {}
    items: dict[str, FusedItem] = {}
    for lane, hits in lane_hits.items():
        w = float(weights.get(lane, 1.0))
        for rank, hit in enumerate(hits):
            rid = hit.record.id
            contrib = w / (RRF_K + rank + 1)
            bonus = trust_bonus * (int(hit.record.provenance.source) - 3)
            contrib *= max(1.0 + max(bonus, 0.0), kind_bonus.get(hit.record.kind, 1.0))
            scores[rid] = scores.get(rid, 0.0) + contrib
            if rid in items:
                if lane not in items[rid].lanes:
                    items[rid].lanes.append(lane)
                items[rid].ranks[lane] = rank + 1
                items[rid].score = scores[rid]
            else:
                items[rid] = FusedItem(
                    record=hit.record, score=scores[rid], lanes=[lane], ranks={lane: rank + 1}
                )
    ranked = sorted(items.values(), key=lambda it: (-it.score, it.record.id))
    return ranked[:limit]


def recency_boost(score: float, t_event_ms: int, now_ms: int, half_life_days: float = 365.0) -> float:
    """Gentle recency tilt for long-horizon memory: a year-old fact keeps
    half its score. Decay is demotion, not deletion (D3 §3.6)."""
    age_days = max(0.0, (now_ms - t_event_ms) / 86_400_000)
    return score * (0.5 ** (age_days / half_life_days))
