"""RRF fusion + trust-aware scoring."""
from __future__ import annotations

import hashlib
from collections import Counter
from dataclasses import dataclass
from typing import Callable, TypeVar

from memd.core.schema import MemoryRecord
from memd.index.sqlite_index import Hit

RRF_K = 60

T = TypeVar("T")


def content_sha(content: str) -> str:
    """Stable content hash for tie-breaks: identical data orders identically
    no matter when (or in what id order) it was ingested."""
    return hashlib.blake2b(content.encode(), digest_size=8).hexdigest()


def deterministic_order(items: list[T], score: Callable[[T], float],
                        record: Callable[[T], MemoryRecord]) -> list[T]:
    """Sort by (-score, -t_event, content_sha, id).

    Ties used to fall to the ULID record id (and ingestion time), which is
    random within a millisecond: re-ingesting identical data changed the
    top-10 for ~23% of queries. The content hash is computed lazily - only
    for items whose (score, t_event) actually collides with another item;
    for every other item the hash could never be compared, so "" stands in
    without changing the order."""
    keyed = [(score(it), record(it), it) for it in items]
    collide = Counter((s, r.time.t_event) for s, r, _ in keyed)

    def key(t: tuple[float, MemoryRecord, T]) -> tuple:
        s, r, _ = t
        h = content_sha(r.content) if collide[(s, r.time.t_event)] > 1 else ""
        return (-s, -r.time.t_event, h, r.id)

    return [it for _, _, it in sorted(keyed, key=key)]


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
    ranked = deterministic_order(list(items.values()), lambda it: it.score, lambda it: it.record)
    return ranked[:limit]


def recency_boost(score: float, t_event_ms: int, now_ms: int, half_life_days: float = 365.0) -> float:
    """Gentle recency tilt for long-horizon memory: a year-old fact keeps
    half its score. Decay is demotion, not deletion."""
    age_days = max(0.0, (now_ms - t_event_ms) / 86_400_000)
    return score * (0.5 ** (age_days / half_life_days))
