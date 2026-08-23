"""Consolidation (session-end, conservative) and quarantine heuristics.

Consolidation rules (survey-backed: conservative beats aggressive merging,
and resolution must not be deferred past the session boundary):
  - exact / near-duplicate facts within an entity cluster are dropped
  - a new fact on an existing entity_key supersedes the old one
    (cluster-local tombstone; O(cluster), never O(namespace))
  - nothing is merged into new sentences; nothing crosses clusters

Quarantine (D7 control #3, ADR-10 - cheapest-to-reverse, iterate behind the
adversarial gate): suspect writes are stored but excluded from retrieval
until review or decay expiry.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from memd.core.schema import MemoryRecord, Source, now_ms
from memd.pipeline.extractor import ExtractedFact

_WORD = re.compile(r"[a-z0-9]+")


def _tokens(s: str) -> set[str]:
    return set(_WORD.findall(s.lower()))


def near_duplicate(a: str, b: str, threshold: float = 0.85) -> bool:
    ta, tb = _tokens(a), _tokens(b)
    if not ta or not tb:
        return False
    j = len(ta & tb) / len(ta | tb)
    return j >= threshold


@dataclass
class ConsolidationResult:
    kept: list[MemoryRecord] = field(default_factory=list)
    superseded_pairs: list[tuple[str, str]] = field(default_factory=list)  # (old_id, new_id)
    dropped_dupes: int = 0


def consolidate_facts(
    existing_cluster: list[MemoryRecord],
    new_facts: list[ExtractedFact],
    make_record,  # callable(fact) -> MemoryRecord
) -> ConsolidationResult:
    """Cluster-local consolidation at segment close.

    `existing_cluster` = current valid facts sharing the entity keys of the
    incoming facts (fetched by the caller via the entity index - bounded)."""
    res = ConsolidationResult()
    pending: list[MemoryRecord] = [make_record(f) for f in new_facts]
    # intra-batch dedup
    unique: list[MemoryRecord] = []
    for r in pending:
        if any(near_duplicate(r.content, u.content) for u in unique):
            res.dropped_dupes += 1
            continue
        unique.append(r)
    for rec in unique:
        supersede_target: MemoryRecord | None = None
        for old in existing_cluster:
            if old.id == rec.id or old.deleted:
                continue
            if not (set(old.entity_keys) & set(rec.entity_keys)):
                continue
            if near_duplicate(old.content, rec.content):
                res.dropped_dupes += 1
                supersede_target = None
                rec = None  # duplicate of existing; drop entirely
                break
            # different content, same entity key => newer wins (bitemporal supersedence)
            if supersede_target is None or old.time.t_event >= supersede_target.time.t_event:
                supersede_target = old
        if rec is None:
            continue
        if supersede_target is not None:
            res.superseded_pairs.append((supersede_target.id, rec.id))
        res.kept.append(rec)
        # the new record joins the cluster for subsequent comparisons
        existing_cluster = existing_cluster + [rec]
    return res


# ---------------------------------------------------------------------------


@dataclass
class QuarantineVerdict:
    quarantined: bool
    reason: str | None = None
    expires_ms: int | None = None


class QuarantinePolicy:
    """Anomaly checks on the async path (MINJA's repeated-injection shape:
    many near-identical untrusted writes in a short window). All tracking
    structures are bounded: windows prune per call, and the content-bucket
    map evicts oldest keys beyond `max_buckets`."""

    def __init__(
        self,
        rate_window_ms: int = 60_000,
        rate_max_writes: int = 120,
        dup_window_ms: int = 600_000,
        dup_max_repeats: int = 5,
        default_ttl_ms: int = 24 * 3600 * 1000,
        max_buckets: int = 50_000,
        max_actors: int = 10_000,
    ):
        self.rate_window_ms = rate_window_ms
        self.rate_max_writes = rate_max_writes
        self.dup_window_ms = dup_window_ms
        self.dup_max_repeats = dup_max_repeats
        self.default_ttl_ms = default_ttl_ms
        self.max_buckets = max_buckets
        self.max_actors = max_actors
        self._actor_events: dict[str, list[int]] = {}
        self._content_events: dict[str, list[tuple[int, str]]] = {}

    def check(self, records: list[MemoryRecord]) -> dict[str, QuarantineVerdict]:
        now = now_ms()
        verdicts: dict[str, QuarantineVerdict] = {}
        by_actor: dict[str, list[MemoryRecord]] = {}
        for r in records:
            actor = r.provenance.actor_id or f"src:{r.provenance.source.name}"
            by_actor.setdefault(actor, []).append(r)
        for actor, recs in by_actor.items():
            # Rate-limit quarantine targets untrusted/automated writers
            # (MINJA shape). Human-tier bulk imports (USER/AGENT only) are a
            # legitimate pattern - D7 scopes out self-poisoning of one's own
            # namespace.
            min_tier = min(int(r.provenance.source) for r in recs)
            if min_tier >= int(Source.AGENT):
                continue
            events = [t for t in self._actor_events.get(actor, []) if t > now - self.rate_window_ms]
            events.extend([r.time.t_ingested for r in recs])
            if len(self._actor_events) >= self.max_actors and actor not in self._actor_events:
                self._actor_events.pop(next(iter(self._actor_events)), None)
            self._actor_events[actor] = events
            if len(events) > self.rate_max_writes:
                for r in recs:
                    verdicts[r.id] = QuarantineVerdict(
                        True, "actor_rate_limit", now + self.default_ttl_ms
                    )
        # repeated identical/near-identical content from low-trust sources
        for r in records:
            if r.id in verdicts:
                continue
            if int(r.provenance.source) > 2:  # USER/AGENT exempt from dup heuristic
                continue
            toks = [
                t.rstrip("0123456789")
                for t in _tokens(r.content)
                if len(t.rstrip("0123456789")) > 2
            ]
            key_tokens = "|".join(sorted(toks)[:12])
            bucket = self._content_events.get(key_tokens)
            if bucket is not None:
                bucket[:] = [(t, rid) for t, rid in bucket if t > now - self.dup_window_ms]
            if len(self._content_events) >= self.max_buckets and key_tokens not in self._content_events:
                self._content_events.pop(next(iter(self._content_events)), None)
            bucket = self._content_events.setdefault(key_tokens, [])
            bucket.append((r.time.t_ingested, r.id))
            if len(bucket) > self.dup_max_repeats:
                verdicts[r.id] = QuarantineVerdict(
                    True, "repeated_untrusted_content", now + self.default_ttl_ms
                )
        ok = QuarantineVerdict(False)
        for r in records:
            verdicts.setdefault(r.id, ok)
        return verdicts
