"""memd.core.schema - the memory record model (ADR-1).

A memory is an immutable, provenance-carrying, bitemporal record.
Two lanes share one schema: raw events (verbatim capture) and derived
records (facts, links, procedures, summaries, pins).
"""
from __future__ import annotations

import json
import os
import secrets
import time as _time
from dataclasses import dataclass, field, replace
from enum import IntEnum
from typing import Any, Iterable, Sequence

# ---------------------------------------------------------------------------
# ULID: 48-bit ms timestamp + 80 random bits, Crockford base32, sortable.

_B32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def ulid_new(ts_ms: int | None = None) -> str:
    if ts_ms is None:
        ts_ms = int(_time.time() * 1000)
    val = (ts_ms & 0xFFFFFFFFFFFF) << 80
    val |= int.from_bytes(secrets.token_bytes(10), "big")
    out = []
    for shift in range(125, -5, -5):
        out.append(_B32[(val >> shift) & 0x1F])
    return "".join(out)


def ulid_ts_ms(u: str) -> int:
    v = 0
    for c in u[:10]:
        v = (v << 5) | _B32.index(c)
    return v


def now_ms() -> int:
    return int(_time.time() * 1000)


# ---------------------------------------------------------------------------
# Trust tiers (D7 control #2): user > agent > tool > web > import.


class Source(IntEnum):
    USER = 5
    AGENT = 4
    TOOL = 3
    WEB = 2
    IMPORT = 1

    @classmethod
    def parse(cls, v: Any) -> "Source":
        if isinstance(v, cls):
            return v
        s = str(v).strip().upper()
        try:
            return cls[s]
        except KeyError:
            raise ValueError(f"unknown source {v!r}; expected one of {[m.name for m in cls]}")


class Kind:
    RAW_EVENT = "raw_event"
    FACT = "fact"
    LINK = "link"
    PROCEDURE = "procedure"
    SUMMARY = "summary"
    PIN = "pin"
    ALL = (RAW_EVENT, FACT, LINK, PROCEDURE, SUMMARY, PIN)


class LinkType:
    SAME_ENTITY = "same_entity"
    CAUSED_BY = "caused_by"
    REFERS_TO = "refers_to"
    FOLLOWS = "follows"


# ---------------------------------------------------------------------------
# Scope: hierarchical tenancy inside a namespace. A query at scope S sees
# records whose scope is S or an ancestor of S (session -> user -> agent -> org).


@dataclass(frozen=True)
class Scope:
    org: str | None = None
    agent: str | None = None
    user: str | None = None
    session: str | None = None

    def normalized(self) -> "Scope":
        return self

    def contains(self, other: "Scope") -> bool:
        """True if `other` (a record's scope) is visible from `self` (the query
        scope). A record binds only the components it sets; the query must
        match those or leave them unconstrained. So a session query sees its
        session + ancestors; a user query sees all that user's sessions."""
        for f in ("org", "agent", "user", "session"):
            mine = getattr(self, f)
            theirs = getattr(other, f)
            if mine is not None and theirs is not None and mine != theirs:
                return False
        return True

    def is_within(self, ancestor: "Scope") -> bool:
        return ancestor.contains(self)

    def to_dict(self) -> dict[str, str]:
        return {k: v for k, v in self.__dict__.items() if v is not None}

    @classmethod
    def from_dict(cls, d: dict | None) -> "Scope":
        d = d or {}
        return cls(org=d.get("org"), agent=d.get("agent"), user=d.get("user"), session=d.get("session"))


# ---------------------------------------------------------------------------
# Provenance & time.


@dataclass
class ExtractorInfo:
    model: str
    prompt_version: str

    def to_dict(self) -> dict:
        return {"model": self.model, "prompt_version": self.prompt_version}

    @classmethod
    def from_dict(cls, d: dict | None) -> "ExtractorInfo | None":
        if not d:
            return None
        return cls(model=d.get("model", ""), prompt_version=d.get("prompt_version", ""))


@dataclass
class Provenance:
    source: Source
    actor_id: str | None = None
    session_id: str | None = None
    lineage: list[str] = field(default_factory=list)  # record ids this was derived from
    extractor: ExtractorInfo | None = None

    def to_dict(self) -> dict:
        return {
            "source": self.source.name.lower(),
            "actor_id": self.actor_id,
            "session_id": self.session_id,
            "lineage": list(self.lineage),
            "extractor": self.extractor.to_dict() if self.extractor else None,
        }

    @classmethod
    def from_dict(cls, d: dict | None) -> "Provenance":
        d = d or {}
        ex = ExtractorInfo.from_dict(d.get("extractor"))
        lineage = d.get("lineage") or []
        src = d.get("source", "import")
        return cls(
            source=src if isinstance(src, Source) else Source.parse(src),
            actor_id=d.get("actor_id"),
            session_id=d.get("session_id"),
            lineage=[str(x) for x in lineage],
            extractor=ex,
        )


@dataclass
class TimeAxis:
    t_event: int  # when it happened / is true about the world
    t_ingested: int  # when we learned it
    valid_from: int | None = None  # fact validity window start
    invalidated_at: int | None = None  # fact validity window end (supersedence)
    superseded_by: str | None = None  # id of replacing record

    def to_dict(self) -> dict:
        return {
            "t_event": self.t_event,
            "t_ingested": self.t_ingested,
            "valid_from": self.valid_from,
            "invalidated_at": self.invalidated_at,
            "superseded_by": self.superseded_by,
        }

    @classmethod
    def from_dict(cls, d: dict | None) -> "TimeAxis":
        d = d or {}
        now = now_ms()
        return cls(
            t_event=int(d.get("t_event", now)),
            t_ingested=int(d.get("t_ingested", now)),
            valid_from=d.get("valid_from"),
            invalidated_at=d.get("invalidated_at"),
            superseded_by=d.get("superseded_by"),
        )


# ---------------------------------------------------------------------------
# The record itself.


@dataclass
class MemoryRecord:
    id: str
    namespace: str
    kind: str
    content: str
    scope: Scope
    provenance: Provenance
    time: TimeAxis
    entity_keys: list[str] = field(default_factory=list)
    embedding_version: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)
    deleted: bool = False  # tombstone flag (read path filters synchronously)

    @staticmethod
    def create(
        namespace: str,
        kind: str,
        content: str,
        scope: Scope | None = None,
        source: Source | str = Source.USER,
        actor_id: str | None = None,
        session_id: str | None = None,
        lineage: Sequence[str] = (),
        extractor: ExtractorInfo | None = None,
        entity_keys: Sequence[str] = (),
        t_event: int | None = None,
        valid_from: int | None = None,
        embedding_version: str | None = None,
        meta: dict[str, Any] | None = None,
        record_id: str | None = None,
    ) -> "MemoryRecord":
        now = now_ms()
        prov = Provenance(
            source=source if isinstance(source, Source) else Source.parse(source),
            actor_id=actor_id,
            session_id=session_id,
            lineage=list(lineage),
            extractor=extractor,
        )
        ta = TimeAxis(t_event=t_event if t_event is not None else now, t_ingested=now, valid_from=valid_from)
        return MemoryRecord(
            id=record_id or ulid_new(now),
            namespace=namespace,
            kind=kind,
            content=content,
            scope=scope or Scope(),
            provenance=prov,
            time=ta,
            entity_keys=list(entity_keys),
            embedding_version=embedding_version,
            meta=meta or {},
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "namespace": self.namespace,
            "kind": self.kind,
            "content": self.content,
            "scope": self.scope.to_dict(),
            "provenance": self.provenance.to_dict(),
            "time": self.time.to_dict(),
            "entity_keys": list(self.entity_keys),
            "embedding_version": self.embedding_version,
            "meta": self.meta,
            "deleted": self.deleted,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "MemoryRecord":
        return cls(
            id=d["id"],
            namespace=d["namespace"],
            kind=d["kind"],
            content=d["content"],
            scope=Scope.from_dict(d.get("scope")),
            provenance=Provenance.from_dict(d.get("provenance")),
            time=TimeAxis.from_dict(d.get("time")),
            entity_keys=list(d.get("entity_keys") or []),
            embedding_version=d.get("embedding_version"),
            meta=dict(d.get("meta") or {}),
            deleted=bool(d.get("deleted", False)),
        )

    def with_tombstone(self, at_ms: int | None = None) -> "MemoryRecord":
        return replace(self, deleted=True, time=replace(self.time, invalidated_at=at_ms or now_ms()))

    @property
    def trust(self) -> int:
        return int(self.provenance.source)


def records_to_jsonl(records: Iterable[MemoryRecord]) -> bytes:
    lines = [r.to_dict() for r in records]
    return ("\n".join(json.dumps(l, separators=(",", ":")) for l in lines) + "\n").encode()


def records_from_jsonl(data: bytes) -> list[MemoryRecord]:
    out = []
    for line in data.decode().splitlines():
        line = line.strip()
        if line:
            out.append(MemoryRecord.from_dict(json.loads(line)))
    return out
