"""Hosted client: same API as embedded Memory, over REST."""
from __future__ import annotations

import logging
from typing import Any

import httpx

_log = logging.getLogger(__name__)


class HostedError(RuntimeError):
    def __init__(self, status: int, message: str, code: str | None = None):
        super().__init__(f"[{status}] {message}")
        self.status = status
        self.code = code  # machine-readable ("not_found", "rate_limited", ...)


# how many unreadable WAL frames the server left out of an export
EXPORT_SKIPPED_HEADER = "X-Memd-Export-Skipped-Frames"
# read replicas: what a read accepts, and who served it
CONSISTENCY_HEADER = "X-Memd-Read-Consistency"
MAX_STALENESS_HEADER = "X-Memd-Max-Staleness-Ms"
SERVED_BY_HEADER = "X-Memd-Served-By"
REPLICA_SEQ_HEADER = "X-Memd-Replica-Seq"
REPLICA_AGE_HEADER = "X-Memd-Replica-Age-Ms"


class HostedMemory:
    # the last export_jsonl()'s count of WAL frames left out (0: complete)
    last_export_skipped_frames = 0
    # who served the last search/get: {"served_by": "leader"|"replica",
    # "applied_seq", "age_ms"} (the replica's seq and age; None for the leader)
    last_read: dict | None = None
    consistency: str | None = None
    max_staleness_ms: int | None = None

    def __init__(self, api_key: str, base_url: str = "http://localhost:8700", namespace: str = "default",
                 transport: httpx.BaseTransport | None = None, *, consistency: str | None = None,
                 max_staleness_ms: int | None = None):
        """`consistency="eventual"` lets searches and gets be served by a read
        replica no staler than `max_staleness_ms` (the
        server's default bound when None); the default, "strong", reads the
        namespace's writer. Both can be given per call too."""
        if consistency not in (None, "strong", "eventual"):
            raise ValueError("consistency must be 'strong' or 'eventual'")
        self.consistency = consistency
        self.max_staleness_ms = max_staleness_ms
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.namespace = namespace
        self._client = httpx.Client(
            base_url=self.base_url,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=30,
            transport=transport,  # injectable for tests (ASGITransport)
        )

    # -- writes -------------------------------------------------------
    def add(self, content: str, **kw) -> list[str]:
        body: dict[str, Any] = {"content": content}
        for k in ("role", "session_id", "user_id", "agent_id", "org_id", "source", "actor_id", "t_event", "kind", "meta"):
            if kw.get(k) is not None:
                body[k] = self._src(kw[k]) if k == "source" else kw[k]
        r = self._client.post(f"/v1/ns/{self._ns(kw)}/events", json={"events": [body]})
        self._raise(r)
        return r.json()["ids"]

    def add_events(self, events: list[dict], **kw) -> list[str]:
        body = []
        for e in events:
            ev: dict[str, Any] = {"content": e["content"]}
            for k in ("role", "session_id", "user_id", "agent_id", "org_id", "source", "actor_id", "t_event", "kind", "meta"):
                if e.get(k) is not None:
                    ev[k] = self._src(e[k]) if k == "source" else e[k]
            body.append(ev)
        r = self._client.post(f"/v1/ns/{self._ns(kw)}/events", json={"events": body})
        self._raise(r)
        return r.json()["ids"]

    def remember(self, content: str, *, entity_keys: list[str] | None = None, **kw) -> str:
        body: dict[str, Any] = {"content": content, "entity_keys": entity_keys or []}
        for k in ("kind", "session_id", "user_id", "agent_id", "org_id", "source", "actor_id",
                  "t_event", "valid_from"):
            if kw.get(k) is not None:
                body[k] = self._src(kw[k]) if k == "source" else kw[k]
        r = self._client.post(f"/v1/ns/{self._ns(kw)}/memories", json=body)
        self._raise(r)
        return r.json()["id"]

    def observe(self, messages: list[dict], response: str, **kw) -> list[str]:
        events = []
        for m in messages:
            if isinstance(m, dict) and m.get("content"):
                events.append({"content": str(m["content"]), "role": m.get("role", "user"),
                               **{k: kw[k] for k in ("session_id", "user_id", "agent_id", "org_id") if kw.get(k) is not None}})
        events.append({"content": response, "role": "assistant",
                       **{k: kw[k] for k in ("session_id", "user_id", "agent_id", "org_id") if kw.get(k) is not None}})
        return self.add_events(events, namespace=kw.get("namespace"))

    # -- reads --------------------------------------------------------
    def search(self, query: str, **kw) -> Any:
        from memd.engine.memory import SearchResult, SearchHit

        body: dict[str, Any] = {"query": query}
        for k in ("user_id", "session_id", "agent_id", "org_id", "budget_tokens", "as_of", "kinds",
                  "include_quarantined"):
            if kw.get(k) is not None:
                body[k] = kw[k]
        r = self._client.post(f"/v1/ns/{self._ns(kw)}/search", json=body, **self._read_headers(kw))
        self._raise(r)
        info = self._note_read(r)
        d = r.json()
        return SearchResult(
            packed_context=d["packed_context"],
            items=[SearchHit(**i) for i in d["items"]],
            tokens_used=d["tokens_used"],
            budget=d["budget"],
            truncated=d["truncated"],
            query_class=d["query_class"],
            latency_ms=d["latency_ms"],
            served_by=info["served_by"],
            replica_seq=info["applied_seq"],
            replica_age_ms=info["age_ms"],
        )

    def pack(self, messages: list[dict], **kw) -> list[dict]:
        last_user = next((m["content"] for m in reversed(messages) if m.get("role") == "user"), "")
        if not last_user:
            return messages
        res = self.search(str(last_user), **kw)
        if not res.items:
            return messages
        out = list(messages)
        insert_at = 0
        for i, m in enumerate(out):
            if m.get("role") == "system":
                insert_at = i + 1
            else:
                break
        out.insert(insert_at, {"role": "system", "content": res.packed_context})
        return out

    def get(self, record_id: str, *, history: bool = False, include_deleted: bool = False,
            **kw) -> dict | None:
        ns = self._ns(kw)
        params = {"history": str(history).lower()}
        if include_deleted:  # admin keys only (403 otherwise)
            params["include_deleted"] = "true"
        r = self._client.get(f"/v1/ns/{ns}/memories/{record_id}", params=params,
                             **self._read_headers(kw))
        if r.status_code == 404:
            self._note_read(r)
            return None
        self._raise(r)
        self._note_read(r)
        return r.json()

    # -- lifecycle ----------------------------------------------------
    def delete(self, record_id: str, *, hard: bool = False, **kw) -> bool:
        r = self._client.delete(f"/v1/ns/{self._ns(kw)}/memories/{record_id}", params={"hard": str(hard).lower()})
        if r.status_code == 404:
            return False
        self._raise(r)
        return True

    def close_session(self, session_id: str, **kw) -> dict:
        r = self._client.post(f"/v1/ns/{self._ns(kw)}/sessions/{session_id}/close")
        self._raise(r)
        return r.json()

    def stats(self, **kw) -> dict:
        r = self._client.get(f"/v1/ns/{self._ns(kw)}/stats")
        self._raise(r)
        return r.json()

    def export_jsonl(self, **kw) -> bytes:
        """The namespace's NDJSON export. An unreadable WAL frame on the
        server is left out of it, not refused: last_export_skipped_frames
        says how many (the X-Memd-Export-Skipped-Frames header; 0 when the
        export is complete), and a warning is logged when it is not 0."""
        ns = self._ns(kw)
        r = self._client.post(f"/v1/ns/{ns}/export")
        self._raise(r)
        try:
            n = int(r.headers.get(EXPORT_SKIPPED_HEADER) or 0)
        except ValueError:
            n = 0
        self.last_export_skipped_frames = n
        if n:
            _log.warning("memd export of namespace %r is incomplete: the server left out %d "
                         "unreadable WAL frame(s) (its audit entry and log say where)", ns, n)
        return r.content

    def status(self, **kw) -> dict:
        r = self._client.get("/v1/status")
        self._raise(r)
        return r.json()

    def compact(self, force: bool = False, **kw) -> dict:
        ns = self._ns(kw)
        r = self._client.post(f"/v1/ns/{ns}/compact", params={"force": str(force).lower()})
        self._raise(r)
        return r.json()

    def destroy_namespace(self, namespace: str | None = None, **kw) -> bool:
        ns = namespace or self._ns(kw)
        r = self._client.delete(f"/v1/ns/{ns}")
        if r.status_code == 404:
            return False
        self._raise(r)
        return True

    # -- destructive query flows (parity with embedded / MCP memory_forget)
    _FIND_KEYS = ("user_id", "session_id", "agent_id", "org_id", "as_of", "kinds")

    def find_ids(self, query: str, **kw) -> list[str]:
        body: dict[str, Any] = {"query": query}
        for k in self._FIND_KEYS:
            if kw.get(k) is not None:
                body[k] = kw[k]
        r = self._client.post(f"/v1/ns/{self._ns(kw)}/find_ids", json=body)
        self._raise(r)
        return r.json()["ids"]

    def forget(self, query: str, *, confirm: bool = False, **kw) -> Any:
        """Without confirm: returns the preview dict (what WOULD be deleted).
        With confirm=True: executes and returns the deleted id list."""
        body: dict[str, Any] = {"query": query, "confirm": confirm}
        for k in self._FIND_KEYS + ("fingerprint",):
            if kw.get(k) is not None:
                body[k] = kw[k]
        r = self._client.post(f"/v1/ns/{self._ns(kw)}/forget", json=body)
        self._raise(r)
        d = r.json()
        return d["deleted"] if confirm else d

    # -- helpers ------------------------------------------------------
    @staticmethod
    def _src(v):
        """Trust tiers arrive as Source enums from embedded-parity callers;
        the REST contract speaks their lowercase names."""
        from memd.core.schema import Source

        return v.name.lower() if isinstance(v, Source) else v

    def _read_headers(self, kw: dict) -> dict:
        """{"headers": ...} for a read's request, or {} when it asks nothing
        (a strong read with no bound sends no header)."""
        consistency = kw.get("consistency") or self.consistency
        if consistency not in (None, "strong", "eventual"):
            raise ValueError("consistency must be 'strong' or 'eventual'")
        out = {}
        if consistency:
            out[CONSISTENCY_HEADER] = consistency
        ms = kw.get("max_staleness_ms")
        ms = self.max_staleness_ms if ms is None else ms
        if ms is not None:
            out[MAX_STALENESS_HEADER] = str(int(ms))
        return {"headers": out} if out else {}

    def _note_read(self, r: httpx.Response) -> dict:
        def num(h):
            v = r.headers.get(h)
            try:
                return int(v) if v is not None else None
            except ValueError:
                return None

        served = r.headers.get(SERVED_BY_HEADER) or "leader"
        info = {"served_by": served,
                "applied_seq": num(REPLICA_SEQ_HEADER) if served == "replica" else None,
                "age_ms": num(REPLICA_AGE_HEADER) if served == "replica" else None}
        self.last_read = info
        return info

    def _ns(self, kw: dict) -> str:
        return kw.get("namespace") or self.namespace

    @staticmethod
    def _raise(r: httpx.Response) -> None:
        if r.status_code >= 400:
            code = None
            try:
                body = r.json()
                detail, code = body.get("detail", r.text), body.get("code")
            except Exception:
                detail = r.text
            raise HostedError(r.status_code, str(detail), code)

    def close(self) -> None:
        self._client.close()
