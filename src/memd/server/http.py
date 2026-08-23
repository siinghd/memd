"""REST server (D4 §4.1) - the substrate; SDK and MCP are thin over it."""
from __future__ import annotations

import json
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field

from memd.core.schema import Kind
from memd.engine.memory import Memory
from memd.metrics import METRICS
from memd.server.auth import FailureLimiter, KeyStore, Principal, RateLimiter


class EventIn(BaseModel):
    content: str = Field(min_length=1, max_length=1_000_000)
    role: str = "user"
    session_id: str | None = None
    user_id: str | None = None
    agent_id: str | None = None
    org_id: str | None = None
    source: str | None = None
    actor_id: str | None = None
    t_event: int | None = None
    kind: str = Kind.RAW_EVENT
    meta: dict[str, Any] | None = None


class EventsIn(BaseModel):
    events: list[EventIn] = Field(min_length=1, max_length=1000)


class MemoryIn(BaseModel):
    content: str = Field(min_length=1, max_length=100_000)
    kind: str = Kind.FACT
    entity_keys: list[str] | None = None
    session_id: str | None = None
    user_id: str | None = None
    agent_id: str | None = None
    org_id: str | None = None
    source: str = "agent"
    actor_id: str | None = None
    t_event: int | None = None
    valid_from: int | None = None


class SearchIn(BaseModel):
    query: str = Field(min_length=1, max_length=10_000)
    budget_tokens: int = Field(default=2000, ge=64, le=128_000)
    as_of: int | None = None
    kinds: list[str] | None = None
    user_id: str | None = None
    session_id: str | None = None
    agent_id: str | None = None
    org_id: str | None = None
    include_quarantined: bool = False


class FindIn(BaseModel):
    """Query-driven id resolution / deletion (forget flow). No packing
    budget: a destructive sweep must see every match."""
    query: str = Field(min_length=1, max_length=10_000)
    as_of: int | None = None
    kinds: list[str] | None = None
    user_id: str | None = None
    session_id: str | None = None
    agent_id: str | None = None
    org_id: str | None = None
    confirm: bool = False  # forget only: two-phase like the MCP tool


def create_app(
    data_dir: str = "./memd-data",
    keys_path: str | None = None,
    admin_key: str | None = None,
) -> FastAPI:
    os.makedirs(data_dir, exist_ok=True)
    keys_path = keys_path or os.path.join(data_dir, "keys.toml.json")
    keystore = KeyStore(keys_path, admin_key=admin_key)
    limiter = RateLimiter()
    failures = FailureLimiter()
    # heavy maintenance endpoints get a separate small budget: they are
    # O(namespace) operations and must not be spam-able by a normal key
    heavy_limiter = RateLimiter(max_buckets=1000)
    engine = Memory(data_dir)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        try:
            yield
        finally:
            engine.close()

    app = FastAPI(title="memd", version="0.1.0", description="Agent memory engine", lifespan=lifespan)
    app.state.engine = engine
    app.state.keystore = keystore

    from memd.metrics import METRICS

    MAX_BODY_BYTES = 8 * 1024 * 1024  # request-body DoS cap

    @app.middleware("http")
    async def instrument_requests(request: Request, call_next):
        path = request.url.path
        # bucket paths: /v1/ns/{ns}/... -> /v1/ns/:ns/... (label cardinality guard)
        parts = path.split("/")
        if len(parts) > 4 and parts[1] == "v1" and parts[2] == "ns":
            safe = ["/".join(parts[:4])] + [":id" if i >= 5 else p for i, p in enumerate(parts[4:], start=5)]
            route_label = "/".join(safe)
        else:
            route_label = path
        cl = request.headers.get("content-length")
        if cl and cl.isdigit() and int(cl) > MAX_BODY_BYTES:
            METRICS.inc("memd_oversized_requests_total", route=route_label)
            return Response(status_code=413, content=json.dumps({"detail": "request body too large"}).encode(),
                            media_type="application/json")
        t0 = time.monotonic()
        try:
            response = await call_next(request)
        except ValueError as ex:
            # engine-boundary guards (size caps, bad enum-ish values) are
            # caller errors: 400 with the guard's message
            METRICS.inc("memd_http_errors_total", route=route_label, method=request.method, kind="value")
            return JSONResponse(status_code=400, content={"detail": str(ex)})
        except sqlite3.Error:
            # lifecycle races surface as driver errors (namespace destroyed /
            # index closed mid-request): clean 503, no internals leaked
            METRICS.inc("memd_http_errors_total", route=route_label, method=request.method, kind="storage")
            return JSONResponse(status_code=503,
                                content={"detail": "namespace unavailable (destroyed or rebuilding)"})
        except Exception:
            METRICS.inc("memd_http_errors_total", route=route_label, method=request.method)
            # sanitized 500: tracebacks go to the server log, never the client
            return JSONResponse(status_code=500, content={"detail": "internal error"})
        finally:
            METRICS.observe("memd_http_request_ms", (time.monotonic() - t0) * 1000,
                            help="HTTP request duration (ms)", route=route_label, method=request.method)
        METRICS.inc("memd_http_requests_total", route=route_label, method=request.method,
                    status=str(response.status_code))
        return response

    @app.exception_handler(RuntimeError)
    async def runtime_error_handler(request: Request, exc: RuntimeError):
        # namespace lifecycle races surface here - map them to clean
        # responses instead of leaking driver internals ("closed database")
        msg = str(exc)
        if "destroyed" in msg:
            return JSONResponse(status_code=410, content={"detail": "namespace destroyed"})
        METRICS.inc("memd_http_errors_total", route=request.url.path)
        return JSONResponse(status_code=500, content={"detail": "internal error"})

    bearer = HTTPBearer(auto_error=False)

    def _client_id(request: Request) -> str:
        # Best available pre-auth identity. Direct connections: the peer IP.
        # Behind a trusted proxy: the forwarded-for first hop is spoofable, so
        # we use it only when a proxy header exists AND the socket peer is
        # loopback (a deployment that terminates untrusted traffic locally).
        client_host = request.client.host if request.client else "unknown"
        fwd = request.headers.get("x-forwarded-for")
        if fwd and client_host in ("127.0.0.1", "::1"):
            return "fwd:" + fwd.split(",")[0].strip()
        return client_host

    def auth(
        request: Request,
        ns: str | None = None,
        creds: HTTPAuthorizationCredentials | None = Depends(bearer),
    ) -> Principal:
        client = _client_id(request)
        if failures.blocked(client):
            METRICS.inc("memd_auth_failures_total", reason="throttled")
            raise HTTPException(429, "too many authentication failures; retry later")
        if creds is None:
            failures.record_failure(client)
            METRICS.inc("memd_auth_failures_total", reason="missing")
            raise HTTPException(401, "missing bearer key")
        p = keystore.authenticate(creds.credentials)
        if p is None:
            failures.record_failure(client)
            METRICS.inc("memd_auth_failures_total", reason="invalid")
            raise HTTPException(401, "invalid key")
        if ns is not None and p.namespace not in ("*", ns):
            failures.record_failure(client)
            METRICS.inc("memd_auth_failures_total", reason="namespace")
            raise HTTPException(403, f"key not valid for namespace {ns!r}")
        failures.record_success(client)
        if not limiter.allow(p.key_id, p.rate_limit_per_min):
            METRICS.inc("memd_rate_limited_total", ns=ns)
            raise HTTPException(429, "rate limit exceeded")
        return p

    def heavy(ns: str, route: str, p: Principal) -> None:
        """Separate small token budget for O(namespace) maintenance calls."""
        if not heavy_limiter.allow(f"{p.key_id}:{route}", 10):
            METRICS.inc("memd_heavy_throttled_total", ns=ns, route=route)
            raise HTTPException(429, f"{route} rate limit exceeded; retry later")

    def apply_scope(p: Principal, body_user: str | None, body_session: str | None):
        """Scope pinning: a user-pinned key cannot widen its scope. Cross-user
        reads require explicit scope_override capability (D7 #6)."""
        user = body_user or p.pinned_user
        if body_user and p.pinned_user and body_user != p.pinned_user and not p.scope_override:
            raise HTTPException(403, "key pinned to another user (scope_override required)")
        if user is None and not p.scope_override and p.pinned_user is None and p.namespace != "*":
            pass  # namespace-admin queries allowed; per-user keys always pin
        return user, body_session

    @app.post("/v1/ns/{ns}/events", status_code=202)
    def post_events(ns: str, body: EventsIn, p: Principal = Depends(auth)):
        events = []
        downgraded = 0
        for e in body.events:
            user, _ = apply_scope(p, e.user_id, e.session_id)
            # trust-tier spoofing guard: keys cannot assert USER tier on raw
            # lane either; role-derived source is authoritative without
            # scope_override
            src = e.source
            if src in ("user", "agent") and not p.scope_override:
                src = None  # derive from role
                downgraded += 1
            events.append({
                "content": e.content,
                "role": e.role,
                "session_id": e.session_id,
                "user_id": user,
                "agent_id": e.agent_id,
                "org_id": e.org_id,
                "source": src,
                "actor_id": e.actor_id or f"key:{p.key_id}",
                "t_event": e.t_event,
                "kind": e.kind,
                "meta": e.meta,
            })
        if downgraded:
            METRICS.inc("memd_source_downgrades_total", ns=ns, amount=downgraded)
        ids = engine.add_events(events, namespace=ns)
        return {"ids": ids, "accepted": len(ids)}

    @app.post("/v1/ns/{ns}/memories", status_code=201)
    def post_memory(ns: str, body: MemoryIn, p: Principal = Depends(auth)):
        user, _ = apply_scope(p, body.user_id, body.session_id)
        # Trust-tier spoofing guard (D7 #1/#2): API keys are agent
        # credentials - they cannot mint USER-tier facts by assertion.
        # Only scope_override-capable principals claim human tier.
        source = body.source
        if source == "user" and not p.scope_override:
            source = "agent"
            METRICS.inc("memd_source_downgrades_total", ns=ns)
        rid = engine.remember(
            body.content,
            kind=body.kind,
            entity_keys=body.entity_keys,
            session_id=body.session_id,
            user_id=user,
            agent_id=body.agent_id,
            org_id=body.org_id,
            source=source,
            actor_id=body.actor_id or f"key:{p.key_id}",
            t_event=body.t_event,
            valid_from=body.valid_from,
            namespace=ns,
        )
        return {"id": rid}

    @app.post("/v1/ns/{ns}/search")
    def search(ns: str, body: SearchIn, p: Principal = Depends(auth)):
        user, _ = apply_scope(p, body.user_id, body.session_id)
        res = engine.search(
            body.query,
            user_id=user,
            session_id=body.session_id,
            agent_id=body.agent_id,
            org_id=body.org_id,
            budget_tokens=body.budget_tokens,
            as_of=body.as_of,
            kinds=body.kinds,
            include_quarantined=body.include_quarantined,
            namespace=ns,
        )
        return {
            "packed_context": res.packed_context,
            "items": [i.__dict__ for i in res.items],
            "tokens_used": res.tokens_used,
            "budget": res.budget,
            "truncated": res.truncated,
            "query_class": res.query_class,
            "latency_ms": res.latency_ms,
        }

    @app.get("/v1/ns/{ns}/memories/{record_id}")
    def get_memory(ns: str, record_id: str, history: bool = False, p: Principal = Depends(auth)):
        got = engine.get(record_id, history=history, namespace=ns)
        if got is None:
            raise HTTPException(404, "not found")
        # user-pinned keys must not read other users' records by id (D7 #6);
        # 404 rather than 403 so existence isn't revealed
        rec_scope = got.get("scope") or {}
        if p.pinned_user and not p.scope_override and rec_scope.get("user") not in (None, p.pinned_user):
            raise HTTPException(404, "not found")
        return got

    @app.delete("/v1/ns/{ns}/memories/{record_id}")
    def delete_memory(ns: str, record_id: str, hard: bool = False, p: Principal = Depends(auth)):
        existing = engine.get(record_id, namespace=ns)
        if existing is None:
            raise HTTPException(404, "not found")
        rec_scope = existing.get("scope") or {}
        if p.pinned_user and not p.scope_override and rec_scope.get("user") not in (None, p.pinned_user):
            raise HTTPException(404, "not found")
        ok = engine.delete(record_id, hard=hard, actor=f"key:{p.key_id}", namespace=ns)
        return {"deleted": record_id, "hard": hard,
                "note": "physical purge guaranteed at next compaction (<=72h)" if not hard else "purged"}

    @app.delete("/v1/ns/{ns}")
    def destroy_ns(ns: str, p: Principal = Depends(auth)):
        # Crypto-shred is tenant-destructive: only scope_override-capable
        # principals (admin class) may trigger it - a leaked agent key must
        # not grant unilateral destruction of all users' data (GDPR erasure
        # for individual records stays available via DELETE /memories/{id}).
        if not p.scope_override:
            METRICS.inc("memd_destroy_forbidden_total", ns=ns)
            raise HTTPException(403, "namespace crypto-shred requires an override-capable key")
        ok = engine.destroy_namespace(ns, actor=f"key:{p.key_id}")
        return {"destroyed": ns, "crypto_shred": True}

    @app.post("/v1/ns/{ns}/export")
    def export_ns(ns: str, p: Principal = Depends(auth)):
        heavy(ns, "export", p)
        data = engine.export_jsonl(namespace=ns)
        return Response(
            content=data,
            media_type="application/x-ndjson",
            headers={"Content-Disposition": f'attachment; filename="{ns}-export.jsonl"'},
        )

    @app.get("/v1/ns/{ns}/stats")
    def stats(ns: str, p: Principal = Depends(auth)):
        return engine.stats(namespace=ns)

    @app.post("/v1/ns/{ns}/sessions/{session_id}/close")
    def close_session(ns: str, session_id: str, p: Principal = Depends(auth)):
        # pinned keys constrain extraction to their user (blocks cross-user
        # session-id injection into the fact lane); override keys may target any
        heavy(ns, "close_session", p)
        return engine.close_session(session_id, user_id=p.pinned_user, namespace=ns)

    @app.post("/v1/ns/{ns}/compact")
    def compact(ns: str, force: bool = False, p: Principal = Depends(auth)):
        heavy(ns, "compact", p)
        return engine.compact(force=force, namespace=ns)

    @app.post("/v1/ns/{ns}/find_ids")
    def find_ids(ns: str, body: FindIn, p: Principal = Depends(auth)):
        user, _ = apply_scope(p, body.user_id, body.session_id)
        heavy(ns, "find_ids", p)  # unbounded sweep is O(namespace)
        ids = engine.find_ids(
            body.query, user_id=user, session_id=body.session_id,
            agent_id=body.agent_id, org_id=body.org_id,
            as_of=body.as_of, kinds=body.kinds, namespace=ns,
        )
        return {"ids": ids}

    @app.post("/v1/ns/{ns}/forget")
    def forget(ns: str, body: FindIn, p: Principal = Depends(auth)):
        """Two-phase query-driven deletion (mirrors the MCP tool): without
        confirm=true returns what WOULD be deleted; with confirm executes.
        Pinned keys are scope-constrained like every other route."""
        user, _ = apply_scope(p, body.user_id, body.session_id)
        heavy(ns, "forget", p)
        if not body.confirm:
            ids = engine.find_ids(
                body.query, user_id=user, session_id=body.session_id,
                agent_id=body.agent_id, org_id=body.org_id,
                as_of=body.as_of, kinds=body.kinds, namespace=ns,
            )
            preview = []
            for rid in ids[:20]:
                got = engine.get(rid, namespace=ns)
                if got:
                    preview.append({"id": rid, "content": got["content"][:200]})
            return {"will_delete": preview, "count": len(ids), "confirmed": False}
        deleted = engine.forget(
            body.query, user_id=user, session_id=body.session_id,
            agent_id=body.agent_id, org_id=body.org_id,
            actor=f"key:{p.key_id}", namespace=ns,
        )
        METRICS.inc("memd_forgets_total", ns=ns)
        return {"deleted": deleted, "count": len(deleted), "confirmed": True}

    @app.get("/health")
    def health():
        return {"ok": True, "version": "0.1.0"}

    @app.get("/metrics")
    def metrics_endpoint(request: Request, creds: HTTPAuthorizationCredentials | None = Depends(bearer)):
        """Prometheus exposition. Authenticated by default: metric labels
        include namespace names. Set MEMD_METRICS_PUBLIC=1 to allow
        unauthenticated scraping from a trusted network segment."""
        import os as _os

        from memd.metrics import METRICS

        if not _os.environ.get("MEMD_METRICS_PUBLIC"):
            client = _client_id(request)
            if failures.blocked(client):
                METRICS.inc("memd_auth_failures_total", reason="throttled")
                raise HTTPException(429, "too many authentication failures; retry later")
            p = keystore.authenticate(creds.credentials if creds else None)
            if p is None:
                failures.record_failure(client)
                METRICS.inc("memd_auth_failures_total", reason="metrics")
                raise HTTPException(401, "missing or invalid bearer key")
            failures.record_success(client)
        return Response(content=METRICS.render_prometheus(), media_type="text/plain; version=0.0.4")

    @app.get("/v1/metrics/json")
    def metrics_json(p: Principal = Depends(auth)):
        """JSON snapshot for graphing pipelines (timestamped on client side)."""
        import time as _t

        from memd.metrics import METRICS

        snap = METRICS.snapshot()
        snap["_epoch_ms"] = int(_t.time() * 1000)
        return snap

    @app.get("/v1/status")
    def status(p: Principal = Depends(auth)):
        # authenticated: the namespace inventory is tenant information, not
        # something to hand to unauthenticated probers
        return engine.status()

    return app


def main():  # pragma: no cover
    import uvicorn

    port = int(os.environ.get("MEMD_PORT", "8700"))
    app = create_app(
        data_dir=os.environ.get("MEMD_DATA", "./memd-data"),
        admin_key=os.environ.get("MEMD_ADMIN_KEY"),
    )
    uvicorn.run(app, host=os.environ.get("MEMD_HOST", "127.0.0.1"), port=port)


if __name__ == "__main__":
    main()
