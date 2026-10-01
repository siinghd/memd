"""REST server (D4 §4.1) - the substrate; SDK and MCP are thin over it."""
from __future__ import annotations

import math
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field, field_validator

from memd.core.schema import Kind
from memd.engine.memory import ForgetPreviewMismatch, Memory, forget_fingerprint
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


def _validate_kinds(v):
    """The kind vocabulary is CLOSED. Leaving `kinds` unbounded and
    unvalidated let a caller inflate per-request work with request size (each
    entry becomes a SQL placeholder plus per-row Python filtering) using
    values that can never match anything - amplification for free. Reject at
    the door instead of carrying junk into the query planner."""
    if v is None:
        return v
    bad = [k for k in v if k not in Kind.ALL]
    if bad:
        raise ValueError(f"unknown kind(s) {bad!r}; expected a subset of {list(Kind.ALL)}")
    return list(dict.fromkeys(v))  # dedupe: repeats only multiply work


class SearchIn(BaseModel):
    query: str = Field(min_length=1, max_length=10_000)
    budget_tokens: int = Field(default=2000, ge=64, le=128_000)
    as_of: int | None = None
    kinds: list[str] | None = Field(default=None, max_length=len(Kind.ALL))
    user_id: str | None = None
    session_id: str | None = None
    agent_id: str | None = None
    org_id: str | None = None
    include_quarantined: bool = False

    _ck = field_validator("kinds")(_validate_kinds)


class FindIn(BaseModel):
    """Query-driven id resolution / deletion (forget flow). No packing
    budget: a destructive sweep must see every match."""
    query: str = Field(min_length=1, max_length=10_000)
    as_of: int | None = None
    kinds: list[str] | None = Field(default=None, max_length=len(Kind.ALL))

    _ck = field_validator("kinds")(_validate_kinds)
    user_id: str | None = None
    session_id: str | None = None
    agent_id: str | None = None
    org_id: str | None = None
    confirm: bool = False  # forget only: two-phase like the MCP tool
    # forget confirm only: the preview's fingerprint - refused (409) if the
    # confirm would delete a different set
    fingerprint: str | None = Field(default=None, max_length=128)


# Error bodies: {"detail": <human text, unchanged>, "code": <machine-readable>}.
_ERROR_CODES = {
    400: "validation_error", 401: "unauthorized", 403: "forbidden", 404: "not_found",
    405: "method_not_allowed", 409: "conflict", 410: "gone", 413: "payload_too_large",
    422: "validation_error", 429: "rate_limited", 500: "internal_error", 503: "unavailable",
}


class ApiError(HTTPException):
    """An HTTPException with a specific `code` for the error body."""

    def __init__(self, status_code: int, detail: str, code: str | None = None,
                 headers: dict | None = None):
        super().__init__(status_code, detail, headers)
        self.code = code


def _error(status: int, detail: Any, code: str | None = None,
           headers: dict | None = None) -> JSONResponse:
    return JSONResponse(status_code=status, headers=headers,
                        content={"detail": detail, "code": code or _ERROR_CODES.get(status, "error")})


def _rate_limited(detail: str, retry_s: float) -> ApiError:
    """429 with Retry-After: whole seconds until a retry can succeed."""
    return ApiError(429, detail, code="rate_limited",
                    headers={"Retry-After": str(max(1, math.ceil(retry_s)))})


def _route_label(path: str) -> str:
    """Bucket dynamic path segments for metric labels.

    Namespace names and record ids are UNBOUNDED cardinality (scales with
    tenants and rows); a raw value in a Prometheus label multiplies every
    http series by both. At registry cap the eviction guard starts silently
    dropping unrelated series - so labels carry only the route SHAPE:
    /v1/ns/{ns}/memories/{id} -> /v1/ns/:ns/memories/:id."""
    parts = path.split("/")
    if len(parts) >= 4 and parts[1] == "v1" and parts[2] == "ns":
        rest = parts[4:]
        tail = [":id" if i >= 1 else p for i, p in enumerate(rest)]
        return "/".join(["/v1/ns/:ns"] + tail)
    return path


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
    # tenancy-level ceiling: per-KEY budgets alone let a tenant multiply its
    # quota by minting more keys, which defeats D6 noisy-neighbour containment
    ns_limiter = RateLimiter()
    ns_rate_limit_per_min = int(os.environ.get("MEMD_NS_RATE_LIMIT_PER_MIN", "3000"))
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

    # Interactive docs and the OpenAPI schema were served unauthenticated,
    # handing an anonymous prober the full route inventory and request shapes
    # of an otherwise entirely authenticated API. Opt-in for development.
    _docs = bool(os.environ.get("MEMD_ENABLE_DOCS"))
    app = FastAPI(title="memd", version=__import__("memd").__version__, description="Agent memory engine",
                  lifespan=lifespan,
                  docs_url="/docs" if _docs else None,
                  redoc_url="/redoc" if _docs else None,
                  openapi_url="/openapi.json" if _docs else None)
    app.state.engine = engine
    app.state.keystore = keystore

    from memd.metrics import METRICS

    MAX_BODY_BYTES = 8 * 1024 * 1024  # request-body DoS cap

    @app.middleware("http")
    async def instrument_requests(request: Request, call_next):
        # bucket paths: /v1/ns/{ns}/memories/{id} -> /v1/ns/:ns/memories/:id
        # (label cardinality guard - see _route_label)
        route_label = _route_label(request.url.path)
        cl = request.headers.get("content-length")
        if cl and cl.isdigit() and int(cl) > MAX_BODY_BYTES:
            METRICS.inc("memd_oversized_requests_total", route=route_label)
            return _error(413, "request body too large")
        t0 = time.monotonic()
        try:
            response = await call_next(request)
        except ValueError as ex:
            # engine-boundary guards (size caps, bad enum-ish values) are
            # caller errors: 400 with the guard's message
            METRICS.inc("memd_http_errors_total", route=route_label, method=request.method, kind="value")
            return _error(400, str(ex))
        except sqlite3.Error:
            # lifecycle races surface as driver errors (namespace destroyed /
            # index closed mid-request): clean 503, no internals leaked
            METRICS.inc("memd_http_errors_total", route=route_label, method=request.method, kind="storage")
            return _error(503, "namespace unavailable (destroyed or rebuilding)")
        except Exception:
            METRICS.inc("memd_http_errors_total", route=route_label, method=request.method)
            # sanitized 500: tracebacks go to the server log, never the client
            return _error(500, "internal error")
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
            return _error(410, "namespace destroyed", "namespace_destroyed")
        if "evicted" in msg:
            return _error(503, "namespace re-opening; retry", headers={"Retry-After": "1"})
        METRICS.inc("memd_http_errors_total", route=request.url.path)
        return _error(500, "internal error")

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(request: Request, exc: StarletteHTTPException):
        # every raised error, the routing 404/405 included: detail + code
        return _error(exc.status_code, exc.detail, getattr(exc, "code", None),
                      getattr(exc, "headers", None))

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(request: Request, exc: RequestValidationError):
        return _error(422, jsonable_encoder(exc.errors()))

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
            raise _rate_limited("too many authentication failures; retry later",
                                failures.retry_after(client))
        if creds is None:
            failures.record_failure(client)
            METRICS.inc("memd_auth_failures_total", reason="missing")
            raise HTTPException(401, "missing bearer key")
        p = keystore.authenticate(creds.credentials)
        if p is None:
            failures.record_failure(client)
            METRICS.inc("memd_auth_failures_total", reason="invalid")
            raise HTTPException(401, "invalid key")
        failures.record_success(client)
        if ns is not None and p.namespace not in ("*", ns):
            # AUTHZ, not authn: this key authenticated fine. Booking it as an
            # auth failure fed the anti-credential-stuffing limiter, whose
            # buckets are keyed by CLIENT (spoofable to an arbitrary value
            # behind a trusted proxy) - so a holder of any valid key could
            # lock a chosen bucket out of the whole API for the failure window
            # by looping requests at a namespace they do not own. Volume abuse
            # here is the rate limiter's job, below; it is charged per key and
            # per namespace, both of which are authenticated facts.
            METRICS.inc("memd_authz_denials_total", help="namespace authorization denials",
                        reason="namespace")
            _charge_rate(p, ns)
            raise HTTPException(403, f"key not valid for namespace {ns!r}")
        _charge_rate(p, ns)
        return p

    def _charge_rate(p: Principal, ns: str | None) -> None:
        """Charge the request against BOTH the key's budget and the owning
        namespace's budget.

        Charging the key alone made a tenant's effective quota scale with how
        many keys it minted: ten keys, ten times the budget, and the
        noisy-neighbour containment D6 asks for evaporates. The namespace
        bucket is the tenancy-level ceiling; the key bucket still contains a
        single runaway client inside a tenant."""
        if not limiter.allow(p.key_id, p.rate_limit_per_min):
            METRICS.inc("memd_rate_limited_total", ns=ns, scope="key")
            raise _rate_limited("rate limit exceeded",
                                limiter.retry_after(p.key_id, p.rate_limit_per_min))
        owner = p.namespace if p.namespace != "*" else (ns or "*")
        if owner != "*" and not ns_limiter.allow(f"ns:{owner}", ns_rate_limit_per_min):
            METRICS.inc("memd_rate_limited_total", ns=owner, scope="namespace")
            raise _rate_limited("namespace rate limit exceeded",
                                ns_limiter.retry_after(f"ns:{owner}", ns_rate_limit_per_min))

    def heavy(ns: str, route: str, p: Principal) -> None:
        """Separate small token budget for O(namespace) maintenance calls."""
        if not heavy_limiter.allow(f"{p.key_id}:{route}", 10):
            METRICS.inc("memd_heavy_throttled_total", ns=ns, route=route)
            raise _rate_limited(f"{route} rate limit exceeded; retry later",
                                heavy_limiter.retry_after(f"{p.key_id}:{route}", 10))

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
    def get_memory(ns: str, record_id: str, history: bool = False, include_deleted: bool = False,
                   p: Principal = Depends(auth)):
        # a deleted record - or deleted version in its history - is served only
        # to an administrative read; `history` alone used to serve soft-deleted
        # content until a compaction purged it
        if include_deleted and not p.scope_override:
            raise HTTPException(403, "include_deleted requires an override-capable key")
        got = engine.get(record_id, history=history, include_deleted=include_deleted, namespace=ns)
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
        # a hard delete of a soft-deleted record is how its text gets purged (D7)
        existing = engine.get(record_id, include_deleted=hard, namespace=ns)
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
        if not engine.has_namespace(ns):
            # it used to be created here, shredded and answered 200
            raise HTTPException(404, f"namespace {ns!r} not found")
        ok = engine.destroy_namespace(ns, actor=f"key:{p.key_id}")
        return {"destroyed": ns, "crypto_shred": True}

    @app.post("/v1/ns/{ns}/export")
    def export_ns(ns: str, p: Principal = Depends(auth)):
        heavy(ns, "export", p)
        # streaming NDJSON: first byte leaves before the namespace is
        # materialized - bulk egress must not buffer O(namespace) bytes in RAM
        from fastapi.responses import StreamingResponse

        gen = engine.export_jsonl_iter(namespace=ns)
        headers = {"Content-Disposition": f'attachment; filename="{ns}-export.jsonl"'}
        return StreamingResponse(gen, media_type="application/x-ndjson", headers=headers)

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
            return {"will_delete": preview, "count": len(ids), "confirmed": False,
                    "fingerprint": forget_fingerprint(ids)}
        # the SAME filters as the preview: the confirm used to drop as_of and
        # kinds and delete every kind the query matched
        try:
            deleted = engine.forget(
                body.query, user_id=user, session_id=body.session_id,
                agent_id=body.agent_id, org_id=body.org_id,
                as_of=body.as_of, kinds=body.kinds, expected=body.fingerprint,
                actor=f"key:{p.key_id}", namespace=ns,
            )
        except ForgetPreviewMismatch as ex:
            raise ApiError(409, str(ex), code="preview_mismatch") from None
        METRICS.inc("memd_forgets_total", ns=ns)
        return {"deleted": deleted, "count": len(deleted), "confirmed": True}

    @app.post("/v1/ns/{ns}/reembed")
    def reembed(ns: str, p: Principal = Depends(auth)):
        """Rebuild the vector lane from raw (ADR-8).

        Restoring segments onto a node without the derived index cache replays
        every record but zero vectors, so retrieval runs with one of its four
        fusion lanes empty - and looks faster, not broken. The heal existed
        only behind the CLI, which a hosted operator cannot reach on the node
        that needs it. O(records missing a current-version vector); charged to
        the heavy-maintenance budget."""
        heavy(ns, "reembed", p)
        return engine.reembed(namespace=ns)

    @app.get("/health")
    def health():
        # how many open namespaces' automatic maintenance (rotate/compaction)
        # is failing - writes succeed meanwhile. A count only: /health is
        # unauthenticated, and namespace names are tenant data
        failing = getattr(getattr(engine, "engine", None), "maintenance_failing", None)
        return {"ok": True, "version": __import__("memd").__version__,
                "maintenance_failing": len(failing()) if callable(failing) else 0}

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
                raise _rate_limited("too many authentication failures; retry later",
                                    failures.retry_after(client))
            p = keystore.authenticate(creds.credentials if creds else None)
            if p is None:
                failures.record_failure(client)
                METRICS.inc("memd_auth_failures_total", reason="metrics")
                raise HTTPException(401, "missing or invalid bearer key")
            failures.record_success(client)
            # rendering the registry is O(series); leaving the one route
            # without a budget made it the cheapest way to burn server CPU
            if not limiter.allow(f"metrics:{p.key_id}", 60):
                METRICS.inc("memd_rate_limited_total", scope="metrics")
                raise _rate_limited("metrics rate limit exceeded",
                                    limiter.retry_after(f"metrics:{p.key_id}", 60))
            ns_filter = None if p.namespace == "*" else {p.namespace}
        else:
            ns_filter = None  # public scrape: operator opted the whole fleet in
        return Response(content=METRICS.render_prometheus(ns_filter=ns_filter),
                        media_type="text/plain; version=0.0.4")

    @app.get("/v1/metrics/json")
    def metrics_json(p: Principal = Depends(auth)):
        """JSON snapshot for graphing pipelines (timestamped on client side)."""
        import time as _t

        from memd.metrics import METRICS

        snap = METRICS.snapshot(ns_filter=None if p.namespace == "*" else {p.namespace})
        snap["_epoch_ms"] = int(_t.time() * 1000)
        return snap

    @app.get("/v1/status")
    def status(p: Principal = Depends(auth)):
        # authenticated AND scoped: the namespace inventory is tenant
        # information, so a key bound to one namespace sees only that one
        return engine.status(ns_filter=None if p.namespace == "*" else p.namespace)

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
