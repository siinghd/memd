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
from memd.storage.engine import NamespaceBusyError


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
    400: "validation_error", 401: "unauthorized", 402: "payment_required", 403: "forbidden", 404: "not_found",
    405: "method_not_allowed", 409: "conflict", 410: "gone", 413: "payload_too_large",
    422: "validation_error", 429: "rate_limited", 500: "internal_error", 503: "unavailable",
}


class ApiError(HTTPException):
    """An HTTPException with a specific `code` (and optional extra fields,
    e.g. a 402's meter/limit) for the error body."""

    def __init__(self, status_code: int, detail: str, code: str | None = None,
                 headers: dict | None = None, extra: dict | None = None):
        super().__init__(status_code, detail, headers)
        self.code = code
        self.extra = extra


def _error(status: int, detail: Any, code: str | None = None,
           headers: dict | None = None, extra: dict | None = None) -> JSONResponse:
    content = {"detail": detail, "code": code or _ERROR_CODES.get(status, "error")}
    if extra:
        content.update({k: v for k, v in extra.items() if k not in content})
    return JSONResponse(status_code=status, headers=headers, content=content)


class _Unmetered:
    """Hosted mode off (the default): no org checks, no quotas, no ledger."""

    allow_rerank = True
    extract_limit = None
    max_facts = None

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        return None

    def authorize(self, p, ns) -> None:
        pass

    def require_memory(self, p) -> None:
        pass

    def admit(self, p, ns, **kw) -> "_Unmetered":
        return self

    def record(self, **kw) -> None:
        pass

    def stored_delta(self, ns, delta) -> None:
        pass

    def stored_reset(self, ns) -> None:
        pass


def _rate_limited(detail: str, retry_s: float) -> ApiError:
    """429 with Retry-After: whole seconds until a retry can succeed."""
    return ApiError(429, detail, code="rate_limited",
                    headers={"Retry-After": str(max(1, math.ceil(retry_s)))})


# every route this server serves, as a label; anything else is "other"
_FIXED_ROUTE_LABELS = frozenset({
    "/health", "/metrics", "/v1/metrics/json", "/v1/status",
    "/v1/billing/checkout", "/v1/billing/portal", "/v1/billing/usage", "/v1/billing/webhook",
    "/docs", "/docs/oauth2-redirect", "/redoc", "/openapi.json",
})
# the path under /v1/ns/{ns}/, with {id} for a record/session id
_NS_ROUTE_SHAPES = {
    (): "/v1/ns/:ns",
    ("events",): "/v1/ns/:ns/events",
    ("memories",): "/v1/ns/:ns/memories",
    ("memories", None): "/v1/ns/:ns/memories/:id",
    ("search",): "/v1/ns/:ns/search",
    ("export",): "/v1/ns/:ns/export",
    ("stats",): "/v1/ns/:ns/stats",
    ("sessions", None, "close"): "/v1/ns/:ns/sessions/:id/close",
    ("compact",): "/v1/ns/:ns/compact",
    ("find_ids",): "/v1/ns/:ns/find_ids",
    ("forget",): "/v1/ns/:ns/forget",
    ("reembed",): "/v1/ns/:ns/reembed",
}
_METHOD_LABELS = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"})


def _route_label(path: str) -> str:
    """The route TEMPLATE a path matches, as a metric label - never a
    request-supplied string.

    Namespace names and record ids are UNBOUNDED cardinality (scales with
    tenants and rows), and the http series carry no `ns` label, so every
    tenant key sees them on /metrics: a raw segment in a label handed one
    tenant's namespace names and ids - or any string a caller put in a path,
    404s included - to every other tenant, and let anyone grow the registry
    until its eviction guard dropped unrelated series. Only the known route
    shapes are labels (/v1/ns/{ns}/memories/{id} -> /v1/ns/:ns/memories/:id);
    everything else is "other"."""
    if path in _FIXED_ROUTE_LABELS:
        return path
    parts = path.split("/")
    if len(parts) >= 4 and parts[0] == "" and parts[1] == "v1" and parts[2] == "ns" and parts[3]:
        rest = parts[4:]
        for shape, label in _NS_ROUTE_SHAPES.items():
            if len(shape) == len(rest) and all(
                    (seg != "") if want is None else seg == want for want, seg in zip(shape, rest)):
                return label
    return "other"


def _method_label(method: str) -> str:
    """The HTTP method is a client-chosen token, too."""
    return method if method in _METHOD_LABELS else "OTHER"


def create_app(
    data_dir: str = "./memd-data",
    keys_path: str | None = None,
    admin_key: str | None = None,
    *,
    hosted: bool | None = None,
    plans: Any = None,
    billing_config: Any = None,
    stripe_client: Any = None,
    cluster: Any = None,
    state_dir: str | None = None,
) -> FastAPI:
    """`hosted` (default: MEMD_HOSTED) turns on tenancy, metering and
    billing (memd.hosted); off, nothing of it is imported - stripe least of
    all - and the server behaves exactly as before.

    `cluster` (a memd.server.cluster.ClusterConfig; `memd serve --node-id`)
    makes this process one node of a fleet on one s3:// data root: requests
    for a namespace another node holds are proxied there (ADR-12).
    `state_dir` (default MEMD_STATE_DIR, else `data_dir`) holds the server's
    own state - keys.toml.json and the hosted admin database. Cluster nodes
    must share it (hosted: one admin database for the fleet)."""
    state_dir = state_dir or os.environ.get("MEMD_STATE_DIR") or None
    if cluster is not None:
        if not str(data_dir).startswith("s3://"):
            raise ValueError("cluster mode needs an s3:// data root: the bucket holds the leases, "
                             "the node registry and (with a KMS key provider) the wrapped keys")
        if state_dir is None:
            raise ValueError("cluster mode needs MEMD_STATE_DIR: the API keys / hosted admin "
                             "database every node shares")
    state_dir = state_dir or data_dir
    os.makedirs(state_dir, exist_ok=True)
    if not str(data_dir).startswith("s3://"):
        os.makedirs(data_dir, exist_ok=True)
    from memd.hosted import hosted_enabled

    hosted_ctx = None
    is_hosted = hosted_enabled(hosted)
    if is_hosted:
        from memd.hosted.billing import BillingConfig

        # the sk_live_ guard runs before anything is opened
        billing_config = billing_config or BillingConfig.from_env()
        keystore = None
    else:
        keys_path = keys_path or os.path.join(state_dir, "keys.toml.json")
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
    node = None
    if cluster is None:
        engine = Memory(data_dir)
    else:
        from memd.server.cluster import Cluster
        from memd.storage.crypto import resolve_key_provider_name

        if resolve_key_provider_name({}) == "local":
            raise ValueError(
                "cluster mode needs a remote key provider (MEMD_KEY_PROVIDER=aws-kms or "
                "vault-transit): with `local` keys only the node that created a namespace can "
                "decrypt it. Existing stores: `memd keys migrate --to ...` first")
        # node-local state (derived index cache) is per NODE, even when every
        # node on a host was given the same MEMD_LOCAL_DIR: two nodes sharing
        # one namespace's SQLite cache across a handoff would corrupt it
        local_dir = os.path.join(os.environ.get("MEMD_LOCAL_DIR") or ".memd-local",
                                 f"node-{cluster.node_id}")
        # The facade's own namespace is per node (every node pins its facade
        # namespace, so a shared "default" would be leased by one node
        # forever). A node restarted right after a crash finds it still
        # leased by its previous process: wait that lease out (reclaimable
        # after the TTL) rather than fail to start.
        deadline = time.monotonic() + cluster.lease_ttl_s + 5
        while True:
            try:
                engine = Memory(data_dir, namespace=cluster.node_namespace,
                                config={"lease_holder": cluster.holder,
                                        "lease_ttl_s": cluster.lease_ttl_s, "local_dir": local_dir})
                break
            except NamespaceBusyError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(1.0)
        node = Cluster(cluster, engine.engine.store)
    bill: Any = _Unmetered()
    if is_hosted:
        from memd.hosted.app import Hosted

        hosted_ctx = Hosted(state_dir, engine, admin_key=admin_key, plans=plans,
                            billing_config=billing_config, stripe_client=stripe_client,
                            router=node.router if node is not None else None)
        keystore = hosted_ctx.keystore
        bill = hosted_ctx.metering

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        if node is not None:
            node.start()
        if hosted_ctx is not None:
            hosted_ctx.jobs.start()
        try:
            yield
        finally:
            # Graceful handoff: stop advertising first (peers stop hashing
            # new namespaces here), then close - which flushes every open
            # namespace and RELEASES its lease, so a peer takes it over on
            # its next request instead of waiting out the TTL.
            if node is not None:
                node.stop_advertising()
            if hosted_ctx is not None:
                hosted_ctx.close()
            engine.close()
            if node is not None:
                await node.aclose()

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
    app.state.hosted = hosted_ctx

    from memd.metrics import METRICS

    MAX_BODY_BYTES = 8 * 1024 * 1024  # request-body DoS cap

    @app.middleware("http")
    async def instrument_requests(request: Request, call_next):
        # bucket paths: /v1/ns/{ns}/memories/{id} -> /v1/ns/:ns/memories/:id
        # (label cardinality guard - see _route_label)
        route_label = _route_label(request.url.path)
        method_label = _method_label(request.method)
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
            METRICS.inc("memd_http_errors_total", route=route_label, method=method_label, kind="value")
            return _error(400, str(ex))
        except sqlite3.Error:
            # lifecycle races surface as driver errors (namespace destroyed /
            # index closed mid-request): clean 503, no internals leaked
            METRICS.inc("memd_http_errors_total", route=route_label, method=method_label, kind="storage")
            return _error(503, "namespace unavailable (destroyed or rebuilding)")
        except Exception:
            METRICS.inc("memd_http_errors_total", route=route_label, method=method_label)
            # sanitized 500: tracebacks go to the server log, never the client
            return _error(500, "internal error")
        finally:
            METRICS.observe("memd_http_request_ms", (time.monotonic() - t0) * 1000,
                            help="HTTP request duration (ms)", route=route_label, method=method_label)
        METRICS.inc("memd_http_requests_total", route=route_label, method=method_label,
                    status=str(response.status_code))
        return response

    @app.exception_handler(NamespaceBusyError)
    async def namespace_busy_handler(request: Request, exc: NamespaceBusyError):
        # another process (another node) holds this namespace's writer lease:
        # retryable, and the cluster router re-resolves on the header
        METRICS.inc("memd_http_not_owner_total", help="requests for a namespace another writer holds")
        return _error(503, "namespace is held by another node; retry", "not_owner",
                      headers={"Retry-After": "1", "X-Memd-Not-Owner": "1"})

    from memd.storage.s3store import LeaseLostError

    @app.exception_handler(LeaseLostError)
    async def lease_lost_handler(request: Request, exc: LeaseLostError):
        # this node was fenced mid-request: the mutation did not happen, but
        # earlier steps of the request may have - so no transparent retry
        # (no X-Memd-Not-Owner); the client's retry is routed to the new owner
        METRICS.inc("memd_http_lease_lost_total", help="requests failed by a lost writer lease")
        return _error(503, "this node lost the namespace's writer lease mid-request; retry",
                      "lease_lost", headers={"Retry-After": "1"})

    @app.exception_handler(RuntimeError)
    async def runtime_error_handler(request: Request, exc: RuntimeError):
        # namespace lifecycle races surface here - map them to clean
        # responses instead of leaking driver internals ("closed database")
        msg = str(exc)
        if "destroyed" in msg:
            return _error(410, "namespace destroyed", "namespace_destroyed")
        if "evicted" in msg:
            return _error(503, "namespace re-opening; retry", headers={"Retry-After": "1"})
        METRICS.inc("memd_http_errors_total", route=_route_label(request.url.path))
        return _error(500, "internal error")

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(request: Request, exc: StarletteHTTPException):
        # every raised error, the routing 404/405 included: detail + code
        return _error(exc.status_code, exc.detail, getattr(exc, "code", None),
                      getattr(exc, "headers", None), getattr(exc, "extra", None))

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(request: Request, exc: RequestValidationError):
        return _error(422, jsonable_encoder(exc.errors()))

    bearer = HTTPBearer(auto_error=False)

    def _client_id(request: Request) -> str:
        # Best available pre-auth identity. Direct connections: the peer IP.
        # Behind a trusted proxy: the forwarded-for first hop is spoofable, so
        # we use it only when a proxy header exists AND the socket peer is
        # loopback (a deployment that terminates untrusted traffic locally).
        routed = getattr(request.state, "memd_client", None)
        if routed:
            return routed  # a cluster peer proxied it: the client it verified
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
        bill.authorize(p, ns)  # hosted: the namespace belongs to the key's org
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
        # labelled with the key's AUTHORIZED namespace, never the path's: this
        # runs for requests being denied too, and a path namespace would let
        # any key mint unbounded series (and show its strings to operators)
        if not limiter.allow(p.key_id, p.rate_limit_per_min):
            METRICS.inc("memd_rate_limited_total", ns=p.namespace, scope="key")
            raise _rate_limited("rate limit exceeded",
                                limiter.retry_after(p.key_id, p.rate_limit_per_min))
        owner = p.namespace if p.namespace != "*" else (ns or "*")
        if owner != "*" and not ns_limiter.allow(f"ns:{owner}", ns_rate_limit_per_min):
            METRICS.inc("memd_rate_limited_total", ns=p.namespace, scope="namespace")
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
        with bill.admit(p, ns, writes=len(events)) as meter:  # hosted: check + reserve
            ids = engine.add_events(events, namespace=ns)
            meter.record(writes=len(ids))  # durable before the ack
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
        with bill.admit(p, ns, writes=1) as meter:
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
            meter.record(writes=1)
        return {"id": rid}

    @app.post("/v1/ns/{ns}/search")
    def search(ns: str, body: SearchIn, p: Principal = Depends(auth)):
        user, _ = apply_scope(p, body.user_id, body.session_id)
        with bill.admit(p, ns, searches=1) as meter:
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
                rerank=meter.allow_rerank,  # hosted: False once the reranked quota is spent
            )
            meter.record(searches=1, reranked=res.reranked)
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
        if ok and not existing.get("deleted"):
            bill.stored_delta(ns, -1)  # deletes are never gated, only counted down
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
        bill.stored_reset(ns)
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
        with bill.admit(p, ns, extract=True, session_id=session_id, user_id=p.pinned_user) as meter:
            res = engine.close_session(session_id, user_id=p.pinned_user, namespace=ns,
                                       extract_limit=meter.extract_limit, max_facts=meter.max_facts)
            meter.record(extraction=res)
        return res

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
        bill.stored_delta(ns, -len(deleted))
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
        return {"ok": True, "version": __import__("memd").__version__}

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
            bill.require_memory(p)  # hosted: operational data is `memory`-scoped
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

        bill.require_memory(p)
        snap = METRICS.snapshot(ns_filter=None if p.namespace == "*" else {p.namespace})
        snap["_epoch_ms"] = int(_t.time() * 1000)
        return snap

    @app.get("/v1/status")
    def status(p: Principal = Depends(auth)):
        # authenticated AND scoped: the namespace inventory is tenant
        # information, so a key bound to one namespace sees only that one
        bill.require_memory(p)
        return engine.status(ns_filter=None if p.namespace == "*" else p.namespace)

    if hosted_ctx is not None:
        hosted_ctx.install(app, auth)
    if node is not None:
        node.install(app)  # outermost: routes before any auth, metering or handler
    app.state.cluster = node
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
