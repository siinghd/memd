"""Namespace router: any node serves any namespace.

memd scales by NAMESPACE, not by concurrent writers inside one: each
namespace has exactly one writer at a time - the node holding its S3 lease
(`ns/<ns>/.owner`, see S3ObjectStore.try_acquire_owner). This module lets a
fleet of `memd serve --http --node-id N` processes on one bucket look like
one server:

  - every node heartbeats a small registry object `_cluster/nodes/<id>.json`
    (its URL + a wall-clock beat) every TTL/3; a node whose beat is older
    than the TTL is not live. A graceful shutdown deletes it.
  - a request for /v1/ns/<ns>/... that reaches any node is resolved:
      1. this node holds the lease          -> serve locally
      2. another node holds a FRESH lease   -> PROXY to that node
      3. nobody holds it (or it is stale)   -> rendezvous hashing over the
         live nodes picks one: local if it is us, else proxy there.
    The hash is only a HINT that stops nodes racing for a free namespace;
    the lease is authoritative. Whoever serves a request opens the
    namespace, which acquires (or CAS-reclaims) the lease; losing that race
    answers 503 + X-Memd-Not-Owner, and the entry node re-resolves.
  - proxy, not redirect: clients (and the SDKs) keep one base URL, API keys
    never have to be valid "for a node", and the entry node can retry a
    handoff for a few seconds instead of bouncing the client. The cost is
    one extra hop on a miss; `memd_router_requests_total{decision}` shows
    the hit rate.
  - a proxied request carries X-Memd-Route: the entry node's id, a
    timestamp, the original client address and an HMAC over them with the
    shared MEMD_CLUSTER_SECRET. The receiving node serves it LOCALLY (never
    re-forwards: no loops) and uses that client address for its
    auth-failure throttle. Without a valid signature the header is ignored.
  - metering and auth happen once, on the node that executes the request:
    the entry node forwards the raw request (Authorization included) before
    any of its own handlers run, so usage is never counted twice.

Handoff: a graceful stop deregisters, then closes the engine (flush +
lease release); a crashed node's leases go stale after the TTL and the next
request for each namespace is routed to a live node, which reclaims it.

Read replicas: a search or get that opted into eventual
consistency (X-Memd-Read-Consistency: eventual) and reaches a node that does
not hold the namespace's lease is served by THAT node's replica of the
namespace - no hop, so a load balancer spreads a hot namespace's reads over
every node. A replica that cannot serve it within the caller's staleness
bound answers 503 X-Memd-Replica-Unavailable, which is intercepted here: the
request then takes the normal route to the writer, invisibly. Writes, strong
reads and requests a peer routed in are never served by a replica.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import ipaddress
import json
import logging
import os
import re
import secrets
import socket
import threading
import time
from dataclasses import dataclass, field

from memd.metrics import METRICS

_log = logging.getLogger("memd.cluster")

NODE_PREFIX = "_cluster/nodes"
NODE_NS_PREFIX = "memd-node."
ROUTE_HEADER = "x-memd-route"
NOT_OWNER_HEADER = "x-memd-not-owner"
CONSISTENCY_HEADER = "x-memd-read-consistency"
REPLICA_UNAVAILABLE_HEADER = "x-memd-replica-unavailable"
# the reads a replica may serve: POST .../search and GET .../memories/{id}
_REPLICA_READS = (("POST", re.compile(r"^/v1/ns/[^/]+/search/?$")),
                  ("GET", re.compile(r"^/v1/ns/[^/]+/memories/[^/]+/?$")))
_NODE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_NS_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")   # memd.storage.engine._validate_ns
# hop-by-hop headers (RFC 9110 7.6.1) plus the ones the client recomputes
_HOP = {b"connection", b"keep-alive", b"proxy-authenticate", b"proxy-authorization", b"te",
        b"trailer", b"transfer-encoding", b"upgrade", b"host", b"content-length", ROUTE_HEADER.encode()}
_ROUTE_MAX_SKEW_S = 60.0


@dataclass
class ClusterConfig:
    node_id: str
    advertise_url: str
    secret: str
    lease_ttl_s: float = 60.0
    route_retry_s: float = 3.0       # how long an entry node retries a handoff before 503
    proxy_timeout_s: float = 300.0   # an export streams for a long time
    route_cache_s: float = 1.0
    incarnation: str = field(default_factory=lambda: f"{socket.gethostname()}:{os.getpid()}:"
                                                     f"{secrets.token_hex(3)}")

    def __post_init__(self) -> None:
        if not _NODE_RE.fullmatch(self.node_id or ""):
            raise ValueError(f"invalid node id {self.node_id!r}: [A-Za-z0-9][A-Za-z0-9_.-]{{0,63}}")
        if not self.secret or len(self.secret) < 16:
            raise ValueError("cluster mode needs MEMD_CLUSTER_SECRET (>= 16 chars), shared by every "
                             "node: it signs proxied requests")
        if not self.advertise_url.startswith(("http://", "https://")):
            raise ValueError(f"advertise URL must be http(s)://host:port, got {self.advertise_url!r}")
        self.advertise_url = self.advertise_url.rstrip("/")

    @property
    def holder(self) -> str:
        """The lease holder string: node id first, so any node can map a
        lease to the node serving it; the incarnation makes a restarted
        node wait out its previous process's lease instead of adopting it."""
        return f"{self.node_id}@{self.incarnation}"

    @property
    def node_namespace(self) -> str:
        """The server facade's own namespace (its admin audit ledger). Per
        node, so nodes never contend for it; reserved - never routed."""
        return f"{NODE_NS_PREFIX}{self.node_id}"

    @classmethod
    def from_env(cls, node_id: str | None = None, advertise: str | None = None,
                 host: str | None = None, port: int | None = None) -> "ClusterConfig | None":
        node_id = node_id or os.environ.get("MEMD_NODE_ID")
        if not node_id:
            return None
        advertise = advertise or os.environ.get("MEMD_ADVERTISE_URL")
        if not advertise:
            h = host or os.environ.get("MEMD_HOST", "127.0.0.1")
            if _is_wildcard(h):
                raise ValueError("binding all interfaces: set --advertise / MEMD_ADVERTISE_URL to the "
                                 "address other nodes reach this one at")
            advertise = f"http://{h}:{port or int(os.environ.get('MEMD_PORT', '8700'))}"
        return cls(node_id=node_id, advertise_url=advertise,
                   secret=os.environ.get("MEMD_CLUSTER_SECRET", ""),
                   lease_ttl_s=float(os.environ.get("MEMD_LEASE_TTL_S", "60")),
                   route_retry_s=float(os.environ.get("MEMD_ROUTE_RETRY_S", "3")))


def _is_wildcard(host: str) -> bool:
    """True for an unspecified address (0.0.0.0, ::, any spelling of them):
    bound there, a node has no address other nodes could reach it at."""
    try:
        return ipaddress.ip_address(host).is_unspecified
    except ValueError:      # a hostname
        return False


def node_of(holder: str) -> str | None:
    """The node id in a cluster lease holder ("n1@host:pid:tok"); None for
    a non-cluster writer ("host:pid")."""
    if "@" not in holder:
        return None
    nid = holder.split("@", 1)[0]
    return nid if _NODE_RE.fullmatch(nid) else None


def rendezvous(ns: str, nodes) -> list[str]:
    """Highest-random-weight order of `nodes` for `ns`: stable under
    membership change - adding or removing a node moves only the
    namespaces whose top choice it was."""
    def score(n: str) -> int:
        return int.from_bytes(hashlib.blake2b(f"{n}\0{ns}".encode(), digest_size=8).digest(), "big")
    return sorted(nodes, key=lambda n: (score(n), n), reverse=True)


def namespace_of_path(path: str) -> str | None:
    parts = path.split("/")
    if len(parts) >= 4 and parts[0] == "" and parts[1] == "v1" and parts[2] == "ns" and parts[3]:
        return parts[3]
    return None


# --------------------------------------------------------------- registry


class NodeRegistry:
    """Membership = registry objects with fresh heartbeats in the bucket.
    No gossip, no consensus: the bucket every node already needs is the
    source of truth, and a stale view only costs a retry (the lease decides
    who writes)."""

    CACHE_S = 1.0

    def __init__(self, store, cfg: ClusterConfig):
        self.store = store
        self.cfg = cfg
        self.key = f"{NODE_PREFIX}/{cfg.node_id}.json"
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._urls: dict[str, str] = {}       # node id -> advertised URL (cached)
        self._live_at = 0.0
        self._live: dict[str, dict] = {}
        self._ver: str | None = None      # our entry's version, as last read or written
        self.lost = False

    def _beat(self, *, stopped: bool = False) -> None:
        """Write our registry entry - conditionally, on the version we last
        read or wrote: if anyone else wrote it since (a twin with
        our node id, an operator), we stop advertising rather than fight."""
        from memd.storage.objectstore import PreconditionFailed

        body = {"node": self.cfg.node_id, "url": self.cfg.advertise_url,
                "incarnation": self.cfg.incarnation, "beat": time.time()}
        # a stopped node's entry is EMPTY: visible as such in a LIST (live())
        data = b"" if stopped else json.dumps(body).encode()
        try:
            self._ver = self.store.put_if_match(self.key, data, self._ver)
        except PreconditionFailed:
            METRICS.inc("memd_cluster_registry_conflicts_total",
                        help="registry entries changed by another process (duplicate node id?)")
            _log.error("memd cluster: registry entry %s was changed by another process; this "
                       "node stops advertising (duplicate --node-id?)", self.key)
            self._stop.set()
            self.lost = True
            raise

    def start(self) -> None:
        """Register. A registration of the same node id that is still
        heartbeating belongs to another live process (a twin started with the
        same id - refused) or to this node's crashed predecessor (waited out:
        its leases are only reclaimable after the TTL anyway)."""
        deadline = time.time() + self.cfg.lease_ttl_s + 2
        while True:
            try:
                got = self.store.get_versioned(self.key)
                self._ver = got[1] if got else None
                cur = json.loads(got[0]) if got and got[0] else {}
            except Exception:
                cur = {}
            age = time.time() - float(cur.get("beat", 0) or 0)
            if not cur or cur.get("incarnation") == self.cfg.incarnation or age > self.cfg.lease_ttl_s:
                break
            if time.time() > deadline:
                raise RuntimeError(f"node id {self.cfg.node_id!r} is registered by another live "
                                   f"process ({cur.get('incarnation')}); node ids must be unique")
            _log.warning("memd cluster: node id %s still registered by %s; waiting for it to expire",
                         self.cfg.node_id, cur.get("incarnation"))
            time.sleep(min(1.0, self.cfg.lease_ttl_s / 3))
        self._beat()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="memd-cluster-registry")
        self._thread.start()

    def _loop(self) -> None:
        period = max(0.5, self.cfg.lease_ttl_s / 3.0)
        from memd.storage.objectstore import PreconditionFailed

        while not self._stop.wait(period):
            try:
                self._beat()
            except PreconditionFailed:
                return            # another process owns our entry: stop (see _beat)
            except Exception:  # noqa: BLE001 - the next beat retries
                METRICS.inc("memd_cluster_registry_beat_failures_total",
                            help="node registry heartbeats that failed")

    def stop(self) -> None:
        """Deregister (graceful): other nodes stop hashing namespaces here
        at once instead of after a TTL."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        if self.lost:
            return
        try:
            # a "stopped" entry, compare-and-swapped onto ours: never removes
            # or overwrites an entry another process wrote since
            self._beat(stopped=True)
        except Exception:  # noqa: BLE001 - it expires on its own
            pass

    def live(self) -> dict[str, dict]:
        """Nodes whose registry entry was written within the TTL - judged
        from ONE LIST: LastModified against the server's own Date, so
        neither node clocks nor a peer frozen mid-write (which can hold an
        object's lock on MinIO) can stall or skew it. A stopped node's entry
        is empty. Cached for CACHE_S."""
        now = time.monotonic()
        with self._lock:
            if now - self._live_at < self.CACHE_S:
                return dict(self._live)
        out: dict[str, dict] = {}
        for o in self.store.list_meta(f"{NODE_PREFIX}/"):
            name = o["key"].rsplit("/", 1)[-1]
            if not name.endswith(".json") or o["size"] == 0:
                continue                   # a stopped node (see _beat)
            nid = name[:-len(".json")]
            if o["age_s"] <= self.cfg.lease_ttl_s:
                out[nid] = {"node": nid, "age_s": o["age_s"]}
        with self._lock:
            self._live, self._live_at = out, time.monotonic()
        return dict(out)

    def url_of(self, node_id: str) -> str | None:
        """The URL a node advertises: read from its entry once and cached
        (it is fixed per process; forget_url drops it after a failed proxy)."""
        with self._lock:
            url = self._urls.get(node_id)
        if url:
            return url
        try:
            got = self.store.get_versioned(f"{NODE_PREFIX}/{node_id}.json")
            url = json.loads(got[0]).get("url") if got and got[0] else None
        except Exception:  # noqa: BLE001 - unknown for now; routing waits
            url = None
        if url:
            with self._lock:
                self._urls[node_id] = url
        return url

    def forget_url(self, node_id: str) -> None:
        with self._lock:
            self._urls.pop(node_id, None)


# ----------------------------------------------------------------- router


@dataclass(frozen=True)
class Decision:
    kind: str                # local | proxy | wait
    node: str | None = None
    url: str | None = None
    reason: str = ""


class Router:
    """Where a namespace's requests go right now. Synchronous (it does
    object-store I/O); the middleware runs it in a worker thread."""

    def __init__(self, cfg: ClusterConfig, store, registry: NodeRegistry):
        self.cfg = cfg
        self.store = store
        self.registry = registry
        self._cache: dict[str, tuple[Decision, float]] = {}
        self._lock = threading.Lock()

    def forget(self, ns: str) -> None:
        with self._lock:
            self._cache.pop(ns, None)

    def _remember(self, ns: str, d: Decision) -> Decision:
        with self._lock:
            self._cache[ns] = (d, time.monotonic() + self.cfg.route_cache_s)
            if len(self._cache) > 10_000:
                self._cache.clear()
        return d

    def live_nodes(self) -> set[str]:
        return set(self.registry.live()) | {self.cfg.node_id}

    def is_leader(self, key: str) -> bool:
        """This node is the rendezvous choice for `key` among live nodes
        (singleton background jobs: the billing push)."""
        return rendezvous(key, self.live_nodes())[0] == self.cfg.node_id

    def still_serving(self, ns: str, node: str) -> bool:
        """Is `node` still a sane place for a request to `ns` that is waiting
        on it? False once it stops heartbeating or another node holds a
        fresh lease on the namespace."""
        if node not in self.registry.live():
            return False
        info = self.store.read_owner(ns)
        return not (info and info["fresh"] and node_of(info["holder"]) != node)

    def resolve(self, ns: str, exclude: frozenset = frozenset()) -> Decision:
        if self.store.holds_lease(ns, fresh=True):
            return Decision("local", self.cfg.node_id, reason="held")
        with self._lock:
            hit = self._cache.get(ns)
        if hit is not None and hit[1] > time.monotonic() and hit[0].node not in exclude:
            return hit[0]
        info = self.store.read_owner(ns)
        if info is not None and info["fresh"]:
            holder = info["holder"]
            if holder == self.cfg.holder:
                return Decision("local", self.cfg.node_id, reason="held")
            node = node_of(holder)
            if node is None:
                return Decision("wait", reason="leased by a writer outside the cluster")
            if node == self.cfg.node_id:
                return Decision("wait", node, reason="leased by this node's previous process until the TTL")
            if node in exclude:
                return Decision("wait", node, reason="leaseholder unreachable; waiting for the TTL")
            if node not in self.registry.live():
                # it stopped heartbeating (crashed, frozen, partitioned, or
                # shutting down): its lease goes stale within the TTL
                return Decision("wait", node, reason="leaseholder not live; waiting for the TTL")
            url = self.registry.url_of(node)
            if not url:
                return Decision("wait", node, reason="leaseholder not registered")
            return self._remember(ns, Decision("proxy", node, url, reason="lease"))
        candidates = (self.live_nodes() - set(exclude)) | {self.cfg.node_id}
        target = rendezvous(ns, candidates)[0]
        if target == self.cfg.node_id:
            return Decision("local", target, reason="hint")
        url = self.registry.url_of(target)
        if not url:
            return Decision("local", self.cfg.node_id, reason="hint target has no url")
        return self._remember(ns, Decision("proxy", target, url, reason="hint"))


# ------------------------------------------------------------- middleware


def _sign(secret: str, node: str, ts: str, client: str, method: str, path: str) -> str:
    msg = f"{node}|{ts}|{client}|{method}|{path}".encode()
    return hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest()


def _clean_client(c: str) -> str:
    return re.sub(r"[^0-9A-Za-z.:_\-\[\]]", "", c)[:64] or "unknown"


def _json(status: int, detail: str, code: str, headers: list | None = None):
    body = json.dumps({"detail": detail, "code": code}).encode()
    hdrs = [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())]
    return {"type": "http.response.start", "status": status, "headers": hdrs + (headers or [])}, body


class ClusterMiddleware:
    """Pure ASGI (not BaseHTTPMiddleware): the body is buffered once and
    replayed to the local app or a peer, and the local app's response can be
    intercepted before a byte of it reaches the client - a local attempt
    that lost the lease race is retried elsewhere, invisibly."""

    def __init__(self, app, router: Router, cfg: ClusterConfig, max_body: int = 8 * 1024 * 1024):
        self.app = app
        self.router = router
        self.cfg = cfg
        self.max_body = max_body
        self._client = None

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _http(self):
        if self._client is None:
            import httpx

            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.cfg.proxy_timeout_s, connect=2.0, pool=5.0),
                limits=httpx.Limits(max_connections=256, max_keepalive_connections=64))
        return self._client

    # -------------------------------------------------------------- helpers

    @staticmethod
    async def _send_json(send, status: int, detail: str, code: str, headers: list | None = None) -> None:
        start, body = _json(status, detail, code, headers)
        await send(start)
        await send({"type": "http.response.body", "body": body, "more_body": False})

    async def _read_body(self, receive) -> bytes | None:
        chunks, n = [], 0
        while True:
            msg = await receive()
            if msg["type"] == "http.disconnect":
                return b"".join(chunks)
            chunk = msg.get("body", b"")
            n += len(chunk)
            if n > self.max_body:
                return None
            chunks.append(chunk)
            if not msg.get("more_body"):
                return b"".join(chunks)

    @staticmethod
    def _replay(body: bytes, receive):
        """The buffered body once, then the real channel (which reports the
        client's disconnect)."""
        sent = False

        async def replay():
            nonlocal sent
            if not sent:
                sent = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()
        return replay

    def _verified_route(self, scope) -> dict | None:
        raw = None
        for k, v in scope.get("headers") or []:
            if k == ROUTE_HEADER.encode():
                raw = v.decode("latin-1")
        if raw is None:
            return None
        try:
            node, ts, client, sig = raw.split("|")
            ok = hmac.compare_digest(sig, _sign(self.cfg.secret, node, ts, client,
                                                scope["method"], scope["path"]))
            fresh = abs(time.time() - float(ts)) <= _ROUTE_MAX_SKEW_S
        except Exception:
            ok = fresh = False
        if not (ok and fresh):
            METRICS.inc("memd_router_bad_route_header_total",
                        help="X-Memd-Route headers that failed verification (ignored)")
            return None
        return {"node": node, "client": client}

    @staticmethod
    def _wants_replica(scope) -> bool:
        """An eligible read that accepts eventual consistency."""
        if not any(scope["method"] == m and rx.match(scope["path"]) for m, rx in _REPLICA_READS):
            return False
        for k, v in scope.get("headers") or []:
            if k == CONSISTENCY_HEADER.encode():
                return v.decode("latin-1").strip().lower() == "eventual"
        from urllib.parse import parse_qs

        qs = parse_qs((scope.get("query_string") or b"").decode("latin-1"))
        return (qs.get("consistency") or [""])[-1].strip().lower() == "eventual"

    @staticmethod
    def _client_of(scope) -> str:
        """The same pre-auth identity rule as the REST server's _client_id."""
        client = (scope.get("client") or ("unknown", 0))[0]
        if client in ("127.0.0.1", "::1"):
            for k, v in scope.get("headers") or []:
                if k == b"x-forwarded-for":
                    return "fwd:" + v.decode("latin-1").split(",")[0].strip()
        return client

    # ----------------------------------------------------------------- ASGI

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        ns = namespace_of_path(scope["path"])
        if ns is None or not _NS_RE.fullmatch(ns):
            # not a namespace route, or not a valid namespace name: the app
            # answers it (400/404) at once - no store lookup, no retry loop
            return await self.app(scope, receive, send)
        if ns.startswith(NODE_NS_PREFIX):
            # a node's own facade namespace: never served to a tenant
            return await self._send_json(send, 404, f"namespace {ns!r} not found", "not_found")
        body = await self._read_body(receive)
        if body is None:
            return await self._send_json(send, 413, "request body too large", "payload_too_large")
        routed = self._verified_route(scope)
        if routed is not None:
            # a peer resolved this namespace to us: serve it, never forward
            # it again (no loops); throttle auth failures by the REAL client
            scope.setdefault("state", {})["memd_client"] = routed["client"]
            METRICS.inc("memd_router_requests_total", help="namespace requests by routing decision",
                        decision="routed_in")
            return await self.app(scope, self._replay(body, receive), send)
        client = _clean_client(self._client_of(scope))
        from starlette.concurrency import run_in_threadpool

        if self._wants_replica(scope):
            try:
                held = await run_in_threadpool(self.router.store.holds_lease, ns, True)
            except Exception:  # noqa: BLE001 - cannot tell: let the replica serve it
                held = False
            if not held:
                # served by this node's replica of the namespace; one that
                # cannot (too stale, a custody refusal, ...) says so and the
                # request takes the writer's route below instead
                rscope = dict(scope)
                rscope["state"] = dict(scope.get("state") or {}, memd_replica_ok=True)
                if await self._local(rscope, body, receive, send,
                                     intercept=(NOT_OWNER_HEADER, REPLICA_UNAVAILABLE_HEADER)):
                    METRICS.inc("memd_router_requests_total", decision="replica")
                    return
                METRICS.inc("memd_router_requests_total", decision="replica_fallback")
        deadline = time.monotonic() + self.cfg.route_retry_s
        exclude: set[str] = set()
        attempt = 0
        last = "no route"

        while True:
            t_r = time.monotonic()
            try:
                d = await run_in_threadpool(self.router.resolve, ns, frozenset(exclude))
            except Exception as ex:  # noqa: BLE001 - the object store is unreachable
                d = Decision("wait", reason=f"routing lookup failed: {type(ex).__name__}")
            last = d.reason
            _log.debug("route %s %s: %s %s (%s) resolved in %.0f ms", scope["method"], ns, d.kind,
                       d.node, d.reason, (time.monotonic() - t_r) * 1000)
            if d.kind == "local":
                if await self._local(scope, body, receive, send):
                    METRICS.inc("memd_router_requests_total", decision="local")
                    return
                METRICS.inc("memd_router_requests_total", decision="not_owner_retry")
                self.router.forget(ns)
            elif d.kind == "proxy":
                outcome = await self._proxy(d, ns, scope, body, send, client)
                if outcome == "done":
                    METRICS.inc("memd_router_requests_total", decision="proxy")
                    return
                METRICS.inc("memd_router_requests_total", decision=outcome)
                self.router.forget(ns)
                if outcome == "unreachable":
                    exclude.add(d.node)
                    self.router.registry.forget_url(d.node)   # it may have moved
            if time.monotonic() >= deadline:
                break
            await asyncio.sleep(min(0.4, 0.05 * (2 ** attempt)))
            attempt += 1
        METRICS.inc("memd_router_requests_total", decision="unavailable")
        await self._send_json(send, 503, f"namespace owner unavailable ({last}); retry",
                              "namespace_unavailable", [(b"retry-after", b"1")])

    async def _local(self, scope, body: bytes, receive, send,
                     intercept: tuple[str, ...] = (NOT_OWNER_HEADER,)) -> bool:
        """Run the local app. False (nothing sent) when it answered with one
        of the `intercept` headers: not-owner (another node won the lease
        between resolve and open), or replica-unavailable."""
        state = {"not_owner": False}
        wanted = {h.encode() for h in intercept}

        async def send_wrapper(msg):
            if msg["type"] == "http.response.start":
                if any(k.lower() in wanted for k, _ in msg.get("headers") or []):
                    state["not_owner"] = True
                    return
            elif state["not_owner"]:
                return
            await send(msg)

        # a fresh copy per attempt: the framework annotates the scope it runs
        await self.app(dict(scope), self._replay(body, receive), send_wrapper)
        return not state["not_owner"]

    async def _await_response(self, d: Decision, ns: str, fut):
        """Wait for the owner's response headers, checking every TTL/3 that
        it is still alive and still the owner. A FROZEN owner (SIGSTOP, a VM
        pause) still accepts connections - the kernel queues them - so
        without this watchdog a request would hang for the whole proxy
        timeout while another node had long taken the namespace over.
        None: the owner was given up on."""
        from starlette.concurrency import run_in_threadpool

        period = max(0.5, self.cfg.lease_ttl_s / 3.0)
        while True:
            done, _ = await asyncio.wait({fut}, timeout=period)
            if done:
                return fut.result()
            try:
                alive = await run_in_threadpool(self.router.still_serving, ns, d.node)
            except Exception:  # noqa: BLE001 - cannot tell: keep waiting
                alive = True
            if not alive:
                fut.cancel()
                try:
                    resp = await fut
                    await resp.aclose()
                except BaseException:
                    pass
                return None

    async def _proxy(self, d: Decision, ns: str, scope, body: bytes, send, client: str) -> str:
        import httpx

        path = scope.get("raw_path") or scope["path"].encode()
        url = d.url + path.decode("latin-1")
        if scope.get("query_string"):
            url += "?" + scope["query_string"].decode("latin-1")
        ts = f"{time.time():.3f}"
        sig = _sign(self.cfg.secret, self.cfg.node_id, ts, client, scope["method"], scope["path"])
        headers = [(k, v) for k, v in scope.get("headers") or [] if k.lower() not in _HOP]
        headers.append((ROUTE_HEADER.encode(), f"{self.cfg.node_id}|{ts}|{client}|{sig}".encode()))
        t0 = time.monotonic()
        http = self._http()
        try:
            req = http.build_request(scope["method"], url, headers=headers, content=body)
            resp = await self._await_response(d, ns, asyncio.ensure_future(http.send(req, stream=True)))
            if resp is None:
                # it may have executed (or may, if it resumes - then fenced):
                # the client decides whether to retry
                await self._send_json(send, 503, "the namespace's node stopped responding; retry",
                                      "owner_unresponsive", [(b"retry-after", b"1")])
                return "done"
        except (httpx.ConnectError, httpx.ConnectTimeout):
            return "unreachable"         # never sent: safe to try elsewhere
        except httpx.TimeoutException:
            # it may have executed there: do not replay a write elsewhere
            await self._send_json(send, 504, "namespace owner timed out", "owner_timeout")
            return "done"
        except (httpx.RemoteProtocolError, httpx.ReadError) as ex:
            # the connection died before a response (a pooled keep-alive the
            # owner closed while shutting down, or the owner died): a read can
            # be retried elsewhere, a write may have executed - say so
            if scope["method"] in ("GET", "HEAD"):
                return "unreachable"
            await self._send_json(send, 502, f"namespace owner failed ({type(ex).__name__})", "owner_error")
            return "done"
        except httpx.HTTPError as ex:
            await self._send_json(send, 502, f"namespace owner failed ({type(ex).__name__})", "owner_error")
            return "done"
        try:
            if resp.status_code == 503 and resp.headers.get(NOT_OWNER_HEADER):
                await resp.aread()
                return "not_owner"
            out_headers = [(k.encode("latin-1"), v.encode("latin-1"))
                           for k, v in resp.headers.multi_items()
                           if k.lower().encode() not in (b"connection", b"keep-alive", b"transfer-encoding")]
            await send({"type": "http.response.start", "status": resp.status_code, "headers": out_headers})
            try:
                async for chunk in resp.aiter_raw():
                    await send({"type": "http.response.body", "body": chunk, "more_body": True})
            except httpx.HTTPError:
                pass     # the owner died mid-stream: end what we have
            await send({"type": "http.response.body", "body": b"", "more_body": False})
            return "done"
        finally:
            await resp.aclose()
            METRICS.observe("memd_router_proxy_ms", (time.monotonic() - t0) * 1000,
                            help="proxied namespace request duration (ms)")


class Cluster:
    """The node's cluster runtime: registry + router + middleware."""

    def __init__(self, cfg: ClusterConfig, store):
        if not hasattr(store, "read_owner"):
            raise ValueError("cluster mode needs an object store with leases (an s3:// data root)")
        self.cfg = cfg
        self.store = store
        self.registry = NodeRegistry(store, cfg)
        self.router = Router(cfg, store, self.registry)
        self.middleware: ClusterMiddleware | None = None

    def install(self, app) -> None:
        cluster = self

        class _Bound(ClusterMiddleware):
            def __init__(self, app):
                super().__init__(app, cluster.router, cluster.cfg)
                cluster.middleware = self

        app.add_middleware(_Bound)

    def start(self) -> None:
        self.registry.start()
        _log.info("memd cluster: node %s at %s (lease TTL %.0fs)", self.cfg.node_id,
                  self.cfg.advertise_url, self.cfg.lease_ttl_s)

    def stop_advertising(self) -> None:
        self.registry.stop()

    async def aclose(self) -> None:
        if self.middleware is not None:
            await self.middleware.aclose()
