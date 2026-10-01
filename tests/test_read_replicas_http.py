"""Read replicas over REST: the eventual-read opt-in, the served-by headers,
the replica-unavailable fallback in the router, and the SDK options.

The cluster router is driven here with a scripted router and app (no
processes); tests/test_read_replicas_cluster.py runs real nodes."""
import asyncio

import httpx
import pytest
from fastapi.testclient import TestClient

from memd.engine.memory import Memory
from memd.sdk.client import HostedMemory
from memd.server.auth import KeyStore
from memd.server.cluster import ClusterConfig, ClusterMiddleware, Decision
from memd.server.http import create_app

CFG = {"embedder": "hash", "rate_max_writes": 10 ** 9, "reranker": "none"}


@pytest.fixture()
def server(tmp_path, monkeypatch):
    monkeypatch.setenv("MEMD_EMBEDDER", "hash")
    monkeypatch.setenv("MEMD_RERANKER", "none")
    app = create_app(data_dir=str(tmp_path / "data"), keys_path=str(tmp_path / "keys.json"))
    ks: KeyStore = app.state.keystore
    full, _ = ks.create("*", name="admin", scope_override=True)
    t = TestClient(app)
    t.headers["Authorization"] = f"Bearer {full}"
    yield app, t, tmp_path
    app.state.engine.close()


def _write_elsewhere(tmp_path, ns: str, content: str) -> str:
    """A writer that is not the server: the server holds no writer store of
    `ns`, so an eventual read there is a replica read."""
    m = Memory(str(tmp_path / "data"), namespace=ns, config=CFG)
    try:
        return m.add(content)[0]
    finally:
        m.close()


def _replica_marked(app):
    """What the cluster middleware does for an eventual read on a node that
    does not hold the lease: mark the request for this node's replica."""
    async def wrapped(scope, receive, send):
        if scope["type"] == "http":
            scope = dict(scope)
            scope["state"] = dict(scope.get("state") or {}, memd_replica_ok=True)
        await app(scope, receive, send)
    return wrapped


def test_a_single_node_serves_every_read_from_the_writer(server):
    app, t, _ = server
    rid = t.post("/v1/ns/acme/memories", json={"content": "the leader serves this alpha"}).json()["id"]
    for headers in ({}, {"X-Memd-Read-Consistency": "eventual"},
                    {"X-Memd-Read-Consistency": "eventual", "X-Memd-Max-Staleness-Ms": "0"}):
        r = t.post("/v1/ns/acme/search", json={"query": "leader serves"}, headers=headers)
        assert r.status_code == 200 and r.json()["items"]
        assert r.headers["X-Memd-Served-By"] == "leader"
        assert "X-Memd-Replica-Seq" not in r.headers
        g = t.get(f"/v1/ns/acme/memories/{rid}", headers=headers)
        assert g.status_code == 200 and g.headers["X-Memd-Served-By"] == "leader"
    r = t.post("/v1/ns/acme/search?consistency=eventual", json={"query": "leader serves"})
    assert r.headers["X-Memd-Served-By"] == "leader"


def test_bad_consistency_options_are_400(server):
    _, t, _ = server
    assert t.post("/v1/ns/acme/search", json={"query": "x"},
                  headers={"X-Memd-Read-Consistency": "sometimes"}).status_code == 400
    assert t.post("/v1/ns/acme/search", json={"query": "x"},
                  headers={"X-Memd-Read-Consistency": "eventual",
                           "X-Memd-Max-Staleness-Ms": "soon"}).status_code == 400
    assert t.get("/v1/ns/acme/memories/x?max_staleness_ms=-5").status_code == 400


def test_a_marked_eventual_read_is_served_by_the_replica(server):
    app, t, tmp = server
    rid = _write_elsewhere(tmp, "other", "a replica serves this bravo")
    rt = TestClient(_replica_marked(app))
    rt.headers.update(t.headers)
    r = rt.post("/v1/ns/other/search", json={"query": "replica serves"},
                headers={"X-Memd-Read-Consistency": "eventual"})
    assert r.status_code == 200, r.text
    assert r.headers["X-Memd-Served-By"] == "replica"
    assert int(r.headers["X-Memd-Replica-Seq"]) >= 1 and int(r.headers["X-Memd-Replica-Age-Ms"]) >= 0
    assert "bravo" in r.json()["packed_context"]
    g = rt.get(f"/v1/ns/other/memories/{rid}", headers={"X-Memd-Read-Consistency": "eventual"})
    assert g.status_code == 200 and g.headers["X-Memd-Served-By"] == "replica"
    missing = rt.get("/v1/ns/other/memories/nope", headers={"X-Memd-Read-Consistency": "eventual"})
    assert missing.status_code == 404 and missing.headers["X-Memd-Served-By"] == "replica"
    # the mark alone is not enough: a strong read goes to the writer
    s = rt.post("/v1/ns/other/search", json={"query": "replica serves"})
    assert s.headers["X-Memd-Served-By"] == "leader"
    # and a write is never served by a replica, whatever it asks
    w = rt.post("/v1/ns/other/memories", json={"content": "a write"},
                headers={"X-Memd-Read-Consistency": "eventual"})
    assert w.status_code == 201


def test_a_replica_that_cannot_serve_answers_replica_unavailable(server, monkeypatch):
    from memd.storage.replica import ReplicaStore, ReplicaUnavailableError

    app, t, tmp = server
    _write_elsewhere(tmp, "stale", "never served by a stale replica charlie")

    def refuse(self, max_staleness_s):
        raise ReplicaUnavailableError("staler than the bound")

    monkeypatch.setattr(ReplicaStore, "ensure_fresh", refuse)
    rt = TestClient(_replica_marked(app))
    rt.headers.update(t.headers)
    r = rt.post("/v1/ns/stale/search", json={"query": "stale replica"},
                headers={"X-Memd-Read-Consistency": "eventual"})
    assert r.status_code == 503
    assert r.headers["X-Memd-Replica-Unavailable"] == "1" and r.json()["code"] == "replica_unavailable"


# ------------------------------------------------------------ the router


class _Store:
    def __init__(self, held):
        self.held = held
        self.asked = 0

    def holds_lease(self, ns, fresh=False):
        self.asked += 1
        return self.held


class _Router:
    def __init__(self, held, *decisions):
        self.store = _Store(held)
        self.decisions = list(decisions)

    def resolve(self, ns, exclude=frozenset()):
        return self.decisions.pop(0) if len(self.decisions) > 1 else self.decisions[0]

    def forget(self, ns):
        pass


def _drive(mw, path, method="POST", headers=(), query=b""):
    sent = []
    chunks = [{"type": "http.request", "body": b"{}", "more_body": False}]

    async def receive():
        if chunks:
            return chunks.pop(0)
        await asyncio.sleep(3600)

    async def send(msg):
        sent.append(msg)

    scope = {"type": "http", "method": method, "path": path, "raw_path": path.encode(),
             "headers": list(headers), "client": ("1.2.3.4", 5), "query_string": query}
    asyncio.run(mw(scope, receive, send))
    return [m["status"] for m in sent if m["type"] == "http.response.start"]


def _app(answers):
    """An app that records whether each call was marked for the replica and
    answers from `answers` (status, headers) in turn."""
    seen = []

    async def app(scope, receive, send):
        await receive()
        seen.append(bool((scope.get("state") or {}).get("memd_replica_ok")))
        status, hdrs = answers.pop(0) if len(answers) > 1 else answers[0]
        await send({"type": "http.response.start", "status": status, "headers": hdrs})
        await send({"type": "http.response.body", "body": b"{}"})
    return app, seen


EVENTUAL = [(b"x-memd-read-consistency", b"eventual")]
CFG_N1 = ClusterConfig("n1", "http://127.0.0.1:1", "s" * 32)


def test_the_router_serves_an_eventual_read_from_this_nodes_replica():
    app, seen = _app([(200, [(b"x-memd-served-by", b"replica")])])
    router = _Router(False, Decision("proxy", "n2", "http://127.0.0.1:2"))
    statuses = _drive(ClusterMiddleware(app, router, CFG_N1), "/v1/ns/a/search", headers=EVENTUAL)
    assert statuses == [200] and seen == [True]
    # GET of a record, opted in through the query string
    app, seen = _app([(200, [])])
    statuses = _drive(ClusterMiddleware(app, _Router(False, Decision("local", "n1")), CFG_N1),
                      "/v1/ns/a/memories/r1", method="GET", query=b"consistency=eventual")
    assert statuses == [200] and seen == [True]


def test_an_unavailable_replica_falls_back_to_the_writer_invisibly():
    app, seen = _app([(503, [(b"x-memd-replica-unavailable", b"1")]), (200, [])])
    router = _Router(False, Decision("local", "n1"))      # the writer route: here
    statuses = _drive(ClusterMiddleware(app, router, CFG_N1), "/v1/ns/a/search", headers=EVENTUAL)
    assert statuses == [200], "the replica's 503 reached the client"
    assert seen == [True, False], "the fallback must be a strong read"


def test_the_leaseholder_and_non_eligible_requests_never_use_a_replica():
    # this node holds the lease: the writer serves it
    app, seen = _app([(200, [])])
    router = _Router(True, Decision("local", "n1"))
    assert _drive(ClusterMiddleware(app, router, CFG_N1), "/v1/ns/a/search", headers=EVENTUAL) == [200]
    assert seen == [False]
    # writes, strong reads, other routes: never asked, never marked
    for method, path, headers in (("POST", "/v1/ns/a/memories", EVENTUAL),
                                  ("DELETE", "/v1/ns/a/memories/r1", EVENTUAL),
                                  ("POST", "/v1/ns/a/export", EVENTUAL),
                                  ("POST", "/v1/ns/a/find_ids", EVENTUAL),
                                  ("POST", "/v1/ns/a/search", [])):
        app, seen = _app([(200, [])])
        router = _Router(False, Decision("local", "n1"))
        assert _drive(ClusterMiddleware(app, router, CFG_N1), path, method=method, headers=headers) == [200]
        assert seen == [False], (method, path)
        assert router.store.asked == 0, (method, path)


# ------------------------------------------------------------------ SDK


def _sdk(handler, **kw):
    h = HostedMemory("memd_k", base_url="http://memd.test", namespace="acme",
                     transport=httpx.MockTransport(handler), **kw)
    return h


SEARCH_BODY = {"packed_context": "", "items": [], "tokens_used": 0, "budget": 2000,
               "truncated": False, "query_class": "general", "latency_ms": 1.0}


def test_the_python_sdk_sends_the_options_and_reports_who_served():
    seen = []

    def handler(request: httpx.Request):
        seen.append(dict(request.headers))
        hdrs = {"X-Memd-Served-By": "replica", "X-Memd-Replica-Seq": "17", "X-Memd-Replica-Age-Ms": "250"}
        if request.url.path.endswith("/search"):
            return httpx.Response(200, json=SEARCH_BODY, headers=hdrs)
        return httpx.Response(200, json={"id": "r1", "content": "x"}, headers=hdrs)

    h = _sdk(handler, consistency="eventual", max_staleness_ms=4000)
    res = h.search("q")
    assert seen[0]["x-memd-read-consistency"] == "eventual" and seen[0]["x-memd-max-staleness-ms"] == "4000"
    assert (res.served_by, res.replica_seq, res.replica_age_ms) == ("replica", 17, 250)
    assert h.last_read == {"served_by": "replica", "applied_seq": 17, "age_ms": 250}
    h.get("r1", consistency="strong", max_staleness_ms=0)
    assert seen[1]["x-memd-read-consistency"] == "strong" and seen[1]["x-memd-max-staleness-ms"] == "0"
    with pytest.raises(ValueError):
        HostedMemory("k", consistency="sometimes")


def test_the_python_sdk_sends_nothing_by_default():
    seen = []

    def handler(request: httpx.Request):
        seen.append(dict(request.headers))
        return httpx.Response(200, json=SEARCH_BODY, headers={"X-Memd-Served-By": "leader"})

    h = _sdk(handler)
    res = h.search("q")
    assert "x-memd-read-consistency" not in seen[0] and "x-memd-max-staleness-ms" not in seen[0]
    assert res.served_by == "leader" and h.last_read["served_by"] == "leader"
    # Memory(api_key=...) passes the options through
    m = Memory(api_key="memd_k", base_url="http://memd.test", namespace="acme",
               transport=httpx.MockTransport(handler))
    m.search("q", consistency="eventual", max_staleness_ms=10)
    assert seen[1]["x-memd-read-consistency"] == "eventual"
