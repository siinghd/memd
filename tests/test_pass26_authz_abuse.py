"""Pass 18 regression: authorization boundaries and abuse by a VALID key.

Earlier passes attack-tested the pre-auth surface (credential stuffing,
oversized bodies, XFF spoofing). Nothing tested what a client holding a
legitimate key can do. Defects fixed here:

 1. HIGH  A record captured with only a session id - the most natural way an
          agent logs a turn - was returned to EVERY other user's scoped query
          in the namespace. Reproduced end to end.
 2. MED   A wrong-namespace 403 was booked as an AUTHENTICATION failure, in a
          bucket keyed by (spoofable) client identity, so any valid key could
          lock a chosen bucket out of the entire API.
 3. HIGH  `kinds` was an unbounded, unvalidated list: per-request work scaled
          with request size using values that can never match.
 4. MED   Rate budgets were per API key, so a tenant multiplied its quota by
          minting keys.
 5. MED   /metrics was the one authenticated route with no rate limit.
 6. LOW   Interactive docs and the OpenAPI schema were served unauthenticated.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from memd.core.schema import Scope  # noqa: E402
from memd.engine.memory import Memory  # noqa: E402
from memd.server.http import create_app  # noqa: E402


@pytest.fixture()
def client(tmp_path):
    app = create_app(data_dir=str(tmp_path / "data"), admin_key="admin-opaque-p18")
    with TestClient(app) as c:
        yield c


def _key(client, ns):
    full, _ = client.app.state.keystore.create(ns)
    return full


# ----------------------------------------------------------------- scope leak

class TestSessionScopePrivacy:
    def test_session_only_record_is_not_visible_to_another_user(self, tmp_path):
        m = Memory(str(tmp_path / "d"))
        try:
            m.add("bobs secret api key is sk-bob-123", session_id="s_bob")
            res = m.search("secret api key", user_id="alice")
            assert res.items == [], f"leaked to another user: {[i.content for i in res.items]}"
        finally:
            m.close()

    def test_session_only_record_is_visible_to_its_own_session(self, tmp_path):
        m = Memory(str(tmp_path / "d"))
        try:
            m.add("bobs secret api key is sk-bob-123", session_id="s_bob")
            assert m.search("secret api key", session_id="s_bob").items
        finally:
            m.close()

    def test_unscoped_query_still_sees_everything(self, tmp_path):
        """Embedded single-user mode asserts no restriction; it must not be
        silently narrowed by a privacy rule meant for multi-tenant queries."""
        m = Memory(str(tmp_path / "d"))
        try:
            m.add("a note captured with only a session", session_id="s1")
            assert m.search("note captured session").items
        finally:
            m.close()

    def test_user_query_still_sees_that_users_own_sessions(self, tmp_path):
        m = Memory(str(tmp_path / "d"))
        try:
            m.add("alice deployed the staging cluster", user_id="alice", session_id="s1")
            assert m.search("staging cluster", user_id="alice").items
        finally:
            m.close()

    def test_session_query_does_not_see_another_session(self, tmp_path):
        m = Memory(str(tmp_path / "d"))
        try:
            m.add("alice deployed the staging cluster", user_id="alice", session_id="s1")
            res = m.search("staging cluster", user_id="alice", session_id="s2")
            assert res.items == [], "session scoping must still narrow within a user"
        finally:
            m.close()

    def test_sql_and_python_filters_agree(self, tmp_path):
        """The leak survived the first fix because the time lane filters in
        Python while bm25 filters in SQL, and the two disagreed."""
        from memd.index.sqlite_index import IndexFilter

        m = Memory(str(tmp_path / "d"))
        try:
            rid = m.add("bobs secret api key", session_id="s_bob")[0]
            rec = m.ns.index.get_by_id(rid)
            f = IndexFilter(scope=Scope(user="alice"))
            assert not Scope(user="alice").contains(rec.scope)
            assert not m.ns.index._passes_filter(rec, f)
            assert m.ns.index.search_bm25("secret api key", f, limit=10) == []
            assert m.ns.index.search_time_lane(f, limit=10) == []
        finally:
            m.close()


# --------------------------------------------------------------- authz != authn

def test_wrong_namespace_403_does_not_lock_the_caller_out(client):
    ka = _key(client, "acme")
    h = {"Authorization": f"Bearer {ka}"}
    # FailureLimiter trips at 30 failures per 60s window, so this must exceed
    # it - at 25 the test passed vacuously on the unfixed code.
    for _ in range(60):
        r = client.get("/v1/ns/globex/stats", headers=h)
        assert r.status_code == 403, r.status_code
    # the key still works on its OWN namespace: a 403 is authorization, not a
    # failed authentication, and must not feed the anti-stuffing limiter
    assert client.get("/v1/ns/acme/stats", headers=h).status_code == 200
    # ...and a different tenant on the same client identity is unaffected
    kb = _key(client, "globex")
    assert client.get("/v1/ns/globex/stats",
                      headers={"Authorization": f"Bearer {kb}"}).status_code == 200


def test_invalid_keys_are_still_throttled(client):
    """The anti-credential-stuffing control must survive the fix above."""
    codes = [client.get("/v1/ns/acme/stats",
                        headers={"Authorization": f"Bearer memd_acme_bad_{i}"}).status_code
             for i in range(40)]
    assert 429 in codes, "invalid-key flooding must still be throttled"


# ------------------------------------------------------------ request shape

def test_kinds_is_bounded_and_validated(client):
    h = {"Authorization": f"Bearer {_key(client, 'acme')}"}
    ok = client.post("/v1/ns/acme/search", headers=h, json={"query": "x", "kinds": ["fact"]})
    assert ok.status_code == 200
    assert client.post("/v1/ns/acme/search", headers=h,
                       json={"query": "x", "kinds": ["not_a_kind"]}).status_code == 422
    assert client.post("/v1/ns/acme/search", headers=h,
                       json={"query": "x", "kinds": ["fact"] * 1000}).status_code == 422
    assert client.post("/v1/ns/acme/find_ids", headers=h,
                       json={"query": "x", "kinds": ["nope"]}).status_code == 422


# --------------------------------------------------------------- rate budgets

def test_quota_does_not_scale_with_the_number_of_keys(tmp_path, monkeypatch):
    monkeypatch.setenv("MEMD_NS_RATE_LIMIT_PER_MIN", "40")
    app = create_app(data_dir=str(tmp_path / "data"), admin_key="admin-p18b")
    with TestClient(app) as c:
        keys = [_key(c, "acme") for _ in range(5)]
        codes = []
        for i in range(120):
            k = keys[i % len(keys)]
            codes.append(c.get("/v1/ns/acme/stats",
                               headers={"Authorization": f"Bearer {k}"}).status_code)
        assert 429 in codes, "minting more keys must not multiply the tenant's quota"


def test_metrics_endpoint_is_rate_limited(client):
    h = {"Authorization": f"Bearer {_key(client, 'acme')}"}
    codes = [client.get("/metrics", headers=h).status_code for _ in range(90)]
    assert 429 in codes, "/metrics renders the whole registry and must have a budget"


# -------------------------------------------------------------------- surface

def test_docs_and_openapi_are_not_public_by_default(client):
    assert client.get("/docs").status_code == 404
    assert client.get("/openapi.json").status_code == 404
    assert client.get("/redoc").status_code == 404


def test_docs_can_be_enabled_for_development(tmp_path, monkeypatch):
    monkeypatch.setenv("MEMD_ENABLE_DOCS", "1")
    app = create_app(data_dir=str(tmp_path / "data"), admin_key="admin-p18c")
    with TestClient(app) as c:
        assert c.get("/openapi.json").status_code == 200


# ------------------------------------------------------------------ packaging

def test_dockerfile_does_not_mask_a_failed_core_install():
    """`pip install . && pip install mcp || true` parses as
    `(A && B) || true`, so a failed CORE install still produced a green
    image that fell over at runtime."""
    here = os.path.dirname(__file__)
    text = open(os.path.join(here, "..", "Dockerfile")).read()
    install = [ln for ln in text.splitlines() if ln.startswith("RUN pip install")]
    assert install, "expected a pip install layer"
    assert not any(ln.rstrip().endswith("|| true") for ln in install), \
        "trailing `|| true` masks a failure of the core install"
    assert 'import memd' in text, "the image should prove memd actually installed"


# ------------------------------------------------- measurement integrity

def test_cached_search_reports_its_own_latency(tmp_path):
    """A cache hit used to return the cached SearchResult verbatim, replaying
    the ORIGINAL MISS's latency_ms to every subsequent hit. bench/slo_bench.py
    graded the retrieval SLO from that field, so the gate was reading a stale
    number that described neither the hit nor a fresh query."""
    m = Memory(str(tmp_path / "d"))
    try:
        for i in range(300):
            m.add(f"deployment note {i} about the staging cluster", user_id="u")
        m.flush()
        miss = m.search("staging cluster deployment", user_id="u")
        hit = m.search("staging cluster deployment", user_id="u")
        assert hit.items == miss.items, "expected the same cached answer"
        assert hit.latency_ms < miss.latency_ms, (
            f"cache hit reported {hit.latency_ms}ms, miss was {miss.latency_ms}ms "
            "- the hit is replaying the miss's number")
    finally:
        m.close()


def test_bench_measures_retrieval_not_the_repeat_query_cache():
    """The retrieval SLO must be graded on distinct queries timed by the
    caller's clock, not on 25 questions asked 200 times."""
    import ast

    src = open(os.path.join(os.path.dirname(__file__), "..", "bench", "slo_bench.py")).read()
    fn = next(n for n in ast.parse(src).body
              if isinstance(n, ast.FunctionDef) and n.name == "bench_retrieval")
    # inspect the CODE, not the prose: the docstring names the old defect
    attrs = {f"{ast.unparse(n.value)}.{n.attr}" for n in ast.walk(fn)
             if isinstance(n, ast.Attribute)}
    assert not any(a.endswith(".latency_ms") for a in attrs), \
        "bench must not grade the SLO on memd's self-reported latency"
    body = ast.unparse(fn)
    assert "time.monotonic()" in body, "bench must time retrieval on the caller's clock"
    assert "cache_hit_p50" in body, "cache-hit latency should be reported separately"
