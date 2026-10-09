"""REST server tests via ASGI transport (no socket)."""
import time

import httpx
import pytest
from fastapi.testclient import TestClient

from memd.server.auth import KeyStore
from memd.server.http import create_app


@pytest.fixture()
def client(tmp_path):
    app = create_app(data_dir=str(tmp_path / "data"), keys_path=str(tmp_path / "keys.json"))
    keystore: KeyStore = app.state.keystore
    full, kid = keystore.create("acme", name="test")
    t = TestClient(app)
    t.headers["Authorization"] = f"Bearer {full}"
    yield t
    app.state.engine.close()


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200 and r.json()["ok"]


def test_events_202_then_search(client):
    r = client.post("/v1/ns/acme/events", json={"events": [
        {"content": "prod deploys happen at 10am UTC", "user_id": "u1", "session_id": "s1"},
        {"content": "the staging api is at staging.internal", "user_id": "u1", "session_id": "s1"},
    ]})
    assert r.status_code == 202
    ids = r.json()["ids"]
    assert len(ids) == 2
    r2 = client.post("/v1/ns/acme/search", json={"query": "when do prod deploys happen?", "user_id": "u1"})
    assert r2.status_code == 200
    body = r2.json()
    assert body["items"], "search should find the just-written event"
    assert "packed_context" in body


def test_memory_explicit_lane_and_history(client):
    r = client.post("/v1/ns/acme/memories",
                    json={"content": "Alice works at Initech", "entity_keys": ["user.employer"],
                          "user_id": "u1"})
    rid = r.json()["id"]
    r2 = client.get(f"/v1/ns/acme/memories/{rid}?history=true")
    assert r2.status_code == 200
    assert r2.json()["history"]


def test_delete_flow(client):
    r = client.post("/v1/ns/acme/memories", json={"content": "delete me", "user_id": "u1"})
    rid = r.json()["id"]
    d = client.delete(f"/v1/ns/acme/memories/{rid}")
    assert d.status_code == 200
    g = client.get(f"/v1/ns/acme/memories/{rid}")
    assert g.status_code == 404


def test_auth_required(client):
    saved = client.headers.pop("Authorization")
    assert client.post("/v1/ns/acme/search", json={"query": "x"}).status_code == 401
    bad = client.post("/v1/ns/acme/search", json={"query": "x"},
                      headers={"Authorization": "Bearer memd_acme_aa_wrongsecret"})
    assert bad.status_code == 401
    client.headers["Authorization"] = saved


def test_cross_namespace_key_rejected(client):
    r = client.post("/v1/ns/other/events", json={"events": [{"content": "x"}]})
    assert r.status_code == 403


def test_pinned_user_cannot_widen(client, tmp_path):
    app = create_app(data_dir=str(tmp_path / "d2"), keys_path=str(tmp_path / "k2.json"))
    ks: KeyStore = app.state.keystore
    full, _ = ks.create("acme", pinned_user="u9")
    c = TestClient(app)
    c.headers["Authorization"] = f"Bearer {full}"
    r = c.post("/v1/ns/acme/search", json={"query": "x", "user_id": "someone_else"})
    assert r.status_code == 403
    app.state.engine.close()


def test_export_ndjson(client):
    client.post("/v1/ns/acme/events", json={"events": [{"content": "export row", "user_id": "u1"}]})
    r = client.post("/v1/ns/acme/export")
    assert r.status_code == 200
    assert b"export row" in r.content


def test_session_close_endpoint(client):
    client.post("/v1/ns/acme/events", json={"events": [
        {"content": "my name is Zelda", "user_id": "u1", "session_id": "s5"},
    ]})
    r = client.post("/v1/ns/acme/sessions/s5/close")
    assert r.status_code == 200
    assert r.json()["raw_considered"] >= 1


def test_rate_limit(client):
    codes = []
    for i in range(50):
        r = client.post("/v1/ns/acme/search", json={"query": f"q{i}"})
        codes.append(r.status_code)
        if r.status_code == 429:
            break
    assert 200 in codes


def test_stats_endpoint(client):
    r = client.get("/v1/ns/acme/stats")
    assert r.status_code == 200 and "records" in r.json()


# ------------------------------------------------- write forwarding errors


def _raises(exc):
    def run(*a, **kw):
        raise exc
    return run


@pytest.mark.parametrize("exc, status, code, may_be_applied, retry_after", [
    # no writer answered the forwarded call: nothing was applied, retry
    ("unavailable", 503, "forward_unavailable", False, "1"),
    # the holder refused this process: a retry gets the same answer
    ("refused", 503, "forward_refused", False, None),
    # sent, no answer: the write may have been applied
    ("timeout", 504, "forward_timeout", True, "1"),
])
def test_forwarding_errors_have_their_own_status_and_code(client, monkeypatch, exc, status, code,
                                                          may_be_applied, retry_after):
    from memd.engine import forward as fw

    err = {"unavailable": fw.ForwardingError("no writer answered"),
           "refused": fw.ForwardAuthError("the holder refused this process"),
           "timeout": fw.ForwardTimeoutError("no answer from the writer")}[exc]
    monkeypatch.setattr(client.app.state.engine, "remember", _raises(err))
    r = client.post("/v1/ns/acme/memories", json={"content": "a forwarded write", "user_id": "u1"})
    assert r.status_code == status
    body = r.json()
    assert body["code"] == code and body["may_be_applied"] is may_be_applied
    assert r.headers.get("Retry-After") == retry_after
    assert "X-Memd-Not-Owner" not in r.headers
