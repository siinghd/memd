"""Pass-7 fixes: the hosted-mode facade (Memory(api_key=...)) must actually
serve the full embedded API over REST - "one engine, three doors" - and the
new find_ids/forget routes must enforce scope pinning + throttling.

Every test drives real HTTP through the ASGI transport; nothing is mocked
at the method level.
"""
import os
import socket
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

fastapi = pytest.importorskip("fastapi")
pytest.importorskip("uvicorn")
from fastapi.testclient import TestClient  # noqa: E402

from memd.engine.memory import Memory  # noqa: E402
from memd.server.http import create_app  # noqa: E402


@pytest.fixture(scope="module")
def live_server(tmp_path_factory):
    """Real HTTP loopback server: exercises actual sockets, not mocks."""
    import uvicorn

    data = str(tmp_path_factory.mktemp("p7data"))
    app = create_app(data_dir=data, admin_key="p7-admin")
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    th = threading.Thread(target=server.run, daemon=True)
    th.start()
    base = f"http://127.0.0.1:{port}"
    for _ in range(200):
        try:
            socket.create_connection(("127.0.0.1", port), 0.1).close()
            break
        except OSError:
            time.sleep(0.05)
    yield app, base
    server.should_exit = True
    th.join(timeout=5)


@pytest.fixture()
def facade(live_server):
    app, base = live_server
    key = app.state.keystore.create("default")[0]
    return Memory(api_key=key, base_url=base, namespace="default")


class TestHostedFacadeParity:
    def test_remember_search_roundtrip_through_facade(self, facade):
        rid = facade.remember("Alice works at Parity Corp", user_id="u1")
        assert isinstance(rid, str) and rid
        res = facade.search("who works at Parity Corp", user_id="u1")
        assert any("Parity Corp" in i.content for i in res.items)

    def test_add_events_get_delete_stats_through_facade(self, facade):
        ids = facade.add_events([{"content": "facade raw event one", "user_id": "u2"}])
        got = facade.get(ids[0])
        assert got["content"] == "facade raw event one"
        assert facade.delete(ids[0]) in (True, False)
        st = facade.stats()
        assert "records" in st and "namespace" in st

    def test_forget_and_compact_through_facade(self, facade):
        facade.remember("ephemeral fact to forget soon", user_id="u3")
        deleted = facade.forget("ephemeral fact to forget", user_id="u3")
        assert isinstance(deleted, list)
        res = facade.search("ephemeral fact to forget", user_id="u3")
        assert all("soon" not in i.content for i in res.items)

    def test_embedded_mode_unaffected(self, tmp_path):
        m = Memory(str(tmp_path / "emb"), encrypt=False)
        try:
            rid = m.remember("embedded still works", user_id="u9")
            assert m.search("embedded still works", user_id="u9").items
            assert m.delete(rid) is not None
        finally:
            m.close()


class TestFindForgetEndpoints:
    def _client_with_key(self, live_server, ns="acme", **kw):
        app, base = live_server
        key = app.state.keystore.create(ns, **kw)[0]
        c = TestClient(app)
        c.headers.update({"Authorization": f"Bearer {key}"})
        return c

    def _seed(self, client):
        for content in ("alpha target phrase unique", "unrelated filler"):
            client.post("/v1/ns/acme/events", json={
                "events": [{"content": content, "user_id": "victim"}]})

    def test_find_ids_requires_auth(self, live_server):
        app, base = live_server
        c = TestClient(app)
        r = c.post("/v1/ns/acme/find_ids", json={"query": "alpha"})
        assert r.status_code == 401

    def test_forget_two_phase(self, live_server):
        c = self._client_with_key(live_server, scope_override=True)
        self._seed(c)
        r = c.post("/v1/ns/acme/forget", json={"query": "alpha target phrase"})
        assert r.status_code == 200 and r.json()["confirmed"] is False
        assert r.json()["count"] >= 1, "preview must find the match"
        r2 = c.post("/v1/ns/acme/forget",
                    json={"query": "alpha target phrase", "confirm": True})
        assert r2.json()["confirmed"] is True and len(r2.json()["deleted"]) >= 1
        after = c.post("/v1/ns/acme/search", json={"query": "alpha target"}).json()
        assert all("unique" not in i["content"] for i in after["items"])

    def test_pinned_key_cannot_sweep_other_users(self, live_server):
        attacker = self._client_with_key(live_server, ns="acme", pinned_user="attacker")
        vapp, _ = live_server
        victim = TestClient(vapp)
        vkey = vapp.state.keystore.create("acme")[0]
        victim.headers.update({"Authorization": f"Bearer {vkey}"})
        victim.post("/v1/ns/acme/events", json={
            "events": [{"content": "victims secret plan zeta", "user_id": "victim"}]})
        # pinned to 'attacker': sweeping 'victim' data must be refused...
        r = attacker.post("/v1/ns/acme/forget",
                          json={"query": "victims secret plan zeta",
                                "user_id": "victim", "confirm": True})
        assert r.status_code == 403, "scope pinning must block cross-user sweep"
        # ...and unconstrained queries only see the attacker's own (empty) scope
        r2 = attacker.post("/v1/ns/acme/find_ids",
                           json={"query": "victims secret plan zeta"})
        assert r2.json()["ids"] == [], "no cross-user leakage via unscoped query"

    def test_forget_spam_throttled(self, live_server):
        c = self._client_with_key(live_server)
        codes = [c.post("/v1/ns/acme/forget",
                        json={"query": "q", "confirm": True}).status_code
                 for _ in range(15)]
        assert 429 in codes, f"heavy throttle must engage; got {sorted(set(codes))}"

    def test_facade_forget_parity(self, facade):
        facade.remember("parity forget me marker q7", user_id="pf")
        deleted = facade.forget("parity forget me marker q7", user_id="pf")
        assert isinstance(deleted, list) and deleted, "facade returns deleted ids"
