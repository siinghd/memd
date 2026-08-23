"""Hosted-mode SDK tests: embedded Memory behind the REST server, driven
through HostedMemory - proves the "same API, three doors" identity."""
import pytest
from fastapi.testclient import TestClient

from memd.sdk.client import HostedMemory
from memd.server.auth import KeyStore
from memd.server.http import create_app


@pytest.fixture()
def hosted(tmp_path):
    app = create_app(data_dir=str(tmp_path / "data"), keys_path=str(tmp_path / "keys.json"))
    ks: KeyStore = app.state.keystore
    full, _ = ks.create("acme")
    t = TestClient(app)
    t.headers["Authorization"] = f"Bearer {full}"
    h = HostedMemory.__new__(HostedMemory)
    h.api_key = full
    h.namespace = "acme"
    h._client = _ASGIAdapterClient(t)  # type: ignore[assignment]
    yield h
    app.state.engine.close()


class _ASGIAdapterClient:
    """httpx.Client-like shim over TestClient with base_url + auth headers."""

    def __init__(self, testclient: TestClient):
        self.tc = testclient

    def post(self, url, json=None, params=None):
        return self.tc.post(url, json=json, params=params)

    def get(self, url, params=None):
        return self.tc.get(url, params=params)

    def delete(self, url, params=None):
        return self.tc.delete(url, params=params)


def test_hosted_add_search(hosted):
    ids = hosted.add("the staging api is at staging.internal", user_id="u1", session_id="s1")
    assert len(ids) == 1
    res = hosted.search("staging api location", user_id="u1")
    assert res.items and "staging.internal" in res.packed_context


def test_hosted_remember_supersede_and_history(hosted):
    r1 = hosted.remember("Bob works at Initech", entity_keys=["user.employer"], user_id="u1")
    r2 = hosted.remember("Bob works at Initrode", entity_keys=["user.employer"], user_id="u1")
    got = hosted.get(r1, history=True)
    assert got["history"]
    res = hosted.search("where does bob work?", user_id="u1")
    assert any("Initrode" in i.content for i in res.items)
    assert not any("Initech" in i.content for i in res.items if i.valid)


def test_hosted_pack_observe(hosted):
    msgs = [{"role": "user", "content": "we ship on fridays only"}]
    hosted.observe(msgs, "Understood: Friday ships.", user_id="u1", session_id="s2")
    packed = hosted.pack([{"role": "user", "content": "when do we ship?"}], user_id="u1")
    roles = [m["role"] for m in packed]
    assert "system" in roles


def test_hosted_delete_and_export(hosted):
    rid = hosted.remember("delete target", user_id="u1")
    assert hosted.delete(rid) is True
    assert hosted.get(rid) is None
    blob = hosted.export_jsonl()
    assert b"delete target" not in blob or b"deleted" in blob  # tombstoned records export with flag
