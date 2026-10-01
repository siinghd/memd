"""Pass 38: REST contract fixes found while building the TypeScript SDK.

1. A confirmed forget ignored `as_of` / `kinds` - only the preview applied
   them - so a preview filtered to kinds=["fact"] followed by the confirm
   deleted every kind that matched the query (data loss). The confirm now
   applies the same filters, and refuses (409) when the preview's
   fingerprint no longer matches what it would delete.
2. A hard DELETE of an already soft-deleted record answered 404 (the
   existence check hid soft-deleted records), so it could not be purged
   through REST.
3. `?history=true` served soft-deleted content until compaction. Deleted
   records are no longer returned by history reads (superseded history
   still is) unless an admin passes include_deleted=true.
4. 429 responses carry `Retry-After` (seconds).
5. `DELETE /v1/ns/{ns}` on a namespace that does not exist is a 404 (it
   used to create the namespace, then shred it, and answer 200).
6. Error bodies carry a machine-readable `code` next to `detail`.
"""
import pytest
from fastapi.testclient import TestClient

from memd.engine.memory import Memory
from memd.server.http import create_app


@pytest.fixture()
def app(tmp_path):
    a = create_app(data_dir=str(tmp_path / "data"), keys_path=str(tmp_path / "keys.json"))
    yield a
    a.state.engine.close()


def _client(app, ns: str = "acme", **kw) -> TestClient:
    full, _ = app.state.keystore.create(ns, **kw)
    c = TestClient(app)
    c.headers["Authorization"] = f"Bearer {full}"
    return c


def _seed_kinds(c: TestClient) -> tuple[str, str]:
    fact = c.post("/v1/ns/acme/memories", json={
        "content": "zebra migration fact kappa", "user_id": "u1"}).json()["id"]
    raw = c.post("/v1/ns/acme/events", json={"events": [
        {"content": "zebra migration chatter kappa", "user_id": "u1"}]}).json()["ids"][0]
    return fact, raw


class TestForgetConfirmAppliesThePreviewFilters:
    def test_a_kinds_filtered_confirm_deletes_only_those_kinds(self, app):
        c = _client(app, scope_override=True)
        fact, raw = _seed_kinds(c)
        body = {"query": "zebra migration kappa", "user_id": "u1", "kinds": ["fact"]}
        prev = c.post("/v1/ns/acme/forget", json=body).json()
        assert [p["id"] for p in prev["will_delete"]] == [fact]
        done = c.post("/v1/ns/acme/forget", json={**body, "confirm": True})
        assert done.status_code == 200
        assert done.json()["deleted"] == [fact]
        assert c.get(f"/v1/ns/acme/memories/{raw}").status_code == 200, \
            "a kind the preview excluded was deleted"
        assert c.get(f"/v1/ns/acme/memories/{fact}").status_code == 404

    def test_an_as_of_confirm_deletes_what_its_preview_showed(self, app):
        """as_of picks the view as of then: the superseded fact is in it (the
        current view hides it), so the unfiltered confirm left it behind."""
        import time

        c = _client(app, scope_override=True)
        old = c.post("/v1/ns/acme/memories", json={
            "content": "quokka lives in perth", "user_id": "u1",
            "entity_keys": ["quokka.home"]}).json()["id"]
        time.sleep(0.02)
        t_mid = int(time.time() * 1000)
        time.sleep(0.02)
        c.post("/v1/ns/acme/memories", json={
            "content": "quokka lives in rottnest", "user_id": "u1",
            "entity_keys": ["quokka.home"]})
        body = {"query": "quokka lives", "user_id": "u1", "as_of": t_mid}
        prev = c.post("/v1/ns/acme/forget", json=body).json()
        want = {p["id"] for p in prev["will_delete"]}
        assert old in want
        done = c.post("/v1/ns/acme/forget", json={**body, "confirm": True}).json()
        assert set(done["deleted"]) == want

    def test_a_confirm_whose_matches_changed_since_the_preview_is_refused(self, app):
        c = _client(app, scope_override=True)
        _seed_kinds(c)
        body = {"query": "zebra migration kappa", "user_id": "u1"}
        prev = c.post("/v1/ns/acme/forget", json=body).json()
        assert prev["fingerprint"]
        extra = c.post("/v1/ns/acme/memories", json={
            "content": "zebra migration kappa arrived later", "user_id": "u1"}).json()["id"]
        r = c.post("/v1/ns/acme/forget",
                   json={**body, "confirm": True, "fingerprint": prev["fingerprint"]})
        assert r.status_code == 409 and r.json()["code"] == "preview_mismatch"
        assert c.get(f"/v1/ns/acme/memories/{extra}").status_code == 200, "nothing deleted"
        again = c.post("/v1/ns/acme/forget", json=body).json()
        ok = c.post("/v1/ns/acme/forget",
                    json={**body, "confirm": True, "fingerprint": again["fingerprint"]})
        assert ok.status_code == 200 and extra in ok.json()["deleted"]

    def test_the_facade_forget_takes_the_same_filters(self, tmp_path):
        m = Memory(str(tmp_path / "d"), encrypt=False, config={"embedder": "hash"})
        try:
            fact = m.remember("zebra migration fact kappa", user_id="u1")
            raw = m.add("zebra migration chatter kappa", user_id="u1")[0]
            gone = m.forget("zebra migration kappa", user_id="u1", kinds=["fact"])
            assert gone == [fact]
            assert m.get(raw) is not None
        finally:
            m.close()


def test_a_soft_deleted_record_can_still_be_hard_deleted(app):
    c = _client(app)
    rid = c.post("/v1/ns/acme/memories", json={"content": "purge me later", "user_id": "u1"}).json()["id"]
    assert c.delete(f"/v1/ns/acme/memories/{rid}").status_code == 200
    r = c.delete(f"/v1/ns/acme/memories/{rid}?hard=true")
    assert r.status_code == 200 and r.json()["hard"] is True
    ns = app.state.engine.engine.namespace("acme")
    assert ns.pending_hard_deletes == 1, "the purge must be scheduled"
    assert c.delete(f"/v1/ns/acme/memories/{rid}?hard=true").status_code == 404, \
        "a hard-deleted record is gone"
    assert c.delete("/v1/ns/acme/memories/01NOSUCHRECORD000000000000?hard=true").status_code == 404


def test_history_does_not_serve_soft_deleted_content(app):
    c = _client(app)
    admin = _client(app, scope_override=True)
    rid = c.post("/v1/ns/acme/memories", json={
        "content": "secret soon deleted", "user_id": "u1"}).json()["id"]
    c.delete(f"/v1/ns/acme/memories/{rid}")
    r = c.get(f"/v1/ns/acme/memories/{rid}?history=true")
    assert r.status_code == 404 and r.json()["code"] == "not_found"
    denied = c.get(f"/v1/ns/acme/memories/{rid}?history=true&include_deleted=true")
    assert denied.status_code == 403 and denied.json()["code"] == "forbidden"
    got = admin.get(f"/v1/ns/acme/memories/{rid}?history=true&include_deleted=true")
    assert got.status_code == 200 and got.json()["deleted"] is True


def test_history_keeps_superseded_records_and_drops_deleted_ones(app):
    c = _client(app)
    old = c.post("/v1/ns/acme/memories", json={
        "content": "Alice works at Initech", "user_id": "u1",
        "entity_keys": ["alice.employer"]}).json()["id"]
    new = c.post("/v1/ns/acme/memories", json={
        "content": "Alice works at Initrode", "user_id": "u1",
        "entity_keys": ["alice.employer"]}).json()["id"]
    chain = c.get(f"/v1/ns/acme/memories/{new}?history=true").json()["history"]
    assert {h["id"] for h in chain} == {old, new}, "superseded history is still served"
    c.delete(f"/v1/ns/acme/memories/{old}")
    chain = c.get(f"/v1/ns/acme/memories/{new}?history=true").json()["history"]
    assert [h["id"] for h in chain] == [new], "a deleted predecessor leaked through history"


def test_429_carries_retry_after(app):
    c = _client(app)
    r = None
    for _ in range(20):
        r = c.post("/v1/ns/acme/forget", json={"query": "q"})
        if r.status_code == 429:
            break
    assert r is not None and r.status_code == 429
    assert int(r.headers["Retry-After"]) >= 1
    assert r.json()["code"] == "rate_limited" and r.json()["detail"]
    anon = TestClient(app)
    for _ in range(40):
        r = anon.get("/v1/ns/acme/stats", headers={"Authorization": "Bearer memd_acme_x_bad"})
        if r.status_code == 429:
            break
    assert r.status_code == 429 and int(r.headers["Retry-After"]) >= 1


def test_destroying_a_namespace_that_does_not_exist_is_a_404(app):
    admin = _client(app, ns="*", scope_override=True)
    r = admin.delete("/v1/ns/never-created")
    assert r.status_code == 404 and r.json()["code"] == "not_found"
    assert "never-created" not in app.state.engine.engine.list_namespaces(), \
        "a 404 must not leave the namespace behind"
    admin.post("/v1/ns/real/events", json={"events": [{"content": "x"}]})
    assert admin.delete("/v1/ns/real").status_code == 200


def test_error_bodies_carry_a_machine_readable_code(app):
    c = _client(app)
    anon = TestClient(app)
    cases = [
        (anon.get("/v1/ns/acme/stats"), 401, "unauthorized"),
        (c.get("/v1/ns/other/stats"), 403, "forbidden"),
        (c.get("/v1/ns/acme/memories/01NOSUCHRECORD000000000000"), 404, "not_found"),
        (c.post("/v1/ns/acme/search", json={"query": ""}), 422, "validation_error"),
        (c.post("/v1/ns/acme/search", json={"query": "x", "kinds": ["nope"]}), 422,
         "validation_error"),
    ]
    for r, status, code in cases:
        assert r.status_code == status, (r.status_code, r.text)
        body = r.json()
        assert body["code"] == code and "detail" in body, body
