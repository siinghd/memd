"""Pass 16 regression: telemetry is tenant data.

Metric labels carry namespace names, and /v1/status returns the namespace
inventory. Both surfaces returned the WHOLE fleet to any authenticated key -
including a key scoped to a single namespace, which is 403'd on every other
entry in the answer. One tenant could read every other tenant's traffic
shape, record counts, error rates and namespace names through a door they
have no access to.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from memd.server.http import create_app  # noqa: E402


@pytest.fixture()
def client(tmp_path):
    app = create_app(data_dir=str(tmp_path / "data"), admin_key="admin-opaque-p16")
    with TestClient(app) as c:
        yield c


def _key(client, ns):
    full, _ = client.app.state.keystore.create(ns)
    return full


def _seed(client, key, ns, text):
    h = {"Authorization": f"Bearer {key}"}
    r = client.post(f"/v1/ns/{ns}/memories", headers=h,
                    json={"content": text, "user_id": "u1"})
    assert r.status_code in (200, 201), r.text
    client.post(f"/v1/ns/{ns}/search", headers=h,
                json={"query": text.split()[0], "user_id": "u1"})


def test_prometheus_scrape_is_scoped_to_the_callers_namespace(client):
    ka, kb = _key(client, "acme"), _key(client, "globex")
    _seed(client, ka, "acme", "acme quarterly revenue figures")
    _seed(client, kb, "globex", "globex secret roadmap")

    body = client.get("/metrics", headers={"Authorization": f"Bearer {ka}"}).text
    assert 'ns="acme"' in body, "own telemetry must still be visible"
    assert 'ns="globex"' not in body, "another tenant's series leaked into the scrape"
    # process-global series (no ns label) stay visible
    assert "memd_process_rss_bytes" in body


def test_json_snapshot_is_scoped_to_the_callers_namespace(client):
    ka, kb = _key(client, "acme"), _key(client, "globex")
    _seed(client, ka, "acme", "acme quarterly revenue figures")
    _seed(client, kb, "globex", "globex secret roadmap")

    snap = client.get("/v1/metrics/json", headers={"Authorization": f"Bearer {ka}"}).json()
    seen = {lbl.get("ns")
            for fam in ("counters", "gauges", "histograms")
            for series in snap[fam].values()
            for lbl in (s["labels"] for s in series)}
    assert "globex" not in seen, seen
    assert "acme" in seen


def test_status_inventory_is_scoped(client):
    ka, kb = _key(client, "acme"), _key(client, "globex")
    _seed(client, ka, "acme", "acme quarterly revenue figures")
    _seed(client, kb, "globex", "globex secret roadmap")

    body = client.get("/v1/status", headers={"Authorization": f"Bearer {ka}"}).json()
    assert body["namespaces"] == ["acme"], body
    assert body["default_namespace"] == "acme"


def test_admin_wildcard_key_still_sees_the_fleet(client):
    ka, kb = _key(client, "acme"), _key(client, "globex")
    _seed(client, ka, "acme", "acme quarterly revenue figures")
    _seed(client, kb, "globex", "globex secret roadmap")

    hdr = {"Authorization": "Bearer admin-opaque-p16"}
    body = client.get("/v1/status", headers=hdr).json()
    assert {"acme", "globex"} <= set(body["namespaces"]), body
    scrape = client.get("/metrics", headers=hdr).text
    assert 'ns="acme"' in scrape and 'ns="globex"' in scrape


def test_scoped_status_costs_one_probe_not_a_store_walk(client, monkeypatch):
    """Scoping also removes the O(namespaces) object-store walk for the
    common case: a scoped key must not enumerate the fleet to answer."""
    ka = _key(client, "acme")
    _seed(client, ka, "acme", "acme quarterly revenue figures")
    engine = client.app.state.engine
    calls = []
    real = engine.engine.list_namespaces
    monkeypatch.setattr(engine.engine, "list_namespaces",
                        lambda: (calls.append(1), real())[1])
    client.get("/v1/status", headers={"Authorization": f"Bearer {ka}"})
    assert calls == [], "scoped status must not walk every namespace"
