"""Pass-3 loop fixes, attack-tested:
- /v1/status requires authentication (tenant inventory is not public)
- /metrics auth-failure flood is throttled (429), not an endless 401 oracle
- per-lane search timings (memd_lane_ms) land with the loop
- FTS query-injection attempts stay inert end-to-end over HTTP
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from memd.server.http import create_app  # noqa: E402


@pytest.fixture()
def client(tmp_path):
    app = create_app(data_dir=str(tmp_path / "data"), admin_key="admin-opaque-pass3")
    with TestClient(app) as c:
        yield c


def _mk_key(client, ns="acme"):
    ks = client.app.state.keystore
    full, kid = ks.create(ns)
    return full


class TestStatusAuthz:
    def test_status_requires_auth(self, client):
        r = client.get("/v1/status")
        assert r.status_code == 401, "namespace inventory must not be public"

    def test_status_with_valid_key(self, client):
        key = _mk_key(client)
        r = client.get("/v1/status", headers={"Authorization": f"Bearer {key}"})
        assert r.status_code == 200
        assert "namespaces" in r.json()


class TestMetricsFloodThrottle:
    def test_bad_auth_flood_eventually_429s(self, client):
        codes = [client.get("/metrics", headers={"Authorization": "Bearer memd_a_b_c"}).status_code
                 for _ in range(80)]
        assert 429 in codes, f"brute force must be throttled; got {sorted(set(codes))}"
        assert codes[-1] in (401, 429)

    def test_valid_scrape_still_works_before_flood(self, client):
        key = _mk_key(client)
        r = client.get("/metrics", headers={"Authorization": f"Bearer {key}"})
        assert r.status_code == 200 and "memd_" in r.text


class TestLaneInstrumentation:
    def test_lane_timings_emitted_per_search(self, client):
        from memd.metrics import METRICS

        key = _mk_key(client)
        h = {"Authorization": f"Bearer {key}"}
        assert client.post("/v1/ns/acme/events", headers=h,
                           json={"events": [{"content": "alpha beta gamma"}]}).status_code == 202
        # a local model loads lazily in the embed worker and search serves
        # without the vector lane until it is up (by design): wait for it
        client.app.state.engine.flush()
        # the hash embedder's lane is not fused by default (patch 3); this
        # test is about per-lane instrumentation, so run every lane
        client.app.state.engine.fuse_vector = True
        assert client.post("/v1/ns/acme/search", headers=h,
                           json={"query": "alpha"}).status_code == 200
        snap = METRICS.snapshot()
        lanes = {h_["labels"].get("lane") for h_ in snap["histograms"].get("memd_lane_ms", [])}
        assert {"bm25", "entity", "vector"} <= lanes, \
            f"per-lane stage timings missing: {lanes}"


class TestFtsInjectionEndToEnd:
    @pytest.mark.parametrize("evil", [
        '" OR 1=1 --',
        'x" NOTINDEXED && y',
        'a* OR b NEAR(*) c',
        '"; DROP TABLE records; --',
    ])
    def test_hostile_queries_served_not_crashed(self, client, evil):
        key = _mk_key(client)
        h = {"Authorization": f"Bearer {key}"}
        client.post("/v1/ns/acme/events", headers=h,
                    json={"events": [{"content": "the deploy command is make prod"}]})
        r = client.post("/v1/ns/acme/search", headers=h, json={"query": evil})
        assert r.status_code == 200, f"FTS injection crashed the search: {evil}"

    def test_records_table_alive_after_injection(self, client):
        key = _mk_key(client)
        h = {"Authorization": f"Bearer {key}"}
        for evil in ['"; DROP TABLE records; --', 'x" NOTINDEXED && y']:
            client.post("/v1/ns/acme/search", headers=h, json={"query": evil})
        r = client.post("/v1/ns/acme/events", headers=h,
                        json={"events": [{"content": "still writable after injection attempts"}]})
        assert r.status_code == 202
