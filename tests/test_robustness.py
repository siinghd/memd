"""Robustness tests: degraded retrieval during embedder outage, extraction
failure isolation, request-body DoS cap."""
import json

from unittest import mock

from fastapi.testclient import TestClient

from memd.engine.memory import Memory


def test_search_degrades_when_embedder_down(tmp_path):
    """BM25+entity lanes must serve during an embedder API outage - retrieval
    degrades, it never dies (BM25-only mode)."""
    m = Memory(str(tmp_path / "d"))
    try:
        m.add("the deploy window is tuesdays at noon", user_id="u1")
        m.flush()
        with mock.patch.object(m.embedder, "embed_one",
                               side_effect=RuntimeError("upstream 503")):
            res = m.search("deploy window", user_id="u1")
        assert res.items, "degraded search returned nothing"
        assert any("tuesdays" in i.content for i in res.items)
        assert all("vector" not in i.lanes for i in res.items), "vector lane must be absent"
        snap = __import__("memd.metrics", fromlist=["METRICS"]).METRICS.snapshot()
        assert "memd_embed_query_failures_total" in snap["counters"]
        # destructive sweep also survives without the vector lane
        ids = m.find_ids("deploy window", user_id="u1")
        assert ids
    finally:
        m.close()


def test_close_session_survives_extractor_crash(tmp_path):
    """Extractor outage must not block the session boundary: rotation still
    happens (WAL cannot grow forever) and raw lane stays intact."""
    m = Memory(str(tmp_path / "d"))
    try:
        m.add("extractor outage probe", session_id="s-crash", user_id="u1")
        with mock.patch.object(m.extractor, "extract",
                               side_effect=RuntimeError("LLM timeout")):
            summary = m.close_session("s-crash")  # must NOT raise
        assert summary["facts_extracted"] == 0
        # segment rotated anyway -> WAL drained
        assert m.ns.manifest.wal_size == 0 or len(m.ns.manifest.segments) >= 1
        # raw lane intact and searchable
        res = m.search("extractor outage probe", user_id="u1")
        assert res.items
    finally:
        m.close()


def test_request_body_size_cap(tmp_path):
    from memd.server.http import create_app

    app = create_app(data_dir=str(tmp_path / "d"), keys_path=str(tmp_path / "k.json"))
    full, _ = app.state.keystore.create("acme")
    t = TestClient(app)
    t.headers["Authorization"] = f"Bearer {full}"
    # >8MB total body -> 413 before any processing
    r = t.post("/v1/ns/acme/events",
               content=json.dumps({"events": [{"content": "x" * 9 * 1024 * 1024, "user_id": "u1"}]}),
               headers={**t.headers, "Content-Type": "application/json"})
    assert r.status_code == 413, f"expected 413, got {r.status_code}"
    app.state.engine.close()


def test_key_name_capped(tmp_path):
    from memd.server.auth import KeyStore

    ks = KeyStore(str(tmp_path / "k.json"))
    ks.create("acme", name="n" * 10_000)
    listed = ks.list_keys()
    assert all(len(k.get("name", "")) <= 128 for k in listed)


def test_empty_string_scope_normalized(tmp_path):
    """Empty-string user_id must behave like None, not bind a bogus scope."""
    m = Memory(str(tmp_path / "d"))
    try:
        rid = m.add("empty scope probe", user_id="", session_id="")[0]
        rec = m.get(rid)
        assert rec["scope"].get("user") in (None, ""), type(rec["scope"])
        # searchable without any scope filter
        res = m.search("empty scope probe")
        assert res.items
    finally:
        m.close()
