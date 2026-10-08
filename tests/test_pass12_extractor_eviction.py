"""Pass 12 regression tests: LLM extractor chunking + eviction fail-fast.

  - LLMExtractor must send bounded chunks (never one giant completion for a
    whole session) and lose only the failed chunk, not the session's facts
  - a store evicted from the open-namespace cache must fail fast for
    stragglers holding stale references (no forked WAL/seq state); the HTTP
    surface maps that to a retryable 503
"""
import httpx
import pytest
from fastapi.testclient import TestClient

from memd.core.schema import MemoryRecord, Scope, Source
from memd.pipeline.extractor import HeuristicExtractor, LLMExtractor
from memd.storage.engine import StorageEngine


def _rec(i: int) -> MemoryRecord:
    return MemoryRecord.create(namespace="default", kind="raw_event",
                               content=f"my name is Tester{ i} and I work at Initech",
                               scope=Scope(user=f"u{i % 3}"), source=Source.USER)


class TestLLMExtractorChunking:
    def _extractor(self, handler, chunk_records=4, chunk_chars=24_000):
        ext = LLMExtractor(model="fake", api_key="k",
                           base_url="http://fake.local/v1",
                           chunk_records=chunk_records, chunk_chars=chunk_chars)
        ext._transport = httpx.MockTransport(handler)
        return ext

    @staticmethod
    def _ok_response(records):
        import json as _json

        facts = [{"content": f"fact about {r.id}", "entity_keys": ["fact.general"]}
                 for r in records]
        body = {"choices": [{"message": {"content": _json.dumps(facts)}}]}
        return httpx.Response(200, json=body)

    def test_large_session_sent_as_multiple_bounded_chunks(self):
        seen_sizes: list[int] = []
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            payload = request.read()
            seen_sizes.append(len(payload))
            lines = [l for l in payload.decode().splitlines() if l.startswith("[")]
            return self._ok_response([]) if not lines else self._ok_response([])

        ext = self._extractor(handler)
        recs = [_rec(i) for i in range(10)]
        ext.extract(recs)
        assert calls["n"] == 3, "10 records at chunk_records=4 must be 3 requests"

    def test_partial_chunk_failure_keeps_other_chunks(self):
        def handler(request: httpx.Request) -> httpx.Response:
            body = request.read().decode()
            # the chunk containing r5 fails hard; others succeed
            if "text 5" in body:
                return httpx.Response(500, json={"error": "provider down"})
            import json as _json

            rid = "r0" if "text 0" in body else ("r8" if "text 8" in body else "rx")
            facts = [{"content": f"fact {rid}", "entity_keys": []}]
            return httpx.Response(200, json={"choices": [
                {"message": {"content": _json.dumps(facts)}}]})

        ext = self._extractor(handler, chunk_records=4)
        recs = [MemoryRecord.create(namespace="default", kind="raw_event",
                                    content=f"text {i}", scope=Scope(user="u"),
                                    source=Source.USER, record_id=f"r{i}")
                for i in range(10)]
        facts = ext.extract(recs)  # used to raise / return nothing on failure
        assert len(facts) >= 2, "failed chunk must not discard sibling chunks"
        assert all("r5" not in f.content for f in facts)

    def test_malformed_model_output_fails_safe(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"choices": [
                {"message": {"content": "prose without json ]"}}]})

        ext = self._extractor(handler)
        out = ext.extract([_rec(1)])
        # counted, and the chunk's turns go through the pattern extractor
        assert out.errors == ["malformed"]
        assert [f.content for f in out] == [f.content for f in HeuristicExtractor().extract([_rec(1)])]


class TestEvictedStoreFailFast:
    def test_straggler_reference_cannot_append_after_eviction(self, tmp_path):
        eng = StorageEngine(str(tmp_path / "root"), max_open_namespaces=1)
        try:
            stale = eng.namespace("victim")
            stale.append([_rec(0)])
            for i in range(3):  # churn forces eviction of 'victim'
                eng.namespace(f"churn-{i}").append([])
            assert "victim" not in eng._namespaces
            with pytest.raises(RuntimeError, match="evicted"):
                stale.append([_rec(1)])  # forked-state append attempt fails fast
            # the engine-resolved store is healthy and sees durable data
            fresh = eng.namespace("victim")
            fresh.append([_rec(2)])
            assert fresh.stats()["records"] == 2
        finally:
            eng.close()

    def test_destroyed_message_still_says_destroyed(self, tmp_path):
        eng = StorageEngine(str(tmp_path / "root"), max_open_namespaces=4)
        try:
            ns = eng.namespace("gone")
            ns.append([_rec(0)])
            assert eng.destroy_namespace("gone")
            with pytest.raises(RuntimeError, match="destroyed"):
                ns.append([_rec(1)])
        finally:
            eng.close()

    def test_http_maps_evicted_to_retryable_503(self, tmp_path):
        from memd.server.http import create_app

        app = create_app(data_dir=str(tmp_path / "data"), keys_path=str(tmp_path / "keys.json"))
        ks = app.state.keystore
        full, _kid = ks.create("acme", name="t")
        client = TestClient(app)
        client.headers["Authorization"] = f"Bearer {full}"
        try:
            # force the lifecycle error class through the real middleware path
            def _boom(*a, **kw):
                raise RuntimeError("namespace 'acme' was evicted from the open cache; "
                                   "re-resolve it via the engine")
            app.state.engine.search = _boom
            r = client.post("/v1/ns/acme/search", json={"query": "x"})
            assert r.status_code == 503
            assert "retry" in r.json()["detail"]
            # and 'destroyed' still maps to 410
            def _gone(*a, **kw):
                raise RuntimeError("namespace 'acme' destroyed")
            app.state.engine.search = _gone
            r2 = client.post("/v1/ns/acme/search", json={"query": "x"})
            assert r2.status_code == 410
        finally:
            app.state.engine.close()
