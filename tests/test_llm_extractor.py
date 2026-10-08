"""LLM extractor: settings from config or env, bounded provider calls,
request options, who said what, and failures that degrade visibly.

A local OpenAI-compatible endpoint stands in for the provider: it records
every request body and answers what each test tells it to."""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from memd.core.schema import MemoryRecord, Scope, Source
from memd.pipeline.extractor import HeuristicExtractor, LLMExtractor, resolve_extractor

_ENV = ("MEMD_EXTRACTION_API_KEY", "MEMD_EXTRACTION_MODEL", "MEMD_EXTRACTION_BASE_URL")


@pytest.fixture(autouse=True)
def _no_ambient_extraction_env(monkeypatch):
    # a developer's MEMD_EXTRACTION_* must never make these tests call out
    for k in _ENV:
        monkeypatch.delenv(k, raising=False)


def _chat(content, finish_reason="stop", **message) -> dict:
    return {"choices": [{"message": {"role": "assistant", "content": content, **message},
                         "finish_reason": finish_reason}]}


class FakeProvider:
    """`reply(body)` returns a JSON-able dict (200), a (status, dict) pair, or
    a callable taking the request handler (to stream or stall)."""

    def __init__(self):
        self.requests: list[dict] = []
        self.reply = lambda body: _chat("[]")
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
                fake.requests.append({"path": self.path, "headers": dict(self.headers), "body": body})
                out = fake.reply(body)
                if callable(out):
                    out(self)
                    return
                status, payload = out if isinstance(out, tuple) else (200, out)
                data = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}/v1"

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture()
def provider():
    p = FakeProvider()
    yield p
    p.close()


def _rec(content: str, rid: str = "r1", role: str = "user", t_event: int | None = None) -> MemoryRecord:
    return MemoryRecord.create(namespace="default", kind="raw_event", content=content,
                               scope=Scope(user="u1", session="s1"),
                               source=Source.USER if role == "user" else Source.AGENT,
                               actor_id=f"{role}:u1", session_id="s1", t_event=t_event,
                               record_id=rid)


# ------------------------------------------------- 1: settings from config or env


def test_env_api_key_turns_the_llm_extractor_on(monkeypatch, provider):
    monkeypatch.setenv("MEMD_EXTRACTION_API_KEY", "sk-env")
    monkeypatch.setenv("MEMD_EXTRACTION_MODEL", "env-model")
    monkeypatch.setenv("MEMD_EXTRACTION_BASE_URL", provider.base_url)
    ext = resolve_extractor({})
    assert isinstance(ext, LLMExtractor)
    assert ext.model == "env-model"
    ext.extract([_rec("I work at Initech")])
    (req,) = provider.requests
    assert req["path"] == "/v1/chat/completions"
    assert req["headers"]["Authorization"] == "Bearer sk-env"
    assert req["body"]["model"] == "env-model"


def test_config_wins_over_env(monkeypatch, provider):
    monkeypatch.setenv("MEMD_EXTRACTION_API_KEY", "sk-env")
    monkeypatch.setenv("MEMD_EXTRACTION_MODEL", "env-model")
    monkeypatch.setenv("MEMD_EXTRACTION_BASE_URL", "http://127.0.0.1:9/never")
    ext = resolve_extractor({"extraction_api_key": "sk-cfg", "extraction_model": "cfg-model",
                             "extraction_base_url": provider.base_url})
    ext.extract([_rec("I work at Initech")])
    (req,) = provider.requests
    assert req["headers"]["Authorization"] == "Bearer sk-cfg"
    assert req["body"]["model"] == "cfg-model"


def test_an_empty_config_key_turns_the_env_key_off(monkeypatch):
    monkeypatch.setenv("MEMD_EXTRACTION_API_KEY", "sk-env")
    assert isinstance(resolve_extractor({"extraction_api_key": ""}), HeuristicExtractor)
    assert isinstance(resolve_extractor({}), LLMExtractor)


def test_memory_extracts_with_the_env_key(monkeypatch, provider, tmp_path):
    from memd.engine.memory import Memory

    monkeypatch.setenv("MEMD_EXTRACTION_API_KEY", "sk-env")
    monkeypatch.setenv("MEMD_EXTRACTION_BASE_URL", provider.base_url)
    provider.reply = lambda body: _chat(json.dumps(
        [{"content": "u1 works at Initech", "entity_keys": ["user.employer"]}]))
    m = Memory(str(tmp_path / "d"))
    try:
        assert m.stats()["extractor"] == "llm"
        m.add("I work at Initech", session_id="s1", user_id="u1")
        res = m.close_session("s1")
        assert res["facts_written"] == 1
        assert len(provider.requests) == 1
    finally:
        m.close()
