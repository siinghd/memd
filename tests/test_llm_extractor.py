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

_ENV = ("MEMD_EXTRACTION_API_KEY", "MEMD_EXTRACTION_MODEL", "MEMD_EXTRACTION_BASE_URL",
        "MEMD_EXTRACTION_MAX_TOKENS", "MEMD_EXTRACTION_TIMEOUT_S", "MEMD_EXTRACTION_REQUEST_OPTIONS")


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


# ------------------------------------------------- 2: bounded provider calls


def _llm(provider, **cfg) -> LLMExtractor:
    ext = resolve_extractor({"extraction_api_key": "k", "extraction_base_url": provider.base_url, **cfg})
    assert isinstance(ext, LLMExtractor)
    return ext


def test_every_call_caps_its_output_tokens(provider):
    _llm(provider).extract([_rec("I work at Initech")])
    assert provider.requests[-1]["body"]["max_tokens"] == 4096


def test_the_output_cap_is_configurable(monkeypatch, provider):
    _llm(provider, extraction_max_tokens=512).extract([_rec("I work at Initech")])
    assert provider.requests[-1]["body"]["max_tokens"] == 512
    monkeypatch.setenv("MEMD_EXTRACTION_MAX_TOKENS", "1024")
    _llm(provider).extract([_rec("I work at Initech")])
    assert provider.requests[-1]["body"]["max_tokens"] == 1024
    _llm(provider, extraction_max_tokens=0).extract([_rec("I work at Initech")])
    assert "max_tokens" not in provider.requests[-1]["body"]  # 0 = no cap


@pytest.mark.parametrize("bad", ["lots", "-1", "1.5"])
def test_a_bad_output_cap_is_refused(monkeypatch, bad):
    monkeypatch.setenv("MEMD_EXTRACTION_MAX_TOKENS", bad)
    with pytest.raises(ValueError, match="extraction_max_tokens"):
        resolve_extractor({"extraction_api_key": "k"})


@pytest.mark.parametrize("bad", ["0", "-3", "soon"])
def test_a_bad_timeout_is_refused(monkeypatch, bad):
    monkeypatch.setenv("MEMD_EXTRACTION_TIMEOUT_S", bad)
    with pytest.raises(ValueError, match="extraction_timeout_s"):
        resolve_extractor({"extraction_api_key": "k"})


def _dribble(seconds: float, gap: float = 0.1):
    """A provider that keeps the connection alive with whitespace (as
    OpenRouter does while a model generates) and never finishes in time:
    every byte resets a per-read timeout."""
    def run(handler):
        import time as _t

        handler.send_response(200)
        handler.send_header("Content-Type", "application/json")
        handler.end_headers()
        end = _t.monotonic() + seconds
        try:
            while _t.monotonic() < end:
                handler.wfile.write(b"\n")
                handler.wfile.flush()
                _t.sleep(gap)
            handler.wfile.write(json.dumps(_chat("[]")).encode())
        except OSError:
            pass  # the client hung up: what the test wants
    return run


def _stall(seconds: float):
    """A provider that accepts the request and sends nothing."""
    def run(handler):
        import time as _t

        _t.sleep(seconds)
        try:
            data = json.dumps(_chat("[]")).encode()
            handler.send_response(200)
            handler.send_header("Content-Length", str(len(data)))
            handler.end_headers()
            handler.wfile.write(data)
        except OSError:
            pass
    return run


def test_a_call_kept_alive_past_its_timeout_is_cut_off(provider):
    import time as _t

    provider.reply = lambda body: _dribble(8.0)
    ext = _llm(provider, extraction_timeout_s=1)
    t0 = _t.monotonic()
    ext.extract([_rec("I work at Initech")])
    assert _t.monotonic() - t0 < 4, "the per-call timeout must bound a call that keeps sending bytes"
    assert len(provider.requests) == 1


def test_a_silent_provider_is_cut_off(provider, monkeypatch):
    import time as _t

    provider.reply = lambda body: _stall(6.0)
    monkeypatch.setenv("MEMD_EXTRACTION_TIMEOUT_S", "1")
    ext = _llm(provider)
    t0 = _t.monotonic()
    ext.extract([_rec("I work at Initech")])
    assert _t.monotonic() - t0 < 4
    assert len(provider.requests) == 1


# ------------------------------------------------- 3: request options


def test_request_options_are_merged_into_the_request(provider):
    opts = {"provider": {"order": ["deepinfra"], "allow_fallbacks": False},
            "reasoning": {"enabled": False}, "temperature": 0.2}
    _llm(provider, extraction_request_options=opts).extract([_rec("I work at Initech")])
    body = provider.requests[-1]["body"]
    assert body["provider"] == {"order": ["deepinfra"], "allow_fallbacks": False}
    assert body["reasoning"] == {"enabled": False}
    assert body["temperature"] == 0.2
    assert body["max_tokens"] == 4096 and body["model"] == "gpt-4o-mini"


def test_request_options_from_env_json_and_config_wins(monkeypatch, provider):
    monkeypatch.setenv("MEMD_EXTRACTION_REQUEST_OPTIONS", '{"reasoning": {"effort": "low"}}')
    _llm(provider).extract([_rec("I work at Initech")])
    assert provider.requests[-1]["body"]["reasoning"] == {"effort": "low"}
    _llm(provider, extraction_request_options={"top_p": 0.5}).extract([_rec("I work at Initech")])
    body = provider.requests[-1]["body"]
    assert body["top_p"] == 0.5 and "reasoning" not in body


def test_a_null_option_removes_the_field(provider):
    # e.g. a model that takes max_completion_tokens and rejects temperature
    opts = {"temperature": None, "max_tokens": None, "max_completion_tokens": 2048}
    _llm(provider, extraction_request_options=opts).extract([_rec("I work at Initech")])
    body = provider.requests[-1]["body"]
    assert "temperature" not in body and "max_tokens" not in body
    assert body["max_completion_tokens"] == 2048


@pytest.mark.parametrize("bad", ['{"reasoning": ', '["not", "an", "object"]',
                                 '{"messages": []}', '{"model": "x"}', '{"stream": true}'])
def test_bad_request_options_are_refused(monkeypatch, bad):
    monkeypatch.setenv("MEMD_EXTRACTION_REQUEST_OPTIONS", bad)
    with pytest.raises(ValueError, match="extraction_request_options"):
        resolve_extractor({"extraction_api_key": "k"})
