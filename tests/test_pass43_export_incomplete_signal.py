"""Pass 43: an export that left out an unreadable WAL frame says so to REST
and SDK callers.

Pass 40 made export skip a damaged WAL frame and export everything readable
(a warning, memd_export_frames_skipped_total and the audit entry's
skipped_frames say where it is). Over HTTP the response was a plain 200
NDJSON stream: a client - the Python and TypeScript SDKs included - could
not tell that export from a complete one.

Now the fold runs before the response starts, and every export answers
X-Memd-Export-Skipped-Frames: N (0 when complete). The NDJSON body is
unchanged (every line is still a record: a trailing marker line would break
every parser and the native import). HostedMemory.export_jsonl() sets
last_export_skipped_frames and logs a warning when N > 0; embedded
Memory.export_jsonl()/export_jsonl_iter() set the same attribute.
"""
import json
import logging
import os

from fastapi.testclient import TestClient

from memd.sdk.client import HostedMemory
from memd.server.http import create_app
from memd.storage.engine import _frames_with_offsets

NS = "acme"
HEADER = "X-Memd-Export-Skipped-Frames"


def _wal(base) -> str:
    for dp, _d, fs in os.walk(os.path.join(str(base), "data")):
        if dp.endswith(os.path.join("ns", NS)) and "wal" in fs:
            return os.path.join(dp, "wal")
    raise AssertionError("no WAL")


def _flip(path: str, k: int) -> None:
    """Flip one byte inside the ciphertext of WAL frame k (and back)."""
    data = open(path, "rb").read()
    end, fr = list(_frames_with_offsets(data))[k]
    at = end - len(fr) - 4 + 4 + 20
    with open(path, "r+b") as fh:
        fh.seek(at)
        b = fh.read(1)
        fh.seek(at)
        fh.write(bytes([b[0] ^ 0x5A]))


class _Shim:
    """httpx.Client-like shim over TestClient (as in test_sdk.py)."""

    def __init__(self, tc: TestClient):
        self.tc = tc

    def post(self, url, json=None, params=None):
        return self.tc.post(url, json=json, params=params)

    def get(self, url, params=None):
        return self.tc.get(url, params=params)

    def delete(self, url, params=None):
        return self.tc.delete(url, params=params)


def _app(tmp_path):
    app = create_app(data_dir=str(tmp_path / "data"), keys_path=str(tmp_path / "keys.json"))
    full, _ = app.state.keystore.create(NS, name="t")
    t = TestClient(app)
    t.headers["Authorization"] = f"Bearer {full}"
    return app, t


def _lines(text: str) -> list[dict]:
    return [json.loads(ln) for ln in text.splitlines() if ln.strip()]


def test_rest_export_says_how_many_frames_it_left_out(tmp_path):
    app, t = _app(tmp_path)
    try:
        ids = [t.post(f"/v1/ns/{NS}/memories", json={"content": f"precious fact {i}"}).json()["id"]
               for i in range(6)]
        app.state.engine.flush()
        r = t.post(f"/v1/ns/{NS}/export")
        assert r.status_code == 200
        assert r.headers.get(HEADER) == "0", "a complete export says it is complete"
        assert {d["id"] for d in _lines(r.text)} == set(ids)
        _flip(_wal(tmp_path), 2)
        r = t.post(f"/v1/ns/{NS}/export")
        assert r.status_code == 200
        assert r.headers.get(HEADER) == "1", f"nothing marks the export incomplete: {dict(r.headers)}"
        got = _lines(r.text)
        assert all("id" in d for d in got), "every NDJSON line is still a record"
        assert {d["id"] for d in got} == set(ids) - {ids[2]}
        _flip(_wal(tmp_path), 2)   # restored from a backup
        r = t.post(f"/v1/ns/{NS}/export")
        assert r.headers.get(HEADER) == "0"
        assert {d["id"] for d in _lines(r.text)} == set(ids)
    finally:
        app.state.engine.close()


def test_the_python_sdk_surfaces_an_incomplete_export(tmp_path, caplog):
    app, t = _app(tmp_path)
    h = HostedMemory.__new__(HostedMemory)
    h.api_key, h.namespace, h._client = "k", NS, _Shim(t)
    try:
        ids = [h.remember(f"precious fact {i}") for i in range(6)]
        app.state.engine.flush()
        h.export_jsonl()
        assert h.last_export_skipped_frames == 0
        _flip(_wal(tmp_path), 2)
        with caplog.at_level(logging.WARNING, logger="memd.sdk.client"):
            blob = h.export_jsonl()
        assert {json.loads(ln)["id"] for ln in blob.splitlines() if ln.strip()} == set(ids) - {ids[2]}
        assert h.last_export_skipped_frames == 1
        assert any("left out 1 unreadable WAL frame" in r.getMessage() for r in caplog.records)
        # the embedded facade says the same
        mem = app.state.engine
        assert b"".join(mem.export_jsonl_iter(namespace=NS)).count(b"\n") == 5
        assert mem.last_export_skipped_frames == 1
        mem.export_jsonl(namespace=NS)
        assert mem.last_export_skipped_frames == 1
        _flip(_wal(tmp_path), 2)
        h.export_jsonl()
        assert h.last_export_skipped_frames == 0
    finally:
        app.state.engine.close()
