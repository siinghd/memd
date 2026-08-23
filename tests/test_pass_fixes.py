"""PASS-fix tests: exact-session extraction, /metrics authn, cache metrics,
keystore perms."""
import json
import os

import pytest
from fastapi.testclient import TestClient

from memd.engine.memory import Memory


def test_close_session_sweeps_only_own_session(tmp_path):
    m = Memory(str(tmp_path / "d"))
    try:
        # org-level raw with NO session must not be swept into session closes
        m.add("org level announcement no session", user_id=None, role="user")
        r = m.close_session("some-unrelated-session")
        assert r["raw_considered"] == 0, "ancestor-scope rows leaked into extraction boundary"
        # own-session still works
        m.add("my name is Zara", session_id="own", user_id="u1", role="user")
        r2 = m.close_session("own")
        assert r2["raw_considered"] == 1 and r2["facts_extracted"] >= 1
    finally:
        m.close()


def test_metrics_endpoint_requires_auth(tmp_path, monkeypatch):
    monkeypatch.delenv("MEMD_METRICS_PUBLIC", raising=False)
    from memd.server.http import create_app

    app = create_app(data_dir=str(tmp_path / "d"), keys_path=str(tmp_path / "k.json"))
    full, _ = app.state.keystore.create("acme")
    t = TestClient(app)
    # unauthenticated: rejected (tenant names in labels must not leak)
    assert t.get("/metrics").status_code == 401
    # authenticated: allowed
    t.headers["Authorization"] = f"Bearer {full}"
    assert t.get("/metrics").status_code == 200
    app.state.engine.close()


def test_metrics_public_optin(tmp_path, monkeypatch):
    monkeypatch.setenv("MEMD_METRICS_PUBLIC", "1")
    from memd.server.http import create_app

    app = create_app(data_dir=str(tmp_path / "d"), keys_path=str(tmp_path / "k.json"))
    t = TestClient(app)
    assert t.get("/metrics").status_code == 200
    app.state.engine.close()


def test_cache_hit_latency_observed_and_labeled(tmp_path):
    from memd.metrics import METRICS

    m = Memory(str(tmp_path / "d"))
    try:
        m.add("cache label probe", user_id="u1")
        m.search("cache label probe", user_id="u1")   # miss
        m.search("cache label probe", user_id="u1")   # hit
        snap = METRICS.snapshot()
        h = snap["histograms"].get("memd_search_latency_ms", [])
        cache_labels = [x["labels"].get("cache") for x in h]
        assert "hit" in cache_labels and "miss" in cache_labels
        assert "memd_search_cache_misses_total" in snap["counters"]
    finally:
        m.close()


def test_keystore_file_permissions(tmp_path):
    from memd.server.auth import KeyStore

    path = str(tmp_path / "k.json")
    ks = KeyStore(path)
    ks.create("acme")
    mode = os.stat(path).st_mode & 0o777
    assert mode == 0o600, f"keystore file too open: {oct(mode)}"


def test_audit_flush_failure_doesnt_crash_writes(tmp_path):
    from memd.storage.audit import BufferedAuditLog

    class ExplodingStore:
        def __init__(self):
            self.fail = False
            self.data = b""
        def get(self, key):
            return self.data
        def append(self, key, payload):
            if self.fail:
                raise OSError("disk gone")
            self.data += payload

    store = ExplodingStore()
    log = BufferedAuditLog(store, "a", flush_every=1)
    log.append(actor="x", action="add", target="t1")  # flush ok
    store.fail = True
    log.append(actor="x", action="add", target="t2")  # flush fails silently
    log.append(actor="x", action="add", target="t3")
    store.fail = False
    log.flush()


def test_reembed_bumps_search_cache_epoch(tmp_path):
    m = Memory(str(tmp_path / "d"))
    try:
        m.add("epoch probe kumquat", user_id="u1")
        m.flush()
        r1 = m.search("epoch probe kumquat", user_id="u1")  # populates cache
        epoch_before = m._qepochs.get(m.namespace_name, 0)
        rep = m.reembed()  # changes vector lane; must invalidate cache
        if rep["embedded"] > 0:
            assert m._qepochs.get(m.namespace_name, 0) > epoch_before
    finally:
        m.close()


def test_session_truncation_metric(tmp_path):
    from memd.index.sqlite_index import NamespaceIndex
    from memd.metrics import METRICS

    idx = NamespaceIndex(str(tmp_path / "idx" / "t.sqlite"))
    idx._ns_hint = "t"
    try:
        for i in range(5):
            from memd.core.schema import Kind, MemoryRecord, Scope, Source

            rec = MemoryRecord.create(
                namespace="t", kind=Kind.RAW_EVENT, content=f"evt {i}",
                scope=Scope(session="s"), source=Source.USER)
            idx.upsert(rec)
        before = METRICS.snapshot()["counters"].get("memd_session_truncated_total", [])
        got = idx.records_of_session("s", limit=3)  # 5 rows, limit 3 -> truncated
        after = METRICS.snapshot()["counters"].get("memd_session_truncated_total", [])
        assert len(got) == 3
        b = sum(x["value"] for x in before) if before else 0
        a = sum(x["value"] for x in after) if after else 0
        assert a == b + 1
    finally:
        idx.close()


def test_snapshot_has_quantiles():
    from memd.metrics import Registry

    r = Registry()
    for v in (0.002, 0.004, 0.006, 0.008, 0.2):
        r.observe("memd_q_seconds", v, help="q")
    h = r.snapshot()["histograms"]["memd_q_seconds"][0]
    assert h["count"] == 5
    assert 0 <= h["p50"] <= 0.01 <= h["p95"] <= h["p99"] <= 0.25, (h["p50"], h["p95"], h["p99"])


def test_encrypted_audit_log_readable_and_verifiable(tmp_path):
    """Regression: encrypted audit framing used plaintext length - logs were
    unreadable. Read-back + hash-chain verify must work with envelope on."""
    from memd.storage.audit import BufferedAuditLog
    from memd.storage.crypto import LocalKeyEnvelope
    from memd.storage.objectstore import LocalObjectStore

    d = tmp_path / "audit-enc"
    env = LocalKeyEnvelope(str(d / "keys"))
    store = LocalObjectStore(str(d / "obj"))
    log = BufferedAuditLog(store, "ns/default/audit", env, flush_every=1)
    log.append(actor="a", action="add", target="r1")
    log.append(actor="b", action="delete", target="r2")
    entries = log.read()
    assert [e["action"] for e in entries] == ["add", "delete"]
    assert log.verify() is True

    # and through the engine facade (default encrypt=True)
    from memd.engine.memory import Memory

    m = Memory(str(tmp_path / "eng"))
    try:
        m.add("audit roundtrip probe", user_id="u1")
        m.search("audit roundtrip", user_id="u1")
        ents = m.audit.read()
        assert any(e["action"] == "search" for e in ents)
        assert m.audit.verify() is True
    finally:
        m.close()


def test_forget_audit_hashes_query_text(tmp_path):
    from memd.metrics import METRICS

    m = Memory(str(tmp_path / "d"))  # default: audit_query_text=False
    try:
        m.add("very private query content xyzzy", user_id="u1")
        m.forget("very private query content xyzzy", user_id="u1")
        for e in m.audit.read():
            assert "xyzzy" not in json.dumps(e), "query text leaked into audit"
    finally:
        m.close()
