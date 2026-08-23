"""Pass 11: attack tests for HTTP boundary behaviors that had only unit-level
coverage, plus verification that spoofed proxy identity cannot dodge the
pre-auth failure limiter.

Attacks attempted here (all must FAIL):
  - credential stuffing from one host -> locked out even with a VALID key
  - oversized request body -> rejected before handler work (413)
  - X-Forwarded-For spoofing from a non-loopback peer -> ignored; the real
    peer's bucket takes the failures (spoofing cannot reset or split state)
"""
import httpx
from fastapi.testclient import TestClient

from memd.server.auth import KeyStore
from memd.server.http import create_app


def _app(tmp_path):
    app = create_app(data_dir=str(tmp_path / "data"), keys_path=str(tmp_path / "keys.json"))
    ks: KeyStore = app.state.keystore
    full, _kid = ks.create("acme", name="t")
    return app, full


def test_credential_stuffing_locks_out_then_valid_key_429(tmp_path):
    """FailureLimiter is keyed by client host: after enough bad attempts,
    EVEN A VALID KEY gets 429 from that host (that is the point - brute
    force must not be free)."""
    app, full = _app(tmp_path)
    t = TestClient(app)
    try:
        r0 = t.get("/v1/status", headers={"Authorization": f"Bearer {full}"})
        assert r0.status_code == 200, "valid key works pre-attack"
        for i in range(35):  # max_failures=30/window
            r = t.get("/v1/status", headers={"Authorization": f"Bearer memd_acme_wrong_{i}"})
            assert r.status_code in (401, 429)
        r = t.get("/v1/status", headers={"Authorization": f"Bearer {full}"})
        assert r.status_code == 429, "valid key must be throttled during lockout"
    finally:
        app.state.engine.close()


def test_oversized_body_rejected_with_413(tmp_path):
    app, full = _app(tmp_path)
    t = TestClient(app)
    try:
        big = "x" * (9 * 1024 * 1024)  # > MAX_BODY_BYTES (8 MiB)
        r = t.post("/v1/ns/acme/events",
                   json={"events": [{"content": big, "user_id": "u1"}]},
                   headers={"Content-Length": str(len(big) + 64)})
        assert r.status_code == 413
        assert "detail" in r.json()
    finally:
        app.state.engine.close()


def test_xff_spoof_from_non_loopback_is_ignored(tmp_path):
    """A remote client rotating X-Forwarded-For identities must NOT get a
    fresh limiter bucket per spoofed hop: from a non-loopback peer the header
    is ignored, so every failure lands on the REAL peer's bucket and the
    lockout bites exactly as if no spoofing had been attempted."""
    app, full = _app(tmp_path)
    t = TestClient(app)
    try:
        for i in range(35):
            r = t.get("/v1/status",
                      headers={"Authorization": f"Bearer memd_acme_bad_{i}",
                               "X-Forwarded-For": f"10.0.{i}.7"})
            assert r.status_code in (401, 429)
        # identity rotation bought the attacker nothing: the real peer is
        # locked out - even presenting a VALID key stays throttled
        r = t.get("/v1/status", headers={"Authorization": f"Bearer {full}",
                                         "X-Forwarded-For": "203.0.113.9"})
        assert r.status_code == 429, (
            "spoofed XFF must not rotate/split limiter state: lockout applies")
    finally:
        app.state.engine.close()
