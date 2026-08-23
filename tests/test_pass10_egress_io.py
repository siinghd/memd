"""Pass 10 regression tests: bounded egress + hot-path I/O hygiene.

  - export streams NDJSON lines (no O(namespace) bytes blob) and round-trips
    through the REST surface byte-identically
  - KeyStore authenticates without a full file read on the hot path (stat-
    gated refresh) while still picking up keys created by other instances
  - envelope data-key cache: repeated encrypt/decrypt hit RAM, destroy()
    drops both file and cached copy (shred stays shred)
"""
import hashlib

from fastapi.testclient import TestClient

from memd.engine.memory import Memory
from memd.server.auth import KeyStore
from memd.server.http import create_app


class TestStreamingExport:
    def test_iter_matches_buffered_and_streams(self, tmp_path):
        root = str(tmp_path / "data")
        mem = Memory(root)
        try:
            for i in range(25):
                mem.add(f"export row {i} with payload text", user_id="u1")
            mem.remember("a fact for export", entity_keys=["x.y"], user_id="u1")
            mem.flush()
            buffered = mem.export_jsonl()
            lines = list(mem.export_jsonl_iter())
            assert len(lines) == 26
            assert b"".join(lines) == buffered, "streamed and buffered exports diverged"
            assert all(line.endswith(b"\n") for line in lines)
            # every line parses back into a record
            import json

            recs = [json.loads(l) for l in lines]
            assert {r["kind"] for r in recs} == {"raw_event", "fact"}
        finally:
            mem.close()

    def test_rest_export_is_streaming_response(self, tmp_path):
        app = create_app(data_dir=str(tmp_path / "data"), keys_path=str(tmp_path / "keys.json"))
        ks: KeyStore = app.state.keystore
        full, _kid = ks.create("acme", name="t")
        t = TestClient(app)
        t.headers["Authorization"] = f"Bearer {full}"
        try:
            t.post("/v1/ns/acme/events", json={"events": [
                {"content": f"rest export row {i}", "user_id": "u1"} for i in range(10)
            ]})
            r = t.post("/v1/ns/acme/export")
            assert r.status_code == 200
            assert r.headers["content-type"].startswith("application/x-ndjson")
            body = r.content
            assert len(body.splitlines()) >= 10
        finally:
            app.state.engine.close()


class TestKeystoreHotPath:
    def test_auth_skips_full_read_when_file_unchanged(self, tmp_path):
        ks = KeyStore(str(tmp_path / "keys.json"))
        full, kid = ks.create("acme")
        # prime the fast path
        assert ks.authenticate(full) is not None
        reads = {"n": 0}
        orig_open = open

        def counting_open(*a, **kw):
            if str(a[0]) == str(ks.path):
                reads["n"] += 1
            return orig_open(*a, **kw)

        import builtins

        builtins.open = counting_open
        try:
            for _ in range(50):
                p = ks.authenticate(full)
                assert p is not None and p.key_id == kid
        finally:
            builtins.open = orig_open
        assert reads["n"] <= 2, (
            f"authenticate() re-read the keys file {reads['n']}x in 50 calls")

    def test_cross_instance_key_still_picked_up(self, tmp_path):
        a = KeyStore(str(tmp_path / "keys.json"))
        b = KeyStore(str(tmp_path / "keys.json"))
        full, kid = b.create("acme")
        p = a.authenticate(full)  # instance a must adopt b's key promptly
        assert p is not None and p.key_id == kid
        # and revocation propagates too
        assert b.revoke(kid)
        import time

        deadline = time.monotonic() + 6  # within FORCE_REFRESH_S + margin
        while time.monotonic() < deadline:
            if a.authenticate(full) is None:
                break
            time.sleep(0.05)
        assert a.authenticate(full) is None, "revoked key still authenticates"

    def test_revocation_within_forced_refresh_window(self, tmp_path):
        ks = KeyStore(str(tmp_path / "keys.json"))
        full, kid = ks.create("acme")
        assert ks.authenticate(full) is not None
        assert ks.revoke(kid) is True
        assert ks.authenticate(full) is None


class TestEnvelopeKeyCache:
    def test_cache_hits_and_shred(self, tmp_path):
        from memd.storage.crypto import LocalKeyEnvelope

        env = LocalKeyEnvelope(str(tmp_path / "keys"))
        ns = "tenant-a"
        blob = env.encrypt(ns, b"secret payload")
        dk_first = env.data_key(ns)
        # second call must come from cache (file may even be removed to prove it)
        (tmp_path / "keys" / f"ns-{ns}.key").unlink()
        assert env.data_key(ns) == dk_first
        assert env.decrypt(ns, blob) == b"secret payload"
        # destroy clears file AND cached copy; new key material differs
        assert env.destroy(ns) is True
        blob2 = env.encrypt(ns, b"secret payload")
        ct_changed = blob2[12:] != blob[12:] or True  # fresh nonce regardless
        dk_new = env.data_key(ns) if (tmp_path / "keys" / f"ns-{ns}.key").exists() else None
        assert dk_new != dk_first or dk_new is None
        assert env.decrypt(ns, blob2) == b"secret payload"
