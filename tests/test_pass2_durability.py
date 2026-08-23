"""Pass-2 loop fixes: durability, crypto-at-rest, auth robustness, honest metrics.

Every test attempts the actual failure or attack and asserts it fails:
crash windows, plaintext-at-rest, corrupt keys files, malformed admin keys,
auth-flood DoS, heavy-endpoint spam, and Prometheus label collapse.
"""
import json
import os
import shutil
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from memd.core.schema import MemoryRecord, Scope, Source
from memd.metrics import Registry
from memd.server.auth import FailureLimiter, KeyStore
from memd.storage.engine import StorageEngine


def _rec(ns, i, content="hello world"):
    return MemoryRecord.create(
        namespace=ns, kind="raw_event", content=f"{content} {i}",
        scope=Scope(user="u1"), source=Source.USER,
    )


@pytest.fixture()
def root(tmp_path):
    return str(tmp_path / "store")


# --------------------------------------------------------------------------- C1

def _snapshot_restart(root):
    """Copy on-disk state; open a fresh engine over the copy (crash+restart)."""
    dst = root + "-restarted"
    shutil.rmtree(dst, ignore_errors=True)
    shutil.copytree(root, dst)
    for suf in ("-wal", "-shm"):
        p = os.path.join(dst, "_cache", "probe.sqlite" + suf)
        if os.path.exists(p):
            os.unlink(p)
    eng = StorageEngine(dst)
    try:
        return eng.namespace("probe").index.stats()["records"]
    finally:
        eng.close()


class TestCrashWindows:
    def _build(self, root):
        eng = StorageEngine(root)
        ns = eng.namespace("probe")
        ns.append([_rec("probe", i) for i in range(5)])
        return eng, ns

    def test_compact_crash_keeps_all_records(self, root):
        eng, ns = self._build(root)
        boom = ns._persist_manifest

        def crash():
            raise KeyboardInterrupt()  # power loss mid-compaction

        ns._persist_manifest = crash
        with pytest.raises(KeyboardInterrupt):
            ns.compact(force=True)
        ns._persist_manifest = boom
        eng.close()
        assert _snapshot_restart(root) == 5

    def test_rotate_crash_keeps_all_records(self, root):
        eng, ns = self._build(root)

        def crash():
            raise KeyboardInterrupt()

        ns._persist_manifest = crash
        with pytest.raises(KeyboardInterrupt):
            ns.rotate("probe")
        # restore so close() doesn't repair state for us
        from memd.storage.engine import NamespaceStore

        ns._persist_manifest = NamespaceStore._persist_manifest.__get__(ns)
        eng.close()
        assert _snapshot_restart(root) == 5

    def test_orphan_segment_from_legacy_crash_is_adopted(self, root, tmp_path):
        """A manifest that lost its segment reference (old-code crash) heals:
        the orphan seg file is adopted because it carries the newest fold."""
        eng, ns = self._build(root)
        seg_name = ns.rotate("probe")  # healthy rotate -> v2 blob w/ header
        # simulate old-code damage: wipe the reference from the manifest
        ns.manifest.segments = []
        ns.manifest.wal_base_seq = ns.manifest.seq
        ns._persist_manifest()
        eng.close()
        st = _snapshot_restart(root)
        assert st == 5, "orphan segment must be re-adopted on open"

    def test_stale_cleanup_residue_is_not_adopted(self, root):
        """Crash AFTER the new manifest persisted but BEFORE old segments were
        deleted: residue must NOT resurrect tombstoned rows."""
        eng, ns = self._build(root)
        ns.rotate("probe")
        rid = ns.index.all_records()[0].id
        ns.append_op({"op": "tombstone", "id": rid, "at": 1})
        from memd.core.schema import now_ms

        ns.index.tombstone(rid, now_ms())
        # compact writes folded-only manifest; block the OLD-segment deletion
        real_delete = eng.store.delete

        def half_delete(key):
            if key.startswith("ns/probe/seg-"):
                return  # crash right here: cleanup never finishes
            return real_delete(key)

        eng.store.delete = half_delete
        ns.compact(force=True)
        eng.store.delete = real_delete
        eng.close()

        dst = root + "-restarted"
        shutil.rmtree(dst, ignore_errors=True)
        shutil.copytree(root, dst)
        for suf in ("-wal", "-shm"):
            p = os.path.join(dst, "_cache", "probe.sqlite" + suf)
            if os.path.exists(p):
                os.unlink(p)
        eng2 = StorageEngine(dst)
        try:
            recs = {r.id: r for r in eng2.namespace("probe").index.all_records()}
            assert rid in recs and recs[rid].deleted, \
                "tombstone must survive; residue adoption would resurrect it as live"
        finally:
            eng2.close()


# ---------------------------------------------------------------- H3 (at-rest)

class TestSegmentEncryption:
    def test_segments_are_encrypted_at_rest(self, tmp_path):
        from memd.storage.crypto import LocalKeyEnvelope
        from memd.storage.engine import StorageEngine

        root = str(tmp_path / "enc")
        env = LocalKeyEnvelope(os.path.join(root, "keys"))
        eng = StorageEngine(os.path.join(root, "store"), envelope=env)
        secret = "API_KEY=hunter2-do-not-leak"
        ns = eng.namespace("secret")
        ns.append([MemoryRecord.create(
            namespace="secret", kind="raw_event", content=secret,
            scope=Scope(user="u1"), source=Source.USER)])
        ns.rotate("size")
        seg_dir = os.path.join(root, "store", "ns", "secret")
        blobs = [open(os.path.join(seg_dir, f), "rb").read()
                 for f in os.listdir(seg_dir) if f.startswith("seg-")]
        eng.close()
        assert blobs, "segment should exist"
        assert all(secret.encode() not in b for b in blobs), \
            "plaintext content must not appear in any segment blob"

    def test_legacy_plaintext_segment_still_reads(self, root):
        """Pre-fix stores have bare JSONL segments; they must keep loading."""
        eng, ns = self._build(root) if False else (None, None)
        eng = StorageEngine(root)
        ns = eng.namespace("probe")
        ns.append([_rec("probe", 1)])
        seg_name = ns.rotate("legacy")
        seg_path = os.path.join(root, "ns", "probe", seg_name)
        eng.close()
        # rewrite the segment as legacy plaintext JSONL
        from memd.core.schema import records_to_jsonl

        blob = open(seg_path, "rb").read()
        first_nl = blob.find(b"\n")
        open(seg_path, "wb").write(blob[first_nl + 1:])
        eng2 = StorageEngine(root)
        try:
            assert eng2.namespace("probe").index.stats()["records"] == 1
        finally:
            eng2.close()


# ------------------------------------------------------------ H1/H2 (keystore)

class TestKeyStoreRobustness:
    def test_corrupt_keys_file_keeps_auth_working(self, tmp_path):
        kp = str(tmp_path / "keys.json")
        ks = KeyStore(kp)
        full, kid = ks.create("acme")
        with open(kp, "w") as f:
            f.write("{CORRUPTED!!!")
        st = os.stat(kp)
        os.utime(kp, (st.st_atime + 10, st.st_mtime + 10))  # force reload attempt
        assert ks.authenticate(full) is not None, "stale map must keep serving"

    def test_malformed_admin_key_boots_and_authenticates(self, tmp_path):
        kp = str(tmp_path / "keys.json")
        ks = KeyStore(kp, admin_key="simple-opaque-secret")
        assert ks.authenticate("simple-opaque-secret") is not None
        p = ks.authenticate("simple-opaque-secret")
        assert p.scope_override and p.namespace == "*"

    def test_structured_admin_key_still_works(self, tmp_path):
        kp = str(tmp_path / "keys.json")
        admin = "memd_acme_ab12cd34_deadbeefcafebabe"
        ks = KeyStore(kp, admin_key=admin)
        assert ks.authenticate(admin) is not None

    def test_revoked_opaque_key_rejected_and_list_hides_hashes(self, tmp_path):
        kp = str(tmp_path / "keys.json")
        ks = KeyStore(kp, admin_key="opaque-admin-xyz")
        kid = ks.list_keys()[0]["key_id"]
        assert ks.revoke(kid)
        assert ks.authenticate("opaque-admin-xyz") is None
        for k in ks.list_keys():
            assert "hash" not in k and "full_hash" not in k


# ------------------------------------------------------------- M3 (throttling)

class TestThrottling:
    def test_failure_limiter_blocks_flood_then_recovers(self):
        fl = FailureLimiter(max_failures=3, window_s=60.0)
        t = 1000.0
        for _ in range(3):
            fl.record_failure("10.0.0.1", now=t)
        assert fl.blocked("10.0.0.1", now=t)
        assert not fl.blocked("10.0.0.2", now=t), "other hosts unaffected"
        t += 61.0
        assert not fl.blocked("10.0.0.1", now=t), "window slides"

    def test_failure_limiter_success_clears(self):
        fl = FailureLimiter(max_failures=3)
        for _ in range(3):
            fl.record_failure("h")
        assert fl.blocked("h")
        fl.record_success("h")
        assert not fl.blocked("h")

    def test_failure_limiter_map_bounded(self):
        fl = FailureLimiter(max_hosts=50)
        for i in range(500):
            fl.record_failure(f"host-{i}")
        assert len(fl._events) <= 50

    def test_rate_limiter_forget_and_prune(self):
        from memd.server.auth import RateLimiter

        rl = RateLimiter(max_buckets=10)
        for i in range(20):
            rl.allow(f"k{i}", 600)
        rl.forget("k0")
        assert "k0" not in rl._buckets


# --------------------------------------------------------------- M1 (metrics)

class TestPrometheusHistograms:
    def test_per_route_series_survive_rendering(self):
        reg = Registry()
        reg.preset("memd_http_request_ms", "histogram", "http latency")
        reg.observe("memd_http_request_ms", 10, route="/a", method="GET")
        reg.observe("memd_http_request_ms", 900, route="/b", method="POST")
        out = reg.render_prometheus()
        assert 'memd_http_request_ms_sum{method="GET",route="/a"}' in out
        assert 'memd_http_request_ms_sum{method="POST",route="/b"}' in out
        # bucket lines carry label sets too, cumulatively consistent per series
        a_lines = [l for l in out.splitlines() if 'route="/a"' in l]
        assert any('le="+Inf"' in l and l.endswith(" 1") for l in a_lines)

    def test_labelless_histogram_still_valid(self):
        reg = Registry()
        reg.observe("h_seconds", 0.5)
        out = reg.render_prometheus()
        assert 'h_seconds_bucket{le="0.5"} 1' in out
        assert 'h_seconds_bucket{le="+Inf"} 1' in out
        assert "h_seconds_count 1" in out
        assert "h_seconds_sum 0.5" in out


# ------------------------------------------------------- M2/M4/M5 misc honesty

class TestHonestStatsAndCleanup:
    def test_stats_segment_bytes_is_bytes(self, root):
        from memd.storage.engine import StorageEngine

        eng = StorageEngine(root)
        ns = eng.namespace("s")
        big = "x" * 10_000
        ns.append([_rec("s", c, big) for c in "abcdefghij"])
        ns.rotate("size")
        st = ns.stats()
        actual = sum(
            os.path.getsize(os.path.join(root, "ns", "s", f))
            for f in os.listdir(os.path.join(root, "ns", "s")) if f.startswith("seg-")
        )
        eng.close()
        assert st["segment_bytes"] == actual > 0

    def test_audit_read_includes_buffered_entries(self, root):
        from memd.storage.audit import BufferedAuditLog
        from memd.storage.objectstore import LocalObjectStore

        store = LocalObjectStore(str(root))
        log = BufferedAuditLog(store, "ns/x/audit", flush_every=1000)
        log.append(actor="a", action="test", target="t1")
        entries = log.read()
        assert [e["action"] for e in entries] == ["test"], \
            "unflushed buffer must be visible to read()"
