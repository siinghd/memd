"""Pass-4 fixes, attack-tested:
- KeyStore concurrent-process flush race (shared tmp + clobber) loses keys
- metrics dumper must persist QUERYABLE HISTORY (JSONL), not one overwritten snapshot
"""
import json
import os
import sys
import threading

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from memd.metrics import Registry, PeriodicDumper  # noqa: E402
from memd.server.auth import KeyStore  # noqa: E402


class TestKeyStoreCrossProcess:
    def test_interleaved_creates_lose_nothing(self, tmp_path):
        """Two 'processes' (independent KeyStore instances over one path)
        creating keys concurrently: every created key must exist on disk and
        authenticate from BOTH instances."""
        kp = str(tmp_path / "keys.json")
        ks1, ks2 = KeyStore(kp), KeyStore(kp)
        made = {"a": [], "b": []}
        errs = []

        def hammer(ks, tag):
            try:
                for i in range(25):
                    full, kid = ks.create(f"ns-{tag}-{i}")
                    made[tag].append((full, kid))
            except Exception as e:
                errs.append(repr(e))

        threads = [threading.Thread(target=hammer, args=(ks1, "a")),
                   threading.Thread(target=hammer, args=(ks2, "b"))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errs, f"create() raised under concurrency: {errs[:3]}"

        on_disk = {r["key_id"] for r in json.load(open(kp))}
        total = len(made["a"]) + len(made["b"])
        assert len(on_disk) == total == 50, \
            f"silent key loss: {len(on_disk)} of {total} persisted"

        # every key authenticates from the OTHER instance too
        for tag, other in (("a", ks2), ("b", ks1)):
            for full, _kid in made[tag]:
                assert other.authenticate(full) is not None

    def test_revoked_key_not_resurrected_by_merge(self, tmp_path):
        kp = str(tmp_path / "keys.json")
        ks1, ks2 = KeyStore(kp), KeyStore(kp)
        full_a, kid_a = ks1.create("acme")
        ks2.authenticate(full_a) is not None  # force ks2 to load it
        assert ks1.revoke(kid_a)
        ks2.create("other")  # triggers a merge-flush on ks2
        assert ks1.authenticate(full_a) is None
        assert ks2.authenticate(full_a) is None, "merge must not resurrect revoked keys"
        disk = json.load(open(kp))
        live = {r["key_id"] for r in disk if not r.get("revoked")}
        tombs = {r["key_id"] for r in disk if r.get("revoked")}
        assert kid_a in tombs, "revocation must persist as a tombstone"
        assert kid_a not in live, "revoked key must have no live record"
        assert all("hash" not in t and "full_hash" not in t
                   for t in disk if t.get("revoked")), "tombstones carry no secret material"

    def test_no_shared_tmp_collision(self, tmp_path):
        """The old bug's mechanism: both instances renamed THE SAME tmp path.
        With per-pid tmp names a concurrent flush can't steal ours."""
        kp = str(tmp_path / "keys.json")
        ks1, ks2 = KeyStore(kp), KeyStore(kp)
        errs = []

        def _safe(ks, ns):
            for j in range(20):
                try:
                    ks.create(f"{ns}-{j}")
                except FileNotFoundError as e:
                    errs.append(repr(e))

        ts = [threading.Thread(target=_safe, args=(ks1, "n0")),
              threading.Thread(target=_safe, args=(ks2, "n1"))]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        leftovers = [f for f in os.listdir(tmp_path) if ".tmp" in f]
        assert not errs, errs[:3]
        assert not leftovers, f"tmp files leaked: {leftovers}"


class TestMetricsHistoryPersistence:
    def test_dumps_accumulate_as_jsonl(self, tmp_path):
        reg = Registry()
        reg.preset("memd_writes_total", "counter", "writes")
        p = str(tmp_path / "metrics.jsonl")
        d = PeriodicDumper(p, interval_s=0.05, registry=reg)
        d.start()
        reg.inc("memd_writes_total", 5)
        import time as _t

        _t.sleep(0.18)  # >= 2 intervals
        d.stop()
        lines = [l for l in open(p).read().splitlines() if l.strip()]
        assert len(lines) >= 2, f"expected history, got {len(lines)} snapshot(s)"
        epochs = []
        for l in lines:
            snap = json.loads(l)
            assert "_epoch_ms" in snap
            epochs.append(snap["_epoch_ms"])
            assert any(h.get("p50") is not None or h.get("count") is not None
                       for h in snap.get("histograms", {}).values()) or snap["counters"] is not None
        assert epochs == sorted(epochs), "history must be chronologically ordered"

    def test_rotation_caps_growth(self, tmp_path):
        reg = Registry()
        p = str(tmp_path / "metrics.jsonl")
        d = PeriodicDumper(p, interval_s=1000, registry=reg, rotate_bytes=2000)
        for i in range(50):
            reg.set_gauge("g", 1.23456789 + i)
            d._dump()
        sizes = {f: os.path.getsize(os.path.join(tmp_path, f))
                 for f in os.listdir(tmp_path)}
        assert sizes["metrics.jsonl"] <= 2000 + 4096, f"active window not rotated: {sizes}"
        assert "metrics.jsonl.1" in sizes, "rotated window missing"

    def test_snapshot_schema_graphable(self, tmp_path):
        """Each line carries what a graphing pipeline needs: epoch, counters,
        gauges, histogram p50/p95/p99."""
        reg = Registry()
        reg.observe("lat_ms", 12.0, ns="acme")
        snap = reg.snapshot()
        h = list(snap["histograms"]["lat_ms"])[0]
        assert {"labels", "count", "sum", "avg", "p50", "p95", "p99"} <= set(h)
        assert snap["_process_uptime_s"] >= 0
