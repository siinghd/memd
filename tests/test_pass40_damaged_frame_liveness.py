"""Pass 40: one damaged WAL frame neither blocks the export nor fails writes.

Export - the recovery path - folded the WAL with the same reader every fold
uses, which refuses a complete frame that does not read: a single damaged
frame made export() raise, so nothing could be exported. And once the WAL
(or ops log) passed its rotate threshold, every write and delete ran the
rotate AFTER the write was durable and raised its refusal: callers saw an
error for writes that had landed - and retried them.

Export now skips a damaged frame, says where it is (a warning with its byte
offset, memd_export_frames_skipped_total, the namespace's
last_export_skipped) and exports everything readable; nothing is deleted. A
write whose follow-up rotate refuses succeeds: the failure is logged,
metered (memd_ns_maintenance_failures_total) and surfaced by stats(),
status() and /health, and retried after a backoff - as is a hard delete's
background purge compaction. Rotate and compaction themselves still refuse
until the frame is fixed.
"""
import json
import logging
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from memd.engine.memory import Memory  # noqa: E402
from memd.metrics import METRICS  # noqa: E402
from memd.storage.crypto import KeyCustodyError  # noqa: E402
from memd.storage.engine import _frames_with_offsets  # noqa: E402

CFG = {"embedder": "hash", "rate_max_writes": 10**9, "dup_max_repeats": 10**9}
NS = "t"


def _open(root: str, enc: bool = True) -> Memory:
    return Memory(root, namespace=NS, config=CFG, encrypt=enc)


def _wal(root: str) -> str:
    return os.path.join(root, "store", "ns", NS, "wal")


def _ids(m: Memory) -> set[str]:
    return {json.loads(ln)["id"] for ln in m.export_jsonl().splitlines() if ln.strip()}


def _counter(name: str, **labels) -> float:
    return sum(s["value"] for s in METRICS.snapshot()["counters"].get(name, [])
               if all(s["labels"].get(k) == v for k, v in labels.items()))


def _build(root: str, enc: bool) -> list[str]:
    m = _open(root, enc)
    try:
        ids = [m.remember(f"precious fact number {i}") for i in range(6)]
        m.flush()
    finally:
        m.close()
    return ids


def _damage(root: str, frame_no: int, enc: bool = True) -> int:
    """Flip one byte of a WAL frame; returns where the frame starts. A
    ciphertext no longer authenticates whatever byte flips; a plaintext
    frame only fails to read when its JSON breaks (a flip inside a string
    value is undetectable without encryption), so it loses the ':' after
    "_wal_seq"."""
    with open(_wal(root), "rb") as f:
        data = bytearray(f.read())
    end, fr = list(_frames_with_offsets(bytes(data)))[frame_no]
    start = end - len(fr) - 4
    data[start + 4 + (20 if enc else 11)] ^= 0x5A
    with open(_wal(root), "wb") as f:
        f.write(bytes(data))
    return start


def _remove_frame(root: str, start: int) -> None:
    """SECURITY.md "Recovering from an unreadable log frame", step 3."""
    with open(_wal(root), "rb") as f:
        b = f.read()
    ln = int.from_bytes(b[start:start + 4], "big")
    with open(_wal(root), "wb") as f:
        f.write(b[:start] + b[start + 4 + ln:])


@pytest.mark.parametrize("enc", [True, False], ids=["encrypted", "plaintext"])
def test_export_skips_a_damaged_wal_frame_and_exports_the_rest(tmp_path, caplog, enc):
    root = str(tmp_path / "d")
    ids = _build(root, enc)
    at = _damage(root, 2, enc)
    with open(_wal(root), "rb") as f:
        before = f.read()
    m = _open(root, enc)   # warm: the open's bisection does not read frame 2
    try:
        skipped0 = _counter("memd_export_frames_skipped_total", ns=NS)
        with caplog.at_level(logging.WARNING, logger="memd.storage.engine"):
            got = _ids(m)
        assert got == set(ids) - {ids[2]}, "everything readable is exported"
        # (with encryption off, a frame that does not parse looks like a
        # ciphertext: _frame_fault calls it "sealed")
        fault = "damaged" if enc else "sealed"
        assert m.ns.last_export_skipped == [{"log": f"ns/{NS}/wal", "at": at, "fault": fault}]
        assert _counter("memd_export_frames_skipped_total", ns=NS) == skipped0 + 1
        assert any(f"at byte {at} of ns/{NS}/wal" in r.getMessage() for r in caplog.records), \
            "the warning names the frame's byte offset"
        audited = [e for e in m.audit.read() if e["action"] == "export"][-1]
        assert audited["detail"]["skipped_frames"] == m.ns.last_export_skipped
        # every fold still refuses: it would delete the log it read
        with pytest.raises(KeyCustodyError):
            m.ns.rotate("probe")
        with pytest.raises(KeyCustodyError):
            m.compact(force=True)
    finally:
        m.close()
    with open(_wal(root), "rb") as f:
        assert f.read() == before, "nothing was cut or deleted"


def test_writes_whose_rotate_refuses_succeed_and_the_failure_is_surfaced(tmp_path):
    root = str(tmp_path / "d")
    ids = _build(root, True)
    at = _damage(root, 0)
    m = _open(root)
    ns = m.ns
    ns.wal_rotate_bytes = 64   # every write from here on crosses the threshold
    try:
        fails0 = _counter("memd_ns_maintenance_failures_total", ns=NS, op="rotate")
        new = [m.remember(f"a write after the damage {i}") for i in range(20)]
        assert all(new) and all(m.get(r) for r in new), "acknowledged and readable"
        assert m.delete(ids[3]) and not m.get(ids[3])
        assert m.delete(ids[4], hard=True) and not m.get(ids[4])
        assert m.delete_many([ids[5]]) == 1 and not m.get(ids[5])
        failing = ns.maintenance_status()
        assert failing and failing["op"] == "rotate"
        assert f"at byte {at} of ns/{NS}/wal" in failing["error"]
        fails = _counter("memd_ns_maintenance_failures_total", ns=NS, op="rotate") - fails0
        assert 1 <= fails <= 3, f"retried after a backoff, not on every write ({fails})"
        assert m.stats()["maintenance"]["op"] == "rotate"
        st = m.status()
        assert st["maintenance"][NS]["op"] == "rotate"
        assert m.status(ns_filter=NS)["maintenance"][NS]["op"] == "rotate"
        # fail-closed: the folds themselves still refuse
        with pytest.raises(KeyCustodyError):
            ns.rotate("probe")
        with pytest.raises(KeyCustodyError):
            m.compact(force=True)
    finally:
        ns.wal_rotate_bytes = 10**12
        m.close()
    _remove_frame(root, at)
    m = _open(root)
    try:
        got = _ids(m)
        assert set(new) <= got, "every acknowledged write is durable"
        assert not got & {ids[3], ids[4], ids[5]}, "every acknowledged delete is durable"
        assert got == (set(ids) - {ids[0], ids[3], ids[4], ids[5]}) | set(new)
        m.ns.rotate("after recovery")
        m.compact(force=True)
        assert m.ns.maintenance_status() is None
        assert m.ns.pending_hard_deletes == 0
    finally:
        m.close()


def test_a_session_close_whose_rotate_refuses_keeps_its_facts(tmp_path):
    root = str(tmp_path / "d")
    _build(root, True)
    at = _damage(root, 0)
    m = _open(root)
    try:
        m.add("I moved to Lisbon in March and I work at Acme as a nurse",
              session_id="s1", user_id="alice")
        res = m.close_session("s1", user_id="alice")
        assert res["segment"] == "", "no segment was folded"
        assert res["facts_written"] >= 1
        assert m.ns.maintenance_status()["op"] == "rotate"
    finally:
        m.close()
    _remove_frame(root, at)
    m = _open(root)
    try:
        facts = [json.loads(ln) for ln in m.export_jsonl().splitlines() if ln.strip()]
        assert sum(1 for f in facts if f["kind"] == "fact" and "Acme" in f["content"]) >= 1, \
            "the session's facts are durable"
    finally:
        m.close()


def test_health_reports_failing_maintenance_without_naming_namespaces(tmp_path):
    from fastapi.testclient import TestClient

    from memd.server.http import create_app

    app = create_app(data_dir=str(tmp_path / "data"), keys_path=str(tmp_path / "keys.json"))
    try:
        t = TestClient(app)
        assert t.get("/health").json()["maintenance_failing"] == 0
        ns = app.state.engine.engine.namespace("acme")
        ns._maintenance_failed("rotate", KeyCustodyError("a complete WAL frame is damaged"))
        body = t.get("/health").json()
        assert body["ok"] and body["maintenance_failing"] == 1
        assert "acme" not in json.dumps(body)
    finally:
        app.state.engine.close()


def test_a_purge_compaction_that_refuses_is_surfaced_and_backed_off(tmp_path):
    root = str(tmp_path / "d")
    cfg = dict(CFG, hard_delete_deadline_ms=0)   # every hard delete is due at once
    m = Memory(root, namespace=NS, config=cfg)
    try:
        ids = [m.remember(f"precious fact number {i}") for i in range(6)]
        m.flush()
    finally:
        m.close()
    _damage(root, 0)
    m = Memory(root, namespace=NS, config=cfg)
    try:
        fails0 = _counter("memd_ns_maintenance_failures_total", ns=NS, op="compact")
        assert m.delete(ids[3], hard=True) and not m.get(ids[3])
        m.flush()   # drains the background purge
        failing = m.ns.maintenance_status()
        assert failing and failing["op"] == "compact" and failing["failures"] == 1
        assert _counter("memd_ns_maintenance_failures_total", ns=NS, op="compact") == fails0 + 1
        # the next due purge waits out the backoff instead of re-reading every log
        assert m.delete(ids[4], hard=True) and not m.get(ids[4])
        m.flush()
        assert _counter("memd_ns_maintenance_failures_total", ns=NS, op="compact") == fails0 + 1
        assert m.ns.pending_hard_deletes == 2, "both purges stay scheduled"
    finally:
        m.close()
