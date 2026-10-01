"""Pass 37: the upgrade never deletes live data on ambiguous evidence.

Pass 36 made the format-1 migration apply every `delete` / `hard_delete` its
audit ledger records to any record durable data still held. The review
showed that deletes live data the user still has:

  - v0.1 (18246db) filed EVERY namespace's entries in the facade default
    namespace's ledger, with no namespace field: a record deleted in `other`
    and restored into `default` was deleted from `default` by the upgrade;
  - a full-fidelity restore (`memd import memd`: original id and
    t_ingested) after a delete in the SAME namespace was dropped too - by the
    ledger, and already by the fold, whose guard held any copy ingested
    before an op on its id before that op;
  - a hard-deleted id re-added with a fresh ingest time lost its re-add: the
    hard delete op carries no time, so the guard held every copy before it.

The fixtures are format-1 stores the old builds wrote through their own
facade (encryption off, audit_flush_every=1), with the old warm index cache
(checkpointed, tantivy dropped - derived): meta.json has the live set the
old binary's warm open served. `restore_xns[_hard]`: X added in `other`,
exported, deleted there (soft / hard, far deadline), restored into
`default`. `restore`: X, Y, Z added, exported, X deleted, Y hard-deleted,
both restored. `readd`: the same with a fresh t_ingested. `plain`: the
deletes alone (control: they must hold).

Also here: the migration report (`memd migrate --report`), a keyed audit
chain for new ledgers, and namespace close/eviction GC outside the engine
lock.
"""
import hashlib
import hmac
import json
import logging
import os
import shutil
import sys
import tarfile
import threading

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from memd.cli import main as cli_main  # noqa: E402
from memd.core.schema import MemoryRecord, Scope  # noqa: E402
from memd.engine.memory import Memory  # noqa: E402
from memd.storage.audit import AuditLog, BufferedAuditLog, read_verified  # noqa: E402
from memd.storage.crypto import LocalKeyEnvelope, NullKeyEnvelope  # noqa: E402
from memd.storage.engine import StorageEngine, _legacy_events  # noqa: E402
from memd.storage.objectstore import LocalObjectStore  # noqa: E402

TARBALL = os.path.join(os.path.dirname(__file__), "fixtures", "legacy_restores.tar.gz")
CFG = {"embedder": "hash", "rate_max_writes": 10**9, "dup_max_repeats": 10**9,
       "hard_delete_deadline_ms": 10**9}
CASES = ["18246db-restore_xns", "18246db-restore_xns_hard", "faa1012-restore_xns_hard",
         "18246db-restore", "f979ea8-restore", "f979ea8-readd",
         "18246db-plain", "f979ea8-plain"]


def _fixture(tmp_path, name: str) -> tuple[str, dict, dict]:
    with tarfile.open(TARBALL) as tf:
        members = [m for m in tf.getmembers() if m.name.lstrip("./").split("/", 1)[0] == name]
        tf.extractall(tmp_path, members=members)  # noqa: S202 - our own fixture
    root = str(tmp_path / name)
    with open(os.path.join(root, "meta.json")) as f:
        meta = json.load(f)
    with open(os.path.join(root, "ids.json")) as f:
        ids = json.load(f)
    return root, meta, ids


def _live(m: Memory, ids: dict) -> tuple[list[str], list[str]]:
    inv = {v: k for k, v in ids.items()}
    m.ns.index.flush()
    live = sorted(inv.get(r.id, r.id) for r in m.ns.index.all_records() if not r.deleted)
    exp = sorted(inv.get(json.loads(line)["id"], "?")
                 for line in m.export_jsonl().splitlines() if line.strip())
    return live, exp


@pytest.mark.parametrize("cold", [False, True], ids=["warm", "cold"])
@pytest.mark.parametrize("name", CASES)
def test_the_upgrade_serves_what_the_old_warm_open_served(tmp_path, name, cold):
    root, meta, ids = _fixture(tmp_path, name)
    data = os.path.join(root, "data")
    if cold:
        shutil.rmtree(os.path.join(data, "store", "_cache"))
    m = Memory(data, encrypt=False, config=CFG)
    try:
        live, exp = _live(m, ids)
    finally:
        m.close()
    assert live == meta["live"], f"lost={sorted(set(meta['live']) - set(live))}"
    assert exp == live
    m = Memory(data, encrypt=False, config=CFG)   # and it stays that way
    try:
        assert _live(m, ids)[0] == meta["live"]
    finally:
        m.close()


def test_an_unattributable_ledger_delete_is_kept_logged_and_reported(tmp_path, caplog):
    """18246db filed `other`'s delete of X in `default`'s ledger; X lives in
    both namespaces, so the entry cannot be attributed: X stays, and the
    warning and the report name it."""
    root, meta, ids = _fixture(tmp_path, "18246db-restore_xns")
    data = os.path.join(root, "data")
    with caplog.at_level(logging.WARNING, logger="memd.storage.engine"):
        m = Memory(data, encrypt=False, config=CFG)
    try:
        assert "X" in _live(m, ids)[0]
    finally:
        m.close()
    warned = [r.getMessage() for r in caplog.records if "did NOT apply" in r.getMessage()]
    assert warned and ids["X"] in warned[0] and "other" in warned[0]
    rep = StorageEngine(os.path.join(data, "store")).migration_report()["namespaces"]["default"]
    assert rep["format"] == 2 and not rep["pending_migration"]
    assert [d["id"] for d in rep["ambiguous_deletes"]] == [ids["X"]]
    assert "other" in rep["ambiguous_deletes"][0]["reason"]
    assert rep["recovered_deletes"] == []


def test_a_restore_after_the_other_namespace_is_gone_is_kept_by_its_wal_copy(tmp_path):
    """No other namespace holds X any more (destroyed), so the entry is
    attributed to `default` - but X's copy there is in the WAL, written after
    any fold that could have lost a delete: a later write, so it stays."""
    root, meta, ids = _fixture(tmp_path, "18246db-restore_xns_hard")
    data = os.path.join(root, "data")
    shutil.rmtree(os.path.join(data, "store", "ns", "other"))
    shutil.rmtree(os.path.join(data, "store", "_cache"))
    m = Memory(data, encrypt=False, config=CFG)
    try:
        assert _live(m, ids)[0] == ["X", "Z"]
    finally:
        m.close()
    rep = StorageEngine(os.path.join(data, "store")).migration_report()["namespaces"]["default"]
    assert [d["id"] for d in rep["ambiguous_deletes"]] == [ids["X"]]
    assert "WAL" in rep["ambiguous_deletes"][0]["reason"]


def _tree(root: str) -> dict[str, tuple[int, int]]:
    out = {}
    for d, _dirs, files in os.walk(root):
        for f in files:
            p = os.path.join(d, f)
            st = os.stat(p)
            out[os.path.relpath(p, root)] = (st.st_size, st.st_mtime_ns)
    return out


def test_the_report_previews_a_pending_migration_without_changing_anything(tmp_path, capsys):
    root, meta, ids = _fixture(tmp_path, "18246db-restore_xns_hard")
    data = os.path.join(root, "data")
    before = _tree(data)
    assert cli_main(["migrate", "--report", data]) == 0
    rep = json.loads(capsys.readouterr().out)
    assert _tree(data) == before, "a report must not write, lock, migrate or create a cache"
    ns = rep["namespaces"]
    assert rep["store_format"] == 2
    assert ns["default"]["format"] == 1 and ns["default"]["pending_migration"]
    assert ns["default"]["preview"]
    assert [d["id"] for d in ns["default"]["ambiguous_deletes"]] == [ids["X"]]
    assert ns["other"]["pending_migration"]
    # after the upgrade the same answer comes from the report it persisted
    Memory(data, encrypt=False, config=CFG).close()
    assert cli_main(["migrate", "--report", data]) == 0
    after = json.loads(capsys.readouterr().out)["namespaces"]["default"]
    assert after["format"] == 2 and not after["pending_migration"] and not after["preview"]
    assert after["ambiguous_deletes"] == ns["default"]["ambiguous_deletes"]
    assert after["migration"]["migrated_at"] > 0


def test_the_report_lists_recovered_deletes_and_losses_it_cannot_recover(tmp_path):
    """The pass-36 fixture: hard deletes an older build lost, recovered from
    the ledger; a batch delete logged only as a count is a legacy loss."""
    lost = os.path.join(os.path.dirname(__file__), "fixtures", "legacy_lost_hard_deletes.tar.gz")
    with tarfile.open(lost) as tf:
        members = [m for m in tf.getmembers() if m.name.split("/", 1)[0] == "f979ea8-110-e0"]
        tf.extractall(tmp_path, members=members)  # noqa: S202 - our own fixture
    data = str(tmp_path / "f979ea8-110-e0" / "data")
    with open(tmp_path / "f979ea8-110-e0" / "ids.json") as f:
        ids = json.load(f)
    led = os.path.join(data, "store", "ns", "default", "audit")
    entries = [json.loads(line) for line in open(led) if line.strip()]
    extra = {"ts": entries[-1]["ts"] + 1, "actor": "api", "action": "delete_batch",
             "target": "2 records", "detail": {"count": 2, "hard": False}, "prev": entries[-1]["h"]}
    extra["h"] = hashlib.sha256(json.dumps(extra, sort_keys=True).encode()).hexdigest()
    with open(led, "a") as f:
        f.write(json.dumps(extra, separators=(",", ":")) + "\n")
    os.unlink(led + ".state")
    shutil.rmtree(os.path.join(data, "store", "_cache"))
    eng = StorageEngine(os.path.join(data, "store"))
    rep = eng.migration_report()["namespaces"]["default"]
    assert rep["pending_migration"]
    assert {d["id"] for d in rep["recovered_deletes"]} == {ids["T7"], ids["T4"], ids["T13"],
                                                          ids["T15"]}
    assert rep["legacy_losses"]["count_only_deletes"] == [
        {"action": "delete_batch", "ts": extra["ts"], "count": 2}]
    assert rep["migration"]["ledger"]["chain_intact"]


# -------------------------------------------------------------- the fold order

def _rec(rid: str, t: int) -> MemoryRecord:
    r = MemoryRecord.create("t", "raw_event", f"c {rid}", Scope(user="u"), record_id=rid)
    r.time.t_ingested = t
    return r


def _fold_ids(events) -> list[str]:
    from memd.storage.engine import _fold_events

    kept, *_ = _fold_events({}, events, 10**13, force=False)
    return sorted(r.id for r in kept)


def test_a_restore_after_its_delete_keeps_its_slot():
    """[X] [Z] del X [X restored, original t_ingested]: the counts reconcile,
    so the restore's gap-filled slot follows the delete - it stays."""
    frames = [(None, [_rec("X", 100)]), (None, [_rec("Z", 110)]), (None, [_rec("X", 100)])]
    ops = [{"op": "tombstone", "id": "X", "at": 120, "seq": 3}]
    events, how, _ = _legacy_events(frames, ops, 0, 4)
    assert how == "exact"
    assert _fold_ids(events) == ["X", "Z"]


def test_a_restore_of_a_segment_record_keeps_its_slot():
    frames = [(None, [_rec("X", 100)])]
    ops = [{"op": "tombstone", "id": "X", "at": 120, "seq": 1}]
    events, how, _ = _legacy_events(frames, ops, 0, 2, prior={"X"})
    assert _fold_ids(events) == ["X"]


def test_a_misplaced_first_copy_makes_every_slot_suspect():
    """A FIRST copy gap-filled after its own delete proves the slots wrong
    (the old counter reused seqs), so a re-write's slot proves nothing
    either: both are held before the deletes - a delete wins."""
    frames = [(None, [_rec("A", 100)]), (None, [_rec("B", 101)]), (None, [_rec("A", 100)])]
    ops = [{"op": "tombstone", "id": "B", "at": 150, "seq": 1},
           {"op": "tombstone", "id": "A", "at": 160, "seq": 2}]
    events, how, _ = _legacy_events(frames, ops, 0, 6)
    assert how == "guarded"
    assert _fold_ids(events) == []


def test_a_fresh_re_add_after_a_hard_delete_survives():
    """The hard delete op has no time of its own; its tombstone's counts."""
    frames = [(None, [_rec("Y", 100)]), (None, [_rec("Y", 500)])]
    ops = [{"op": "tombstone", "id": "Y", "at": 200, "seq": 2},
           {"op": "hard_delete", "id": "Y", "deadline": 10**15, "seq": 3}]
    events, how, _ = _legacy_events(frames, ops, 0, 4)
    assert how == "exact"
    assert _fold_ids(events) == ["Y"]


# -------------------------------------------------------------- keyed chain

def _stores(tmp_path, enc: bool):
    root = str(tmp_path / "r")
    env = LocalKeyEnvelope(root + "/keys") if enc else NullKeyEnvelope()
    return LocalObjectStore(root + "/store"), env


def test_an_encrypted_namespace_chains_its_ledger_with_an_hmac(tmp_path):
    store, env = _stores(tmp_path, enc=True)
    log = BufferedAuditLog(store, "ns/t/audit", env, flush_every=1)
    for i in range(3):
        log.append(actor="u", action="add", target=f"r{i}")
    es = log.read()
    assert all(e["alg"] == "hmac-sha256" for e in es)
    key = hmac.new(env.data_key("t"), b"memd/audit-chain/v1", hashlib.sha256).digest()
    body = json.dumps({k: v for k, v in es[0].items() if k != "h"}, sort_keys=True).encode()
    assert es[0]["h"] == hmac.new(key, body, hashlib.sha256).hexdigest()
    assert log.verify()
    assert BufferedAuditLog(store, "ns/t/audit", env, flush_every=1).verify()  # across a reopen


def _rechain_plain(entries):
    prev, out = "0" * 64, []
    for e in entries:
        e = {k: v for k, v in e.items() if k not in ("h", "alg")}
        e["prev"] = prev
        e["h"] = hashlib.sha256(json.dumps(e, sort_keys=True).encode()).hexdigest()
        prev = e["h"]
        out.append(e)
    return b"".join(json.dumps(e, separators=(",", ":")).encode() + b"\n" for e in out)


def test_a_forged_ledger_does_not_verify_without_the_key(tmp_path):
    """Someone who can write the store but has no key re-chains a forged
    delete with SHA-256 and puts the ledger back as plaintext: it used to
    verify (the decoder passes plaintext through). Now it breaks at once."""
    store, env = _stores(tmp_path, enc=True)
    log = AuditLog(store, "ns/t/audit", env)
    log.append(actor="u", action="add", target="r1")
    forged = log.read() + [{"ts": 1, "actor": "api", "action": "hard_delete",
                            "target": "victim", "detail": {}}]
    store.put("ns/t/audit", _rechain_plain(forged))
    store.delete("ns/t/audit.state")
    assert not AuditLog(store, "ns/t/audit", env).verify()
    led = read_verified(store, "ns/t/audit", env)
    assert led.entries == [] and not led.ok and len(led.unverified) == 2


def test_a_keyed_chain_cannot_be_downgraded_or_forged_with_another_key(tmp_path):
    store, env = _stores(tmp_path, enc=True)
    log = AuditLog(store, "ns/t/audit", env)
    log.append(actor="u", action="add", target="r1")
    good = log.read()[0]
    other = hmac.new(b"k" * 32, b"memd/audit-chain/v1", hashlib.sha256).digest()
    for alg, key in (("hmac-sha256", other), (None, None)):
        e = {"ts": 2, "actor": "api", "action": "hard_delete", "target": "victim",
             "detail": {}, "prev": good["h"]}
        if alg:
            e["alg"] = alg
        body = json.dumps(e, sort_keys=True).encode()
        e["h"] = (hmac.new(key, body, hashlib.sha256).hexdigest() if key
                  else hashlib.sha256(body).hexdigest())
        payload = json.dumps(e, separators=(",", ":")).encode() + b"\n"
        enc = env.encrypt("t", payload)
        store.put("ns/t/audit", store.get("ns/t/audit") + len(enc).to_bytes(4, "big") + enc)
        led = read_verified(store, "ns/t/audit", env)
        assert [x["target"] for x in led.entries] == ["r1"] and not led.ok
        store.truncate("ns/t/audit", len(store.get("ns/t/audit")) - 4 - len(enc))


def test_an_unencrypted_ledger_stays_unkeyed_and_old_ledgers_still_verify(tmp_path):
    store, env = _stores(tmp_path, enc=False)
    log = AuditLog(store, "ns/t/audit", env)
    log.append(actor="u", action="add", target="r1")
    assert "alg" not in log.read()[0] and log.verify()
    # a pre-0.2 encrypted ledger (unkeyed entries) continued by this version
    store2, env2 = _stores(tmp_path / "b", enc=True)
    legacy = {"ts": 1, "actor": "u", "action": "add", "target": "old", "detail": {},
              "prev": "0" * 64}
    legacy["h"] = hashlib.sha256(json.dumps(legacy, sort_keys=True).encode()).hexdigest()
    enc = env2.encrypt("t", json.dumps(legacy, separators=(",", ":")).encode() + b"\n")
    store2.put("ns/t/audit", len(enc).to_bytes(4, "big") + enc)
    log2 = AuditLog(store2, "ns/t/audit", env2)
    log2.append(actor="u", action="add", target="new")
    assert [e.get("alg") for e in log2.read()] == [None, "hmac-sha256"]
    assert log2.verify()


# -------------------------------------------------------------- lock scope

def test_close_and_eviction_list_the_store_outside_the_engine_lock(tmp_path):
    """A clean close collects the namespace's garbage - a store LIST. Under
    the engine lock that stalled every other namespace's open for as long."""
    eng = StorageEngine(str(tmp_path / "s"), max_open_namespaces=2)
    seen: list[tuple[str, bool]] = []
    real = eng.store.list

    def spy(prefix):
        seen.append((prefix, eng._lock._is_owned()))
        return real(prefix)

    eng.store.list = spy
    for i in range(4):
        ns = eng.namespace(f"n{i}")
        ns.append([_rec(f"R{i:024d}", 1)])
        ns.rotate("test")  # a checkpoint, so close() collects
    eng.close()
    closes = [owned for prefix, owned in seen if prefix.startswith("ns/n")]
    assert closes, "close/eviction should have collected garbage"
    assert not any(closes), "the store was listed under the engine lock"


def test_a_namespace_evicted_mid_close_is_reopened_only_after_its_close(tmp_path):
    eng = StorageEngine(str(tmp_path / "s"), max_open_namespaces=1)
    a = eng.namespace("a")
    a.append([_rec("A" * 26, 1)])
    a.rotate("test")
    gate, entered = threading.Event(), threading.Event()
    real = a._collect_garbage

    def slow():
        entered.set()
        gate.wait(5)
        real()

    a._collect_garbage = slow
    t = threading.Thread(target=eng.namespace, args=("b",))  # evicts a
    t.start()
    assert entered.wait(5)
    got: list = []
    r = threading.Thread(target=lambda: got.append(eng.namespace("a")))
    r.start()
    r.join(0.3)
    assert r.is_alive(), "reopened while its old store was still closing"
    gate.set()
    t.join(5)
    r.join(5)
    assert got and got[0] is not a and got[0].index.get_by_id("A" * 26) is not None
    eng.close()
