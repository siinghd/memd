"""Pass 36: an upgrade must not serve a hard delete an older build lost.

Every older build recorded a hard delete in its local index by deleting the
row. When it was killed between a fold's ops-log delete and its re-append of
the pending hard deletes, the delete survived nowhere durable - its warm open
hid the record (no row), but the format-2 migration served it again: a
hard-deleted record resurfacing is a compliance failure. The old facade also
appended `hard_delete` / `delete` (target: the record id) to the namespace's
hash-chained audit ledger; the migration now reads it (chain verified first,
nothing past a break applied) and deletes every record durable data still
holds that the ledger says was deleted.

The fixtures are the verifier's kill-matrix histories that ended with such a
loss (seeds 108 and 110 on f979ea8 and faa1012; 108, 110 and 111 on 18246db),
replayed through each OLD build's Memory facade (audit_flush_every=1, the
compliance setting) with the process killed at that re-append; plus two
encrypted runs. Each holds the old store, its ledger, the old binary's warm
index cache (WAL checkpointed and vacuumed - same rows), meta.json (the acked
model: live tags and supersedes) and ids.json (tag -> record id). The old
binary's own warm open served exactly the model on every one.
"""
import hashlib
import json
import logging
import os
import shutil
import sys
import tarfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from memd.engine.memory import Memory  # noqa: E402

TARBALL = os.path.join(os.path.dirname(__file__), "fixtures", "legacy_lost_hard_deletes.tar.gz")
CFG = {"embedder": "hash", "rate_max_writes": 10**9, "dup_max_repeats": 10**9}
RECOVERABLE = ["f979ea8-108-e0", "f979ea8-110-e0", "faa1012-108-e0", "faa1012-110-e0",
               "18246db-108-e0", "18246db-110-e0", "18246db-111-e0", "f979ea8-108-e1"]


def _fixture(tmp_path, name: str) -> tuple[str, dict, dict]:
    with tarfile.open(TARBALL) as tf:
        members = [m for m in tf.getmembers() if m.name.split("/", 1)[0] == name]
        tf.extractall(tmp_path, members=members)  # noqa: S202 - our own fixture
    root = str(tmp_path / name)
    with open(os.path.join(root, "meta.json")) as f:
        meta = json.load(f)
    with open(os.path.join(root, "ids.json")) as f:
        ids = json.load(f)
    return root, meta, ids


def _state(m: Memory, ids: dict) -> tuple[list[str], dict, list[str]]:
    inv = {v: k for k, v in ids.items()}
    m.ns.index.flush()
    rows = [r for r in m.ns.index.all_records() if not r.deleted]
    live = sorted(inv.get(r.id, r.id) for r in rows)
    sup = {inv[r.id]: inv.get(r.time.superseded_by, r.time.superseded_by)
           for r in rows if r.time.superseded_by}
    exp = sorted(inv.get(json.loads(line)["id"], "?")
                 for line in m.export_jsonl().splitlines() if line.strip())
    return live, sup, exp


def _hard_deleted(meta: dict) -> list[str]:
    return [a[1] for acts in meta["sessions"] for a in acts if a[0] == "hdel"]


def _files_with(root: str, needle: bytes) -> list[str]:
    out = []
    for dirpath, _dirs, files in os.walk(root):
        for fn in files:
            with open(os.path.join(dirpath, fn), "rb") as f:
                if needle in f.read():
                    out.append(os.path.relpath(os.path.join(dirpath, fn), root))
    return out


@pytest.mark.parametrize("cold", [False, True], ids=["warm", "cold"])
@pytest.mark.parametrize("name", RECOVERABLE)
def test_a_hard_delete_the_old_build_lost_is_not_served_after_the_upgrade(tmp_path, name, cold):
    root, meta, ids = _fixture(tmp_path, name)
    data = os.path.join(root, "data")
    if cold:  # a node without the old cache: the ledger alone decides
        shutil.rmtree(os.path.join(data, "store", "_cache"))
    m = Memory(data, encrypt=meta["enc"], config=CFG)
    try:
        live, sup, exp = _state(m, ids)
        assert live == meta["live"], f"extra={sorted(set(live) - set(meta['live']))}"
        assert sup == meta["sup"]
        assert exp == live, "export must hold exactly what reads serve"
        m.compact()
    finally:
        m.close()
    if not meta["enc"]:
        for tag in _hard_deleted(meta):
            assert _files_with(data, f'"content {tag}"'.encode()) == [], \
                f"{tag}'s text outlived the purge"
    m = Memory(data, encrypt=meta["enc"], config=CFG)   # and it stays that way
    try:
        assert _state(m, ids)[0] == meta["live"]
    finally:
        m.close()


@pytest.mark.parametrize("cold", [False, True], ids=["warm", "cold"])
@pytest.mark.parametrize("name", ["f979ea8-108-e0", "18246db-110-e0"])
def test_a_recovered_hard_delete_is_purged_at_open_without_a_write(tmp_path, name, cold):
    """The migration schedules the purge of a recovered hard delete due at
    once, but the older segments that hold its text were only compacted
    when the next write came: an upgraded node that only served reads kept
    it on disk. Opening the namespace now runs the due purge."""
    root, meta, ids = _fixture(tmp_path, name)
    data = os.path.join(root, "data")
    if cold:
        shutil.rmtree(os.path.join(data, "store", "_cache"))
    m = Memory(data, encrypt=meta["enc"], config=CFG)
    try:
        m.flush()  # background maintenance drained - no write, no compact()
        assert _state(m, ids)[0] == meta["live"]
    finally:
        m.close()
    for tag in _hard_deleted(meta):
        assert _files_with(data, f'"content {tag}"'.encode()) == [], \
            f"{tag}'s text outlived the purge due at open"


def _ledger(root: str) -> str:
    return os.path.join(root, "data", "store", "ns", "default", "audit")


def _entries(path: str) -> list[dict]:
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def _write(path: str, entries: list[dict]) -> None:
    with open(path, "w") as f:
        for e in entries:
            f.write(json.dumps(e, separators=(",", ":")) + "\n")
    state = path + ".state"
    if os.path.exists(state):
        os.unlink(state)  # the sidecar only caches the tail


def _upgrade(root: str, meta: dict, ids: dict, caplog) -> tuple[list[str], list[str]]:
    data = os.path.join(root, "data")
    shutil.rmtree(os.path.join(data, "store", "_cache"))  # cold: the ledger alone decides
    with caplog.at_level(logging.ERROR, logger="memd.storage.engine"):
        m = Memory(data, encrypt=meta["enc"], config=CFG)
    try:
        live = _state(m, ids)[0]
    finally:
        m.close()
    return live, [r.getMessage() for r in caplog.records if "hash chain BROKEN" in r.getMessage()]


class TestTamperedLedger:
    def test_a_forged_delete_past_the_last_valid_link_is_not_applied(self, tmp_path, caplog):
        root, meta, ids = _fixture(tmp_path, "f979ea8-108-e0")
        entries = _entries(_ledger(root))
        forged = {"ts": entries[-1]["ts"] + 1, "actor": "api", "action": "hard_delete",
                  "target": ids["T5"], "detail": {},
                  "prev": "f" * 64}          # well formed, but not chained to the last entry
        forged["h"] = hashlib.sha256(json.dumps(forged, sort_keys=True).encode()).hexdigest()
        _write(_ledger(root), entries + [forged])
        live, errors = _upgrade(root, meta, ids, caplog)
        assert "T5" in live, "a delete past the break was applied"
        assert "T2" not in live, "the valid prefix must still be applied"
        assert live == meta["live"]
        assert errors, "a broken chain must be logged loudly"

    def test_an_edited_entry_stops_the_ledger_there(self, tmp_path, caplog):
        root, meta, ids = _fixture(tmp_path, "f979ea8-108-e0")
        entries = _entries(_ledger(root))
        at = next(i for i, e in enumerate(entries)
                  if e["action"] == "hard_delete" and e["target"] == ids["T2"])
        entries[at - 1]["actor"] = "someone-else"   # its digest no longer matches
        _write(_ledger(root), entries)
        live, errors = _upgrade(root, meta, ids, caplog)
        assert errors
        assert "T2" in live, "an entry past the break was applied"
        # entries before the break still count: T6's hard delete, T3's delete
        assert "T6" not in live and "T3" not in live

    def test_a_forked_chain_applies_nothing_past_the_fork(self, tmp_path, caplog):
        """18246db reset an ENCRYPTED ledger's chain on every reopen (fixed
        in 0.2.0): its second session starts at prev=0*64, which verification
        cannot tell from a forgery - so its deletes are not applied."""
        root, meta, ids = _fixture(tmp_path, "18246db-110-e1")
        live, errors = _upgrade(root, meta, ids, caplog)
        assert errors
        assert "T7" in live
