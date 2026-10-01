"""Pass 45: a compaction also settles a supersede or quarantine an older
cache never applied.

Through 0.3.0 a durable op could miss the index (the rotate that raised ran
first, and close() stamped the watermark past the op): no replay applies it
again. Pass 39 made the compaction that retires such an op remove or
tombstone any record it dropped that the index still served - deletes only.
A skipped supersede or quarantine stayed wrong in the index until a cold
rebuild: get() and search served the superseded fact as current, and a
quarantined record unfenced, while export (folded from the log) had the
flags.

Now the compaction also sets the index to its fold's state for every record
a supersede or quarantine op it retires targets.
"""
import json
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from memd.engine.memory import Memory  # noqa: E402
from memd.metrics import METRICS  # noqa: E402

CFG = {"embedder": "hash", "rate_max_writes": 10**9, "dup_max_repeats": 10**9}
NS = "t"
WORD = "quokkasettle"


def _open(root: str) -> Memory:
    return Memory(root, namespace=NS, config=CFG)


def _exported(m: Memory) -> dict:
    return {d["id"]: d for d in (json.loads(ln) for ln in m.export_jsonl().splitlines() if ln.strip())}


def _row(m: Memory, rid: str):
    got = m.ns.index.get_many([rid])
    return got[0] if got else None


def _found(m: Memory, rid: str) -> bool:
    return any(it.id == rid for it in m.search(f"{WORD} payload").items)


def _settled() -> float:
    return sum(s["value"] for s in METRICS.snapshot()["counters"].get("memd_index_settled_total", [])
               if s["labels"].get("ns") == NS)


def _skip_in_index(m: Memory, ops: list[dict]) -> None:
    """Append `ops` durably while the index cannot apply them, and forget
    that it missed them - the state an older build left behind."""
    ns = m.ns
    real = ns.index.apply_ops_batch, ns.index.mark_superseded, ns.index.mark_quarantined

    def broken(*_a, **_k):
        raise sqlite3.OperationalError("disk I/O error")

    ns.index.apply_ops_batch = ns.index.mark_superseded = ns.index.mark_quarantined = broken
    try:
        with pytest.raises(sqlite3.OperationalError):
            ns.append_ops(ops)
    finally:
        ns.index.apply_ops_batch, ns.index.mark_superseded, ns.index.mark_quarantined = real
    ns._index_owed = None


@pytest.mark.parametrize("kind", ["supersede", "quarantine", "unquarantine"])
def test_a_compaction_settles_a_flag_an_older_cache_never_applied(tmp_path, kind):
    root = str(tmp_path / "d")
    m = _open(root)
    try:
        victim = m.remember(f"the {WORD} payload, first version")
        newer = m.remember(f"the {WORD} payload, second version")
        keep = m.remember("a survivor note that stays")
        if kind == "unquarantine":
            m.ns.append_ops([{"op": "quarantine", "id": victim, "flag": True}])
        m.flush()
        op = {"supersede": {"op": "supersede", "old": victim, "new": newer, "at": 2},
              "quarantine": {"op": "quarantine", "id": victim, "flag": True},
              "unquarantine": {"op": "quarantine", "id": victim, "flag": False}}[kind]
        _skip_in_index(m, [op])
    finally:
        m.close()
    m = _open(root)
    try:
        exp = _exported(m)
        row = _row(m, victim)
        # the older cache's state: the log has the flag, the index does not
        if kind == "supersede":
            assert exp[victim]["time"]["superseded_by"] == newer
            assert row.time.superseded_by is None
        elif kind == "quarantine":
            assert exp[victim]["meta"].get("quarantined") and not row.meta.get("quarantined")
            assert _found(m, victim)
        else:
            assert not exp[victim]["meta"].get("quarantined") and row.meta.get("quarantined")
            assert not _found(m, victim)
        before = _settled()
        m.compact(force=True)
        row = _row(m, victim)
        if kind == "supersede":
            assert row.time.superseded_by == newer, "the index still serves the superseded fact as current"
            assert row.time.invalidated_at == 2
            assert m.get(victim)["time"]["superseded_by"] == newer
        elif kind == "quarantine":
            assert row.meta.get("quarantined"), "the index still serves a quarantined record unfenced"
            assert not _found(m, victim)
        else:
            assert not row.meta.get("quarantined"), "the index still hides an unquarantined record"
            assert _found(m, victim)
        assert _settled() == before + 1
        assert _row(m, keep) and not _row(m, keep).meta.get("quarantined")
        assert _row(m, newer).time.superseded_by is None
    finally:
        m.close()
