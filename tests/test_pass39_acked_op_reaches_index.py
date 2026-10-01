"""Pass 39: an op that is durable in the log is applied to the index - always.

append_ops wrote the op, then ran the rotate its size threshold asked for,
and applied the op to the index only after that. A rotate that raised - a
damaged WAL frame makes every fold refuse - skipped the apply: the delete
was in the ops log but get() and search still served the record, its purge
was not scheduled, and close() stamped the index watermark past the op, so
no reopen ever replayed it. After the documented recovery (remove the frame,
reopen, compact) export said the record was gone while get() and search
served it, and a hard-deleted record's text stayed in the local index.

The op now reaches the index (and the purge schedule) before any
maintenance runs. An index apply that fails leaves the watermark below the
op - a reopen replays it, a fold catches the index up first - and a
compaction makes sure the index serves nothing its fold dropped.
"""
import json
import os
import shutil
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from memd.engine.memory import Memory  # noqa: E402
from memd.storage.crypto import KeyCustodyError  # noqa: E402
from memd.storage.engine import _frames_with_offsets  # noqa: E402

CFG = {"embedder": "hash", "rate_max_writes": 10**9, "dup_max_repeats": 10**9}
NS = "t"
SECRET = "zebracorn-7731-hardgone"


def _open(root: str) -> Memory:
    return Memory(root, namespace=NS, config=CFG)


def _ns_dir(root: str) -> str:
    return os.path.join(root, "store", "ns", NS)


def _cache(root: str) -> str:
    return os.path.join(root, "store", "_cache")


def _ids(m: Memory) -> set[str]:
    return {json.loads(ln)["id"] for ln in m.export_jsonl().splitlines() if ln.strip()}


def _in_cache(root: str, needle: bytes) -> list[str]:
    hits = []
    for dp, _d, fs in os.walk(_cache(root)):
        for fn in fs:
            with open(os.path.join(dp, fn), "rb") as f:
                if needle in f.read():
                    hits.append(os.path.relpath(os.path.join(dp, fn), _cache(root)))
    return hits


def _damage_frame(root: str, frame_no: int) -> tuple[int, int]:
    """Flip a byte inside complete WAL frame `frame_no` -> (start, length)."""
    p = os.path.join(_ns_dir(root), "wal")
    with open(p, "rb") as f:
        data = bytearray(f.read())
    frames = list(_frames_with_offsets(bytes(data)))
    end, fr = frames[frame_no]
    start = end - len(fr) - 4
    data[start + 4 + 20] ^= 0x5A
    with open(p, "wb") as f:
        f.write(bytes(data))
    return start, len(fr)


def _remove_frame(root: str, start: int) -> None:
    """SECURITY.md "Recovering from an unreadable log frame", step 3."""
    p = os.path.join(_ns_dir(root), "wal")
    with open(p, "rb") as f:
        b = f.read()
    ln = int.from_bytes(b[start:start + 4], "big")
    with open(p, "wb") as f:
        f.write(b[:start] + b[start + 4 + ln:])


def _searchable(m: Memory, rid: str) -> bool:
    return any(it.id == rid for it in m.search(f"{SECRET} payload").items)


@pytest.mark.parametrize("crossing", ["tombstone", "hard_delete"])
def test_a_delete_whose_rotate_refuses_still_reaches_the_index(tmp_path, crossing):
    root = str(tmp_path / "d")
    m = _open(root)
    try:
        filler = [m.remember(f"ordinary note number {i}") for i in range(4)]
        victim = m.remember(f"the {SECRET} payload to erase")
        keep = m.remember("a survivor note that stays")
        m.flush()
    finally:
        m.close()
    start, _ = _damage_frame(root, 0)   # filler[0]'s frame; the warm open never reads it
    m = _open(root)
    ns = m.ns
    real = ns.append_ops
    hard_logged = []

    def append_ops(ops):
        # the ops log reaches its rotate threshold with this append
        if any(o.get("op") == crossing for o in ops):
            ns.wal_rotate_bytes = 1
        if any(o.get("op") == "hard_delete" for o in ops):
            hard_logged.append(True)
        return real(ops)

    ns.append_ops = append_ops
    try:
        try:
            m.delete(victim, hard=True)
        except KeyCustodyError:
            pass   # the rotate's refusal (the op is durable all the same)
        assert not m.get(victim), "a durable delete is not served in-process"
        assert not _searchable(m, victim)
        if hard_logged:
            assert ns.pending_hard_deletes >= 1, "a durable hard delete is scheduled"
    finally:
        ns.wal_rotate_bytes = 10**12
        m.close()
    _remove_frame(root, start)
    for cache in ("warm", "cold"):
        if cache == "cold":
            shutil.rmtree(_cache(root))
        m = _open(root)
        try:
            m.compact(force=True)
            assert not m.get(victim), f"get() serves a deleted record ({cache})"
            assert not _searchable(m, victim), f"search serves a deleted record ({cache})"
            got = _ids(m)
            assert victim not in got
            assert keep in got and set(filler[1:]) <= got
            assert filler[0] not in got   # the damaged frame's write: lost, as documented
            assert m.ns.pending_hard_deletes == 0
        finally:
            m.close()
        if hard_logged:
            assert not _in_cache(root, SECRET.encode()), \
                f"hard-deleted text left in the local index ({cache})"


@pytest.mark.parametrize("then", ["close", "write"])
def test_an_op_whose_index_apply_failed_is_replayed_by_the_next_open(tmp_path, then):
    """The watermark close() stamps is the last event the index applied - not
    the last one logged - so the reopen replays the op (and the next write
    catches the index up in order first)."""
    root = str(tmp_path / "d")
    m = _open(root)
    try:
        victim = m.remember(f"the {SECRET} payload to erase")
        keep = m.remember("a survivor note that stays")
        m.flush()
        ns = m.ns
        real = ns.index.apply_ops_batch, ns.index.tombstone

        def broken(*_a, **_k):
            raise sqlite3.OperationalError("disk I/O error")

        ns.index.apply_ops_batch = ns.index.tombstone = broken
        with pytest.raises(sqlite3.OperationalError):
            m.delete(victim)
        ns.index.apply_ops_batch, ns.index.tombstone = real
        if then == "write":
            m.remember("a later write the index does apply")
            assert not m.get(victim)
    finally:
        m.close()
    m = _open(root)
    try:
        assert not m.get(victim), "the reopen replays the durable op the index missed"
        assert not _searchable(m, victim)
        assert m.get(keep)
        assert victim not in _ids(m)
    finally:
        m.close()


@pytest.mark.parametrize("fold", ["rotate", "compact"])
def test_a_fold_catches_the_index_up_before_it_retires_an_op(tmp_path, fold):
    """A fold retires the ops it folds: one the index missed must be applied
    first, or no replay could apply it any more."""
    root = str(tmp_path / "d")
    m = _open(root)
    try:
        victim = m.remember(f"the {SECRET} payload to erase")
        keep = m.remember("a survivor note that stays")
        m.flush()
        ns = m.ns
        real = ns.index.apply_ops_batch, ns.index.tombstone

        def broken(*_a, **_k):
            raise sqlite3.OperationalError("disk I/O error")

        ns.index.apply_ops_batch = ns.index.tombstone = broken
        with pytest.raises(sqlite3.OperationalError):
            m.delete(victim)
        ns.index.apply_ops_batch, ns.index.tombstone = real
        if fold == "rotate":
            ns.rotate("probe")
        else:
            m.compact(force=True)
        assert not m.get(victim), "the fold caught the index up"
        assert m.get(keep)
    finally:
        m.close()
    for cache in ("warm", "cold"):
        if cache == "cold":
            shutil.rmtree(_cache(root))
        m = _open(root)
        try:
            assert not m.get(victim), f"served after the fold ({cache})"
            assert not _searchable(m, victim)
            assert victim not in _ids(m)
        finally:
            m.close()


@pytest.mark.parametrize("hard", [False, True], ids=["soft", "hard"])
def test_a_compaction_settles_a_delete_an_older_cache_never_applied(tmp_path, hard):
    """A cache left by an older build: the op is in the log, the index never
    applied it, and the watermark is past it (what close() used to stamp).
    No replay applies it; the compaction that retires it makes sure the
    index serves nothing it dropped - and a purged row's text goes."""
    root = str(tmp_path / "d")
    m = _open(root)
    try:
        victim = m.remember(f"the {SECRET} payload to erase")
        keep = m.remember("a survivor note that stays")
        m.flush()
        ns = m.ns
        real = ns.index.apply_ops_batch, ns.index.tombstone, ns.index.hard_delete

        def broken(*_a, **_k):
            raise sqlite3.OperationalError("disk I/O error")

        ns.index.apply_ops_batch = ns.index.tombstone = ns.index.hard_delete = broken
        with pytest.raises(sqlite3.OperationalError):
            ns.append_ops([{"op": "tombstone", "id": victim, "at": 1}]
                          + ([{"op": "hard_delete", "id": victim, "deadline": 0}] if hard else []))
        ns.index.apply_ops_batch, ns.index.tombstone, ns.index.hard_delete = real
        ns._index_owed = None   # the older build: no record of what the index missed
    finally:
        m.close()
    m = _open(root)
    try:
        assert m.get(victim), "(the older cache's state: served)"
        m.compact(force=True)
        assert not m.get(victim)
        assert not _searchable(m, victim)
        assert m.get(keep)
        assert victim not in _ids(m)
    finally:
        m.close()
    if hard:
        assert not _in_cache(root, SECRET.encode()), "hard-deleted text left in the local index"
