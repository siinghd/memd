"""The SQLite index never hands out a rowid twice.

SQLite gives a new row MAX(rowid)+1, so hard-deleting the newest rows made
the next write reuse their rowids. The tantivy accelerator indexes by rowid
order (every row above its watermark is new), and only a delete of THE max
rowid lowered its scan floor, to rowid-1: hard-deleting the two newest rows
(lower one first) put the next write below the floor, missing from the bm25
lane for good (v0.2.0 verifier, HIGH). The fix is at the source and
backend-independent: the highest rowid ever used is recorded in the delete's
own transaction and new rows go above it (AUTOINCREMENT semantics).
"""
import sqlite3

import pytest

from memd.engine.memory import Memory
from memd.index.sqlite_index import IndexFilter


def _mem(root, backend: str) -> Memory:
    return Memory(str(root), encrypt=False,
                  config={"embedder": "hash", "reranker": "none", "lexical_backend": backend,
                          "rate_max_writes": 10 ** 9, "dup_max_repeats": 10 ** 9})


def _backends():
    from memd.index.tantivy_lexical import tantivy_available

    return ["fts5"] + (["tantivy"] if tantivy_available() else [])


def _rowid(m: Memory, rid: str) -> int:
    with m.ns.index._read() as c:
        return int(c.execute("SELECT rowid FROM records WHERE id=?", (rid,)).fetchone()[0])


def _max_rowid(m: Memory) -> int:
    with m.ns.index._read() as c:
        return int(c.execute("SELECT MAX(rowid) FROM records").fetchone()[0])


@pytest.mark.parametrize("backend", _backends())
@pytest.mark.parametrize("mode", ["single", "batch", "top-only"])
def test_hard_deleted_rowids_are_never_reused(tmp_path, backend, mode):
    root = tmp_path / "d"
    m = _mem(root, backend)
    try:
        m.add_events([{"content": f"filler note {i}", "user_id": "u"} for i in range(20)])
        a = m.add("second newest walrusine", user_id="u")[0]
        b = m.add("newest walrusine", user_id="u")[0]
        m.flush()
        top = _max_rowid(m)
        if mode == "single":
            m.delete(a, hard=True)
            m.delete(b, hard=True)
        elif mode == "batch":
            m.delete_many([a, b], hard=True)
        else:
            m.delete(b, hard=True)
        c = m.add("fresh narwhalic record", user_id="u")[0]
        assert _rowid(m, c) > top
        m._bump_epoch()
        assert [i.id for i in m.search("narwhalic", user_id="u").items] == [c]
        m.flush()
        top = _max_rowid(m)
        m.delete(c, hard=True)  # the newest row again, then a clean reopen
    finally:
        m.close()
    m = _mem(root, backend)
    try:
        d = m.add("after reopen: dugongish", user_id="u")[0]
        assert _rowid(m, d) > top, "the high-water mark did not survive a reopen"
        e = m.add_events([{"content": f"batch {i} dugongish", "user_id": "u"} for i in range(3)])
        assert sorted(_rowid(m, x) for x in e) == [_rowid(m, x) for x in e]
        assert min(_rowid(m, x) for x in e) > _rowid(m, d)
        m.flush()
        hits = m.ns.index.search_bm25("dugongish", IndexFilter(), limit=10)
        assert {h.record.id for h in hits} == {d, *e}
    finally:
        m.close()


def test_the_high_water_mark_commits_with_the_delete(tmp_path):
    """No crash can separate the two: the mark is in the delete's own
    transaction (read back through a second connection right after)."""
    m = _mem(tmp_path / "d", "fts5")
    try:
        m.add_events([{"content": f"note {i}", "user_id": "u"} for i in range(5)])
        newest = m.add("the newest note", user_id="u")[0]
        m.flush()
        top = _max_rowid(m)
        m.delete(newest, hard=True)
        con = sqlite3.connect(m.ns.index.path)
        try:
            row = con.execute("SELECT v FROM meta WHERE k='rowid_hwm'").fetchone()
            assert row is not None and int(row[0]) == top
            assert con.execute("SELECT COUNT(*) FROM records WHERE id=?", (newest,)).fetchone()[0] == 0
        finally:
            con.close()
    finally:
        m.close()


def test_an_overwrite_keeps_its_rowid(tmp_path):
    """Upserting an existing id updates it in place: the explicit rowid a
    new row gets after a top delete never moves an existing row."""
    import dataclasses

    m = _mem(tmp_path / "d", "fts5")
    try:
        ids = m.add_events([{"content": f"note {i}", "user_id": "u"} for i in range(5)])
        m.flush()
        m.delete(ids[-1], hard=True)
        before = _rowid(m, ids[0])
        rec = dataclasses.replace(m.ns.index.get_by_id(ids[0]), content="rewritten note")
        rec.namespace = "default"
        m._ns_for("default").append([rec])
        assert _rowid(m, ids[0]) == before
        assert m.ns.index.get_by_id(ids[0]).content == "rewritten note"
        new = m.add("a new note", user_id="u")[0]
        assert _rowid(m, new) > _rowid(m, ids[-2]) + 1  # above the deleted top
    finally:
        m.close()
