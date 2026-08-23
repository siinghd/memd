"""Pass 21 regression: durable bytes per raw byte.

Measured against D2's write-amplification SLO: 7.29x at 800-byte records and
48.08x at 90-byte records, with the vector lane fully materialized. Two of the
copies were pure waste:

  - fts5 was CONTENT-DUPLICATING: `fts5(id UNINDEXED, content)` keeps its own
    full copy of text that already sits in `records.content` two feet away -
    10.27MB beside 10.27MB on a 10K x 800B corpus.
  - vectors were float32, and they are L2-normalised with cosine computed in
    float32 after upcast, so half those bytes bought nothing.

The rest is inherent: one dense vector per record is a FIXED cost, so a flat
<=3x bar stated per RAW byte is unreachable below ~4KB records at any
embedding dimension. D2 was amended accordingly (see 02-slos.md); this file
guards the engineering half of that ruling.
"""
import os
import sqlite3
import sys

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from memd.engine.memory import Memory  # noqa: E402
from memd.index.sqlite_index import SCHEMA_VERSION, NamespaceIndex, _vec_blob, _vec_from_blob  # noqa: E402


def test_fts_is_external_content_not_a_second_copy(tmp_path):
    m = Memory(str(tmp_path / "d"))
    try:
        sql = m.ns.index._con.execute(
            "SELECT sql FROM sqlite_master WHERE name='fts'").fetchone()[0]
        assert "content='records'" in sql.replace('"', "'"), sql
        assert "UNINDEXED" not in sql, "fts is storing its own copy again"
    finally:
        m.close()


def test_vectors_are_stored_halved(tmp_path):
    m = Memory(str(tmp_path / "d"))
    try:
        m.add("a record to embed", user_id="u")
        m.flush()
        row = m.ns.index._con.execute("SELECT dim, length(vec) FROM vectors LIMIT 1").fetchone()
        assert row, "expected a vector"
        dim, nbytes = row
        assert nbytes == dim * 2, f"vector stored as {nbytes / dim} bytes/dim, expected 2"
    finally:
        m.close()


def test_pre_v2_float32_vectors_still_decode():
    """A store written before pass 21 must keep working without a re-embed."""
    v = np.linspace(-1, 1, 384).astype(np.float32)
    v = v / np.linalg.norm(v)
    old = v.tobytes()                      # float32, 1536 bytes
    new = _vec_blob(v)                     # float16, 768 bytes
    assert len(old) == 384 * 4 and len(new) == 384 * 2
    back_old = _vec_from_blob(old, 384)
    back_new = _vec_from_blob(new, 384)
    assert back_old.dtype == np.float32 and back_new.dtype == np.float32
    assert np.allclose(back_old, v, atol=1e-6)
    assert float(np.dot(back_new, v)) > 0.9999, "float16 must not disturb cosine"


def test_v1_index_migrates_and_stays_searchable(tmp_path):
    """The derived index is rebuildable, so v1 -> v2 drops and repopulates fts
    and converts vectors IN PLACE (a rebuild would be correct but would cost a
    full re-embed, which for a BYO-key deployment is real money)."""
    root = str(tmp_path / "d")
    m = Memory(root)
    for i in range(60):
        m.add(f"deployment note {i} about the staging cluster", user_id="u")
    m.flush()
    path = m.ns.index.path
    before = len(m.search("staging cluster deployment", user_id="u").items)
    assert before > 0
    m.close()

    # rewind the schema stamp so the next open runs the migration
    con = sqlite3.connect(path)
    con.execute("UPDATE meta SET v='1' WHERE k='schema_version'")
    con.commit()
    con.close()

    idx = NamespaceIndex(path)
    try:
        assert int(idx.get_meta("schema_version")) == SCHEMA_VERSION
        sql = idx._con.execute("SELECT sql FROM sqlite_master WHERE name='fts'").fetchone()[0]
        assert "content='records'" in sql.replace('"', "'")
        n = idx._con.execute("SELECT count(*) FROM fts WHERE fts MATCH 'staging'").fetchone()[0]
        assert n > 0, "fts was not repopulated by the migration"
        row = idx._con.execute("SELECT dim, length(vec) FROM vectors LIMIT 1").fetchone()
        assert row and row[1] == row[0] * 2, "vectors were not converted in place"
    finally:
        idx.close()


def test_deleting_a_record_removes_its_fts_entry(tmp_path):
    """fts is now trigger-maintained; a hard delete must not leave a ghost."""
    m = Memory(str(tmp_path / "d"))
    try:
        rid = m.add("a uniquely spelled zanzibar record", user_id="u")[0]
        m.flush()
        assert m.search("zanzibar", user_id="u").items
        m.delete(rid, hard=True)
        m.flush()
        n = m.ns.index._con.execute(
            "SELECT count(*) FROM fts WHERE fts MATCH 'zanzibar'").fetchone()[0]
        assert n == 0, "fts still indexes a hard-deleted record"
        assert not m.search("zanzibar", user_id="u").items
    finally:
        m.close()


def test_recall_survives_the_byte_reductions(tmp_path):
    m = Memory(str(tmp_path / "d"))
    try:
        for i in range(400):
            m.add(f"routine note {i} about caching and deploys", user_id="u")
        m.add("the deployment codeword is zanzibar", user_id="u")
        m.flush()
        res = m.search("deployment codeword zanzibar", user_id="u")
        assert any("zanzibar" in i.content for i in res.items)
    finally:
        m.close()


@pytest.mark.parametrize("size,ceiling", [(800, 7.0), (4000, 4.0)])
def test_write_amplification_meets_the_amended_bar(tmp_path, size, ceiling):
    """Guards the byte reductions, not the SLO itself.

    D2's bar is stated at >=10K records, where this engine reads 5.02x at 800B
    and 2.80x at 4KB. This test runs a 3K corpus (fixed costs have not
    amortized yet, so the ratio reads higher) and asserts ceilings calibrated
    there: 6.05x/3.39x measured now against 7.95x/4.60x before pass 21, so the
    thresholds discriminate rather than merely pass.
    """
    n = 3000
    m = Memory(str(tmp_path / f"d{size}"), encrypt=False,
               config={"rate_max_writes": 10 ** 9})
    try:
        body = "x" * max(1, size - 30)
        raw = 0
        for i in range(0, n, 500):
            evs = [{"content": f"rec {i + j} {body}", "user_id": "u"} for j in range(500)]
            raw += sum(len(e["content"].encode()) for e in evs)
            m.add_events(evs)
        m.flush()
        for _ in range(10):
            if m.ns.index.stats()["vectors"] >= n:
                break
            m.reembed()
            m.flush()
        total = sum(os.path.getsize(os.path.join(dp, f))
                    for dp, _d, fs in os.walk(str(tmp_path / f"d{size}")) for f in fs)
        ratio = total / raw
        assert ratio <= ceiling, f"{size}B records: {ratio:.2f}x exceeds the {ceiling}x bar"
    finally:
        m.close()
