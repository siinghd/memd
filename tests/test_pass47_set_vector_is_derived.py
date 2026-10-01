"""Pass 47: a logged `set_vector` op is derived state, like every vector.

A verifier's probe appended a `set_vector` op (an arbitrary vector, under
the model name "hash") and found it gone after close + reopen, warm and
cold. Triage: nothing in memd writes that op - the embed worker and
reembed() put vectors straight into the index (`index.set_vectors`) and
never log them; the op kind is only applied when a log carries one. Vectors
are derived state (ADR-5, ADR-8): a record's vector is the embedder's output
on its raw text, which is retained, and an index cache that lost it - or
holds one of another model - is healed by re-embedding (the open's
vector-lane self-heal, Memory._report_vector_health). The probe's warm loss
was that self-heal: "hash" is not the embedder's model name, so the vector
was stale by version and replaced. Under the embedder's own model name the
vector survives a warm reopen and a compaction; a cold open re-derives it
from the raw text. These tests pin that contract.
"""
import os
import shutil
import sys
import time

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from memd.engine.memory import Memory  # noqa: E402
from memd.index.sqlite_index import _vec_from_blob  # noqa: E402

CFG = {"embedder": "hash", "rate_max_writes": 10**9, "dup_max_repeats": 10**9}
NS = "t"
TEXT = "the quarterly lighthouse inventory lists fourteen spare lenses"


def _open(root: str) -> Memory:
    return Memory(root, namespace=NS, config=CFG)


def _vector(m: Memory, rid: str) -> tuple[str, np.ndarray] | None:
    row = m.ns.index._con.execute("SELECT model, dim, vec FROM vectors WHERE id=?", (rid,)).fetchone()
    if row is None:
        return None
    return row[0], np.asarray(_vec_from_blob(row[2], row[1]), dtype=np.float32)


def _same(a: np.ndarray, b: np.ndarray) -> bool:
    return float(a @ b / (np.linalg.norm(a) * np.linalg.norm(b))) > 0.999


def _wait_for(pred, timeout: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(0.05)
    return pred()


def _logged_vector(root: str, model: str | None = None) -> tuple[str, np.ndarray, str]:
    """A record, and a set_vector op for it logged with an arbitrary vector."""
    m = _open(root)
    try:
        rid = m.remember(TEXT)
        m.flush()
        dim = _vector(m, rid)[1].shape[0]
        vec = np.full(dim, 0.125, dtype=np.float32)
        vec[0] = 0.9
        name = model or m.embedder.name
        m.ns.append_ops([{"op": "set_vector", "id": rid, "vec_hex": vec.tobytes().hex(),
                          "model": name}])
        got = _vector(m, rid)
        assert got is not None and got[0] == name and _same(got[1], vec), "applied live"
    finally:
        m.close()
    return rid, vec, name


def test_nothing_memd_writes_logs_a_vector(tmp_path):
    root = str(tmp_path / "data")
    m = _open(root)
    try:
        rid = m.remember(TEXT)
        m.flush()
        m.reembed()
        assert _vector(m, rid) is not None
        kinds = {op.get("op") for op in m.ns._read_ops()}
        frames = [r for _, recs in m.ns._wal_events("test") for r in recs]
    finally:
        m.close()
    assert "set_vector" not in kinds, "vectors are derived state: never logged"
    assert frames and not any({"vec", "vec_hex", "vector", "embedding"} & set(r.to_dict())
                              for r in frames), "a record carries no vector"


def test_a_vector_of_the_embedders_model_survives_a_warm_reopen_and_compaction(tmp_path):
    root = str(tmp_path / "data")
    rid, vec, model = _logged_vector(root)
    for step in ("reopen", "compact", "reopen after compaction"):
        m = _open(root)
        try:
            if step == "compact":
                m.compact(force=True)
            m.flush()
            got = _vector(m, rid)
            assert got is not None and got[0] == model and _same(got[1], vec), step
        finally:
            m.close()


def test_a_cold_open_rederives_the_vector_from_the_raw_text(tmp_path):
    root = str(tmp_path / "data")
    rid, vec, model = _logged_vector(root)
    m = _open(root)
    try:
        m.compact(force=True)     # the op is folded: the log no longer carries it
    finally:
        m.close()
    shutil.rmtree(os.path.join(root, "store", "_cache"))
    m = _open(root)
    try:
        want = np.asarray(m.embedder.embed([TEXT])[0], dtype=np.float32)
        assert _wait_for(lambda: (_vector(m, rid) or ("", None))[0] == model), \
            "the vector lane was not healed"
        got = _vector(m, rid)[1]
        assert _same(got, want), "the vector is the embedder's output on the raw text"
        assert not _same(got, vec)
        assert m.get(rid) is not None
        assert rid in [it.id for it in m.search("lighthouse spare lenses").items]
    finally:
        m.close()


def test_a_vector_of_another_model_is_replaced_by_the_embedders(tmp_path):
    root = str(tmp_path / "data")
    rid, vec, _ = _logged_vector(root, model="hash")
    m = _open(root)
    try:
        assert m.embedder.name != "hash"
        assert _wait_for(lambda: (_vector(m, rid) or ("", None))[0] == m.embedder.name), \
            "a stale-model vector was kept"
        want = np.asarray(m.embedder.embed([TEXT])[0], dtype=np.float32)
        assert _same(_vector(m, rid)[1], want)
    finally:
        m.close()
