"""Pass 17 regression: correctness and data integrity on the write/read paths.

Six defects, each reproduced against the pass-16 baseline before the fix:

 1. CRIT  Two processes on one data root silently destroyed acked data - no
          lock, lease or leader anywhere. 8 of 150 durably-acked writes were
          lost with no error, warning or metric.
 2. CRIT  A due hard-delete purge deadline ran a FULL-NAMESPACE compaction
          inline on the next ordinary write: 804.8ms write ack at 20K records
          (vs 5.4ms), scaling linearly - 150x the embedded p99 SLO, and D2
          says per-write maintenance is "O(entity cluster), never O(namespace)".
 3. HIGH  The BM25 AND tier DISCARDED its hits when it found fewer than ten -
          exactly the high-precision case - and then failed to re-find the
          document in a saturated OR window.
 4. HIGH  The bounded OR window applied its LIMIT BEFORE any predicate, so
          ineligible rows starved eligible ones out of the lane entirely.
 5. HIGH  ABBA deadlock: destroy_namespace took ns->engine while close() took
          engine->ns. Both reachable from the public surface.
 6. MED   close() discarded queued embeddings silently; drain() returned while
          a batch was still in flight, so flush() did not mean what it said.
"""
import os
import subprocess
import sys
import tempfile
import threading
import time

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from memd.core.schema import Scope  # noqa: E402
from memd.engine.memory import Memory  # noqa: E402
from memd.index.sqlite_index import IndexFilter  # noqa: E402
from memd.storage.engine import NamespaceBusyError, NamespaceStore  # noqa: E402


# --------------------------------------------------------------- single writer

def test_second_process_is_refused_not_silently_destructive(tmp_path):
    root = str(tmp_path / "d")
    m = Memory(root, encrypt=False, namespace="shared")
    try:
        code = (
            "import sys; sys.path.insert(0, %r)\n"
            "from memd.engine.memory import Memory\n"
            "try:\n"
            "    Memory(%r, encrypt=False, namespace='shared')\n"
            "    print('OPENED')\n"
            "except Exception as e:\n"
            "    print(type(e).__name__)\n"
        ) % (os.path.join(os.path.dirname(__file__), "..", "src"), root)
        out = subprocess.run([sys.executable, "-c", code], capture_output=True,
                             text=True, timeout=60).stdout.strip()
        assert "NamespaceBusyError" in out, out
    finally:
        m.close()


def test_owner_lock_is_refcounted_within_the_process(tmp_path):
    """The lock guards against a second OS PROCESS. In-process, a second
    NamespaceStore over the same namespace (LRU churn, maintenance paths,
    the pattern test_pass6_purge uses) must still open - the refcount, not a
    second flock, is what makes that work.

    (Two full Memory instances on one root remain unsupported for an unrelated
    and pre-existing reason: they contend on the shared derived-index SQLite
    cache. That is unchanged by the owner lock.)"""
    root = str(tmp_path / "d")
    m = Memory(root, encrypt=False, namespace="shared")
    try:
        m.add("one", user_id="u")
        second = NamespaceStore("shared", m.engine.store, str(tmp_path / "c2"), None)
        try:
            assert second.namespace == "shared"
        finally:
            second.close()
        m.add("two", user_id="u")  # original still usable after the second closed
    finally:
        m.close()


def test_namespace_survives_lru_eviction_and_reopen(tmp_path):
    """Eviction releases the owner lock; reopening must re-acquire it rather
    than trip over the lock this process itself just released."""
    m = Memory(str(tmp_path / "d"), encrypt=False, namespace="main",
               config={"max_open_namespaces": 2})
    try:
        for i in range(8):
            m.add(f"record {i}", user_id="u", namespace=f"t{i}")
        for i in range(8):
            hits = m.search("record", user_id="u", namespace=f"t{i}")
            assert hits is not None
    finally:
        m.close()


def test_lock_is_released_on_close(tmp_path):
    root = str(tmp_path / "d")
    Memory(root, encrypt=False, namespace="shared").close()
    m2 = Memory(root, encrypt=False, namespace="shared")  # must not raise
    m2.close()


def test_override_is_available_for_operators_who_accept_the_risk(tmp_path, monkeypatch):
    root = str(tmp_path / "d")
    m = Memory(root, encrypt=False, namespace="shared")
    try:
        monkeypatch.setenv("MEMD_ALLOW_MULTI_PROCESS", "1")
        ns = NamespaceStore("shared", m.engine.store, str(tmp_path / "c2"), None)
        ns.close()
    finally:
        m.close()


# ------------------------------------------------------- purge off the ack path

def test_due_purge_does_not_inflate_an_ordinary_write(tmp_path):
    m = Memory(str(tmp_path / "d"), encrypt=False, config={"hard_delete_deadline_ms": 40})
    try:
        for i in range(0, 3000, 300):
            m.add_events([{"content": f"record {i + j} about staging", "user_id": "u"}
                          for j in range(300)])
        rid = m.remember("secret to erase", user_id="u")
        m.delete(rid, hard=True)
        time.sleep(0.10)  # deadline now due

        t0 = time.monotonic()
        m.add_events([{"content": "an ordinary later turn", "user_id": "u"}])
        due_write_ms = (time.monotonic() - t0) * 1000
        baseline = []
        for _ in range(5):
            t0 = time.monotonic()
            m.add_events([{"content": "another turn", "user_id": "u"}])
            baseline.append((time.monotonic() - t0) * 1000)
        typical = sorted(baseline)[len(baseline) // 2]
        assert due_write_ms < max(10 * typical, 50), (
            f"write with a due purge deadline took {due_write_ms:.1f}ms vs "
            f"{typical:.1f}ms typical - namespace-scale work is back on the ack path")
        m.flush()
        recs, _ = m.ns.load_all_records()
        assert all(r.id != rid for r in recs), "the purge guarantee must still hold"
    finally:
        m.close()


# --------------------------------------------------------------- bm25 recall

def test_and_tier_match_is_never_discarded(tmp_path):
    m = Memory(str(tmp_path / "d"))
    try:
        gold = None
        for i in range(1200):
            if i == 1000:
                gold = m.add("mango allergy: patient reacts badly, epipen required",
                             user_id="u")[0]
            m.add(f"mango smoothie recipe number {i}", user_id="u")
        hits = m.ns.index.search_bm25("mango allergy", IndexFilter(scope=Scope(user="u")),
                                      limit=10)
        assert gold in [h.record.id for h in hits], (
            "the only document matching EVERY term was absent from the lexical lane")
    finally:
        m.close()


def test_bounded_window_counts_eligible_rows_not_all_rows(tmp_path):
    m = Memory(str(tmp_path / "d"))
    try:
        for i in range(400):
            m.add(f"mango note {i}", user_id="u", session_id="A")
        for i in range(5):
            m.add(f"mango important {i}", user_id="u", session_id="B")
        hits = m.ns.index.search_bm25(
            "mango", IndexFilter(scope=Scope(user="u", session="B")), limit=10)
        assert len(hits) == 5, f"eligible records starved out of the window: {len(hits)}/5"
    finally:
        m.close()


def test_bulk_delete_does_not_blind_the_lane_to_survivors(tmp_path):
    """tombstone() leaves fts rows in place, so a routine bulk delete used to
    make every survivor invisible to BM25."""
    m = Memory(str(tmp_path / "d"))
    try:
        noise = [m.add(f"mango noise {i}", user_id="u")[0] for i in range(400)]
        live = [m.add(f"mango allergy critical {i}", user_id="u")[0] for i in range(5)]
        m.delete_many(noise, actor="t")
        hits = m.ns.index.search_bm25("mango allergy", IndexFilter(scope=Scope(user="u")),
                                      limit=10)
        got = {h.record.id for h in hits}
        assert set(live) <= got, f"survivors invisible after bulk delete: {len(got & set(live))}/5"
    finally:
        m.close()


# ------------------------------------------------------------------ lock order

def test_destroy_racing_close_does_not_deadlock(tmp_path, monkeypatch):
    from memd.storage import engine as engine_mod

    real = engine_mod.StorageEngine.destroy_namespace

    def slow_destroy(self, ns):
        time.sleep(0.25)  # widen the window; engine lock NOT yet taken
        return real(self, ns)

    monkeypatch.setattr(engine_mod.StorageEngine, "destroy_namespace", slow_destroy)
    m = Memory(str(tmp_path / "d"), namespace="main")
    m.add("x", user_id="u", namespace="doomed")
    done = {}

    def destroyer():
        try:
            m.destroy_namespace("doomed", actor="t")
        except Exception:
            pass
        done["destroy"] = True

    def closer():
        time.sleep(0.12)
        try:
            m.engine.close()
        except Exception:
            pass
        done["close"] = True

    ts = [threading.Thread(target=destroyer, daemon=True),
          threading.Thread(target=closer, daemon=True)]
    [t.start() for t in ts]
    [t.join(timeout=8) for t in ts]
    assert len(done) == 2, f"deadlock: threads still wedged after 8s ({done})"


# ------------------------------------------------------------ embed durability

class _SlowEmbedder:
    name = "hash-ngram-384"
    dim = 384

    def embed(self, texts):
        time.sleep(0.05)
        return np.ones((len(texts), 384), dtype=np.float32) / 20.0

    def embed_one(self, t):
        return np.ones(384, dtype=np.float32) / 20.0


def test_clean_close_keeps_the_vectors_it_was_asked_to_persist(tmp_path):
    root = str(tmp_path / "d")
    m = Memory(root)
    m.embedder = _SlowEmbedder()
    m._embed_worker.embedder = m.embedder
    for i in range(40):
        m.add(f"record number {i} about deployments", user_id="u")
    m.close()

    m2 = Memory(root)
    try:
        st = m2.ns.index.stats()
        assert st["vectors"] == 40, (
            f"a CLEAN close discarded {40 - st['vectors']} of 40 embeddings")
    finally:
        m2.close()


def test_drain_waits_for_in_flight_batches(tmp_path):
    m = Memory(str(tmp_path / "d"))
    m.embedder = _SlowEmbedder()
    m._embed_worker.embedder = m.embedder
    try:
        for i in range(20):
            m.add(f"note {i} about caching", user_id="u")
        m.flush()
        assert m._embed_worker._pending() == 0, "flush() returned with work still in flight"
        assert m.ns.index.stats()["vectors"] == 20
    finally:
        m.close()
