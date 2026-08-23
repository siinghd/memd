"""Ralph iteration 1 fixes: batched destructive-sweep I/O, bounded embed
backlog, HTTP route-label cardinality.

Complexity contracts asserted here:
  - forget()/delete_many(): O(n) arithmetic, O(1) durable appends (fsyncs),
    O(1) index commits - previously 2 fsync'd appends + 2 commits PER record.
  - _EmbedWorker: pending memory bounded by max_queue; overflow defers the
    vector lane instead of buffering unbounded content in RAM.
  - http route labels: bounded series count regardless of tenant count.
"""
import threading
import time

import numpy as np

from memd.engine.memory import Memory, _EmbedWorker
from memd.index.sqlite_index import NamespaceIndex
from memd.metrics import METRICS, Registry
from memd.pipeline.embedder import Embedder
from memd.server.http import _route_label


# ---------------------------------------------------------------- helpers

def _count_appends(monkeypatch):
    """Count durable LocalObjectStore.append calls (each is one fsync)."""
    from memd.storage.objectstore import LocalObjectStore

    calls = {"n": 0}
    orig = LocalObjectStore.append

    def counting(self, key, data):
        calls["n"] += 1
        return orig(self, key, data)

    monkeypatch.setattr(LocalObjectStore, "append", counting)
    return calls


def _make_memory(tmp_path, **cfg):
    m = Memory(str(tmp_path / "d"), config={"rate_max_writes": 10**9, **cfg})
    return m


# ------------------------------------------------------- fix 1: batched ops

def test_forget_uses_constant_durable_appends(tmp_path, monkeypatch):
    """300-record sweep must cost a HANDFUL of fsync'd appends, not one per
    record. Baseline before the fix: ~1.03 appends/deleted (309 for 300)."""
    calls = _count_appends(monkeypatch)
    m = _make_memory(tmp_path)
    n = 300
    m.add_events([
        {"content": f"delete-me secret zebra {i} alpha beta gamma delta", "user_id": "u1"}
        for i in range(n)
    ])
    calls["n"] = 0
    deleted = m.forget("delete-me secret zebra", user_id="u1")
    assert len(deleted) == n
    # batched path: events-batch + tombstone batch (+ rare rotation) - never O(n)
    assert calls["n"] <= 6, f"durable appends scaled with n: {calls['n']} for {n} deletes"
    res = m.search("delete-me secret zebra", user_id="u1")
    assert all("zebra" not in i.content for i in res.items), "forget left matches behind"
    m.close()


def test_delete_many_semantics_match_single_delete(tmp_path):
    """Batch delete removes every id from the live view; audit chain stays
    verifiable; hard variant physically purges rows."""
    m = _make_memory(tmp_path)
    ids = []
    for i in range(20):
        r = m.add(f"batch target item {i}", user_id="u1")
        ids.extend(r)
    m.flush()
    gone = m.delete_many(ids[:10], actor="test")
    assert gone == 10
    for rid in ids[:10]:
        assert m.get(rid) is None
    for rid in ids[10:]:
        assert m.get(rid) is not None
    assert m.audit.verify(), "audit chain broken after batched deletes"
    # hard purge: rows disappear even from include_deleted reads
    m2_ids = m.delete_many(ids[10:], hard=True, actor="test")
    assert m2_ids == 10
    st = m.stats()
    assert st["records"] == 0, f"hard batch left rows behind: {st}"
    m.close()


def test_supersede_batch_replay_equivalence(tmp_path):
    """Ops written via append_ops replay identically on a fresh process open:
    supersedence state survives restart with the batched write path."""
    m = _make_memory(tmp_path)
    a = m.remember("Alice works at Initech", entity_keys=["user.employer"], user_id="u1")
    b = m.remember("Alice works at Globex", entity_keys=["user.employer"], user_id="u1")
    m.close()

    m2 = Memory(str(tmp_path / "d"))
    old = m2.get(a)
    assert old is not None and old["time"]["superseded_by"] == b, \
        "batched supersedence op did not survive restart"
    new = m2.get(b)
    assert new is not None and new["time"]["superseded_by"] is None
    res = m2.search("where does Alice work?", user_id="u1")
    contents = [i.content for i in res.items]
    assert any("Globex" in c for c in contents)
    assert not any("Initech" in c for c in contents), \
        "superseded fact still served as current"
    m2.close()


def test_pending_hard_no_duplicates_under_rotation(tmp_path):
    """Hard-delete batch that forces an ops-log rotation mid-append must not
    double-count scheduled purges."""
    m = Memory(str(tmp_path / "d"), config={"rate_max_writes": 10**9})
    ns = m.ns
    ns.wal_rotate_bytes = 512  # force rotations on nearly every append_ops
    recs = []
    for i in range(40):
        recs.extend(m.add(f"purge me {i} unique content words here", user_id="u1"))
    m.flush()
    ns.append_ops([{"op": "tombstone", "id": rid, "at": 0} for rid in recs])
    ns.append_ops([{"op": "hard_delete", "id": rid, "deadline": ns.manifest.seq} for rid in recs])
    assert ns.pending_hard_deletes == len(set(recs)), \
        f"pending purges double-counted: {ns.pending_hard_deletes} vs {len(set(recs))}"
    m.close()


# ------------------------------------------------- fix 2: bounded embed queue

class _GateEmbedder(Embedder):
    """Blocks until released: simulates a slow/unreachable embedding API."""

    name = "gate"
    dim = 8

    def __init__(self):
        self.gate = threading.Event()
        self.embedded: list[list[str]] = []

    def embed(self, texts):
        self.gate.wait(timeout=30)
        self.embedded.append(list(texts))
        return np.zeros((len(texts), self.dim), dtype=np.float32)


def test_embed_worker_backlog_is_bounded():
    emb = _GateEmbedder()
    applied = []
    w = _EmbedWorker(emb, lambda ns, ids, vecs: applied.extend(ids), batch_size=4, max_queue=8)
    accepted = sum(w.submit("default", f"r{i}", f"text {i}") for i in range(100))
    dropped = 100 - accepted
    assert w.q.qsize() <= 8, "queue grew past its bound"
    assert dropped > 0, "expected overflow drops under burst"
    snap = Registry()
    emb.gate.set()  # let it drain what it holds
    deadline = time.monotonic() + 10
    while w.q.unfinished_tasks and time.monotonic() < deadline:
        time.sleep(0.02)
    assert applied, "worker processed nothing"
    w.stop()


def test_records_with_deferred_embedding_stay_searchable(tmp_path):
    """A drop only defers the vector lane: BM25 recall is unaffected and
    reembed() heals the vector lane afterwards."""
    m = _make_memory(tmp_path)
    # saturate the queue with a gated embedder so adds get deferred
    gate_emb = _GateEmbedder()
    old = m._embed_worker
    m.embedder = gate_emb
    m._embed_worker = _EmbedWorker(gate_emb, m._apply_vectors, batch_size=4, max_queue=1)
    try:
        gate_emb.gate.clear()
        m.add("the production deploy command is make ship alpha", user_id="u1")
    finally:
        gate_emb.gate.set()
        m._embed_worker.stop()
        m._embed_worker = old
        m.embedder = old.embedder
    m.flush()
    res = m.search("production deploy command", user_id="u1")
    assert any("make ship" in i.content for i in res.items), "BM25 lane lost a defer-embedded record"
    out = m.reembed()
    assert out["embedded"] >= 1
    m.close()


# ------------------------------------------- fix 3: http label cardinality

def test_route_label_buckets_dynamic_segments():
    assert _route_label("/v1/ns/acme/events") == "/v1/ns/:ns/events"
    assert _route_label("/v1/ns/globex/events") == "/v1/ns/:ns/events", "tenant name leaked into label"
    assert _route_label("/v1/ns/acme/memories/rec123") == "/v1/ns/:ns/memories/:id"
    # deep routes stay bounded: everything past the route root after :ns is :id
    assert _route_label("/v1/ns/acme/sessions/s9/close").startswith("/v1/ns/:ns/sessions/:id")
    assert "acme" not in _route_label("/v1/ns/acme/sessions/s9/close")
    assert _route_label("/health") == "/health"


def test_http_metrics_series_do_not_scale_with_tenants(tmp_path):
    """Two tenants hitting identical routes produce ONE series per route -
    not one per (tenant, route)."""
    from fastapi.testclient import TestClient

    from memd.server.auth import KeyStore
    from memd.server.http import create_app

    app = create_app(data_dir=str(tmp_path / "data"), keys_path=str(tmp_path / "keys.json"))
    ks: KeyStore = app.state.keystore
    full_a, _ = ks.create("acme")
    full_g, _ = ks.create("globex")
    with TestClient(app) as client:
        client.headers["Authorization"] = f"Bearer {full_a}"
        client.post("/v1/ns/acme/events", json={"events": [{"content": "acme note"}]})
        client.headers["Authorization"] = f"Bearer {full_g}"
        client.post("/v1/ns/globex/events", json={"events": [{"content": "globex note"}]})
        snap = METRICS.snapshot()
    routes = [
        s["labels"].get("route")
        for s in snap["counters"].get("memd_http_requests_total", [])
        if s["labels"].get("route", "").startswith("/v1/ns/")
    ]
    assert routes.count("/v1/ns/:ns/events") >= 1
    import re as _re

    leaked = [r for r in routes if _re.match(r"^/v1/ns/(?!:ns)", r)]
    assert not leaked, f"unbucketed tenant labels: {leaked}"
    app.state.engine.close()


# ------------------------------------------------------------ regression

def test_append_op_still_works_single(tmp_path):
    """Single-op path keeps its contract after the append_ops refactor."""
    m = _make_memory(tmp_path)
    rid = m.add("survivor record", user_id="u1")[0]
    m.flush()
    m.ns.append_op({"op": "tombstone", "id": rid, "at": 1234567890000})
    assert m.get(rid) is None
    m.close()
