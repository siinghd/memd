"""Pass 8 regression tests: derived-lane plumbing correctness.

Covers the audit findings fixed in this pass:
  - vectors must land in the namespace that owns the record (was: always
    misfiled into the facade default -> orphaned rows + dead vector lanes)
  - append() must work on ObjectStores without open_log (group-commit
    fallback path used to raise NotImplementedError)
  - vector lane must push scope/kind/time predicates BEFORE top-k
    truncation (was: filter-after-truncate recall loss)
  - reembed worklist must exclude deleted/superseded rows
  - kind is validated at the engine boundary (REST maps to 400)
"""
import sqlite3

import pytest
from fastapi.testclient import TestClient

from memd.core.schema import MemoryRecord, Scope, Source
from memd.engine.memory import Memory
from memd.index.sqlite_index import IndexFilter
from memd.server.auth import KeyStore
from memd.server.http import create_app
from memd.storage.objectstore import ObjectStore


def _vector_rows(path: str) -> tuple[int, int]:
    con = sqlite3.connect(path)
    try:
        total = con.execute("SELECT COUNT(*) FROM vectors").fetchone()[0]
        orphans = con.execute(
            "SELECT COUNT(*) FROM vectors v LEFT JOIN records r ON r.id=v.id WHERE r.id IS NULL"
        ).fetchone()[0]
        return total, orphans
    finally:
        con.close()


class TestCrossNamespaceVectors:
    def test_vectors_land_in_own_namespace(self, tmp_path):
        root = str(tmp_path / "data")
        # vector routing is under test: fuse the lane even for the hash
        # embedder (not fused by default since patch 3)
        mem = Memory(root, config={"fuse_vector": True})
        try:
            other_id = mem.add("quantum zebra probe unique tokens", session_id="s1",
                               user_id="u1", namespace="other")[0]
            def_id = mem.add("quantum zebra probe default ns twin", session_id="s2",
                             user_id="u1")[0]
            mem.flush()
            mem.engine.namespace("other").index.flush()  # commit before inspecting the db file
            cache = f"{root}/store/_cache"
            total_def, orphan_def = _vector_rows(f"{cache}/default.sqlite")
            total_oth, orphan_oth = _vector_rows(f"{cache}/other.sqlite")
            assert total_oth >= 1, "record written to 'other' must get its vector in 'other'"
            assert orphan_oth == 0 and orphan_def == 0, (
                f"orphaned vectors found: default={orphan_def} other={orphan_oth}")
            # the vector lane must actually fire for the non-default namespace
            res = mem.search("quantum zebra probe", user_id="u1", namespace="other")
            assert any("vector" in h.lanes for h in res.items), (
                "non-default namespace lost its vector lane")
            assert other_id in {h.id for h in res.items}
            # ...and the default namespace resolves its own record, not the foreign one
            res_d = mem.search("quantum zebra probe", user_id="u1", namespace="default")
            assert def_id in {h.id for h in res_d.items}
            assert all(h.namespace == "default" for h in res_d.items)
        finally:
            mem.close()

    def test_batch_and_fact_lanes_route_by_namespace(self, tmp_path):
        root = str(tmp_path / "data")
        mem = Memory(root)
        try:
            ids = mem.add_events([
                {"content": "kafka consumer lag alerting threshold is 5m", "user_id": "u9"},
                {"content": "grafana dashboards live under /observability", "user_id": "u9"},
            ], namespace="batchns")
            assert len(ids) == 2
            rid = mem.remember("the deploy window is tuesday 14:00 UTC",
                               entity_keys=["deploy.window"], user_id="u9",
                               namespace="batchns")
            mem.close_session("close-batch", user_id="u9", namespace="batchns")
            mem.flush()
            mem.engine.namespace("batchns").index.flush()
            cache = f"{root}/store/_cache"
            _, orphan = _vector_rows(f"{cache}/batchns.sqlite")
            assert orphan == 0
            res = mem.search("deploy window tuesday", user_id="u9", namespace="batchns")
            assert rid in {h.id for h in res.items}
        finally:
            mem.close()


class _NoOpenLogStore(ObjectStore):
    """Minimal store implementing ONLY the abstract surface (no open_log)."""

    def __init__(self, root: str):
        self.root = root
        import os

        os.makedirs(root, exist_ok=True)
        self.data: dict[str, bytes] = {}

    def _path(self, key):  # not used; dict-backed
        return key

    def put(self, key, data):
        self.data[key] = data

    def get(self, key):
        return self.data.get(key)

    def delete(self, key):
        self.data.pop(key, None)

    def exists(self, key):
        return key in self.data

    def list(self, prefix):
        return sorted(k for k in self.data if k.startswith(prefix))

    def append(self, key, data) -> int:
        cur = self.data.get(key, b"") + data
        self.data[key] = cur  # contract: durable on return
        return len(cur)

    def size(self, key):
        return len(self.data.get(key, b""))

    def truncate(self, key, size):
        self.data[key] = self.data.get(key, b"")[:size]

    def remove_prefix(self, prefix):
        n = 0
        for k in [k for k in self.data if k.startswith(prefix)]:
            del self.data[k]
            n += 1
        return n

    def copy(self, src, dst):
        if src in self.data:
            self.data[dst] = self.data[src]


class TestAppendWithoutOpenLog:
    def test_append_survives_and_replays(self, tmp_path):
        from memd.storage.engine import StorageEngine

        store = _NoOpenLogStore(str(tmp_path / "blobs"))
        eng = StorageEngine(str(tmp_path / "root"), store=store)
        ns = eng.namespace("plain")
        recs = [
            MemoryRecord.create(namespace="plain", kind="raw_event", content=f"event {i}",
                                scope=Scope(user="u1"), source=Source.USER)
            for i in range(5)
        ]
        ns.append(recs)  # used to raise NotImplementedError inside group commit
        st = ns.stats()
        assert st["records"] == 5
        eng.close()

        # fresh engine over the same blob set: replay must recover everything
        eng2 = StorageEngine(str(tmp_path / "root"), store=_NoOpenLogStore(str(tmp_path / "blobs")))
        # share underlying data with the first store
        eng2.store.data = dict(store.data)
        ns2 = eng2.namespace("plain")
        got = ns2.index.all_records()
        assert len(got) == 5
        eng2.close()


class TestVectorFilterExactness:
    def test_kind_filter_reaches_deeper_matches(self, tmp_path):
        """50 raw events crowd the top cosine ranks sharing most query tokens;
        one fact shares only a rare token. kinds=['fact'] must still find it."""
        root = str(tmp_path / "data")
        mem = Memory(root, encrypt=False)
        try:
            ns = mem.ns
            qtok = "zzqqx"
            for i in range(50):
                r = MemoryRecord.create(
                    namespace="default", kind="raw_event",
                    content=f"alpha beta gamma delta epsilon zeta eta theta iota {i}",
                    scope=Scope(user="u1"), source=Source.TOOL)
                ns.index.upsert(r)
            fact = MemoryRecord.create(
                namespace="default", kind="fact",
                content=f"the {qtok} protocol governs retention windows",
                scope=Scope(user="u1"), source=Source.AGENT,
                entity_keys=["retention.policy"])
            ns.index.upsert(fact)
            ns.index.flush()

            vecs = mem.embedder.embed([
                f"alpha beta gamma delta epsilon zeta eta theta iota query",
                f"{qtok} protocol retention"])
            for rid, v in ((fact.id, vecs[1]),):
                ns.index.set_vector(rid, v, mem.embedder.name)
            # give every junk row a strong vector too (they dominate cosine)
            for i in range(50):
                pass
            junk_ids = [r.id for r in ns.index.query_records(
                IndexFilter(kinds=("raw_event",)), limit=100)]
            for jid in junk_ids:
                ns.index.set_vector(jid, vecs[0], mem.embedder.name)

            filt = IndexFilter(scope=Scope(user="u1"), kinds=("fact",))
            hits = ns.index.search_vector(vecs[1], filt, limit=3)
            assert [h.record.id for h in hits] == [fact.id], (
                "kind-filtered vector search dropped the only valid match "
                "because top-cosine candidates were all raw_events")
        finally:
            mem.close()


class TestReembedWorklist:
    def test_deleted_and_superseded_rows_excluded(self, tmp_path):
        root = str(tmp_path / "data")
        mem = Memory(root, config={"rate_max_writes": 10**9})
        try:
            a = mem.add("keep me embedded forever", user_id="u1")[0]
            b = mem.add("delete me before reembed", user_id="u1")[0]
            c = mem.remember("old value of policy X", entity_keys=["policy.x"], user_id="u1")
            d = mem.remember("new value of policy X", entity_keys=["policy.x"], user_id="u1")
            mem.flush()
            mem.delete(b)  # tombstone b
            # c was superseded by d at remember() time (same entity key).
            # Simulate a stale/lost vector lane (model-switch / restore path):
            mem.ns.index._con.execute("DELETE FROM vectors")
            mem.ns.index._con.commit()
            mem.ns.index.invalidate_vec_cache()

            stale = {r.id for r in mem.ns.index.records_missing_embedding(mem.embedder.name)}
            assert stale == {a, d}, (
                f"worklist must exclude deleted ({b}) and superseded ({c}); got {sorted(stale)}")

            rep = mem.reembed()
            assert rep["embedded"] == 2
            vec_ids = {r[0] for r in mem.ns.index._con.execute("SELECT id FROM vectors")}
            assert vec_ids == {a, d}
        finally:
            mem.close()


class TestKindValidation:
    def test_engine_rejects_unknown_kind(self, tmp_path):
        mem = Memory(str(tmp_path / "d"))
        try:
            with pytest.raises(ValueError):
                mem.add("x", kind="not_a_kind")
            with pytest.raises(ValueError):
                mem.remember("x", kind="weaponized")
            with pytest.raises(ValueError):
                mem.add_events([{"content": "x", "kind": "bogus"}])
        finally:
            mem.close()

    def test_rest_maps_bad_kind_to_400(self, tmp_path):
        app = create_app(data_dir=str(tmp_path / "data"), keys_path=str(tmp_path / "keys.json"))
        ks: KeyStore = app.state.keystore
        full, _kid = ks.create("acme", name="t")
        t = TestClient(app)
        t.headers["Authorization"] = f"Bearer {full}"
        try:
            r = t.post("/v1/ns/acme/events",
                       json={"events": [{"content": "hi", "kind": "nope"}]})
            assert r.status_code == 400
            assert "kind" in r.json()["detail"]
        finally:
            app.state.engine.close()
