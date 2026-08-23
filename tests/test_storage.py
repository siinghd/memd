"""Unit tests for the storage engine: WAL durability, rotation, ops,
compaction, hard-delete deadlines, crypto-shred, recovery."""
import os

import numpy as np
import pytest

from memd.core.schema import Kind, MemoryRecord, Scope, Source, now_ms
from memd.storage.crypto import LocalKeyEnvelope
from memd.storage.engine import StorageEngine


@pytest.fixture()
def engine(tmp_path):
    return StorageEngine(str(tmp_path / "data"))


def _rec(ns="n1", content="hello", kind=Kind.RAW_EVENT, **kw):
    return MemoryRecord.create(namespace=ns, kind=kind, content=content, **kw)


def test_append_durable_and_visible(engine):
    ns = engine.namespace("n1")
    r = _rec(scope=Scope(user="u1"))
    ns.append([r])
    got = ns.index.query_records(IndexFilterNone(), limit=10)
    assert [g.id for g in got] == [r.id]


def IndexFilterNone():
    from memd.index.sqlite_index import IndexFilter

    return IndexFilter()


def test_rotation_creates_immutable_segment(engine):
    ns = engine.namespace("n1")
    recs = [_rec(content=f"msg {i}") for i in range(10)]
    ns.append(recs)
    name = ns.rotate("test")
    assert ns.store.exists(f"ns/n1/{name}")
    assert ns.manifest.wal_size == 0
    # data still queryable after rotation
    got = ns.index.query_records(IndexFilterNone(), limit=100)
    assert len(got) == 10
    # new appends go to fresh wal
    ns.append([_rec(content="after")])
    assert ns.manifest.wal_size > 0


def test_recovery_replays_wal(tmp_path):
    root = str(tmp_path / "data")
    e1 = StorageEngine(root)
    ns = e1.namespace("n1")
    r = _rec()
    ns.append([r])
    e1.close()  # simulate process death without rotate

    e2 = StorageEngine(root)
    ns2 = e2.namespace("n1")
    got = ns2.index.query_records(IndexFilterNone(), limit=10)
    assert [g.id for g in got] == [r.id]


def test_tombstone_and_supersede_ops(engine):
    ns = engine.namespace("n1")
    a = _rec(content="old fact", kind=Kind.FACT, entity_keys=["user.lang"])
    b = _rec(content="new fact", kind=Kind.FACT, entity_keys=["user.lang"])
    ns.append([a, b])
    ns.append_op({"op": "supersede", "old": a.id, "new": b.id, "at": now_ms()})
    cur = ns.index.query_records(IndexFilterNone(), limit=10)
    assert [c.id for c in cur] == [b.id]
    hist = ns.index.history(a.id)
    assert {h.id for h in hist} == {a.id, b.id}
    ns.append_op({"op": "tombstone", "id": b.id, "at": now_ms()})
    assert ns.index.query_records(IndexFilterNone(), limit=10) == []


def test_compaction_purges_tombstones_keeps_live(engine):
    ns = engine.namespace("n1")
    keep = _rec(content="keep me")
    dead = _rec(content="dead")
    ns.append([keep, dead])
    ns.rotate("pre-compact")  # both land in a segment, alive
    ns.append_op({"op": "tombstone", "id": dead.id, "at": now_ms()})
    rep = ns.compact(force=True)
    assert rep.records_purged >= 1 and rep.segments_out == 1
    got = ns.index.query_records(IndexFilterNone(), limit=10)
    assert [g.id for g in got] == [keep.id]
    # physical: dead record bytes gone from object store
    seg = ns.manifest.segments[0]["name"]
    blob = ns.store.get(f"ns/n1/{seg}").decode()
    assert "dead" not in blob and "keep me" in blob


def test_hard_delete_deadline_deferred_then_forced(engine):
    ns = engine.namespace("n1")
    victim = _rec(content="gdpr me")
    ns.append([victim])
    ns.append_op({"op": "hard_delete", "id": victim.id, "deadline": now_ms() + 3600_000})
    # not yet due: still physically present after normal compaction
    ns.compact()
    all_recs, _ = ns.load_all_records()
    assert any(r.id == victim.id for r in all_recs)
    # forced compaction (deadline enforcement) purges it
    rep = ns.compact(force=True)
    all_recs, _ = ns.load_all_records()
    assert not any(r.id == victim.id for r in all_recs)
    assert rep.hard_deleted_purged == 1


def test_crypto_shred_destroys_everything(tmp_path):
    root = str(tmp_path / "data")
    env = LocalKeyEnvelope(os.path.join(root, "keys"))
    e = StorageEngine(root, envelope=env)
    ns = e.namespace("secret")
    ns.append([_rec(ns="secret", content="classified")])
    key_before = env.data_key("secret")
    assert e.destroy_namespace("secret") is True
    assert e.list_namespaces() == []
    assert e.store.list("ns/secret") == []
    key_after = env.data_key("secret")  # recreates a *fresh* key
    assert key_before != key_after


def test_encryption_at_rest(tmp_path):
    root = str(tmp_path / "data")
    env = LocalKeyEnvelope(os.path.join(root, "keys"))
    e = StorageEngine(root, envelope=env)
    ns = e.namespace("enc")
    secret = "the launch codes are hunter2"
    ns.append([MemoryRecord.create(namespace="enc", kind=Kind.RAW_EVENT, content=secret)])
    blob = e.store.get("ns/enc/wal")
    assert secret.encode() not in blob
    # readable through the front door
    got = ns.index.query_records(IndexFilterNone(), limit=10)
    assert got[0].content == secret


def test_index_rebuild_from_segments(tmp_path):
    root = str(tmp_path / "data")
    e = StorageEngine(root)
    ns = e.namespace("n1")
    rs = [_rec(content=f"fact number {i}", kind=Kind.FACT) for i in range(5)]
    ns.append(rs)
    ns.rotate("rebuild-test")
    ns.index.wipe()
    assert ns.index.stats()["records"] == 0
    n = ns.rebuild_index()
    assert n == 5
    assert ns.index.stats()["records"] == 5


def test_invalid_namespace_rejected(engine):
    with pytest.raises(ValueError):
        engine.namespace("../evil")
    with pytest.raises(ValueError):
        engine.namespace("")
