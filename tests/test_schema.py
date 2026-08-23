import time

import pytest

from memd.core.schema import (
    Kind,
    MemoryRecord,
    Scope,
    Source,
    now_ms,
    records_from_jsonl,
    records_to_jsonl,
    ulid_new,
    ulid_ts_ms,
)


def test_ulid_sortable_and_decodable():
    a = ulid_new(1000)
    b = ulid_new(2000)
    assert a < b
    assert ulid_ts_ms(a) == 1000
    ids = {ulid_new() for _ in range(5000)}
    assert len(ids) == 5000


def test_scope_visibility_chain():
    org = Scope(org="acme")
    agent = Scope(org="acme", agent="coder")
    user = Scope(org="acme", agent="coder", user="u1")
    sess = Scope(org="acme", agent="coder", user="u1", session="s1")
    other_user = Scope(org="acme", agent="coder", user="u2")

    # query at session scope sees its own scope + all ancestors
    for anc in (org, agent, user, sess):
        assert sess.contains(anc), f"session should see {anc}"
    # query at user scope sees that user's sessions + ancestors (user profile view)
    assert user.contains(sess) and user.contains(user) and user.contains(agent) and user.contains(org)
    # cross-user isolation
    assert not other_user.contains(user)
    assert not user.contains(other_user)
    assert not sess.contains(other_user)
    # mismatched components hide records
    other_agent = Scope(org="acme", agent="other")
    assert not other_agent.contains(user)


def test_record_roundtrip_jsonl():
    r = MemoryRecord.create(
        namespace="ns1",
        kind=Kind.FACT,
        content="deploys with make ship",
        scope=Scope(org="acme", user="u1"),
        source=Source.AGENT,
        actor_id="agent-7",
        lineage=["01ABC"],
        entity_keys=["repo.deploy_cmd"],
        meta={"score": 0.9},
    )
    blob = records_to_jsonl([r])
    back = records_from_jsonl(blob)
    assert len(back) == 1
    r2 = back[0]
    assert r2.id == r.id and r2.content == r.content
    assert r2.provenance.source is Source.AGENT
    assert r2.entity_keys == ["repo.deploy_cmd"]
    assert r2.time.t_event == r.time.t_event


def test_trust_ordering():
    order = [Source.IMPORT, Source.WEB, Source.TOOL, Source.AGENT, Source.USER]
    ranks = [s.value for s in order]
    assert ranks == sorted(ranks)
    with pytest.raises(ValueError):
        Source.parse("not_a_source")


def test_tombstone_sets_invalidated_at():
    before = now_ms()
    r = MemoryRecord.create(namespace="n", kind=Kind.FACT, content="x")
    t = r.with_tombstone()
    assert t.deleted and t.time.invalidated_at >= before
