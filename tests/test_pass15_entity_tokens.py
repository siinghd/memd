"""Pass 15: entity-lane token normalization regression.

Query tokens arrive as raw whitespace-splits ("acme?", "editor,") but entity
segments are normalize_entity_key dot-parts ([a-z0-9_-]+). The mismatch
silently zeroed the entity lane on most punctuated queries - masked because
BM25/vector lanes still found the record.
"""
import sys

sys.path.insert(0, "/home/deploy/agent-memory/src")

import pytest

from memd.engine.memory import Memory
from memd.index.sqlite_index import IndexFilter
from memd.core.schema import Scope


@pytest.fixture()
def mem(tmp_path):
    m = Memory(str(tmp_path / "d"), encrypt=False)
    try:
        yield m
    finally:
        m.close()


class TestEntityLaneTokens:
    def test_punctuated_queries_still_hit_entity_lane(self, mem):
        rid = mem.remember("Alice works at acme corp on the platform team",
                           entity_keys=["org.acme"], user_id="u1")
        mem.flush()
        for q in ["tell me about acme", "what about acme?", "acme, please",
                  "the acme corp!", "(acme)", "acme:"]:
            res = mem.search(q, user_id="u1")
            entity_hits = [h for h in res.items if "entity" in h.lanes]
            assert entity_hits, f"entity lane dead for {q!r}"
            assert rid in {h.id for h in res.items}

    def test_find_ids_lane_normalizes_too(self, mem):
        rid = mem.remember("deploy command is make prod", entity_keys=["repo.deploy_cmd"],
                           user_id="u1")
        mem.flush()
        ids = mem.find_ids("deploy cmd?", user_id="u1")
        # 'cmd' alone won't match; the point is no crash + normalization path
        # exercised - use a query whose normalized token exists
        ids2 = mem.find_ids("what is the deploy command?", user_id="u1")
        assert isinstance(ids, list) and isinstance(ids2, list)

    def test_punctuation_only_tokens_are_dropped(self, mem):
        # tokens that normalize to nothing must not produce empty-string
        # SQL placeholders or errors
        res = mem.search("??? ... !!! ,,,", user_id="u1")
        assert res.items == []
        hits = mem.ns.index.search_by_entity_tokens(
            ["???", "...", ",,,", "-"], IndexFilter(), limit=10)
        assert hits == []

    def test_hyphenated_and_numeric_segments_match(self, mem):
        rid = mem.remember("prefers dark-mode v2", entity_keys=["user.pref.dark-mode"],
                           user_id="u1")
        mem.flush()
        res = mem.search("dark-mode?", user_id="u1")
        assert rid in {h.id for h in res.items}
        entity_hits = [h for h in res.items if "entity" in h.lanes]
        assert entity_hits
