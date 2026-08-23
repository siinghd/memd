"""Pass 16: the time lane must actually exist.

The planner has always emitted `time` weights (1.4x for temporal queries)
and the architecture diagram promises a time/entity btree fan-out - but no
lane produced "time" hits, so temporal queries had no recency-proximate
candidates. This pins the lane's existence, its contribution, and its
ordering effect.
"""
import sys

sys.path.insert(0, "/home/deploy/agent-memory/src")

from memd.engine.memory import Memory


def _seed(mem):
    # OLD doc: lexically dominant (every query token, repeated) but stale
    mem.add("quarterly deployment retrospective notes about deployment cadence "
            "and deployment tooling last year deployment",
            user_id="u1", session_id="old-s", t_event=_ms(days_ago=400))
    # FRESH doc: weak lexical overlap, only the distinctive token
    rid_fresh = mem.add("zebra migration finished today",
                        user_id="u1", session_id="new-s", t_event=_ms(days_ago=1))[0]
    mem.flush()
    return rid_fresh


def _ms(days_ago: float) -> int:
    import time as _t

    return int((_t.time() - days_ago * 86_400) * 1000)


class TestTimeLane:
    def test_temporal_query_produces_time_lane_hits(self, tmp_path):
        mem = Memory(str(tmp_path / "d"), encrypt=False)
        try:
            fresh = _seed(mem)
            res = mem.search("when did the zebra migration happen recently?",
                             user_id="u1")
            lanes_seen = {tuple(sorted(i.lanes)) for i in res.items}
            assert any("time" in l for l in lanes_seen), (
                f"time lane absent from fused items: {lanes_seen}")
            ids = [i.id for i in res.items]
            assert fresh in ids
        finally:
            mem.close()

    def test_time_lane_rescues_fresh_weak_match(self, tmp_path):
        """Fresh doc has almost no lexical evidence; only the time lane puts
        it into fusion where recency-aware packing can keep it."""
        mem = Memory(str(tmp_path / "d"), encrypt=False)
        try:
            fresh = _seed(mem)
            res = mem.search("recent zebra status?", user_id="u1")
            hit = next((i for i in res.items if i.id == fresh), None)
            assert hit is not None, "fresh weak-match record missing entirely"
            assert "time" in hit.lanes
        finally:
            mem.close()

    def test_non_matching_query_still_serves(self, tmp_path):
        mem = Memory(str(tmp_path / "d"), encrypt=False)
        try:
            _seed(mem)
            res = mem.search("completely unrelated xylophone query", user_id="u1")
            assert isinstance(res.packed_context, str)
        finally:
            mem.close()
