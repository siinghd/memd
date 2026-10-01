"""Pass 13: search-cache byte bounding.

The result cache previously capped ENTRIES only. A caller may legally
request budget_tokens up to 128K (~0.5MB packed context per entry), so 256
entries could pin >100MB of RAM from one authenticated client. Now:
  - oversized contexts are never cached
  - total cached bytes are capped with LRU eviction
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from memd.engine.memory import _SearchCache, PackedContext, SearchHit, SearchResult


def _result(context_chars: int, items: int = 1) -> SearchResult:
    hits = [SearchHit(id=f"i{k}", content="c" * 100, kind="fact", source="agent",
                      actor_id=None, t_event=0, valid=True, score=1.0,
                      lanes=["bm25"], entity_keys=[], namespace="default")
            for k in range(items)]
    return SearchResult(packed_context="x" * context_chars, items=hits,
                        tokens_used=context_chars // 4, budget=128_000,
                        truncated=False, query_class="factual", latency_ms=1.0)


class TestSearchCacheByteBound:
    def test_oversized_entries_never_cached(self):
        c = _SearchCache()
        big = _result(300_000)  # > MAX_ENTRY_BYTES (262_144)
        c.put(("k",), big)
        assert c.get(("k",)) is None

    def test_total_bytes_cap_evicts_lru(self):
        c = _SearchCache()
        # ~200 entries x ~180KB would be >36MB without the cap; cap is 32MB
        for i in range(200):
            c.put((f"k{i}",), _result(180_000))
        total = sum(len(v.packed_context) for v in c._map.values())
        assert total <= _SearchCache.MAX_TOTAL_BYTES + 180_000, (
            f"cache pinned {total/1e6:.1f}MB")
        assert len(c._map) < 200

    def test_lru_order_preserved_under_byte_pressure(self):
        c = _SearchCache(capacity=10_000)
        c.put(("old",), _result(150_000))
        for i in range(300):  # push past 32MB -> 'old' must be evicted first
            c.put((f"n{i}",), _result(150_000))
        assert c.get(("old",)) is None

    def test_small_results_still_cache_and_hit(self):
        c = _SearchCache()
        small = _result(2_000)
        c.put(("k",), small)
        assert c.get(("k",)) is small
