"""Session packing on long sessions: finding a turn's neighbours costs two
index seeks, whatever the session's length. Reading every turn of each
session a hit fell in made one 50,000-turn session take ~316 ms per search
(the flat layout: ~20 ms)."""
import random
import statistics
import time
import tracemalloc

import pytest

from memd.engine.memory import Memory
from memd.index.sqlite_index import IndexFilter

T0 = 1_684_540_800_000
WORDS = [f"w{i}" for i in range(5000)]


def _build(path, n_sessions, turns):
    rng = random.Random(7)
    m = Memory(path, encrypt=False, config={"embedder": "hash", "rate_max_writes": 10 ** 9})
    batch = []
    for s in range(n_sessions):
        for j in range(turns):
            batch.append({"content": " ".join(rng.choice(WORDS) for _ in range(rng.randint(6, 24))),
                          "role": "user" if j % 2 == 0 else "assistant", "user_id": "u1",
                          "session_id": f"s{s}", "t_event": T0 + s * 3_600_000 + j * 1000})
            if len(batch) >= 2000:
                m.add_events(batch)
                batch = []
    if batch:
        m.add_events(batch)
    m.flush()
    return m


@pytest.fixture(scope="module", params=[(1, 50_000), (10, 5_000)], ids=["1x50K", "10x5K"])
def big(request, tmp_path_factory, ):
    n, t = request.param
    m = _build(str(tmp_path_factory.mktemp(f"s{n}x{t}") / "d"), n, t)
    yield m
    m.close()


def _p50(m, packing, queries):
    ts = []
    for q in queries:
        m._qcache.clear()
        t0 = time.perf_counter()
        m.search(q, user_id="u1", packing=packing)
        ts.append((time.perf_counter() - t0) * 1000)
    return statistics.median(ts)


def test_neighbour_lookups_are_index_seeks(big):
    ix = big.ns.index
    args: list = []
    filt = ix._filter_where(IndexFilter(), args)
    with ix._read() as c:
        for cmp_, order in (("<", "DESC"), (">", "ASC")):
            plan = " ".join(str(r[-1]) for r in c.execute(
                "EXPLAIN QUERY PLAN SELECT id, rowid FROM records INDEXED BY ix_rec_session_t "
                f"WHERE scope_session = ? AND kind = 'raw_event' AND t_event {cmp_}= ? "
                f"AND (t_event {cmp_} ? OR rowid {cmp_} ?) AND {filt} "
                f"ORDER BY t_event {order}, rowid {order} LIMIT 1", ["s0", T0, T0, 1, *args]))
            assert "ix_rec_session_t" in plan and "TEMP B-TREE" not in plan, plan


def test_session_packing_cost_does_not_grow_with_the_session(big):
    rng = random.Random(3)
    queries = [" ".join(rng.choice(WORDS) for _ in range(4)) for _ in range(25)]
    for q in queries[:5]:  # warm-up
        big.search(q, user_id="u1")
    sessions, flat = _p50(big, "sessions", queries[5:]), _p50(big, "flat", queries[5:])
    # measured ~19-22 ms vs ~8 ms (a loaded 8-core host); reading whole sessions: 316 ms vs 20 ms
    assert sessions < 4 * flat + 15, (sessions, flat)
    tracemalloc.start()
    try:
        big._qcache.clear()
        big.search(queries[0], user_id="u1")
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    assert peak < 8 * 2 ** 20, peak  # ~0.5 MB; the whole-session read held every turn id
