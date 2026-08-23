"""Pass 16 regression: the metrics module must actually measure the tail.

The defect this locks out: latency call sites recorded MILLISECONDS while
DEFAULT_BUCKETS was a second-scale ladder topping out at 10.0. Every
observation overflowed, no bucket was ever incremented, and _quantile's
fall-through returned the MEAN - so p50 == p95 == p99 == avg for every
latency family in the system. bench/slo_bench.py computed its own
percentiles, which is why the SLO gates stayed green over a blind registry.
"""
import random

import pytest

from memd.engine.memory import Memory
from memd.metrics import CORE_HISTOGRAMS, DEFAULT_BUCKETS, LATENCY_MS_BUCKETS, Registry


def _quantiles(vals):
    sv = sorted(vals)
    return (sv[int(0.50 * (len(sv) - 1))],
            sv[int(0.95 * (len(sv) - 1))],
            sv[int(0.99 * (len(sv) - 1))])


def test_millisecond_latencies_actually_land_in_buckets():
    """The original bug: 1000 observations -> 0 bucket increments."""
    r = Registry()
    random.seed(7)
    vals = [random.uniform(12.0, 20.0) for _ in range(950)]
    vals += [random.uniform(40.0, 60.0) for _ in range(50)]
    for v in vals:
        r.observe("memd_search_latency_ms", v, help="ms")
    h = r.snapshot()["histograms"]["memd_search_latency_ms"][0]
    assert sum(h["buckets"].values()) == 1000, "every ms observation must be bucketed"
    assert h["overflow"] == 0


def test_quantiles_are_not_all_the_mean():
    """p50/p95/p99 must be distinguishable on a skewed distribution."""
    r = Registry()
    random.seed(7)
    vals = [random.uniform(12.0, 20.0) for _ in range(950)]
    vals += [random.uniform(40.0, 60.0) for _ in range(50)]
    for v in vals:
        r.observe("memd_search_latency_ms", v, help="ms")
    h = r.snapshot()["histograms"]["memd_search_latency_ms"][0]
    t50, t95, t99 = _quantiles(vals)
    assert h["p50"] < h["p95"] < h["p99"], (h["p50"], h["p95"], h["p99"])
    assert h["p50"] != pytest.approx(h["avg"], rel=0.01) or h["p99"] != pytest.approx(h["avg"], rel=0.01)
    # near the SLO-graded band the estimate must be tight
    assert abs(h["p50"] - t50) / t50 < 0.10
    assert abs(h["p95"] - t95) / t95 < 0.10
    # the tail may be coarse, but it must be biased HIGH - an SLO alarm may
    # never be silenced by bucket granularity
    assert h["p99"] >= t99 * 0.95


def test_overflow_band_never_reports_the_mean():
    """Values past the top bound must report >= the top bound, not the average."""
    r = Registry()
    for _ in range(90):
        r.observe("memd_slow_ms", 1.0, help="ms")
    for _ in range(10):
        r.observe("memd_slow_ms", 500_000.0, help="ms")  # 500s: past the 60s ladder
    h = r.snapshot()["histograms"]["memd_slow_ms"][0]
    assert h["overflow"] == 10
    assert h["p99"] >= LATENCY_MS_BUCKETS[-1], "tail must not be averaged away"


def test_core_latency_families_obey_the_ms_unit_contract():
    """No duration family may be named _seconds while recording milliseconds."""
    for name in CORE_HISTOGRAMS:
        assert not name.endswith("_seconds"), (
            f"{name}: memd's unit contract is milliseconds (suffix _ms)")


def test_bucket_ladder_covers_every_slo_boundary():
    top = LATENCY_MS_BUCKETS[-1]
    for slo_ms in (10, 20, 100, 150, 400, 1500, 60_000):  # D2 SLO table, ms
        assert slo_ms <= top, f"ladder cannot grade an SLO at {slo_ms}ms"
    assert DEFAULT_BUCKETS is LATENCY_MS_BUCKETS


def test_end_to_end_search_records_a_real_histogram(tmp_path):
    m = Memory(str(tmp_path / "d"))
    for i in range(40):
        m.add(f"deployment note number {i} about the staging cluster", user_id="u1")
    for i in range(25):
        m.search(f"staging cluster note {i}", user_id="u1")
    snap = m.metrics()["histograms"] if hasattr(m, "metrics") else None
    from memd.metrics import METRICS
    snap = METRICS.snapshot()["histograms"]
    assert "memd_write_ack_ms" in snap and "memd_write_ack_seconds" not in snap
    lat = [h for h in snap["memd_search_latency_ms"] if h["count"] > 0]
    assert lat, "search latency histogram must have observations"
    assert any(sum(h["buckets"].values()) > 0 for h in lat), (
        "real search latencies must land in buckets, not the overflow band")
    # per-stage attribution (plan / fuse / pack) must exist, not just lanes
    stages = {h["labels"].get("stage") for h in snap.get("memd_search_stage_ms", [])}
    assert {"plan", "fuse", "pack"} <= stages, stages
    m.close()
