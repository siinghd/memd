"""Metrics registry unit tests: O(1) observations, exports, cardinality."""
import json
import threading

from memd.metrics import METRICS, Registry


def test_counter_gauge_histogram_roundtrip():
    r = Registry()
    r.inc("memd_test_writes_total", 3, ns="a", help="writes")
    r.inc("memd_test_writes_total", 2, ns="a")
    r.set_gauge("memd_queue_depth", 7.0, help="depth")
    for v in (2.0, 20.0, 300.0):  # MILLISECONDS - the memd unit contract
        r.observe("memd_write_ack_ms", v, help="ack", ns="a")
    snap = r.snapshot()
    assert sum(x["value"] for x in snap["counters"]["memd_test_writes_total"]) == 5
    h = snap["histograms"]["memd_write_ack_ms"][0]
    assert h["count"] == 3
    assert abs(h["sum"] - 322.0) < 1e-6
    # each observation lands in its first bucket >= value
    assert h["buckets"]["2.5"] == 1 and h["buckets"]["20"] == 1 and h["buckets"]["500"] == 1
    assert h["overflow"] == 0


def test_prometheus_render():
    r = Registry()
    r.inc("memd_x_total", 4, ns="b", help="x counter")
    r.set_gauge("memd_y", 1.5, help="y gauge")
    r.observe("memd_z_ms", 4.0, help="z hist")
    text = r.render_prometheus()
    assert "# TYPE memd_x_total counter" in text
    assert 'memd_x_total{ns="b"} 4' in text
    assert "# TYPE memd_z_ms histogram" in text
    assert 'memd_z_ms_bucket{le="5"} 1' in text
    assert 'memd_z_ms_bucket{le="+Inf"} 1' in text
    assert "memd_z_ms_sum" in text and "memd_z_ms_count 1" in text


def test_thread_safety():
    r = Registry()
    def worker():
        for _ in range(2000):
            r.inc("memd_t_total")
            r.observe("memd_t_ms", 1.0)
    ts = [threading.Thread(target=worker) for _ in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    snap = r.snapshot()
    assert snap["counters"]["memd_t_total"][0]["value"] == 16000
    assert snap["histograms"]["memd_t_ms"][0]["count"] == 16000


def test_cardinality_guard_evicts_oldest():
    r = Registry(max_series=50)
    for i in range(100):
        r.inc("memd_card_total", ns=f"ns{i}")
    snap = r.snapshot()
    assert len(snap["counters"]["memd_card_total"]) <= 50


def test_global_metrics_usable_and_json_serializable():
    METRICS.inc("memd_smoke_total")
    METRICS.observe("memd_smoke_ms", 10.0)
    blob = json.dumps(METRICS.snapshot())  # must be JSON-safe for dumpers/harness
    assert "memd_smoke_total" in blob


def test_timer_context():
    r = Registry()
    with r.timer("memd_timer_ms", help="t"):
        pass
    snap = r.snapshot()
    assert snap["histograms"]["memd_timer_ms"][0]["count"] == 1
