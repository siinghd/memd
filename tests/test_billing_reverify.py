"""Hosted billing, re-verification round (findings A-F). Offline: Stripe is
tests/hosted_helpers.FakeStripe; webhooks are signed with a test secret."""
from __future__ import annotations

import threading
import time

import pytest

pytest.importorskip("stripe")

from fastapi.testclient import TestClient  # noqa: E402

from memd.engine.memory import Memory  # noqa: E402
from memd.hosted.store import AdminStore  # noqa: E402
from memd.metrics import METRICS  # noqa: E402
from memd.server.http import create_app  # noqa: E402
from tests.hosted_helpers import event  # noqa: E402
from tests.test_billing_hardening import World  # noqa: E402

DEV_PRICES = ("price_dev_monthly", "price_dev_extractions", "price_dev_reranked")


# ------------------------- A: a session close reserves only what it writes


def test_a_a_write_alongside_an_in_flight_close_succeeds_with_headroom(tmp_path):
    w = World(tmp_path, plans={"free": {"meters": {"memories_stored": {"limit": 100, "hard": True}}}})
    try:
        org = w.org()
        c = w.client(org, "one", scopes="memory")
        c.post("/v1/ns/one/events", json={"events": [
            {"role": "user", "content": "I live in Paris, France.", "session_id": "s", "user_id": "u"}]})
        ex = w.engine.extractor
        real = ex.extract
        started, go = threading.Event(), threading.Event()

        def slow(recs):
            started.set()
            go.wait(30)
            return real(recs)

        ex.extract = slow
        out = {}
        t = threading.Thread(target=lambda: out.setdefault("close", c.post("/v1/ns/one/sessions/s/close")))
        t.start()
        assert started.wait(10)
        held = sum(r["quantity"] for r in w.store.reservations() if r["meter"] == "memories_stored")
        write = c.post("/v1/ns/one/memories", json={"content": "an ordinary write"})
        go.set()
        t.join(30)
        assert held <= 1, held  # room for one, not the whole headroom
        assert write.status_code == 201, write.text
        assert out["close"].status_code == 200 and out["close"].json()["facts_written"] >= 1
        assert w.store.reservations() == []
    finally:
        w.close()


def test_a_facts_are_reserved_exactly_once_extracted_and_capped_by_headroom(tmp_path):
    w = World(tmp_path, plans={"free": {"meters": {"memories_stored": {"limit": 7, "hard": True}}}})
    try:
        org = w.org()
        c = w.client(org, "y", scopes="memory")
        facts = ["My name is Alice Johnson.", "I live in Paris, France.", "I work at Acme Corp as an engineer.",
                 "My favorite color is blue.", "I prefer tea over coffee."]
        c.post("/v1/ns/y/events", json={"events": [
            {"role": "user", "content": t, "session_id": "s1", "user_id": "u1"} for t in facts]})
        seen = []
        real_reserve = w.store.reserve

        def spy(*a, **kw):
            out = real_reserve(*a, **kw)
            seen.append((a[2], out[2]))
            return out

        w.store.reserve = spy
        r = c.post("/v1/ns/y/sessions/s1/close")
        assert r.status_code == 200
        body = r.json()
        assert body["facts_written"] + body["facts_capped"] <= body["facts_extracted"] + body["facts_capped"]
        assert w.engine.stats(namespace="y")["records"] <= 7
        # the fact reservation followed the extraction: exactly min(facts, headroom = 2)
        fact_grants = [g.get("memories_stored") for checks, g in seen
                       if any(ch[0] == "memories_stored" and ch[2] == 0 for ch in checks)]
        assert fact_grants and fact_grants[-1] == min(body["facts_extracted"] + body["facts_capped"], 2)
        assert w.store.reservations() == []
    finally:
        w.close()


# ------------------------------- B: every duplicate subscription is tracked


def _dup_world(tmp_path):
    w = World(tmp_path)
    org = w.org(stripe_customer_id="cus_d", stripe_subscription_id="sub_a", plan="dev", status="active")
    w.sub("sub_a", "cus_d", prices=DEV_PRICES)
    return w, org


def test_b_pushes_stay_held_while_any_duplicate_is_live(tmp_path):
    w, org = _dup_world(tmp_path)
    try:
        now = int(time.time())
        w.post(event("customer.subscription.created", w.sub("sub_b", "cus_d", prices=DEV_PRICES), created=now))
        w.post(event("customer.subscription.created", w.sub("sub_c", "cus_d", prices=DEV_PRICES), created=now + 1))
        assert sorted(w.store.duplicates(org)) == ["sub_b", "sub_c"]
        w.store.record_usage(org, "acme", {"reranked_searches": 20_000}, w.h.plans.get("dev"), ts=now)
        w.post(event("customer.subscription.deleted", w.sub("sub_c", "cus_d", status="canceled"), created=now + 2))
        assert w.store.duplicates(org) == ["sub_b"]
        assert w.h.billing.push_usage()["sent"] == 0 and w.fake.requests == []  # sub_b still live: held
        w.post(event("customer.subscription.deleted", w.sub("sub_b", "cus_d", status="canceled"), created=now + 3))
        assert w.store.duplicates(org) == []
        assert w.h.billing.push_usage()["sent"] == 1 and w.fake.total() == 10_000
        assert w.snap(org)["plan"] == "dev" and w.snap(org)["stripe_subscription_id"] == "sub_a"
    finally:
        w.close()


# ---------------- C: the surviving subscription is promoted, not dropped


def test_c_current_canceled_while_a_duplicate_is_live_promotes_it(tmp_path):
    w, org = _dup_world(tmp_path)
    try:
        now = int(time.time())
        w.post(event("customer.subscription.created", w.sub("sub_b", "cus_d", prices=DEV_PRICES), created=now))
        assert w.store.duplicates(org) == ["sub_b"]
        r = w.post(event("customer.subscription.deleted", w.sub("sub_a", "cus_d", status="canceled"), created=now + 1))
        assert r.status_code == 200
        o = w.store.get_org(org)
        assert o["stripe_subscription_id"] == "sub_b" and o["plan"] == "dev" and o["status"] == "active"
        assert w.store.duplicates(org) == []
        # the promoted subscription is the org's now: its invoices count
        w.store.update_org(org, status="past_due", grace_until=now + 3600)
        r = w.post(event("invoice.paid", {"id": "in_b", "object": "invoice", "customer": "cus_d",
                                          "subscription": "sub_b"}, created=now + 2))
        assert r.json()["handled"] is True and w.snap(org)["status"] == "active"
        c = w.client(org, "acme", scopes="billing")
        assert c.post("/v1/billing/checkout", json={"plan": "dev"}).json()["code"] == "already_subscribed"
    finally:
        w.close()


def test_c_a_survivor_stripe_reports_dead_is_not_promoted(tmp_path):
    w, org = _dup_world(tmp_path)
    try:
        now = int(time.time())
        w.post(event("customer.subscription.created", w.sub("sub_b", "cus_d", prices=DEV_PRICES), created=now))
        w.sub("sub_b", "cus_d", status="canceled")  # Stripe's current copy: already gone
        w.post(event("customer.subscription.deleted", w.sub("sub_a", "cus_d", status="canceled"), created=now + 1))
        o = w.store.get_org(org)
        assert o["plan"] == "free" and o["stripe_subscription_id"] is None
        assert w.store.duplicates(org) == []
    finally:
        w.close()


# --------------------------------------- D: namespace names, full match


@pytest.mark.parametrize("ns", ["alpha\n", "alpha\r\n", "\nalpha", "al\npha"])
def test_d_namespace_names_must_match_in_full(tmp_path, ns):
    store = AdminStore.for_data_root(str(tmp_path / "d"))
    org = store.create_org("acme")
    with pytest.raises(ValueError):
        store.create_key(org, ns)
    with pytest.raises(ValueError):
        store.import_key(org, ns, "abcdef12", "0" * 64)
    store.close()
    m = Memory(str(tmp_path / "m"))
    try:
        with pytest.raises(ValueError):
            m.add("x", namespace=ns)
    finally:
        m.close()


# --------------------- E: observability routes need the `memory` scope


def test_e_status_and_metrics_need_the_memory_scope(tmp_path):
    w = World(tmp_path)
    try:
        org = w.org()
        for scopes, expect in (("billing", 403), ("override", 403), ("billing,override", 403),
                               ("memory", 200), ("memory,billing", 200)):
            c = w.client(org, "acme", scopes=scopes)
            for path in ("/v1/status", "/metrics", "/v1/metrics/json"):
                assert c.get(path).status_code == expect, (scopes, path)
        assert w.client(org, "acme", scopes="billing").get("/v1/billing/usage").status_code == 200
    finally:
        w.close()


# ------------- F: rate-limit series carry the authorized namespace only


def test_f_rate_limited_series_never_take_the_namespace_from_the_path(tmp_path):
    import uuid

    app = create_app(data_dir=str(tmp_path / "d"), keys_path=str(tmp_path / "keys.json"),
                     admin_key="operator-secret-f")
    try:
        key, _ = app.state.keystore.create("alpha")
        c = TestClient(app)
        c.headers["Authorization"] = f"Bearer {key}"
        mark = "cardf" + uuid.uuid4().hex[:8]
        limited = 0
        for i in range(5000):  # the bucket refills as we go: stop at the first 429s
            if c.post(f"/v1/ns/{mark}{i}/search", json={"query": "q"}).status_code == 429:
                limited += 1
                if limited >= 5:
                    break
        assert limited, "the key's budget ran out while it was denied elsewhere"
        op = TestClient(app)
        op.headers["Authorization"] = "Bearer operator-secret-f"
        prom = op.get("/metrics").text
        assert mark not in prom, [ln for ln in prom.splitlines() if mark in ln][:3]
        limited = [ln for ln in prom.splitlines() if ln.startswith("memd_rate_limited_total") and 'ns="alpha"' in ln]
        assert limited, "the rejection is still counted - under the key's own namespace"
        assert mark not in str(METRICS.snapshot())
    finally:
        app.state.engine.close()


# ------------- C4: one unreadable duplicate never blocks the cancellation


def test_c4_a_duplicate_stripe_no_longer_has_is_dropped_and_the_cancellation_applies(tmp_path):
    w, org = _dup_world(tmp_path)
    try:
        now = int(time.time())
        w.post(event("customer.subscription.created", w.sub("sub_b", "cus_d", prices=DEV_PRICES), created=now))
        del w.fake.subscriptions["sub_b"]  # Stripe answers 404 resource_missing
        r = w.post(event("customer.subscription.deleted", w.sub("sub_a", "cus_d", status="canceled"), created=now + 1))
        assert r.status_code == 200, r.text
        o = w.store.get_org(org)
        assert o["plan"] == "free" and o["stripe_subscription_id"] is None
        assert w.store.duplicates(org) == [] and o["duplicate_subscription_id"] is None
    finally:
        w.close()


def test_c4_an_unreadable_duplicate_stays_listed_but_the_cancellation_applies(tmp_path):
    w, org = _dup_world(tmp_path)
    try:
        now = int(time.time())
        w.post(event("customer.subscription.created", w.sub("sub_b", "cus_d", prices=DEV_PRICES), created=now))
        w.fake.down = True  # a Stripe outage while re-reading the duplicate
        r = w.post(event("customer.subscription.deleted", w.sub("sub_a", "cus_d", status="canceled"), created=now + 1))
        w.fake.down = False
        assert r.status_code == 200, r.text
        o = w.store.get_org(org)
        assert o["plan"] == "free" and o["stripe_subscription_id"] is None  # the cancellation took effect
        assert w.store.duplicates(org) == ["sub_b"]  # unknown: still listed, pushes still held
        w.store.record_usage(org, "acme", {"reranked_searches": 20_000}, w.h.plans.get("dev"), ts=now)
        assert w.h.billing.push_usage()["sent"] == 0
        # its own next event settles it: it becomes the org's subscription
        r = w.post(event("customer.subscription.updated", w.sub("sub_b", "cus_d", prices=DEV_PRICES), created=now + 2))
        o = w.store.get_org(org)
        assert o["stripe_subscription_id"] == "sub_b" and o["plan"] == "dev" and w.store.duplicates(org) == []
    finally:
        w.close()


# --------- tombstones: a late event cannot re-list an ended subscription


def test_out_of_order_duplicate_events_never_relist_an_ended_subscription(tmp_path):
    w, org = _dup_world(tmp_path)
    try:
        now = int(time.time())
        live = dict(w.sub("sub_c", "cus_d", prices=DEV_PRICES))
        ended = dict(live, status="canceled")
        # deleted delivered before created
        w.post(event("customer.subscription.deleted", ended, created=now + 2))
        r = w.post(event("customer.subscription.created", live, created=now + 1))
        assert r.status_code == 200 and w.store.duplicates(org) == []
        # a live event stamped even later cannot resurrect it either (canceled is final)
        w.post(event("customer.subscription.updated", live, created=now + 9))
        assert w.store.duplicates(org) == []
        # an older update of a listed duplicate cannot undo a newer one
        d = dict(w.sub("sub_d", "cus_d", prices=DEV_PRICES))
        w.post(event("customer.subscription.created", d, created=now + 3))
        assert w.store.duplicates(org) == ["sub_d"]
        w.post(event("customer.subscription.updated", dict(d, status="incomplete_expired"), created=now + 5))
        assert w.store.duplicates(org) == []
        w.post(event("customer.subscription.updated", d, created=now + 4))
        assert w.store.duplicates(org) == []
        assert w.store.get_org(org)["duplicate_subscription_id"] is None
        assert w.snap(org)["plan"] == "dev" and w.snap(org)["stripe_subscription_id"] == "sub_a"
    finally:
        w.close()
