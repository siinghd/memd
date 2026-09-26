"""Hosted billing against Stripe - fully offline.

The webhook tests sign payloads with a test secret exactly as Stripe does;
the meter-push tests talk HTTP to an in-process fake with Stripe's
idempotency semantics (tests/hosted_helpers.FakeStripe); the end-to-end test
runs against stripe/stripe-mock in docker (skipped when docker is not
available, or with MEMD_TEST_STRIPE_MOCK=off)."""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time

import pytest

pytest.importorskip("stripe")

from fastapi.testclient import TestClient  # noqa: E402

from memd.hosted.billing import Billing, BillingConfig  # noqa: E402
from memd.hosted.plans import Plans  # noqa: E402
from memd.hosted.store import AdminStore  # noqa: E402
from memd.metrics import METRICS  # noqa: E402
from memd.query.rerank import RerankStage  # noqa: E402
from memd.server.http import create_app  # noqa: E402
from tests.hosted_helpers import WEBHOOK_SECRET, FakeStripe, StripeMock, event, sign  # noqa: E402

SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")

PRICES = {
    "MEMD_STRIPE_PRICE_DEV_MONTHLY": "price_dev_monthly",
    "MEMD_STRIPE_PRICE_DEV_YEARLY": "price_dev_yearly",
    "MEMD_STRIPE_PRICE_DEV_EXTRACTIONS_OUR_KEY": "price_dev_extractions",
    "MEMD_STRIPE_PRICE_DEV_RERANKED_SEARCHES": "price_dev_reranked",
    "MEMD_STRIPE_PRICE_SCALE_STORED_GB": "price_scale_gb",
    "MEMD_STRIPE_PRICE_SCALE_SEARCHES": "price_scale_searches",
    "MEMD_STRIPE_PRICE_SCALE_WRITES": "price_scale_writes",
    "MEMD_STRIPE_PRICE_SCALE_EXTRACTIONS_OUR_KEY": "price_scale_extractions",
    "MEMD_STRIPE_PRICE_SCALE_RERANKED_SEARCHES": "price_scale_reranked",
}
SMALL = {
    "free": {"meters": {"searches": {"limit": 2, "hard": True}}},
    "dev": {"meters": {"reranked_searches": {"limit": 1, "hard": False}}},
}


def _env(api_base: str = "http://127.0.0.1:9", **extra) -> dict:
    return {"MEMD_STRIPE_SECRET_KEY": "sk_test_memdOffline123", "MEMD_STRIPE_WEBHOOK_SECRET": WEBHOOK_SECRET,
            "MEMD_STRIPE_API_BASE": api_base, **PRICES, **extra}


class _Scorer:
    name = "fake"
    model = "fake-1"
    calibrated = False
    timeout_s = 1.0

    def scores(self, query, cands):
        return [1.0 / (i + 1) for i in range(len(cands))]


def _app(tmp_path, env: dict, plans=None):
    app = create_app(data_dir=str(tmp_path / "data"), hosted=True, plans=Plans(plans or SMALL),
                     billing_config=BillingConfig.from_env(env))
    return app, app.state.hosted


def _post_webhook(client, payload: str, sig: str | None):
    headers = {"Content-Type": "application/json"}
    if sig is not None:
        headers["Stripe-Signature"] = sig
    return client.post("/v1/billing/webhook", content=payload, headers=headers)


@pytest.fixture()
def billed(tmp_path):
    app, h = _app(tmp_path, _env())
    org = h.store.create_org("acme")
    h.store.update_org(org, stripe_customer_id="cus_acme")
    yield app, h, org, TestClient(app)
    app.state.engine.close()
    h.store.close()


def _sub(status="active", prices=("price_dev_monthly", "price_dev_extractions", "price_dev_reranked"),
         customer="cus_acme", period_end=None, sid="sub_acme"):
    return {"id": sid, "object": "subscription", "customer": customer, "status": status,
            "items": {"object": "list", "data": [
                {"id": f"si_{i}", "price": {"id": p}, "current_period_end": period_end or int(time.time()) + 30 * 86400}
                for i, p in enumerate(prices)]}}


# ------------------------------------------------------ signature verification


def test_webhook_signature_valid_tampered_and_wrong_secret(billed):
    app, h, org, c = billed
    ev = event("invoice.payment_succeeded", {"id": "in_1", "object": "invoice", "customer": "cus_acme"})
    payload = json.dumps(ev)
    r = _post_webhook(c, payload, sign(payload))
    assert r.status_code == 200 and r.json()["received"] is True and r.json()["handled"] is True
    # one flipped byte in the body, same signature
    tampered = payload.replace('"in_1"', '"in_2"')
    r = _post_webhook(c, tampered, sign(payload))
    assert r.status_code == 400 and r.json()["code"] == "invalid_signature"
    # a signature by another secret
    r = _post_webhook(c, payload, sign(payload, secret="whsec_attacker"))
    assert r.status_code == 400 and r.json()["code"] == "invalid_signature"
    # no header at all / garbage header
    assert _post_webhook(c, payload, None).status_code == 400
    assert _post_webhook(c, payload, "t=1,v1=deadbeef").status_code == 400


def test_webhook_replay_old_timestamp_is_rejected(billed):
    app, h, org, c = billed
    ev = event("invoice.payment_failed", {"id": "in_old", "object": "invoice", "customer": "cus_acme"})
    payload = json.dumps(ev)
    old = int(time.time()) - 3600  # a delivery captured an hour ago, correctly signed
    r = _post_webhook(c, payload, sign(payload, t=old))
    assert r.status_code == 400 and r.json()["code"] == "invalid_signature"
    assert h.store.processed_event(ev["id"]) is None
    assert h.store.get_org(org)["status"] == "active"


def test_webhook_replay_within_tolerance_is_a_no_op(billed):
    app, h, org, c = billed
    h.store.update_org(org, plan="dev")
    ev = event("invoice.payment_failed", {"id": "in_f", "object": "invoice", "customer": "cus_acme"})
    payload = json.dumps(ev)
    sig = sign(payload)
    first = _post_webhook(c, payload, sig).json()
    grace = h.store.get_org(org)["grace_until"]
    second = _post_webhook(c, payload, sig).json()  # the identical request, replayed
    assert first["handled"] is True and second.get("duplicate") is True
    assert h.store.get_org(org)["grace_until"] == grace
    assert len(h.store.log_entries(org, "payment_failed")) == 1


# ------------------------------------------------------------ idempotency


def test_same_event_twice_has_one_effect(billed):
    app, h, org, c = billed
    ev = event("customer.subscription.updated", _sub(), eid="evt_sub_once")
    payload = json.dumps(ev)
    for _ in range(3):
        assert _post_webhook(c, payload, sign(payload)).status_code == 200
    o = h.store.get_org(org)
    assert o["plan"] == "dev" and o["status"] == "active" and o["stripe_subscription_id"] == "sub_acme"
    assert len(h.store.log_entries(org, "updated_subscription")) == 1
    with h.store.txn() as con:
        assert con.execute("SELECT COUNT(*) FROM processed_events WHERE id = 'evt_sub_once'").fetchone()[0] == 1


def test_unknown_customer_asks_for_retry_and_is_not_claimed(billed):
    app, h, org, c = billed
    ev = event("customer.subscription.created", _sub(customer="cus_later"), eid="evt_early")
    payload = json.dumps(ev)
    r = _post_webhook(c, payload, sign(payload))
    assert r.status_code == 500 and r.json()["code"] == "retry"
    assert h.store.processed_event("evt_early") is None  # Stripe's retry will be processed
    other = h.store.create_org("later")
    h.store.update_org(other, stripe_customer_id="cus_later")
    r = _post_webhook(c, payload, sign(payload))
    assert r.status_code == 200 and r.json()["handled"] is True
    assert h.store.get_org(other)["plan"] == "dev"


def test_out_of_order_subscription_events_cannot_roll_back(billed):
    app, h, org, c = billed
    now = int(time.time())
    newer = event("customer.subscription.updated", _sub(status="active"), created=now)
    older = event("customer.subscription.updated", _sub(status="canceled"), created=now - 60)
    for ev in (newer, older):
        p = json.dumps(ev)
        assert _post_webhook(c, p, sign(p)).status_code == 200
    assert h.store.get_org(org)["plan"] == "dev"
    gone = event("customer.subscription.deleted", _sub(status="canceled"), created=now + 60)
    p = json.dumps(gone)
    assert _post_webhook(c, p, sign(p)).json()["plan"] == "free"
    o = h.store.get_org(org)
    assert o["plan"] == "free" and o["status"] == "canceled" and o["stripe_subscription_id"] is None


def test_payment_failed_starts_grace_then_read_only_then_recovery(billed):
    app, h, org, c = billed
    key, _ = h.keystore.create("acme", org_id=org, scopes="memory")
    data = TestClient(app)
    data.headers["Authorization"] = f"Bearer {key}"
    t0 = int(time.time())
    for etype, obj in (("customer.subscription.created", _sub()),
                       ("invoice.payment_failed", {"id": "in_1", "object": "invoice", "customer": "cus_acme"})):
        p = json.dumps(event(etype, obj, created=t0))
        assert _post_webhook(c, p, sign(p)).status_code == 200
    o = h.store.get_org(org)
    assert o["status"] == "past_due" and o["grace_until"] == t0 + 7 * 86400
    # a second failure (Stripe's retry of the invoice) does not extend grace
    p = json.dumps(event("invoice.payment_failed", {"id": "in_1", "object": "invoice", "customer": "cus_acme"},
                         created=t0 + 86400))
    _post_webhook(c, p, sign(p))
    assert h.store.get_org(org)["grace_until"] == t0 + 7 * 86400
    assert data.post("/v1/ns/acme/memories", json={"content": "in grace"}).status_code == 201
    h.metering.clock = lambda: t0 + 7 * 86400 + 1
    assert data.post("/v1/ns/acme/memories", json={"content": "blocked"}).status_code == 402
    assert data.post("/v1/ns/acme/search", json={"query": "grace"}).status_code == 200
    p = json.dumps(event("invoice.payment_succeeded", {"id": "in_1", "object": "invoice", "customer": "cus_acme"}))
    assert _post_webhook(c, p, sign(p)).status_code == 200
    assert h.store.get_org(org)["grace_until"] is None
    assert data.post("/v1/ns/acme/memories", json={"content": "paid again"}).status_code == 201


def test_checkout_session_cannot_repoint_another_customer(billed):
    app, h, org, c = billed
    ev = event("checkout.session.completed", {
        "id": "cs_x", "object": "checkout.session", "customer": "cus_attacker", "client_reference_id": org,
        "payment_status": "paid", "subscription": _sub(customer="cus_attacker"), "metadata": {"plan": "dev"}})
    p = json.dumps(ev)
    r = _post_webhook(c, p, sign(p))
    assert r.status_code == 200 and r.json()["reason"] == "customer_mismatch"
    o = h.store.get_org(org)
    assert o["stripe_customer_id"] == "cus_acme" and o["plan"] == "free"


def test_unpaid_checkout_does_not_upgrade(billed):
    app, h, org, c = billed
    ev = event("checkout.session.completed", {
        "id": "cs_u", "object": "checkout.session", "customer": "cus_acme", "client_reference_id": org,
        "payment_status": "unpaid", "subscription": _sub()})
    p = json.dumps(ev)
    assert _post_webhook(c, p, sign(p)).json()["reason"] == "payment_incomplete"
    o = h.store.get_org(org)
    assert o["plan"] == "free" and o["status"] == "incomplete"


# --------------------------------------------------------------- meter push


def _ledger(tmp_path, n_events=12):
    store = AdminStore.for_data_root(str(tmp_path / "data"))
    plans = Plans()
    org = store.create_org("acme", plan="scale")
    store.update_org(org, stripe_customer_id="cus_acme")
    free = store.create_org("freeloader")  # no customer: nothing to push
    base = time.time() - 7200
    for i in range(n_events):
        store.record_usage(org, "acme", {"searches": 1, "writes": 2}, plans.get("scale"), ts=base + i * 600)
        store.record_usage(free, "free-ns", {"searches": 1}, plans.get("free"), ts=base + i * 600)
    return store, plans, org


def test_meter_push_batches_with_idempotency_keys_and_marks_pushed(tmp_path):
    fake = FakeStripe({"searches": "memd_searches", "writes": "memd_writes"})
    try:
        store, plans, org = _ledger(tmp_path)
        b = Billing(store, plans, BillingConfig.from_env(_env(fake.url)))
        out = b.push_usage()
        assert out["failed"] == 0 and out["sent"] == out["batches"] > 0
        assert out["not_billable"] == 12  # the free org's rows close out, unsent
        got = fake.accepted()
        assert all(e["identifier"] == e["idempotency_key"] for e in got)
        assert all(e["customer"] == "cus_acme" for e in got)
        by_name = {}
        for e in got:
            by_name[e["event_name"]] = by_name.get(e["event_name"], 0) + int(e["value"])
        assert by_name == {"memd_searches": 12, "memd_writes": 24}
        assert len(got) < 24  # batched per org x meter x hour, not one call per row
        assert store.unpushed_count() == 0
        assert b.push_usage()["sent"] == 0  # nothing left: a second run is a no-op
        # reconciliation against the (idempotent) fake agrees...
        day0 = int(time.time() // 86400) * 86400 - 86400
        rep = b.reconcile(day0, day0 + 3 * 86400)
        assert rep["alerts"] == 0 and {r["meter"] for r in rep["pairs"]} == {"searches", "writes"}
        assert all(r["drift"] == 0 for r in rep["pairs"])
        # ...and flags drift when Stripe lost an event
        fake.drop.add(got[0]["identifier"])
        rep = b.reconcile(day0, day0 + 3 * 86400)
        assert rep["alerts"] == 1
        assert store.log_entries(org, "drift_alert")
        prom = METRICS.render_prometheus()
        assert "memd_billing_drift_alerts_total" in prom
        # operator-only: a tenant's filtered view does not carry billing series
        assert "memd_billing_drift_alerts_total" not in METRICS.render_prometheus(ns_filter={"acme"})
    finally:
        fake.close()


def test_meter_push_failure_keeps_the_batch_pending(tmp_path):
    store, plans, org = _ledger(tmp_path, n_events=2)
    b = Billing(store, plans, BillingConfig.from_env(_env("http://127.0.0.1:9")))  # nothing listens
    b._client = __import__("stripe").StripeClient("sk_test_x", base_addresses={"api": "http://127.0.0.1:9"},
                                                  max_network_retries=0)
    out = b.push_usage()
    assert out["sent"] == 0 and out["failed"] == out["batches"] > 0
    pending = store.pending_pushes()
    assert pending and all(p["attempts"] == 1 and p["last_error"] for p in pending)


PUSHER = r"""
import sys
sys.path.insert(0, {src!r})
from memd.hosted.billing import Billing, BillingConfig
from memd.hosted.plans import Plans
from memd.hosted.store import AdminStore
store = AdminStore.for_data_root(sys.argv[1])
env = {{"MEMD_STRIPE_SECRET_KEY": "sk_test_x", "MEMD_STRIPE_API_BASE": sys.argv[2]}}
print(Billing(store, Plans(), BillingConfig.from_env(env)).push_usage(), flush=True)
"""


@pytest.mark.parametrize("kill", ["before_accept", "after_accept"])
def test_meter_push_crash_is_exactly_once_via_idempotency(tmp_path, kill):
    """SIGKILL the pusher while Stripe holds its first request - before
    Stripe accepted it, or after Stripe accepted it but before the pusher
    could mark the batch pushed. The rerun re-sends the SAME idempotency
    keys: every ledger row is billed exactly once."""
    fake = FakeStripe()
    try:
        store, plans, org = _ledger(tmp_path)
        expected = sum(e["billable"] for e in store.events(org))
        store.close()
        data = str(tmp_path / "data")
        state = {"pid": None, "killed": False}

        def maybe_kill(params):
            if not state["killed"] and state["pid"]:
                state["killed"] = True
                os.kill(state["pid"], signal.SIGKILL)
                return "kill_before" if kill == "before_accept" else None
            return None

        if kill == "before_accept":
            fake.on_meter_event = maybe_kill
        else:
            fake.after_accept = maybe_kill
        child = subprocess.Popen([sys.executable, "-c", PUSHER.format(src=SRC), data, fake.url],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        state["pid"] = child.pid
        child.wait(timeout=120)
        assert state["killed"] and child.returncode == -signal.SIGKILL
        store = AdminStore.for_data_root(data)
        assert store.pending_pushes(), "the killed batch must still be pending"
        first_keys = {p["id"] for p in store.pending_pushes()}
        fake.on_meter_event = fake.after_accept = None
        out = Billing(store, plans, BillingConfig.from_env(_env(fake.url))).push_usage()
        assert out["failed"] == 0 and store.pending_pushes() == [] and store.unpushed_count() == 0
        got = fake.accepted()
        assert sum(int(e["value"]) for e in got) == expected  # nothing lost, nothing doubled
        assert len({e["idempotency_key"] for e in got}) == len(got)
        assert first_keys <= {e["idempotency_key"] for e in got}  # retried under the SAME keys
        if kill == "after_accept":
            assert fake.replays == 1  # Stripe saw the batch twice and deduplicated it
    finally:
        fake.close()


def test_gauges_snapshot_once_per_day_and_push_in_milli_gb(tmp_path):
    fake = FakeStripe()
    try:
        app, h = _app(tmp_path, _env(fake.url), plans={})
        org = h.store.create_org("acme", plan="scale")
        h.store.update_org(org, stripe_customer_id="cus_acme")
        key, _ = h.keystore.create("acme", org_id=org)
        c = TestClient(app)
        c.headers["Authorization"] = f"Bearer {key}"
        for i in range(5):
            assert c.post("/v1/ns/acme/memories", json={"content": f"stored thing {i}"}).status_code == 201
        app.state.engine.flush()
        now = time.time()
        first = h.jobs.run_once(now=now)
        assert first["snapshot"][org]["memories_stored"] == 5
        h.jobs._snapshot_day = None  # a restart re-runs the day's snapshot...
        h.jobs.run_once(now=now)
        gauges = [e for e in h.store.events(org) if e["meter"] in ("memories_stored", "stored_gb")]
        assert len(gauges) == 2  # ...which is a no-op: one per meter per day
        gb = [e for e in fake.accepted() if e["event_name"] == "memd_stored_gb"]
        assert len(gb) == 1 and int(gb[0]["value"]) >= 1  # whole milli-GB units, never 0 for data
        assert not [e for e in fake.accepted() if e["event_name"] == "memd_memories_stored"]  # not metered
        app.state.engine.close()
        h.store.close()
    finally:
        fake.close()


# ------------------------------------------------- checkout / portal / usage


def test_checkout_validation_without_network(billed):
    app, h, org, c = billed
    key, _ = h.keystore.create("acme", org_id=org, scopes="billing")
    c.headers["Authorization"] = f"Bearer {key}"
    assert c.post("/v1/billing/checkout", json={"plan": "free"}).json()["code"] == "invalid_plan"
    assert c.post("/v1/billing/checkout", json={"plan": "enterprise"}).json()["code"] == "invalid_plan"
    assert c.post("/v1/billing/checkout", json={"plan": "dev", "interval": "week"}).json()["code"] == \
        "invalid_interval"
    h.store.update_org(org, plan="dev", stripe_subscription_id="sub_live", status="active")
    assert c.post("/v1/billing/checkout", json={"plan": "scale"}).status_code == 409


def test_billing_routes_503_when_stripe_is_not_configured(tmp_path):
    app, h = _app(tmp_path, {})
    org = h.store.create_org("acme")
    key, _ = h.keystore.create("acme", org_id=org, scopes="billing")
    c = TestClient(app)
    c.headers["Authorization"] = f"Bearer {key}"
    assert c.post("/v1/billing/checkout", json={"plan": "dev"}).status_code == 503
    assert c.post("/v1/billing/portal").status_code == 503
    assert _post_webhook(c, "{}", "t=1,v1=x").status_code == 503
    assert c.get("/v1/billing/usage").status_code == 200  # usage needs no Stripe
    app.state.engine.close()


# ------------------------------------------- end to end against stripe-mock


@pytest.fixture(scope="module")
def stripe_mock():
    sm = StripeMock().start()
    if sm.url is None:
        pytest.skip(sm.skip_reason or "stripe-mock unavailable")
    yield sm.url
    sm.stop()


def test_end_to_end_checkout_webhook_entitlement_usage_push(tmp_path, stripe_mock):
    import stripe

    # stripe-mock is stateless: GET /v1/subscriptions/<id> returns a fixture.
    # Point the dev monthly price at the fixture's price, so the webhook's
    # "plan = the price Stripe bills" lookup resolves through the real API.
    probe = stripe.StripeClient("sk_test_123", base_addresses={"api": stripe_mock})
    fixture_price = probe.v1.subscriptions.retrieve("sub_e2e").to_dict()["items"]["data"][0]["price"]["id"]
    fixture_meter = probe.v1.billing.meters.list().data[0].id
    env = _env(stripe_mock, MEMD_STRIPE_PRICE_DEV_MONTHLY=fixture_price,
               MEMD_STRIPE_METER_ID_RERANKED_SEARCHES=fixture_meter)
    app, h = _app(tmp_path, env)
    try:
        org = h.store.create_org("acme")
        key, _ = h.keystore.create("acme", org_id=org, scopes="memory,billing")
        c = TestClient(app)
        c.headers["Authorization"] = f"Bearer {key}"
        app.state.engine.rerank = RerankStage(_Scorer())
        for i in range(3):
            assert c.post("/v1/ns/acme/memories", json={"content": f"kumquat memo {i}"}).status_code == 201

        # free plan: the hard search cap bites
        assert c.post("/v1/ns/acme/search", json={"query": "kumquat a"}).status_code == 200
        assert c.post("/v1/ns/acme/search", json={"query": "kumquat b"}).status_code == 200
        r = c.post("/v1/ns/acme/search", json={"query": "kumquat c"})
        assert r.status_code == 402 and r.json()["meter"] == "searches"

        # 1. checkout -> a Checkout Session (and a Stripe customer for the org)
        r = c.post("/v1/billing/checkout", json={"plan": "dev"})
        assert r.status_code == 200, r.text
        assert r.json()["url"].startswith("https://") and r.json()["id"].startswith("cs_")
        customer = h.store.get_org(org)["stripe_customer_id"]
        assert customer and customer.startswith("cus_")

        # 2. the signed webhook Stripe sends when the customer paid
        ev = event("checkout.session.completed", {
            "id": r.json()["id"], "object": "checkout.session", "customer": customer,
            "client_reference_id": org, "subscription": "sub_e2e", "payment_status": "paid",
            "metadata": {"memd_org_id": org, "plan": "dev"}})
        p = json.dumps(ev)
        r = _post_webhook(c, p, sign(p))
        assert r.status_code == 200 and r.json()["plan"] == "dev", r.text

        # 3. entitlement: dev lifts the search cap; reranking past the
        #    included 1 is overage, allowed and metered
        for q in ("kumquat c", "kumquat d", "kumquat e"):
            assert c.post("/v1/ns/acme/search", json={"query": q}).status_code == 200
        usage = c.get("/v1/billing/usage").json()
        assert usage["plan"] == "dev" and usage["status"] == "active"
        # 2 reranked on free (not metered there) + 3 on dev; dev includes 1
        # per period and the period's rollup already holds 2, so all 3 are
        # overage
        rr = usage["meters"]["reranked_searches"]
        assert rr["used"] == 5 and rr["billable"] == 3 and rr["metered"] is True

        # 4. meter push -> Stripe Billing Meter Events
        out = h.billing.push_usage()
        assert out["failed"] == 0 and out["sent"] >= 1
        sent = [e for e in h.store.events(org) if e["push_status"] == "sent"]
        assert sum(e["billable"] for e in sent) == 3
        assert {e["meter"] for e in sent} == {"reranked_searches"}
        assert h.store.unpushed_count() == 0

        # 5. portal for the now-paying org
        r = c.post("/v1/billing/portal")
        assert r.status_code == 200 and r.json()["url"]

        # 6. reconciliation reads Stripe's meter event summaries; stripe-mock
        #    answers a fixed fixture total, which differs from the ledger's 3
        #    - exactly the drift the daily job must alert on
        today = int(time.time() // 86400) * 86400
        rep = h.billing.reconcile(today, today + 86400)
        [row] = rep["pairs"]
        assert row["ledger"] == 3 and row["stripe"] != 3 and row["alert"] is True
        assert rep["alerts"] == 1
    finally:
        app.state.engine.close()
        h.store.close()


# ------------------------------- stale usage: never auto-pushed, reconciled


class _Sim:
    """A simulated clock shared by the ledger, the pusher and FakeStripe
    (whose idempotency keys expire after 24 h, as Stripe's do)."""

    def __init__(self, tmp_path, fake: FakeStripe):
        self.fake = fake
        fake.key_ttl_s = 24 * 3600
        fake.clock = lambda: self.now
        self.base = (int(time.time()) // 3600 - 40) * 3600  # 40 h ago, hour-aligned
        self.now = self.base
        self.store = AdminStore.for_data_root(str(tmp_path / "data"))
        self.plans = Plans()
        self.org = self.store.create_org("acme", plan="scale")
        self.store.update_org(self.org, stripe_customer_id="cus_acme")
        self.billing = Billing(self.store, self.plans, BillingConfig.from_env(
            _env(fake.url, MEMD_STRIPE_MAX_NETWORK_RETRIES="0")))
        self.hours_recorded = 0

    def usage_through(self, hour: int) -> None:
        """One search per simulated hour, a minute past the hour."""
        while self.hours_recorded <= hour:
            self.store.record_usage(self.org, "acme", {"searches": 1}, self.plans.get("scale"),
                                    ts=self.base + self.hours_recorded * 3600 + 60)
            self.hours_recorded += 1

    def tick(self, hour: float) -> dict:
        """The billing job's hourly tick at base + hour."""
        self.now = int(self.base + hour * 3600)
        return {"push": self.billing.push_usage(now=self.now),
                "reconcile": self.billing.reconcile_pending(now=self.now)}

    def ledger_units(self) -> float:
        return sum(e["billable"] for e in self.store.events(self.org) if e["meter"] == "searches")


def test_30h_outage_never_double_counts(tmp_path):
    """Hour 0's batch reaches Stripe but its response is lost; then Stripe is
    unreachable for 30 h while the job keeps ticking. Retrying that batch
    after recovery under its key would bill it twice (Stripe forgot the key
    after 24 h), so nothing older than 20 h is auto-pushed: those events are
    settled against Stripe's meter summary, which already holds hour 0 -
    only the verified missing quantity is sent, under a fresh key."""
    fake = FakeStripe({"searches": "memd_searches"})
    try:
        sim = _Sim(tmp_path, fake)
        sim.usage_through(0)
        fake.fail_after_accept = True  # Stripe bills hour 0, the pusher never hears back
        assert sim.tick(1)["push"]["failed"] == 1
        [batch_a] = sim.store.pending_pushes()
        fake.fail_after_accept, fake.down = False, True
        for h in range(1, 31):  # the 30 h outage; every tick fails or defers
            sim.usage_through(h)
            t = sim.tick(h + 1)
            assert t["push"]["sent"] == 0 and t["reconcile"]["reconciled"] == 0
        assert sim.store.needs_reconcile_count() > 0
        assert any(b["id"] == batch_a["id"] and b["abandoned_at"] for b in sim.store.batches())

        fake.down = False  # recovery
        sim.usage_through(31)
        seen_before_send = []

        def check_recorded(params):  # the fresh key is persisted BEFORE the send
            row = [b for b in sim.store.batches() if b["id"] == params["idempotency_key"]]
            seen_before_send.append(bool(row) and row[0]["pushed_at"] is None)

        fake.on_meter_event = check_recorded
        first = sim.tick(32)
        second = sim.tick(34)  # a deferred window settles on a later tick
        fake.on_meter_event = None
        assert first["push"]["failed"] == 0 and second["push"]["failed"] == 0
        assert all(seen_before_send)

        assert fake.total("memd_searches") == sim.ledger_units() == 32  # nothing lost, nothing doubled
        assert fake.expired_key_reuse == 0  # no key was re-sent after Stripe forgot it
        a_sends = [r for r in fake.requests if r["idempotency_key"] == batch_a["id"]]
        assert len(a_sends) == 1  # batch A was never retried after the outage
        [rec] = [b for b in sim.store.batches() if b["kind"] == "reconcile"]
        assert rec["credited"] == 1  # hour 0: verified already in Stripe, not re-sent
        # hours 1-11 (older than 20 h at the recovery tick, 32 h): sent once
        assert rec["value"] == 11 and rec["pushed_at"] is not None
        assert sim.store.needs_reconcile_count() == 0 and sim.store.unpushed_count() == 0
        assert {e["push_status"] for e in sim.store.events(sim.org)} <= {"sent", "reconciled"}
        # the drift report over the quota period to date is clean
        rep = sim.billing.reconcile(min(e["ts"] for e in sim.store.events()) // 3600 * 3600, sim.now)
        assert rep["alerts"] == 0 and rep["pairs"][0]["drift"] == 0
    finally:
        fake.close()


def test_stale_batch_that_already_landed_is_not_resent(tmp_path):
    fake = FakeStripe({"searches": "memd_searches"})
    try:
        sim = _Sim(tmp_path, fake)
        sim.usage_through(2)
        fake.fail_after_accept = True  # all three hours land; every response is lost
        assert sim.tick(3)["push"]["failed"] == 3
        fake.fail_after_accept = False
        # the pusher only comes back 22 h later: too late to retry the keys
        out = sim.tick(25)
        assert out["push"]["abandoned"] == 3 and out["push"]["sent"] == 0
        assert out["reconcile"]["reconciled"] == 1 and out["reconcile"]["pushed_units"] == 0
        assert out["reconcile"]["credited_units"] == 3
        assert fake.total() == sim.ledger_units() == 3 and len(fake.requests) == 3
        assert {e["push_status"] for e in sim.store.events(sim.org)} == {"reconciled"}
    finally:
        fake.close()


def test_reconcile_that_cannot_decide_alerts_and_sends_nothing(tmp_path):
    fake = FakeStripe({"searches": "memd_searches"})
    try:
        sim = _Sim(tmp_path, fake)
        sim.usage_through(4)
        # Stripe holds usage for this customer the ledger cannot account for
        # (a manual adjustment, another system): more than the ledger expects
        fake.events.append({"event_name": "memd_searches", "identifier": "manual", "customer": "cus_acme",
                            "value": "7", "timestamp": sim.base + 120, "idempotency_key": None, "_at": 0})
        out = sim.tick(26)  # 5 hours of usage, all older than 20 h
        assert out["push"]["needs_reconcile"] == 5 and out["push"]["sent"] == 0
        assert out["reconcile"]["undecidable"] == 1 and out["reconcile"]["reconciled"] == 0
        assert len(fake.requests) == 0  # nothing was pushed
        assert sim.store.needs_reconcile_count() == 5  # left for a human, still alerting
        [alert] = sim.store.log_entries(sim.org, "drift_alert")
        assert json.loads(alert["detail"])["reason"] == "stripe_differs"
        assert "memd_billing_drift_alerts_total" in METRICS.render_prometheus()
    finally:
        fake.close()


def test_usage_older_than_20h_is_never_auto_pushed(tmp_path):
    fake = FakeStripe({"searches": "memd_searches"})
    try:
        sim = _Sim(tmp_path, fake)
        sim.usage_through(0)
        sim.now = sim.base + 20 * 3600 + 61  # the event is 20 h and 1 s old
        out = sim.billing.push_usage(now=sim.now)
        assert out["needs_reconcile"] == 1 and out["batches"] == 0 and fake.requests == []
        # younger usage in the same run still goes out normally
        sim.store.record_usage(sim.org, "acme", {"searches": 1}, sim.plans.get("scale"), ts=sim.now - 60)
        out = sim.billing.push_usage(now=sim.now)
        assert out["sent"] == 1 and fake.total() == 1
    finally:
        fake.close()
