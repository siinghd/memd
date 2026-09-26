"""Hosted billing hardening: one test per finding of the security review
(numbers refer to it). Fully offline: Stripe is tests/hosted_helpers.FakeStripe
over real HTTP, webhooks are signed with a test secret."""
from __future__ import annotations

import io
import json
import logging
import os
import stat
import threading
import time

import httpx
import pytest

pytest.importorskip("stripe")

from fastapi.testclient import TestClient  # noqa: E402

from memd.hosted.billing import Billing, BillingConfig, LiveKeyRefused, guard_live_key  # noqa: E402
from memd.hosted.plans import Plans  # noqa: E402
from memd.hosted.store import AdminStore, period_of  # noqa: E402
from memd.metrics import METRICS  # noqa: E402
from memd.query.rerank import RerankStage  # noqa: E402
from memd.server.http import create_app  # noqa: E402
from tests.hosted_helpers import WEBHOOK_SECRET, FakeStripe, event, sign  # noqa: E402
from tests.test_hosted_tenancy import _KeyedExtractor, _Scorer, _serve  # noqa: E402

PRICES = {
    "MEMD_STRIPE_PRICE_DEV_MONTHLY": "price_dev_monthly",
    "MEMD_STRIPE_PRICE_DEV_EXTRACTIONS_OUR_KEY": "price_dev_extractions",
    "MEMD_STRIPE_PRICE_DEV_RERANKED_SEARCHES": "price_dev_reranked",
}


def _env(url: str, **extra) -> dict:
    return {"MEMD_STRIPE_SECRET_KEY": "sk_test_hardening123", "MEMD_STRIPE_WEBHOOK_SECRET": WEBHOOK_SECRET,
            "MEMD_STRIPE_API_BASE": url, "MEMD_STRIPE_MAX_NETWORK_RETRIES": "0", **PRICES, **extra}


class World:
    def __init__(self, tmp_path, plans=None, **env):
        self.fake = FakeStripe({"searches": "memd_searches", "reranked_searches": "memd_reranked_searches"})
        self.app = create_app(data_dir=str(tmp_path / "data"), hosted=True, plans=Plans(plans or {}),
                              billing_config=BillingConfig.from_env(_env(self.fake.url, **env)))
        self.h = self.app.state.hosted
        self.store: AdminStore = self.h.store
        self.engine = self.app.state.engine
        self.web = TestClient(self.app)

    def client(self, org, ns, scopes="memory,billing"):
        key, _ = self.h.keystore.create(ns, org_id=org, scopes=scopes)
        c = TestClient(self.app)
        c.headers["Authorization"] = f"Bearer {key}"
        return c

    def post(self, ev):
        p = json.dumps(ev)
        return self.web.post("/v1/billing/webhook", content=p,
                             headers={"Stripe-Signature": sign(p), "Content-Type": "application/json"})

    def sub(self, sid, customer, status="active", prices=("price_dev_monthly",), meta=None):
        self.fake.subscriptions[sid] = {
            "id": sid, "object": "subscription", "customer": customer, "status": status,
            "metadata": meta or {}, "items": {"object": "list", "data": [
                {"id": f"si_{i}", "price": {"id": p}, "current_period_end": int(time.time()) + 30 * 86400}
                for i, p in enumerate(prices)]}}
        return self.fake.subscriptions[sid]

    def org(self, **fields):
        oid = self.store.create_org(fields.pop("name", "acme"))
        if fields:
            self.store.update_org(oid, **fields)
        return oid

    def snap(self, oid):
        o = self.store.get_org(oid)
        return {k: o[k] for k in ("plan", "status", "stripe_customer_id", "stripe_subscription_id", "grace_until")}

    def close(self):
        self.engine.close()
        self.store.close()
        self.fake.close()


@pytest.fixture()
def world(tmp_path):
    w = World(tmp_path)
    yield w
    w.close()


def _session(org, customer, sub, mode="subscription", payment_status="paid", sid="cs_1"):
    return {"id": sid, "object": "checkout.session", "mode": mode, "customer": customer,
            "client_reference_id": org, "subscription": sub, "payment_status": payment_status,
            "metadata": {"memd_org_id": org}}


# ------------------------------------------ 1 + 6: extraction and fact caps


def test_1_extraction_records_never_exceed_the_reservation(tmp_path):
    w = World(tmp_path, plans={"free": {"meters": {"extractions_our_key": {"limit": 3, "hard": True}}}})
    try:
        org = w.org()
        c = w.client(org, "x", scopes="memory")
        w.engine.extractor = _KeyedExtractor()
        c.post("/v1/ns/x/events", json={"events": [{"content": f"raw {i}", "session_id": "big"} for i in range(500)]})
        r = c.post("/v1/ns/x/sessions/big/close")
        assert r.status_code == 200
        body = r.json()
        assert body["raw_considered"] == 3 and body["raw_skipped"] == 497  # extracted up to the allowance
        assert w.store.rollup(org, "extractions_our_key", period_of(time.time())) == 3
        assert w.store.reservations() == []
        c.post("/v1/ns/x/events", json={"events": [{"content": "more", "session_id": "next"}]})
        r = c.post("/v1/ns/x/sessions/next/close")
        assert r.status_code == 402 and r.json()["meter"] == "extractions_our_key"
    finally:
        w.close()


def test_6_session_close_checks_and_reserves_memories_stored(tmp_path):
    w = World(tmp_path, plans={"free": {"meters": {"memories_stored": {"limit": 6, "hard": True}}}})
    try:
        org = w.org()
        c = w.client(org, "y", scopes="memory")
        facts = ["My name is Alice Johnson.", "I live in Paris, France.", "I work at Acme Corp as an engineer.",
                 "My favorite color is blue.", "I prefer tea over coffee.", "My sister is called Maria."]
        c.post("/v1/ns/y/events", json={"events": [
            {"role": "user", "content": t, "session_id": "s1", "user_id": "u1"} for t in facts[:4]]})
        # 4 stored, 2 left: the facts the close writes are capped at 2
        r = c.post("/v1/ns/y/sessions/s1/close")
        assert r.status_code == 200, r.text
        assert r.json()["facts_written"] <= 2
        assert w.engine.stats(namespace="y")["records"] <= 6
        fill = 6 - w.engine.stats(namespace="y")["records"]
        if fill:
            c.post("/v1/ns/y/events", json={"events": [
                {"role": "user", "content": t, "session_id": "s2", "user_id": "u1"} for t in facts[4:4 + fill]]})
        assert w.engine.stats(namespace="y")["records"] == 6
        r = c.post("/v1/ns/y/sessions/s2/close")  # at the cap: a close is a write
        assert r.status_code == 402 and r.json()["meter"] == "memories_stored"
        assert w.engine.stats(namespace="y")["records"] == 6
        assert w.store.reservations() == []
    finally:
        w.close()


# ----------------------------------------------------------- 2: exact scopes


def test_2_scopes_are_exact(world):
    org = world.org()
    override = world.client(org, "acme", scopes="override")
    mem_override = world.client(org, "acme", scopes="memory,override")
    bill_override = world.client(org, "acme", scopes="billing,override")
    assert override.get("/v1/billing/usage").status_code == 403
    assert override.post("/v1/ns/acme/memories", json={"content": "x"}).status_code == 403
    assert override.post("/v1/ns/acme/search", json={"query": "x"}).status_code == 403
    assert mem_override.get("/v1/billing/usage").status_code == 403
    assert mem_override.post("/v1/billing/portal").status_code == 403
    assert mem_override.post("/v1/ns/acme/memories", json={"content": "x"}).status_code == 201
    assert bill_override.post("/v1/ns/acme/search", json={"query": "x"}).status_code == 403
    assert bill_override.get("/v1/billing/usage").status_code == 200


# ------------------------------------------- 3: checkout.session.completed


def test_3a_payment_mode_session_never_clears_past_due(world):
    org = world.org(stripe_customer_id="cus_pd", stripe_subscription_id="sub_pd", plan="dev",
                    status="past_due", grace_until=int(time.time()) - 10)
    before = world.snap(org)
    for obj in (_session(org, None, None, mode="payment"), _session(org, "cus_pd", None, mode="payment"),
                _session(org, "cus_pd", None, mode="subscription")):
        r = world.post(event("checkout.session.completed", obj))
        assert r.status_code == 200 and r.json()["handled"] is False
    assert world.snap(org) == before


@pytest.mark.parametrize("status,plan", [("active", "dev"), ("trialing", "dev"), ("incomplete", "free"),
                                         ("canceled", "free"), ("unpaid", "free"), ("incomplete_expired", "free")])
def test_3b_completed_session_applies_the_subscriptions_verified_status(world, status, plan):
    org = world.org(stripe_customer_id="cus_a")
    world.sub("sub_a", "cus_a", status=status)
    # the payload claims an active subscription; Stripe's copy is what counts
    obj = _session(org, "cus_a", {"id": "sub_a", "status": "active", "customer": "cus_a",
                                  "items": {"data": [{"price": {"id": "price_dev_monthly"}}]}})
    r = world.post(event("checkout.session.completed", obj))
    assert r.status_code == 200
    assert world.snap(org)["plan"] == plan


def test_3c_late_completed_cannot_resurrect_a_deleted_subscription(world):
    org = world.org(stripe_customer_id="cus_s")
    now = int(time.time())
    world.sub("sub_s", "cus_s", status="active")
    world.post(event("customer.subscription.created", world.fake.subscriptions["sub_s"], created=now - 20))
    assert world.snap(org)["plan"] == "dev"
    gone = dict(world.fake.subscriptions["sub_s"], status="canceled")
    world.post(event("customer.subscription.deleted", gone, created=now - 10))
    # even if Stripe's copy still read active (a lagging read), the cursor holds
    r = world.post(event("checkout.session.completed", _session(org, "cus_s", "sub_s"), created=now - 30))
    assert r.status_code == 200 and r.json()["handled"] is False and r.json()["reason"] == "stale_event"
    assert world.snap(org)["plan"] == "free"


def test_3d_stripe_outage_on_the_subscription_fetch_asks_for_a_retry(world):
    org = world.org(stripe_customer_id="cus_o")
    world.fake.down = True
    r = world.post(event("checkout.session.completed", _session(org, "cus_o", "sub_o"), eid="evt_retry"))
    assert r.status_code == 500
    assert world.store.processed_event("evt_retry") is None
    world.fake.down = False
    world.sub("sub_o", "cus_o")
    r = world.post(event("checkout.session.completed", _session(org, "cus_o", "sub_o"), eid="evt_retry"))
    assert r.status_code == 200 and world.snap(org)["plan"] == "dev"


# ------------------------------------------------- 4: status -> entitlement


@pytest.mark.parametrize("status", ["incomplete", "incomplete_expired", "unpaid", "paused"])
def test_4_unpaid_statuses_never_grant_a_paid_plan(world, status):
    org = world.org(stripe_customer_id="cus_i")
    sub = world.sub("sub_i", "cus_i", status=status)
    r = world.post(event("customer.subscription.created", sub))
    assert r.status_code == 200
    assert world.snap(org)["plan"] == "free"


def test_4_past_due_keeps_the_plan_in_grace(world):
    org = world.org(stripe_customer_id="cus_g")
    world.post(event("customer.subscription.created", world.sub("sub_g", "cus_g"), created=int(time.time()) - 5))
    r = world.post(event("customer.subscription.updated", world.sub("sub_g", "cus_g", status="past_due")))
    assert r.status_code == 200
    s = world.snap(org)
    assert s["plan"] == "dev" and s["status"] == "past_due" and s["grace_until"] > time.time()


# ------------------------------------------------ 5: one subscription per org


def test_5a_checkout_refuses_while_pending_or_active_and_races_create_one_session(world):
    org = world.org()
    c = world.client(org, "acme", scopes="billing")
    codes = []
    barrier = threading.Barrier(8)

    def go():
        barrier.wait()
        codes.append(c.post("/v1/billing/checkout", json={"plan": "dev"}).status_code)

    ts = [threading.Thread(target=go) for _ in range(8)]
    [t.start() for t in ts]
    [t.join(60) for t in ts]
    assert sorted(codes) == [200] + [409] * 7, codes
    assert sum(1 for k, _ in world.fake.created if k == "checkout") == 1
    r = c.post("/v1/billing/checkout", json={"plan": "dev"})
    assert r.status_code == 409 and r.json()["code"] == "checkout_pending" and r.json()["url"]
    # the session expires unpaid: checkout is possible again
    sess = world.store.get_org(org)["checkout_session_id"]
    world.post(event("checkout.session.expired", {"id": sess, "object": "checkout.session",
                                                  "client_reference_id": org, "mode": "subscription"}))
    assert c.post("/v1/billing/checkout", json={"plan": "dev"}).status_code == 200
    # once a subscription is active: 409 already_subscribed
    cust = world.store.get_org(org)["stripe_customer_id"]
    world.sub("sub_live", cust)
    sess = world.store.get_org(org)["checkout_session_id"]
    world.post(event("checkout.session.completed", _session(org, cust, "sub_live", sid=sess)))
    assert world.snap(org)["plan"] == "dev"
    r = c.post("/v1/billing/checkout", json={"plan": "dev"})
    assert r.status_code == 409 and r.json()["code"] == "already_subscribed"


def test_5b_canceling_a_non_current_subscription_does_not_downgrade(world):
    org = world.org(stripe_customer_id="cus_two", stripe_subscription_id="sub_main", plan="dev", status="active")
    r = world.post(event("customer.subscription.deleted", world.sub("sub_other", "cus_two", status="canceled")))
    assert r.status_code == 200
    assert world.snap(org)["plan"] == "dev" and world.snap(org)["stripe_subscription_id"] == "sub_main"


def test_5c_a_duplicate_subscription_is_flagged_and_its_usage_is_held(world):
    org = world.org(stripe_customer_id="cus_d", stripe_subscription_id="sub_1", plan="dev", status="active")
    r = world.post(event("customer.subscription.created", world.sub("sub_2", "cus_d")))
    assert r.status_code == 200 and r.json()["reason"] == "duplicate_subscription"
    o = world.store.get_org(org)
    assert o["stripe_subscription_id"] == "sub_1" and o["duplicate_subscription_id"] == "sub_2"
    assert world.store.log_entries(org, "duplicate_subscription")
    # metered usage is not pushed while two subscriptions would each bill it
    world.store.record_usage(org, "acme", {"reranked_searches": 20_000}, world.h.plans.get("dev"))
    out = world.h.billing.push_usage()
    assert out["sent"] == 0 and world.fake.requests == []
    # the duplicate is canceled: the hold lifts and the usage is billed once
    world.post(event("customer.subscription.deleted", world.sub("sub_2", "cus_d", status="canceled")))
    assert world.store.get_org(org)["duplicate_subscription_id"] is None
    assert world.snap(org)["plan"] == "dev"
    out = world.h.billing.push_usage()
    assert out["sent"] == 1 and world.fake.total() == 20_000 - 10_000


# --------------------------------------------------- 7: customer binding


def test_7_a_session_cannot_bind_a_foreign_customer(world):
    org = world.org()
    world.fake.customers["cus_attacker"] = {"id": "cus_attacker", "object": "customer", "metadata": {}}
    world.sub("sub_att", "cus_attacker")
    r = world.post(event("checkout.session.completed", _session(org, "cus_attacker", "sub_att")))
    assert r.status_code == 200 and r.json()["handled"] is False
    assert world.snap(org)["stripe_customer_id"] is None and world.snap(org)["plan"] == "free"
    assert world.store.log_entries(org, "customer_unverified")
    # a subscription event carrying our metadata cannot bind it either
    sub = world.sub("sub_att2", "cus_attacker", meta={"memd_org_id": org})
    world.post(event("customer.subscription.created", sub))
    assert world.snap(org)["stripe_customer_id"] is None and world.snap(org)["plan"] == "free"


def test_7_a_customer_our_checkout_created_for_the_org_is_bound(world):
    org = world.org()
    world.fake.customers["cus_ours"] = {"id": "cus_ours", "object": "customer", "metadata": {"memd_org_id": org}}
    world.sub("sub_ours", "cus_ours")
    r = world.post(event("checkout.session.completed", _session(org, "cus_ours", "sub_ours")))
    assert r.status_code == 200 and r.json()["handled"] is True
    assert world.snap(org)["stripe_customer_id"] == "cus_ours" and world.snap(org)["plan"] == "dev"


# -------------------------------------------------- 8: unmapped customers


def test_8_unmapped_customer_events_are_acked_and_counted(world):
    before = world.store.list_orgs()
    for et, obj in [("customer.subscription.created", world.sub("sub_zz", "cus_nobody")),
                    ("customer.subscription.updated", world.sub("sub_zz", "cus_nobody")),
                    ("invoice.payment_failed", {"id": "in_1", "customer": "cus_nobody", "subscription": "sub_zz"})]:
        r = world.post(event(et, obj))
        assert r.status_code == 200 and r.json()["reason"] == "unknown_customer", r.text
    assert world.store.list_orgs() == before
    assert "memd_billing_webhook_unmapped_total" in METRICS.render_prometheus()


# ------------------------------------------------------- 9: redaction


def test_9_stripe_errors_are_redacted_in_logs_and_the_store(tmp_path):
    secret = "sk_test_4eC39HqLyjWDarjtT1zdp7dc"
    store = AdminStore.for_data_root(str(tmp_path / "d"))
    org = store.create_org("acme", plan="scale")
    store.update_org(org, stripe_customer_id="cus_1")
    store.record_usage(org, "n", {"searches": 3}, Plans().get("scale"))

    class Boom:
        class v1:
            class billing:
                class meter_events:
                    @staticmethod
                    def create(params=None, options=None):
                        raise RuntimeError(f"Invalid API Key provided: {secret}; Authorization was 'Bearer "
                                           f"{secret}'; webhook whsec_abcdef0123456789; rk_live_XYZ123abc")

    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    logging.getLogger("memd").addHandler(handler)
    try:
        b = Billing(store, Plans(), BillingConfig.from_env({"MEMD_STRIPE_SECRET_KEY": secret}), client=Boom())
        assert b.push_usage()["failed"] == 1
    finally:
        logging.getLogger("memd").removeHandler(handler)
    # the Stripe SDK's own logger echoes response bodies (and so the key)
    stripe_log = logging.getLogger("stripe")
    stripe_log.addHandler(handler)
    old_level = stripe_log.level
    stripe_log.setLevel(logging.INFO)
    try:
        stripe_log.info("error_message=\"Authorization was 'Bearer %s'\"", secret)
    finally:
        stripe_log.removeHandler(handler)
        stripe_log.setLevel(old_level)
    # and a traceback logged by the webhook route / the billing job
    hosted_log = logging.getLogger("memd.hosted")
    hosted_log.addHandler(handler)
    try:
        try:
            raise RuntimeError(f"Authorization was 'Bearer {secret}'")
        except RuntimeError:
            hosted_log.exception("memd billing: webhook handler failed for %s", "evt_1")
    finally:
        hosted_log.removeHandler(handler)
    stored = json.dumps(store.batches())
    for blob in (buf.getvalue(), stored):
        assert secret not in blob and "whsec_abcdef" not in blob and "rk_live_XYZ" not in blob
        assert "[REDACTED]" in blob
    store.close()


# ----------------------------------------------------- 10: key whitespace


@pytest.mark.parametrize("key", [" sk_live_abc", "\nsk_live_abc", "sk_live_abc\n", "\tsk_live_abc "])
def test_10_live_key_guard_strips_whitespace(key):
    with pytest.raises(LiveKeyRefused):
        guard_live_key(key, {})


@pytest.mark.parametrize("key", [" sk_test_abc", "sk_test_abc\n", "sk_test_a bc"])
def test_10_keys_with_whitespace_are_rejected(key):
    with pytest.raises(ValueError):
        BillingConfig.from_env({"MEMD_STRIPE_SECRET_KEY": key})


# ------------------------------------------------------ 11: file modes


def test_11_admin_store_files_are_private(tmp_path):
    store = AdminStore.for_data_root(str(tmp_path / "d"))
    org = store.create_org("acme")
    store.create_key(org, "acme")
    admin_dir = os.path.dirname(store.path)
    assert stat.S_IMODE(os.stat(admin_dir).st_mode) == 0o700
    for suffix in ("", "-wal", "-shm"):
        p = store.path + suffix
        assert os.path.exists(p), p
        assert stat.S_IMODE(os.stat(p).st_mode) == 0o600, (p, oct(os.stat(p).st_mode))
    store.close()


# -------------------------------------------------- 12: namespace names


@pytest.mark.parametrize("ns", ["_billing", "_x", "", "a/b", "../x", "a b", "x" * 200, "*"])
def test_12_create_key_rejects_invalid_or_reserved_namespaces(tmp_path, ns):
    store = AdminStore.for_data_root(str(tmp_path / "d"))
    org = store.create_org("acme")
    with pytest.raises(ValueError):
        store.create_key(org, ns)
    assert store.list_keys() == []
    store.close()


# ------------------------------------------------ 13: long-running requests


def test_13_a_long_running_requests_reservation_is_not_double_spent(tmp_path):
    w = World(tmp_path, plans={"free": {"meters": {"searches": {"limit": 1, "hard": True}}}})
    try:
        org = w.org()
        c = w.client(org, "z", scopes="memory")
        m = w.h.metering
        base = time.time()
        m.clock = lambda: base
        started, release = threading.Event(), threading.Event()
        real = w.engine.search

        def slow(*a, **kw):
            started.set()
            release.wait(30)
            return real(*a, **kw)

        w.engine.search = slow
        out = {}
        t = threading.Thread(target=lambda: out.setdefault("a", c.post("/v1/ns/z/search", json={"query": "a"})))
        t.start()
        assert started.wait(10)
        w.engine.search = real
        m.clock = lambda: base + 10 * m.RESERVATION_TTL_S  # far past the TTL; the first is still running
        second = c.post("/v1/ns/z/search", json={"query": "b"})
        release.set()
        t.join(30)
        assert out["a"].status_code == 200 and second.status_code == 402
        assert w.store.rollup(org, "searches", period_of(base)) == 1
    finally:
        w.close()


# ------------------------------------------- 14: reranking quota never 402s


def test_14_reranked_quota_exhausted_skips_reranking_not_the_search(tmp_path):
    w = World(tmp_path, plans={"free": {"meters": {"searches": {"limit": 5, "hard": True},
                                                   "reranked_searches": {"limit": 2, "hard": True}}}})
    try:
        org = w.org()
        c = w.client(org, "r", scopes="memory")
        w.engine.rerank = RerankStage(_Scorer())
        for i in range(3):
            c.post("/v1/ns/r/memories", json={"content": f"kumquat note {i}"})
        codes = [c.post("/v1/ns/r/search", json={"query": f"kumquat {i}"}).status_code for i in range(6)]
        assert codes == [200] * 5 + [402]  # only the searches cap refuses
        p = period_of(time.time())
        assert w.store.rollup(org, "reranked_searches", p) == 2
        assert w.store.rollup(org, "searches", p) == 5
        prom = METRICS.render_prometheus()
        assert 'reason="quota"' in prom
        assert w.store.reservations() == []
    finally:
        w.close()


# ------------------------------------------------------------ 15: notes


def test_15_config_repr_hides_secrets():
    cfg = BillingConfig.from_env({"MEMD_STRIPE_SECRET_KEY": "sk_test_SECRETabc123",
                                  "MEMD_STRIPE_WEBHOOK_SECRET": "whsec_SECRETabc123", **PRICES})
    assert "SECRETabc123" not in repr(cfg) and "SECRETabc123" not in str(cfg)
    assert cfg.flat_price("dev") == "price_dev_monthly"  # prices still configured


@pytest.mark.parametrize("tol", ["0", "-5"])
def test_15_non_positive_webhook_tolerance_is_refused(tol):
    with pytest.raises(ValueError):
        BillingConfig.from_env({"MEMD_STRIPE_WEBHOOK_TOLERANCE_S": tol})


def test_15_webhook_body_is_capped_by_bytes_read(tmp_path):
    w = World(tmp_path)
    server = None
    try:
        server, t, url = _serve(w.app)

        def chunks():
            for _ in range(4):
                yield b"x" * (512 * 1024)  # 2 MiB, no Content-Length

        r = httpx.post(url + "/v1/billing/webhook", content=chunks(), headers={"Stripe-Signature": "t=1,v1=00"},
                       timeout=60)
        assert r.status_code == 413
        small = json.dumps(event("invoice.payment_succeeded", {"id": "in_x", "customer": "cus_none"}))
        r = httpx.post(url + "/v1/billing/webhook", content=small.encode(),
                       headers={"Stripe-Signature": sign(small)}, timeout=60)
        assert r.status_code == 200
    finally:
        if server is not None:
            server.should_exit = True
            t.join(30)
        w.close()
