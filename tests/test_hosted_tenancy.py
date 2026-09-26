"""Hosted mode: tenancy, entitlements and the usage ledger (no Stripe calls).

Covers the billing spec's required tests that do not need the Stripe API:
quota enforcement (free hard cap -> 402, paid overage metered, grace ->
read-only, deletes always allowed), tenant isolation, the sk_live_ guard,
the usage ledger's crash safety under SIGKILL, and "embedded memd never
imports stripe"."""
from __future__ import annotations

import os
import signal
import sqlite3
import subprocess
import sys
import textwrap
import threading
import time

import httpx
import pytest
from fastapi.testclient import TestClient

from memd.hosted.billing import BillingConfig, LiveKeyRefused, guard_live_key
from memd.hosted.plans import Plans
from memd.hosted.store import AdminStore, OwnershipError, period_of
from memd.query.rerank import RerankStage
from memd.server.http import create_app

SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")

SMALL = {
    "free": {"meters": {"memories_stored": {"limit": 4, "hard": True},
                        "searches": {"limit": 3, "hard": True},
                        "extractions_our_key": {"limit": 3, "hard": True},
                        "reranked_searches": {"limit": 2, "hard": True}}},
    "dev": {"meters": {"memories_stored": {"limit": 1000, "hard": True},
                       "searches": {"limit": 1000, "hard": True},
                       "extractions_our_key": {"limit": 2, "hard": False},
                       "reranked_searches": {"limit": 1, "hard": False}}},
}


class _Scorer:
    name = "fake"
    model = "fake-1"
    calibrated = False
    timeout_s = 1.0

    def scores(self, query, cands):
        return [1.0 / (i + 1) for i in range(len(cands))]


class _KeyedExtractor:
    """Stands in for the LLM extractor running on the operator's key."""
    name = "llm"
    api_key = "sk-extractor-test"

    def extract(self, records):
        return []


class Hosted:
    def __init__(self, tmp_path, plans=None, env=None):
        self.app = create_app(data_dir=str(tmp_path / "data"), hosted=True, plans=Plans(plans or SMALL),
                              billing_config=BillingConfig.from_env(env or {}))
        self.h = self.app.state.hosted
        self.store: AdminStore = self.h.store
        self.engine = self.app.state.engine

    def org(self, name: str, plan: str = "free", **fields) -> str:
        oid = self.store.create_org(name, plan=plan)
        if fields:
            self.store.update_org(oid, **fields)
        return oid

    def client(self, org_id: str, ns: str, scopes: str = "memory,billing") -> TestClient:
        key, _ = self.h.keystore.create(ns, org_id=org_id, scopes=scopes)
        c = TestClient(self.app)
        c.headers["Authorization"] = f"Bearer {key}"
        c.key = key
        return c

    def close(self):
        self.engine.close()
        self.store.close()


@pytest.fixture()
def hosted(tmp_path):
    h = Hosted(tmp_path)
    yield h
    h.close()


def _mem(c, ns, text="a fact"):
    return c.post(f"/v1/ns/{ns}/memories", json={"content": text, "user_id": "u1"})


def _search(c, ns, q):
    return c.post(f"/v1/ns/{ns}/search", json={"query": q, "user_id": "u1"})


# ------------------------------------------------------------------ quotas


def test_free_hard_cap_on_searches_is_402_with_meter_and_limit(hosted):
    org = hosted.org("acme")
    c = hosted.client(org, "acme")
    for i in range(3):
        assert _search(c, "acme", f"query number {i}").status_code == 200
    r = _search(c, "acme", "one too many")
    assert r.status_code == 402
    body = r.json()
    assert body["code"] == "quota_exceeded"
    assert body["meter"] == "searches" and body["limit"] == 3 and body["used"] == 3
    # the refused search was not billed
    assert hosted.store.rollup(org, "searches", period_of(time.time())) == 3


def test_free_hard_cap_on_memories_counts_batch_writes(hosted):
    org = hosted.org("acme")
    c = hosted.client(org, "acme")
    r = c.post("/v1/ns/acme/events", json={"events": [{"content": f"event {i}"} for i in range(3)]})
    assert r.status_code == 202
    # 3 stored + 2 more would cross the cap of 4: the whole batch is refused
    r = c.post("/v1/ns/acme/events", json={"events": [{"content": "e4"}, {"content": "e5"}]})
    assert r.status_code == 402 and r.json()["meter"] == "memories_stored"
    assert _mem(c, "acme", "exactly at the cap").status_code == 201
    r = _mem(c, "acme", "over the cap")
    assert r.status_code == 402 and r.json()["limit"] == 4
    assert hosted.engine.stats(namespace="acme")["records"] == 4  # nothing dropped, nothing extra


def test_deletes_are_always_allowed_and_free_capacity(hosted):
    org = hosted.org("acme")
    c = hosted.client(org, "acme")
    ids = [_mem(c, "acme", f"memory {i}").json()["id"] for i in range(4)]
    assert _mem(c, "acme", "blocked").status_code == 402
    assert c.delete(f"/v1/ns/acme/memories/{ids[0]}").status_code == 200
    assert c.delete(f"/v1/ns/acme/memories/{ids[1]}?hard=true").status_code == 200
    # a hard delete of an already-deleted record frees nothing twice
    assert c.delete(f"/v1/ns/acme/memories/{ids[0]}?hard=true").status_code == 200
    assert _mem(c, "acme", "fits again").status_code == 201
    assert _mem(c, "acme", "fits again 2").status_code == 201
    assert _mem(c, "acme", "full again").status_code == 402


def test_free_reranked_and_extraction_caps(hosted):
    org = hosted.org("acme")
    c = hosted.client(org, "acme")
    hosted.engine.rerank = RerankStage(_Scorer())
    hosted.engine.extractor = _KeyedExtractor()
    for i in range(3):
        assert _mem(c, "acme", f"kumquat note {i}").status_code == 201
    assert _search(c, "acme", "kumquat one").status_code == 200
    assert _search(c, "acme", "kumquat two").status_code == 200
    r = _search(c, "acme", "kumquat three")  # reranked cap (2) hits before searches (3)
    assert r.status_code == 402 and r.json()["meter"] == "reranked_searches"
    # extraction on our key: 3 raw records considered -> at the cap of 3
    c.post("/v1/ns/acme/events", json={"events": [{"content": "x", "session_id": "s1"}]})
    r = c.post("/v1/ns/acme/sessions/s1/close")
    assert r.status_code == 200
    p = period_of(time.time())
    assert hosted.store.rollup(org, "extractions_our_key", p) == r.json()["raw_considered"] >= 1


def test_paid_overage_is_allowed_and_metered_as_billable(hosted):
    org = hosted.org("acme", plan="dev", stripe_customer_id="cus_acme")
    c = hosted.client(org, "acme")
    hosted.engine.rerank = RerankStage(_Scorer())
    hosted.engine.extractor = _KeyedExtractor()
    for i in range(3):
        assert _mem(c, "acme", f"kumquat fact {i}").status_code == 201
    for i in range(3):  # included 1, then overage - never refused
        assert _search(c, "acme", f"kumquat {i}").status_code == 200
    c.post("/v1/ns/acme/events", json={"events": [{"content": f"raw {i}", "session_id": "s9"} for i in range(4)]})
    for _ in range(2):  # the soft extraction limit (2) never refuses either
        assert c.post("/v1/ns/acme/sessions/s9/close").status_code == 200
    p = period_of(time.time())
    roll = hosted.store.rollups(org, p)
    assert roll["reranked_searches"] == {"quantity": 3, "billable": 2}
    ext = roll["extractions_our_key"]
    assert ext["quantity"] >= 4 and ext["billable"] == ext["quantity"] - 2
    # searches are hard-capped (not metered) on dev: nothing billable
    assert roll["searches"]["billable"] == 0
    usage = c.get("/v1/billing/usage").json()
    assert usage["plan"] == "dev"
    assert usage["meters"]["reranked_searches"]["billable"] == 2
    assert usage["meters"]["reranked_searches"]["metered"] is True


def test_grace_period_then_read_only_but_searches_and_deletes_work(hosted):
    now = time.time()
    org = hosted.org("acme", plan="dev", stripe_customer_id="cus_acme", status="past_due",
                     grace_until=int(now + 7 * 86400))
    c = hosted.client(org, "acme")
    rid = _mem(c, "acme", "written during grace").json()["id"]  # inside the grace period
    assert rid
    hosted.h.metering.clock = lambda: now + 8 * 86400  # the grace period is over
    r = _mem(c, "acme", "blocked write")
    assert r.status_code == 402 and r.json()["code"] == "payment_required"
    assert r.json()["grace_until"] == int(now + 7 * 86400)
    assert c.post("/v1/ns/acme/events", json={"events": [{"content": "x"}]}).status_code == 402
    assert c.post("/v1/ns/acme/sessions/s1/close").status_code == 402
    assert _search(c, "acme", "written during grace").status_code == 200
    assert c.get(f"/v1/ns/acme/memories/{rid}").status_code == 200
    assert c.post("/v1/ns/acme/export").status_code == 200
    assert c.delete(f"/v1/ns/acme/memories/{rid}").status_code == 200
    f = c.post("/v1/ns/acme/forget", json={"query": "anything", "confirm": True})
    assert f.status_code == 200
    assert c.get("/v1/billing/usage").json()["read_only"] is True
    # payment recovered -> writable again
    hosted.store.update_org(org, status="active", grace_until=None)
    assert _mem(c, "acme", "writable again").status_code == 201


# ------------------------------------------------------------------ isolation


def test_tenant_isolation_org_a_cannot_read_or_bill_org_b(hosted):
    a, b = hosted.org("alpha"), hosted.org("beta")
    ca, cb = hosted.client(a, "alpha-ns"), hosted.client(b, "beta-ns")
    rid = _mem(cb, "beta-ns", "beta secret").json()["id"]
    assert ca.get(f"/v1/ns/beta-ns/memories/{rid}").status_code == 403
    assert _search(ca, "beta-ns", "beta secret").status_code == 403
    assert _mem(ca, "beta-ns", "planted").status_code == 403
    assert ca.delete(f"/v1/ns/beta-ns/memories/{rid}").status_code == 403
    assert ca.get("/v1/ns/beta-ns/stats").status_code == 403
    # a bearer re-labelled with B's namespace does not authenticate at all
    kid_secret = ca.key.split("_", 2)[2]
    forged = TestClient(hosted.app)
    forged.headers["Authorization"] = f"Bearer memd_beta-ns_{kid_secret}"
    assert forged.get("/v1/ns/beta-ns/stats").status_code == 401
    # org A cannot mint a key into org B's namespace
    with pytest.raises(OwnershipError):
        hosted.store.create_key(a, "beta-ns")
    # A's traffic is billed to A only; B's usage view never shows it
    assert _search(ca, "alpha-ns", "anything").status_code == 200
    ua, ub = ca.get("/v1/billing/usage").json(), cb.get("/v1/billing/usage").json()
    assert ua["org_id"] == a and ub["org_id"] == b
    assert ua["meters"]["searches"]["used"] == 1 and ub["meters"]["searches"]["used"] == 0
    assert {e["org_id"] for e in hosted.store.events() if e["ns"] == "alpha-ns"} == {a}
    assert {e["org_id"] for e in hosted.store.events() if e["ns"] == "beta-ns"} == {b}
    # the billing body cannot name another org: the org is the key's
    r = ca.post("/v1/billing/checkout", json={"plan": "dev", "org_id": b})
    assert r.status_code == 503 and r.json()["code"] == "billing_not_configured"


def test_scopes_split_data_and_billing_keys(hosted):
    org = hosted.org("acme")
    data_only = hosted.client(org, "acme", scopes="memory")
    billing_only = hosted.client(org, "acme", scopes="billing")
    assert data_only.get("/v1/billing/usage").status_code == 403
    assert billing_only.get("/v1/billing/usage").status_code == 200
    assert billing_only.get("/v1/ns/acme/stats").status_code == 403
    assert data_only.get("/v1/ns/acme/stats").status_code == 200


def test_keys_are_hashed_at_rest_and_revocable(hosted, tmp_path):
    org = hosted.org("acme")
    c = hosted.client(org, "acme")
    raw = open(hosted.store.path, "rb").read()
    for suffix in ("", "-wal"):
        p = hosted.store.path + suffix
        if os.path.exists(p):
            assert c.key.rsplit("_", 1)[1].encode() not in open(p, "rb").read()
    assert raw  # the db exists and holds the key's row, not its secret
    kid = c.key.split("_")[2]
    assert hosted.h.keystore.revoke(kid)
    assert c.get("/v1/ns/acme/stats").status_code == 401


def test_operator_key_is_not_metered(tmp_path):
    app = create_app(data_dir=str(tmp_path / "d"), admin_key="operator-secret", hosted=True,
                     plans=Plans(SMALL), billing_config=BillingConfig.from_env({}))
    try:
        c = TestClient(app)
        c.headers["Authorization"] = "Bearer operator-secret"
        for i in range(5):  # past every free cap: the operator has no org
            assert _search(c, "anyns", f"q{i}").status_code == 200
        assert app.state.hosted.store.events() == []
        assert c.get("/v1/billing/usage").json()["code"] == "no_org"
    finally:
        app.state.engine.close()


# ------------------------------------------------------------ sk_live guard


@pytest.mark.parametrize("key", ["sk_live_abc123", "rk_live_abc123"])
def test_live_key_is_refused_without_the_explicit_opt_in(key, tmp_path, monkeypatch):
    with pytest.raises(LiveKeyRefused):
        BillingConfig.from_env({"MEMD_STRIPE_SECRET_KEY": key})
    guard_live_key(key, {"MEMD_ALLOW_LIVE_BILLING": "1"})  # explicit opt-in passes
    guard_live_key("sk_test_abc", {})
    # the server refuses to start in hosted mode...
    monkeypatch.setenv("MEMD_STRIPE_SECRET_KEY", key)
    monkeypatch.delenv("MEMD_ALLOW_LIVE_BILLING", raising=False)
    with pytest.raises(LiveKeyRefused):
        create_app(data_dir=str(tmp_path / "h"), hosted=True)
    # ...and self-hosted mode never looks at billing config at all
    app = create_app(data_dir=str(tmp_path / "s"), hosted=False)
    app.state.engine.close()


def test_hosted_cli_refuses_a_live_key(tmp_path):
    env = {**os.environ, "PYTHONPATH": SRC, "MEMD_HOSTED": "1", "MEMD_DATA": str(tmp_path / "d"),
           "MEMD_STRIPE_SECRET_KEY": "sk_live_nope"}
    env.pop("MEMD_ALLOW_LIVE_BILLING", None)
    r = subprocess.run([sys.executable, "-c", "from memd.cli import create_app_from_env; create_app_from_env()"],
                       env=env, capture_output=True, text=True, timeout=120)
    assert r.returncode != 0 and "LIVE Stripe key" in r.stderr


# ---------------------------------------------------- embedded never imports stripe


def test_embedded_and_self_hosted_never_import_stripe(tmp_path):
    code = textwrap.dedent(f"""
        import sys
        import memd
        m = memd.Memory({str(tmp_path / 'emb')!r})
        m.add("hello", user_id="u")
        m.search("hello", user_id="u")
        m.close()
        from memd.server.http import create_app
        app = create_app(data_dir={str(tmp_path / 'srv')!r})
        app.state.engine.close()
        bad = [k for k in sys.modules if k == "stripe" or k.startswith("stripe.")
               or k.startswith("memd.hosted.")]
        print("LOADED", bad)
        assert not bad, bad
    """)
    env = {**os.environ, "PYTHONPATH": SRC, "MEMD_HOSTED": "", "MEMD_STRIPE_SECRET_KEY": "sk_live_ignored"}
    r = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True, timeout=180)
    assert r.returncode == 0, r.stderr[-2000:]


# ------------------------------------------------------- CLI: keys join orgs


def test_cli_key_create_attaches_to_an_org_in_hosted_mode(tmp_path, capsys):
    import json

    from memd.cli import main

    data = str(tmp_path / "d")
    assert main(["org", "create", "--name", "acme", "--data", data]) == 0
    org = json.loads(capsys.readouterr().out)["org"]
    assert main(["key", "create", "--ns", "acme", "--hosted", "--data", data]) == 2  # --org required
    capsys.readouterr()
    assert main(["key", "create", "--ns", "acme", "--hosted", "--org", org, "--scopes", "memory,billing",
                 "--data", data]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["org"] == org and out["scopes"] == ["billing", "memory"]
    other = AdminStore.for_data_root(data).create_org("other")
    assert main(["key", "create", "--ns", "acme", "--hosted", "--org", other, "--data", data]) == 1
    # non-hosted `memd key create` is unchanged: the keys.toml.json store
    assert main(["key", "create", "--ns", "plain", "--data", data]) == 0
    assert json.loads(capsys.readouterr().out.split("\n# ")[0])["namespace"] == "plain"
    assert os.path.exists(os.path.join(data, "keys.toml.json"))
    # ...and its keys can be adopted into an org, keeping the key string
    assert main(["key", "migrate", "--hosted", "--org", org, "--data", data]) == 0
    assert '"migrated": 1' in capsys.readouterr().out
    assert any(k["ns"] == "plain" and k["org_id"] == org for k in AdminStore.for_data_root(data).list_keys())


# ---------------------------------------------- usage ledger: crash safety


SERVER = r"""
import os, sys
sys.path.insert(0, {src!r})
import uvicorn
from memd.hosted.billing import BillingConfig
from memd.hosted.plans import Plans
from memd.server.http import create_app
app = create_app(data_dir=sys.argv[1], hosted=True, plans=Plans(),
                 billing_config=BillingConfig.from_env({{}}))
uvicorn.run(app, host="127.0.0.1", port=int(sys.argv[2]), log_level="warning")
"""


@pytest.mark.parametrize("kill_after_acks", [3, 11, 24])
def test_usage_ledger_survives_sigkill_between_op_and_ack(tmp_path, kill_after_acks):
    """SIGKILL the server mid-stream. Every ACKNOWLEDGED operation must be in
    the ledger (the event commits before the response), no event may exist
    beyond the operations actually sent (at most the in-flight one, whose
    ack was lost - the at-least-once side), and ledger and rollup agree
    (one transaction)."""
    from tests.hosted_helpers import free_port

    data = str(tmp_path / "data")
    store = AdminStore.for_data_root(data)
    org = store.create_org("acme", plan="scale")
    key, _ = store.create_key(org, "acme")
    store.close()
    port = free_port()
    env = {**os.environ, "MEMD_EMBEDDER": "hash", "MEMD_BILLING_JOBS": "0"}
    child = subprocess.Popen([sys.executable, "-c", SERVER.format(src=SRC), data, str(port)],
                             env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    sent = {"searches": 0, "writes": 0}
    acked = {"searches": 0, "writes": 0}
    try:
        base = f"http://127.0.0.1:{port}"
        deadline = time.time() + 90
        while time.time() < deadline:
            try:
                if httpx.get(f"{base}/health", timeout=1).status_code == 200:
                    break
            except httpx.HTTPError:
                time.sleep(0.2)
        else:
            pytest.fail("server did not start: " + child.stderr.read().decode()[-1000:])
        hdr = {"Authorization": f"Bearer {key}"}
        stop = threading.Event()

        def killer():
            while not stop.is_set():
                if sum(acked.values()) >= kill_after_acks:
                    time.sleep(0.003 * (kill_after_acks % 5))  # land at varying points
                    os.kill(child.pid, signal.SIGKILL)
                    return
                time.sleep(0.0005)

        t = threading.Thread(target=killer, daemon=True)
        t.start()
        with httpx.Client(base_url=base, headers=hdr, timeout=5) as http:
            i = 0
            while child.poll() is None and i < 10_000:
                meter = "writes" if i % 2 else "searches"
                sent[meter] += 1
                try:
                    if meter == "writes":
                        r = http.post("/v1/ns/acme/memories", json={"content": f"fact {i}", "user_id": "u"})
                    else:
                        r = http.post("/v1/ns/acme/search", json={"query": f"fact {i}", "user_id": "u"})
                except httpx.HTTPError:
                    break  # killed mid-request: sent, never acknowledged
                if r.status_code in (200, 201):
                    acked[meter] += 1
                i += 1
        stop.set()
        child.wait(timeout=30)
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=30)
    assert child.returncode == -signal.SIGKILL
    assert sum(acked.values()) >= kill_after_acks

    con = sqlite3.connect(os.path.join(data, "admin", "admin.sqlite3"))
    assert con.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    for meter in ("searches", "writes"):
        n = con.execute("SELECT COUNT(*) FROM usage_event WHERE meter = ?", (meter,)).fetchone()[0]
        total = con.execute("SELECT COALESCE(SUM(quantity), 0) FROM usage_event WHERE meter = ?",
                            (meter,)).fetchone()[0]
        roll = con.execute("SELECT COALESCE(SUM(quantity), 0) FROM usage_rollup WHERE meter = ?",
                           (meter,)).fetchone()[0]
        assert acked[meter] <= n <= sent[meter], (meter, acked, n, sent)
        assert total == roll == n
    ids = [r[0] for r in con.execute("SELECT id FROM usage_event")]
    assert len(ids) == len(set(ids))
    con.close()
