"""Shared helpers for the hosted-mode / billing tests. Fully offline:

- FakeStripe: a tiny in-process HTTP server speaking the three Stripe
  endpoints the meter push and reconciliation use, WITH Stripe's
  idempotency-key semantics (a replayed key returns the first response and
  is not counted twice). The real `stripe` SDK talks to it over HTTP.
- stripe_mock_url(): stripe/stripe-mock in docker for the full API surface
  (stateless fixtures; no idempotency), or MEMD_TEST_STRIPE_MOCK=<url>.
- sign(): a Stripe-Signature header for a payload, as Stripe computes it.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import shutil
import socket
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

WEBHOOK_SECRET = "whsec_memd_test_only"


def sign(payload: str | bytes, secret: str = WEBHOOK_SECRET, t: int | None = None) -> str:
    body = payload.decode() if isinstance(payload, bytes) else payload
    t = int(time.time()) if t is None else t
    mac = hmac.new(secret.encode(), f"{t}.{body}".encode(), hashlib.sha256).hexdigest()
    return f"t={t},v1={mac}"


def event(etype: str, obj: dict, *, eid: str | None = None, created: int | None = None) -> dict:
    return {"id": eid or f"evt_{os.urandom(6).hex()}", "object": "event", "type": etype,
            "created": int(time.time()) if created is None else created, "api_version": "2025-01-01",
            "livemode": False, "data": {"object": obj}}


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class FakeStripe:
    """The Stripe endpoints memd uses, over real HTTP for the real SDK:

    - POST /v1/billing/meter_events, GET /v1/billing/meters and
      GET /v1/billing/meters/{id}/event_summaries, idempotent like Stripe -
      including Stripe's LIMIT: a key is remembered for `key_ttl_s`
      (Stripe: ~24 h) on the fake's own `clock`, after which the same key
      is a new event;
    - GET /v1/subscriptions/{id} and GET /v1/customers/{id} from the
      `subscriptions` / `customers` dicts the test fills in;
    - POST /v1/customers, /v1/checkout/sessions, /v1/billing_portal/sessions
      (recorded in `created`)."""

    def __init__(self, event_names: dict[str, str] | None = None):
        self.lock = threading.Lock()
        self.by_key: dict[str, dict] = {}   # idempotency key -> accepted event (latest)
        self.events: list[dict] = []        # every accepted (billed) event
        self.requests: list[dict] = []      # every request, replays included
        self.replays = 0
        self.expired_key_reuse = 0          # a key re-sent after Stripe forgot it: billed twice
        self.clock = time.time
        self.key_ttl_s: float | None = None
        self.down = False                    # outage: every request answers 503
        self.fail_after_accept = False       # accept, then answer 500 (the response is lost)
        self.drop: set[str] = set()          # identifiers to "lose" (drift tests)
        self.on_meter_event = None           # hook(params) -> "kill_before" | None; runs pre-response
        self.after_accept = None             # hook(params) after the event is stored, pre-response
        self.meters = {f"mtr_{i}": name for i, name in enumerate(sorted((event_names or {}).values()))}
        self.subscriptions: dict[str, dict] = {}
        self.customers: dict[str, dict] = {}
        self.created: list[tuple[str, dict]] = []  # (kind, form) of every create call
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _send(self, code: int, body: dict):
                raw = json.dumps(body).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_POST(self):
                n = int(self.headers.get("Content-Length") or 0)
                form = {k: v[0] for k, v in parse_qs(self.rfile.read(n).decode()).items()}
                if fake.down:
                    return self._send(503, {"error": {"message": "outage", "type": "api_error"}})
                path = urlparse(self.path).path
                if path in ("/v1/customers", "/v1/checkout/sessions", "/v1/billing_portal/sessions"):
                    kind = path.rsplit("/", 2)[-2] if path.endswith("sessions") else "customer"
                    with fake.lock:
                        fake.created.append((kind, form))
                        n_created = len(fake.created)
                    if kind == "customer":
                        cid = f"cus_fake{n_created}"
                        meta = {k[len("metadata["):-1]: v for k, v in form.items() if k.startswith("metadata[")}
                        fake.customers[cid] = {"id": cid, "object": "customer", "metadata": meta}
                        return self._send(200, fake.customers[cid])
                    obj = "checkout.session" if kind == "checkout" else "billing_portal.session"
                    return self._send(200, {"id": f"{'cs' if kind == 'checkout' else 'bps'}_fake{n_created}",
                                            "object": obj, "url": f"https://stripe.test/{kind}/{n_created}",
                                            "customer": form.get("customer"),
                                            "expires_at": int(form.get("expires_at") or 0) or None})
                if path != "/v1/billing/meter_events":
                    return self._send(404, {"error": {"message": "unknown path"}})
                key = self.headers.get("Idempotency-Key")
                params = {"event_name": form.get("event_name"), "identifier": form.get("identifier"),
                          "customer": form.get("payload[stripe_customer_id]"),
                          "value": form.get("payload[value]"), "timestamp": int(form.get("timestamp") or 0),
                          "idempotency_key": key}
                if fake.on_meter_event is not None and fake.on_meter_event(params) == "kill_before":
                    return  # the pusher dies before Stripe accepted anything
                with fake.lock:
                    fake.requests.append(params)
                    now = fake.clock()
                    known = fake.by_key.get(key) if key else None
                    if known is not None and fake.key_ttl_s is not None and now - known["_at"] > fake.key_ttl_s:
                        fake.expired_key_reuse += 1
                        known = None
                    if known is not None:
                        fake.replays += 1
                        stored = known
                    else:
                        stored = dict(params, _at=now)
                        fake.events.append(stored)
                        if key:
                            fake.by_key[key] = stored
                if fake.after_accept is not None:
                    fake.after_accept(params)
                if fake.fail_after_accept:
                    return self._send(500, {"error": {"message": "lost response", "type": "api_error"}})
                self._send(200, {"object": "billing.meter_event", "event_name": stored["event_name"],
                                 "identifier": stored["identifier"], "livemode": False,
                                 "payload": {"stripe_customer_id": stored["customer"], "value": stored["value"]},
                                 "timestamp": stored["timestamp"], "created": int(time.time())})

            def do_GET(self):
                if fake.down:
                    return self._send(503, {"error": {"message": "outage", "type": "api_error"}})
                u = urlparse(self.path)
                q = {k: v[0] for k, v in parse_qs(u.query).items()}
                parts = u.path.strip("/").split("/")
                if len(parts) == 3 and parts[:2] in (["v1", "subscriptions"], ["v1", "customers"]):
                    table = fake.subscriptions if parts[1] == "subscriptions" else fake.customers
                    if parts[2] not in table:
                        return self._send(404, {"error": {"message": "No such object", "type": "invalid_request_error",
                                                          "code": "resource_missing"}})
                    return self._send(200, table[parts[2]])
                if u.path == "/v1/billing/meters":
                    return self._send(200, {"object": "list", "has_more": False, "url": u.path, "data": [
                        {"object": "billing.meter", "id": mid, "event_name": name, "status": "active"}
                        for mid, name in fake.meters.items()]})
                if len(parts) == 5 and parts[:3] == ["v1", "billing", "meters"] and parts[4] == "event_summaries":
                    name = fake.meters.get(parts[3])
                    start, end = int(q.get("start_time", 0)), int(q.get("end_time", 0))
                    with fake.lock:
                        vals = [e for e in fake.events
                                if e["event_name"] == name and e["customer"] == q.get("customer")
                                and start <= e["timestamp"] < end and e["identifier"] not in fake.drop]
                    total = sum(float(e["value"]) for e in vals)
                    return self._send(200, {"object": "list", "has_more": False, "url": u.path, "data": [
                        {"object": "billing.meter_event_summary", "id": "mtrusg_1", "meter": parts[3],
                         "aggregated_value": total, "start_time": start, "end_time": end, "livemode": False}]})
                return self._send(404, {"error": {"message": "unknown path"}})

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def accepted(self) -> list[dict]:
        with self.lock:
            return list(self.events)

    def total(self, event_name: str | None = None) -> float:
        return sum(float(e["value"]) for e in self.accepted()
                   if event_name is None or e["event_name"] == event_name)

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


def _docker_ok() -> bool:
    if not shutil.which("docker"):
        return False
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=20).returncode == 0
    except Exception:
        return False


class StripeMock:
    """stripe/stripe-mock: MEMD_TEST_STRIPE_MOCK=<url> reuses a running one,
    =off skips; otherwise a container is started on a free port (and
    removed by stop())."""

    def __init__(self):
        self.url: str | None = None
        self.container: str | None = None
        self.skip_reason: str | None = None

    def start(self) -> "StripeMock":
        pre = os.environ.get("MEMD_TEST_STRIPE_MOCK", "")
        if pre == "off":
            self.skip_reason = "MEMD_TEST_STRIPE_MOCK=off"
            return self
        if pre:
            self.url = pre.rstrip("/")
            return self
        if not _docker_ok():
            self.skip_reason = "docker unavailable: stripe-mock end-to-end skipped"
            return self
        port = free_port()
        name = f"memd-test-stripe-mock-{os.getpid()}-{port}"
        r = subprocess.run(["docker", "run", "-d", "--rm", "--name", name, "-p", f"127.0.0.1:{port}:12111",
                            "stripe/stripe-mock:latest"], capture_output=True, text=True, timeout=300)
        if r.returncode != 0:
            self.skip_reason = f"could not start stripe-mock: {r.stderr.strip()[:200]}"
            return self
        self.container = name
        url = f"http://127.0.0.1:{port}"
        import httpx

        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                if httpx.get(f"{url}/v1/billing/meters", auth=("sk_test_123", ""), timeout=2).status_code == 200:
                    self.url = url
                    return self
            except Exception:
                pass
            time.sleep(0.5)
        self.stop()
        self.skip_reason = "stripe-mock did not become ready"
        return self

    def stop(self) -> None:
        if self.container:
            subprocess.run(["docker", "rm", "-f", self.container], capture_output=True, timeout=60)
            self.container = None
