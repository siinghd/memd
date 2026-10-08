"""Write forwarding: several processes on one data root.

One process holds a namespace (its lock file on a local root, its lease on
S3). Another process opening it used to get NamespaceBusyError; now its
writes - and strong reads - run in the holder (memd.engine.forward), its
eventual reads on its own read replica, and it takes the namespace over
when the holder goes away. What must hold, across processes:

  - every acknowledged write is present exactly once (never lost to a
    failover, never applied twice by a retry), deletes - hard ones and the
    physical purge included - are honoured, and a forwarding process reads
    its own writes;
  - a holder SIGKILLed mid-stream: the forwarders fail over, nothing lost
    or duplicated;
  - only a process holding the root's secret is served;
  - forwarding="off" keeps NamespaceBusyError.

The S3 variants need MEMD_TEST_S3_ENDPOINT (MinIO); skipped otherwise.
"""
import json
import os
import signal
import socket
import subprocess
import sys
import time
import uuid
from collections import Counter

import pytest

SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
sys.path.insert(0, SRC)

from memd.core.schema import records_from_jsonl  # noqa: E402
from memd.engine import forward as fw  # noqa: E402
from memd.engine.memory import Memory  # noqa: E402
from memd.metrics import METRICS  # noqa: E402
from memd.storage.engine import NamespaceBusyError, owner_lock_path, read_owner_lock  # noqa: E402

WORKER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "forward_worker.py")
ENDPOINT = os.environ.get("MEMD_TEST_S3_ENDPOINT")
BUCKET = os.environ.get("MEMD_TEST_S3_BUCKET", "memd-engine")
KEY = os.environ.get("MEMD_TEST_S3_KEY", "minioadmin")
SECRET = os.environ.get("MEMD_TEST_S3_SECRET", "minioadmin")
NS = "shared"
PURGE = {"hard_delete_deadline_ms": 50, "rate_max_writes": 10 ** 9}


def _counter(name: str) -> float:
    return sum(e["value"] for e in METRICS.snapshot()["counters"].get(name, []))


def _child(code: str, *args: str, timeout: float = 120) -> subprocess.CompletedProcess:
    pre = f"import sys; sys.path.insert(0, {SRC!r})\n"
    return subprocess.run([sys.executable, "-c", pre + code, *args], capture_output=True,
                          text=True, timeout=timeout)


class Fleet:
    """Worker processes (tests/forward_worker.py) on one data root."""

    def __init__(self, tmp_path, root: str, config: dict, *, encrypt: bool = False,
                 per_worker=None):
        self.tmp = tmp_path
        self.root = root
        self.config = dict(config)
        self.encrypt = encrypt
        self.per_worker = per_worker or (lambda tag: {})
        self.go_path = str(tmp_path / "go")
        self.procs: dict[str, subprocess.Popen] = {}

    def spawn(self, tag: str, n: int, **spec) -> subprocess.Popen:
        cfg = dict(self.config, **self.per_worker(tag))
        body = {"root": self.root, "config": cfg, "namespace": NS, "encrypt": self.encrypt,
                "tag": tag, "n": n, "out": self.out(tag), "ready": str(self.tmp / f"{tag}.ready"),
                "go": self.go_path}
        body.update(spec)
        path = self.tmp / f"{tag}.json"
        path.write_text(json.dumps(body))
        err = open(self.tmp / f"{tag}.err", "w")
        p = subprocess.Popen([sys.executable, WORKER, str(path)], stdout=err, stderr=err)
        self.procs[tag] = p
        return p

    def out(self, tag: str) -> str:
        return str(self.tmp / f"{tag}.log")

    def log(self, tag: str) -> list[dict]:
        try:
            with open(self.out(tag)) as f:
                return [json.loads(ln) for ln in f if ln.strip()]
        except FileNotFoundError:
            return []

    def stderr(self, tag: str) -> str:
        return (self.tmp / f"{tag}.err").read_text()[-4000:]

    def wait_ready(self, *tags: str, timeout: float = 120) -> None:
        deadline = time.monotonic() + timeout
        for t in tags:
            while not (self.tmp / f"{t}.ready").exists():
                p = self.procs[t]
                assert p.poll() is None, f"{t} exited {p.returncode}: {self.stderr(t)}"
                assert time.monotonic() < deadline, f"{t} never opened: {self.stderr(t)}"
                time.sleep(0.02)

    def go(self) -> None:
        open(self.go_path, "w").close()

    def acked_adds(self, tag: str) -> int:
        return sum(1 for e in self.log(tag) if e["op"] == "add")

    def finish(self, *tags: str, timeout: float = 300) -> None:
        for t in tags:
            p = self.procs[t]
            try:
                p.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                p.send_signal(signal.SIGUSR1)      # its threads' stacks, into its .err
                time.sleep(1)
                p.kill()
                raise AssertionError(f"{t} hung: {self.stderr(t)}")
            assert p.returncode == 0, f"{t} exited {p.returncode}: {self.stderr(t)}"

    def kill_all(self) -> None:
        for p in self.procs.values():
            if p.poll() is None:
                p.kill()
                p.wait()


def _acked(fleet: Fleet, tags) -> tuple[dict, dict, list]:
    adds, deletes, violations = {}, {}, []
    for t in tags:
        for e in fleet.log(t):
            if e["op"] == "add":
                adds[e["id"]] = e["content"]
            elif e["op"] == "delete":
                deletes[e["id"]] = e
            elif e["op"] == "violation":
                violations.append((t, e))
    return adds, deletes, violations


def _verify(m: Memory, adds: dict, deletes: dict, raw_bytes) -> None:
    """Every acked write present exactly once, every acked delete honoured,
    every acked hard delete's text physically gone from the store."""
    lines = [json.loads(ln) for ln in m.export_jsonl(namespace=NS).splitlines() if ln.strip()]
    by_content = Counter(r["content"] for r in lines)
    assert not [c for c, k in by_content.items() if k > 1], "a write was applied twice"
    exported = {r["id"] for r in lines}
    for rid, content in adds.items():
        if rid in deletes:
            assert rid not in exported, f"deleted record {rid} still exported"
            assert m.get(rid, namespace=NS) is None, f"deleted record {rid} still served"
        else:
            assert rid in exported, f"acked write {rid} ({content!r}) lost"
            assert by_content[content] == 1
    # the same record id written twice (a retry run again) into the log
    ns = m.engine.namespace(NS)
    per_id = Counter()
    for fr in ns._read_frames(ns.wal_key):
        for r in records_from_jsonl(ns._decrypt_frame(fr)):
            per_id[r.id] += 1
    assert not [i for i, k in per_id.items() if k > 1], "a record id appears in two log frames"
    blob = raw_bytes()
    for rid, e in deletes.items():
        if e["hard"]:
            assert e["content"].encode() not in blob, f"hard-deleted {rid} not purged"


def _local_raw(root: str):
    def raw() -> bytes:
        d = os.path.join(root, "store", "ns", NS)
        out = []
        for fn in os.listdir(d):
            if fn not in ("manifest.json", ".owner") and os.path.isfile(os.path.join(d, fn)):
                with open(os.path.join(d, fn), "rb") as f:
                    out.append(f.read())
        return b"".join(out)
    return raw


def _open_after(root: str, config: dict, **kw) -> Memory:
    """The verifier: every worker is gone; a due purge runs at open."""
    time.sleep(0.1)    # past every hard delete's deadline
    m = Memory(root, namespace=NS, config=dict(config, forwarding="off"), **kw)
    m.flush()
    return m


# ------------------------------------------------------------------ basics


def test_a_second_process_forwards_instead_of_failing(tmp_path):
    """Failing-first: a second process opening a held namespace raised
    NamespaceBusyError. Now its writes run in the holder - quarantine,
    supersedence and the ledger stay the holder's - and it reads them
    back (strong reads go to the holder too)."""
    root = str(tmp_path / "d")
    a = Memory(root, namespace=NS, encrypt=False, config=PURGE)
    try:
        a.add("held by the first process", user_id="u1")
        out = _child(r'''
import json, sys
from memd.engine.memory import Memory, forget_fingerprint
b = Memory(sys.argv[1], namespace="shared", encrypt=False)
r = {"holds": b.ns is not None}
rid = b.add("we deploy with make ship, never CI", user_id="u1")[0]
r["get"] = b.get(rid)["content"]
r["search"] = [h.id for h in b.search("how do we deploy", user_id="u1").items]
r["rid"] = rid
r["remember"] = b.remember("the deploy target is staging", entity_keys=["deploy.target"], user_id="u1")
r["remember2"] = b.remember("the deploy target is prod", entity_keys=["deploy.target"], user_id="u1")
r["events"] = b.add_events([{"content": "turn one", "user_id": "u1", "session_id": "s1"},
                            {"content": "turn two", "user_id": "u1", "session_id": "s1",
                             "role": "assistant"}])
r["close"] = b.close_session("s1", user_id="u1")
ids = b.find_ids("turn", user_id="u1")
r["forgot"] = sorted(b.forget("turn", user_id="u1", expected=forget_fingerprint(ids)))
r["stats"] = b.stats()["records"]
r["export"] = len(b.export_jsonl().splitlines())
r["hard"] = b.delete(rid, hard=True)
r["after"] = b.get(rid)
b.close()
print(json.dumps(r))
''', root)
        assert out.returncode == 0, out.stderr[-3000:]
        r = json.loads(out.stdout.strip().splitlines()[-1])
        assert r["holds"] is False
        assert r["get"] == "we deploy with make ship, never CI"
        assert r["rid"] in r["search"], "read-your-writes over forwarding"
        assert set(r["events"]) <= set(r["forgot"])
        assert r["after"] is None and r["hard"] is True
        # it all happened in the holder's namespace and ledger: the second
        # remember superseded the first there (consolidation is the writer's)
        assert a.get(r["remember"])["time"]["superseded_by"] == r["remember2"]
        assert a.get(r["remember2"])["content"] == "the deploy target is prod"
        assert a.get(r["rid"]) is None
        actions = [e["action"] for e in a.audit.read()]
        assert "hard_delete" in actions and "forget" in actions and "remember" in actions
    finally:
        a.close()


def test_a_forwarded_destroy_then_the_namespace_moves(tmp_path):
    """A crypto-shred forwarded to the holder runs there (its ledger records
    it); the holder no longer has the namespace open, so the next write
    takes it - in the forwarding process, now its writer."""
    root = str(tmp_path / "d")
    a = Memory(root, namespace=NS, encrypt=True)
    try:
        a.add("doomed record", user_id="u1", namespace="side")
        assert a.engine.holds("side")
        out = _child(r'''
import json, sys
from memd.engine.memory import Memory
b = Memory(sys.argv[1], namespace="shared")
r = {"destroyed": b.destroy_namespace(namespace="side")}
r["after"] = [h.content for h in b.search("doomed", user_id="u1", namespace="side").items]
b.add("a new life", user_id="u1", namespace="side")
r["holds"] = b.engine.holds("side")
r["count"] = len(b.export_jsonl(namespace="side").splitlines())
b.close()
print(json.dumps(r))
''', root)
        assert out.returncode == 0, out.stderr[-3000:]
        r = json.loads(out.stdout.strip().splitlines()[-1])
        assert r == {"destroyed": True, "after": [], "holds": True, "count": 1}, r
        assert any(e["action"] == "destroy_namespace" and e["target"] == "side" for e in a.audit.read())
        assert not a.engine.holds("side")
    finally:
        a.close()


def test_forwarding_off_keeps_namespace_busy_error(tmp_path):
    root = str(tmp_path / "d")
    a = Memory(root, namespace=NS, encrypt=False)
    try:
        out = _child(r'''
import sys
from memd.engine.memory import Memory
try:
    Memory(sys.argv[1], namespace="shared", encrypt=False, forwarding="off")
    print("OPENED")
except Exception as e:
    print(type(e).__name__)
''', root)
        assert out.stdout.strip() == "NamespaceBusyError", out.stdout + out.stderr[-2000:]
        # the environment says the same
        out = _child(r'''
import os, sys
os.environ["MEMD_FORWARDING"] = "off"
from memd.engine.memory import Memory
m = Memory(sys.argv[1], namespace="other", encrypt=False)
try:
    m.add("x", namespace="shared")
    print("ADDED")
except Exception as e:
    print(type(e).__name__)
m.close()
''', root)
        assert out.stdout.strip() == "NamespaceBusyError", out.stdout + out.stderr[-2000:]
    finally:
        a.close()


def test_a_holder_that_does_not_forward_still_refuses(tmp_path):
    """A holder with forwarding off (or an older memd) advertises no
    endpoint: the second process fails fast, as before."""
    root = str(tmp_path / "d")
    a = Memory(root, namespace=NS, encrypt=False, forwarding="off")
    try:
        out = _child(r'''
import sys, time
from memd.engine.memory import Memory
from memd.storage.engine import NamespaceBusyError
t0 = time.monotonic()
try:
    Memory(sys.argv[1], namespace="shared", encrypt=False)
    print("OPENED")
except NamespaceBusyError as e:
    print(type(e).__name__, round(time.monotonic() - t0, 1))
''', root)
        name, took = out.stdout.split()
        assert name == "ForwardingError", out.stdout + out.stderr[-2000:]
        assert float(took) < 5, "it waits for nothing"
    finally:
        a.close()


def test_the_lock_file_names_the_holder_endpoint(tmp_path):
    root = str(tmp_path / "d")
    a = Memory(root, namespace=NS, encrypt=False)
    try:
        holder = read_owner_lock(owner_lock_path(a.engine.store.root, NS))
        ep = fw.parse_holder(holder)
        assert ep is not None and ep.port == a._fwd_server._sock.getsockname()[1]
        assert ep.endpoint_id == a._fwd_server.endpoint_id
        st = os.stat(os.path.join(root, fw.SECRET_FILE))
        assert st.st_mode & 0o077 == 0, "the forwarding secret is readable by others"
    finally:
        a.close()
    b = Memory(str(tmp_path / "e"), encrypt=False, forwarding="off")
    try:
        holder = read_owner_lock(owner_lock_path(b.engine.store.root, "default"))
        assert fw.parse_holder(holder) is None, "forwarding off advertises no endpoint"
    finally:
        b.close()


def test_eventual_reads_use_the_forwarders_own_replica(tmp_path):
    root = str(tmp_path / "d")
    a = Memory(root, namespace=NS, encrypt=False, config={"replica_refresh_s": 0.2})
    try:
        a.add("replicated everywhere", user_id="u1")
        out = _child(r'''
import json, sys, time
from memd.engine.memory import Memory
b = Memory(sys.argv[1], namespace="shared", encrypt=False, config={"replica_refresh_s": 0.2})
rid = b.add("written through the holder", user_id="u1")[0]
seen = None
for _ in range(100):
    r = b.search("written through the holder", user_id="u1", consistency="eventual")
    if rid in [h.id for h in r.items]:
        seen = r.served_by
        break
    time.sleep(0.05)
b.close()
print(json.dumps({"served_by": seen}))
''', root)
        assert out.returncode == 0, out.stderr[-3000:]
        assert json.loads(out.stdout.strip().splitlines()[-1])["served_by"] == "replica"
        # its replica reads are audited in its facade's ledger - the
        # holder's, which appended the entries it sent
        assert any(e["action"] == "replica_search" for e in a.audit.read())
    finally:
        a.close()


def _uvicorn_workers(tmp_path, forwarding: str) -> tuple:
    """`uvicorn --workers 3` on one data root: 80 writes and 80 searches, each
    on a new connection (spread over the workers). -> (write statuses,
    searches that found their record, forwarded calls the workers' /metrics
    count, the workers' log)."""
    httpx = pytest.importorskip("httpx")
    pytest.importorskip("uvicorn")
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    key = "admin-key-for-the-workers-test-0001"
    env = dict(os.environ, MEMD_DATA=str(tmp_path / f"data-{forwarding}"), MEMD_ADMIN_KEY=key,
               MEMD_EMBEDDER="hash", MEMD_RERANKER="none", MEMD_FORWARDING=forwarding,
               PYTHONPATH=SRC + os.pathsep + os.environ.get("PYTHONPATH", ""))
    err = open(tmp_path / f"uvicorn-{forwarding}.err", "w")
    p = subprocess.Popen([sys.executable, "-m", "uvicorn", "memd.cli:create_app_from_env", "--factory",
                          "--workers", "3", "--port", str(port), "--log-level", "warning"],
                         env=env, stdout=err, stderr=err)
    base, hdr = f"http://127.0.0.1:{port}", {"Authorization": f"Bearer {key}"}
    try:
        deadline = time.monotonic() + 60
        while True:
            try:
                if httpx.get(base + "/health", timeout=2).status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            assert time.monotonic() < deadline and p.poll() is None, \
                (tmp_path / f"uvicorn-{forwarding}.err").read_text()[-2000:]
            time.sleep(0.1)
        time.sleep(1.0)       # every worker up
        codes, found = Counter(), 0
        for i in range(80):
            with httpx.Client(base_url=base, headers=hdr, timeout=60) as c:
                codes[c.post("/v1/ns/acme/memories",
                             json={"content": f"worker record {i} tag{i}x", "user_id": "u1"}).status_code] += 1
        for i in range(80):
            with httpx.Client(base_url=base, headers=hdr, timeout=60) as c:
                r = c.post("/v1/ns/acme/search", json={"query": f"tag{i}x", "user_id": "u1"})
                found += r.status_code == 200 and any(f"tag{i}x" in it["content"] for it in r.json()["items"])
        forwarded = 0.0
        for _ in range(30):     # each worker's own counters, whichever answers
            with httpx.Client(base_url=base, headers=hdr, timeout=60) as c:
                for ln in c.get("/metrics").text.splitlines():
                    if ln.startswith("memd_forward_calls_total{") and 'outcome="ok"' in ln:
                        forwarded = max(forwarded, float(ln.rsplit(" ", 1)[1]))
        return codes, found, forwarded, (tmp_path / f"uvicorn-{forwarding}.err").read_text()
    finally:
        p.terminate()
        try:
            p.wait(timeout=60)
        except subprocess.TimeoutExpired:
            p.kill()


def test_uvicorn_workers_share_one_data_root(tmp_path):
    """The deployment the lock used to forbid: `uvicorn --workers N` on one
    data root. Forwarding off: every worker but the first fails to start
    (NamespaceBusyError on the server's own namespace). On: all three run,
    and the ones not holding a namespace forward to the one that does."""
    _codes, _found, _fwd, log = _uvicorn_workers(tmp_path, "off")
    assert "NamespaceBusyError" in log
    codes, found, forwarded, log = _uvicorn_workers(tmp_path, "auto")
    assert "NamespaceBusyError" not in log and "Traceback" not in log, log[-3000:]
    assert codes == Counter({201: 80}), codes
    assert found == 80
    assert forwarded > 0, "no worker forwarded a call"


# ------------------------------------------------------------------ the secret


def test_a_process_without_the_secret_is_refused(tmp_path):
    root = str(tmp_path / "d")
    a = Memory(root, namespace=NS, encrypt=False)
    try:
        a.add("only for processes with the secret", user_id="u1")
        before = a.stats()["records"]
        out = _child(r'''
import sys
from memd.engine.memory import Memory
from memd.storage.engine import NamespaceBusyError
try:
    Memory(sys.argv[1], namespace="shared", encrypt=False,
           config={"forward_secret": "not-the-secret-of-this-root"})
    print("OPENED")
except NamespaceBusyError as e:
    print(type(e).__name__)
''', root)
        assert out.stdout.strip() == "ForwardAuthError", out.stdout + out.stderr[-2000:]

        # a raw client: a wrong proof is refused before any call is read
        ep = fw.parse_holder(read_owner_lock(owner_lock_path(a.engine.store.root, NS)))
        fails = _counter("memd_forward_auth_failures_total")
        with socket.create_connection((ep.host, ep.port), timeout=5) as s:
            ch = fw._Channel(s, b"c")
            kind, hello = ch.recv(4096)
            ch.send(fw._HELLO, fw._jdump({"v": 1, "client": "intruder", "cnonce": "0" * 32,
                                          "proof": "f" * 64}))
            kind, body = ch.recv(4096)
            assert json.loads(body) == {"ok": False, "error": "auth"}
            ch.send(fw._REQUEST, fw._jdump({"rid": "r", "op": "add", "ns": NS,
                                            "args": {"content": "injected"}, "ids": None}))
            with pytest.raises((ConnectionError, OSError)):
                ch.recv()
        assert _counter("memd_forward_auth_failures_total") == fails + 1

        # the right secret, then a frame altered in flight: dropped, not run
        secret = open(os.path.join(root, fw.SECRET_FILE)).read().strip()
        client = fw.ForwardClient(fw.ForwardConfig(), secret, "tester")
        ch = client._connect(ep)
        body = fw._jdump({"rid": "r2", "op": "add", "ns": NS, "args": {"content": "tampered"},
                          "ids": None})
        mac = fw._hmac(ch.key, ch.out_tag, fw._SEQ.pack(ch.seq_out), fw._REQUEST, body)
        evil = body.replace(b"tampered", b"TAMPERED")
        ch.sock.sendall(fw._LEN.pack(1 + len(evil) + len(mac)) + fw._REQUEST + evil + mac)
        ch.sock.settimeout(5)
        with pytest.raises((ConnectionError, OSError)):
            ch.recv()
        ch.close()
        client.close()
        assert _counter("memd_forward_auth_failures_total") == fails + 2
        assert a.stats()["records"] == before, "a refused caller's call ran"
    finally:
        a.close()


# ------------------------------------------------------------------ idempotency


def test_a_retried_write_is_applied_once(tmp_path):
    """The same request id twice (a lost reply): answered from the first
    attempt. The same record ids under a new request id (the holder that
    ran the first attempt died, a new one replayed its log): recognised in
    the namespace, not written again."""
    root = str(tmp_path / "d")
    a = Memory(root, namespace=NS, encrypt=False)
    try:
        ep = fw.parse_holder(read_owner_lock(owner_lock_path(a.engine.store.root, NS)))
        secret = open(os.path.join(root, fw.SECRET_FILE)).read().strip()
        client = fw.ForwardClient(fw.ForwardConfig(), secret, "tester")
        rid = "01JRETRYRETRYRETRYRETRYRE1"
        req = {"rid": uuid.uuid4().hex, "op": "add", "ns": NS,
               "args": {"content": "exactly once please", "user_id": "u1"}, "ids": [rid]}
        repeats = _counter("memd_forward_repeats_total")
        r1 = client.call(ep, req, timeout_s=10)
        r2 = client.call(ep, req, timeout_s=10)
        assert r1 == r2 == {"ok": True, "result": [rid]}
        assert _counter("memd_forward_repeats_total") == repeats + 1
        r3 = client.call(ep, dict(req, rid=uuid.uuid4().hex), timeout_s=10)
        assert r3 == {"ok": True, "result": [rid]}
        client.close()
        lines = [json.loads(ln) for ln in a.export_jsonl(namespace=NS).splitlines()]
        assert [r["id"] for r in lines if r["content"] == "exactly once please"] == [rid]
        ns = a.engine.namespace(NS)
        frames = [r.id for fr in ns._read_frames(ns.wal_key)
                  for r in records_from_jsonl(ns._decrypt_frame(fr))]
        assert frames.count(rid) == 1
    finally:
        a.close()


def test_an_overloaded_holder_pushes_back_and_nothing_is_lost(tmp_path):
    """A holder running one forwarded call at a time, which waits 1 ms for a
    slot: most concurrent calls are answered "overloaded" - not run - and
    their callers retry until each is applied, once."""
    root = str(tmp_path / "d")
    a = Memory(root, namespace=NS, encrypt=False,
               config={"forward_max_inflight": 1, "forward_queue_wait_s": 0.001,
                       "rate_max_writes": 10 ** 9})
    try:
        refused = sum(e["value"] for e in METRICS.snapshot()["counters"].get("memd_forward_refused_total", [])
                      if e["labels"].get("reason") == "overloaded")
        out = _child(r'''
import json, sys, threading
from memd.engine.memory import Memory
b = Memory(sys.argv[1], namespace="shared", encrypt=False)
acked, errors = [], []
def run(k):
    for i in range(25):
        try:
            acked.append((b.add(f"pushed back {k}-{i}", user_id="u1")[0], f"pushed back {k}-{i}"))
        except Exception as e:
            errors.append(repr(e))
ts = [threading.Thread(target=run, args=(k,)) for k in range(8)]
[t.start() for t in ts]; [t.join() for t in ts]
b.close()
print(json.dumps({"acked": acked, "errors": errors}))
''', root)
        assert out.returncode == 0, out.stderr[-3000:]
        r = json.loads(out.stdout.strip().splitlines()[-1])
        assert not r["errors"] and len(r["acked"]) == 200
        now = sum(e["value"] for e in METRICS.snapshot()["counters"].get("memd_forward_refused_total", [])
                  if e["labels"].get("reason") == "overloaded")
        assert now > refused, "the holder never pushed back"
        contents = Counter(json.loads(ln)["content"] for ln in a.export_jsonl().splitlines())
        assert all(contents[c] == 1 for _rid, c in r["acked"])
    finally:
        a.close()


# ------------------------------------------------------------------ several processes


def _run_concurrent(tmp_path, root: str, config: dict, tags, *, per_worker=None, encrypt=False,
                    raw=None, n=60):
    fleet = Fleet(tmp_path, root, config, encrypt=encrypt, per_worker=per_worker)
    try:
        for t in tags:
            fleet.spawn(t, n, ryw=True, delete_every=5, hard_every=10, events_every=7)
        fleet.wait_ready(*tags)
        fleet.go()
        fleet.finish(*tags)
    finally:
        fleet.kill_all()
    adds, deletes, violations = _acked(fleet, tags)
    assert not violations, violations[:5]
    holders = [t for t in tags if fleet.log(t)[0]["holds"]]
    assert len(holders) == 1, f"exactly one process held the namespace at open: {holders}"
    assert len(adds) == len(tags) * (n + 2 * (n // 7)), "a worker lost acks"
    return fleet, adds, deletes


def test_two_processes_write_one_namespace(tmp_path):
    root = str(tmp_path / "d")
    _f, adds, deletes = _run_concurrent(tmp_path, root, PURGE, ["A", "B"])
    m = _open_after(root, PURGE, encrypt=False)
    try:
        _verify(m, adds, deletes, _local_raw(root))
    finally:
        m.close()


def test_three_processes_write_one_namespace(tmp_path):
    root = str(tmp_path / "d")
    _f, adds, deletes = _run_concurrent(tmp_path, root, PURGE, ["A", "B", "C"])
    m = _open_after(root, PURGE, encrypt=False)
    try:
        _verify(m, adds, deletes, _local_raw(root))
    finally:
        m.close()


def _sigkill_holder(tmp_path, root: str, config: dict, *, per_worker=None, encrypt=False):
    fleet = Fleet(tmp_path, root, config, encrypt=encrypt, per_worker=per_worker)
    try:
        fleet.spawn("A", 10 ** 6, sleep=0.01)
        fleet.wait_ready("A")
        fleet.go()
        assert fleet.log("A")[0]["holds"] is True
        for t in ("B", "C"):
            fleet.spawn(t, 120, ryw=True, delete_every=6, hard_every=12, sleep=0.004)
        fleet.wait_ready("B", "C")
        deadline = time.monotonic() + 120
        while fleet.acked_adds("B") < 25 or fleet.acked_adds("C") < 25:
            assert time.monotonic() < deadline, (fleet.stderr("B"), fleet.stderr("C"))
            time.sleep(0.01)
        t_kill = time.time()
        fleet.procs["A"].kill()                     # SIGKILL, mid-stream
        fleet.procs["A"].wait()
        fleet.finish("B", "C")
        took = time.time() - t_kill
    finally:
        fleet.kill_all()
    adds, deletes, violations = _acked(fleet, ["A", "B", "C"])
    assert not violations, violations[:5]
    done = {t: fleet.log(t)[-2] for t in ("B", "C")}
    # one took the namespace over (and the other, finishing later, may have
    # taken it from that one when it closed)
    assert any(d["op"] == "done" and d["holds"] for d in done.values()), done
    assert fleet.acked_adds("B") == fleet.acked_adds("C") == 120
    # the failover stall: the longest pause in the forwarders' acks around the kill
    ts = sorted(e["t"] for t in ("B", "C") for e in fleet.log(t) if e["op"] == "add")
    stall = max(b - a for a, b in zip(ts, ts[1:]) if b > t_kill and a < t_kill + 60)
    print(f"\nSIGKILL failover: acks paused {stall:.2f} s around the kill; B and C finished "
          f"{took:.1f} s after it; {len(adds)} acked writes, {len(deletes)} acked deletes")
    return adds, deletes


def test_holder_sigkilled_mid_stream_forwarders_fail_over(tmp_path):
    root = str(tmp_path / "d")
    cfg = dict(PURGE)
    adds, deletes = _sigkill_holder(tmp_path, root, cfg, encrypt=True)
    m = _open_after(root, cfg, encrypt=True)
    try:
        _verify(m, adds, deletes, lambda: b"")    # encrypted: no plaintext to look for
    finally:
        m.close()


def test_holder_closing_hands_the_namespace_over(tmp_path):
    """A clean close: calls already running finish there, the next ones
    find the namespace free and one forwarder becomes its writer - the
    other forwards to that one."""
    root = str(tmp_path / "d")
    fleet = Fleet(tmp_path, root, PURGE)
    stop_a, stop_bc = str(tmp_path / "stop-A"), str(tmp_path / "stop-BC")
    try:
        fleet.spawn("A", 10 ** 6, sleep=0.01, stop=stop_a)
        fleet.wait_ready("A")
        for t in ("B", "C"):
            fleet.spawn(t, 10 ** 6, ryw=True, sleep=0.004, delete_every=6, hard_every=12, stop=stop_bc)
        fleet.wait_ready("B", "C")
        fleet.go()
        deadline = time.monotonic() + 120
        while fleet.acked_adds("B") < 25 or fleet.acked_adds("C") < 25:
            assert time.monotonic() < deadline, (fleet.stderr("B"), fleet.stderr("C"))
            time.sleep(0.01)
        open(stop_a, "w").close()                   # A flushes and closes, mid-stream
        fleet.finish("A")
        t_closed = fleet.log("A")[-1]["t"]
        after = {t: fleet.acked_adds(t) for t in ("B", "C")}
        while any(fleet.acked_adds(t) < after[t] + 20 for t in ("B", "C")):
            assert time.monotonic() < deadline, (fleet.stderr("B"), fleet.stderr("C"))
            time.sleep(0.01)
        open(stop_bc, "w").close()
        fleet.finish("B", "C")
    finally:
        fleet.kill_all()
    adds, deletes, violations = _acked(fleet, ["A", "B", "C"])
    assert not violations, violations[:5]
    assert fleet.log("A")[0]["holds"] is True and fleet.log("A")[-1]["op"] == "closed"
    assert all(any(e["op"] == "add" and e["t"] > t_closed for e in fleet.log(t)) for t in ("B", "C")), \
        "both went on writing after the holder closed"
    assert any(fleet.log(t)[-2]["holds"] for t in ("B", "C")), "a forwarder took the namespace over"
    m = _open_after(root, PURGE, encrypt=False)
    try:
        _verify(m, adds, deletes, _local_raw(root))
    finally:
        m.close()


# ------------------------------------------------------------------ S3


@pytest.fixture()
def s3_root():
    if not ENDPOINT:
        pytest.skip("no S3 endpoint (set MEMD_TEST_S3_ENDPOINT)")
    boto3 = pytest.importorskip("boto3")
    c = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET, region_name="us-east-1")
    try:
        c.create_bucket(Bucket=BUCKET)
    except Exception:
        pass
    prefix = f"fwd-{uuid.uuid4().hex[:10]}"

    def raw() -> bytes:
        out = []
        pages = c.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=f"{prefix}/ns/{NS}/")
        for page in pages:
            for o in page.get("Contents", []):
                if o["Key"].endswith(("/manifest.json", "/.owner")):
                    continue
                out.append(c.get_object(Bucket=BUCKET, Key=o["Key"])["Body"].read())
        return b"".join(out)
    return f"s3://{BUCKET}/{prefix}", raw


def _s3_config(tmp_path, ttl: float = 3.0) -> tuple[dict, callable]:
    base = dict(PURGE, s3_endpoint_url=ENDPOINT, s3_access_key=KEY, s3_secret_key=SECRET,
                s3_region="us-east-1", lease_ttl_s=ttl,
                forward_secret="a-secret-the-test-processes-share")
    return base, (lambda tag: {"local_dir": str(tmp_path / f"local-{tag}")})


@pytest.mark.s3
def test_s3_two_processes_write_one_namespace(tmp_path, s3_root):
    root, raw = s3_root
    cfg, per = _s3_config(tmp_path)
    _f, adds, deletes = _run_concurrent(tmp_path, root, cfg, ["A", "B"], per_worker=per, n=40)
    m = _open_after(root, dict(cfg, local_dir=str(tmp_path / "verify")), encrypt=False)
    try:
        _verify(m, adds, deletes, raw)
    finally:
        m.close()


@pytest.mark.s3
def test_s3_three_processes_write_one_namespace(tmp_path, s3_root):
    root, raw = s3_root
    cfg, per = _s3_config(tmp_path)
    _f, adds, deletes = _run_concurrent(tmp_path, root, cfg, ["A", "B", "C"], per_worker=per, n=30)
    m = _open_after(root, dict(cfg, local_dir=str(tmp_path / "verify")), encrypt=False)
    try:
        _verify(m, adds, deletes, raw)
    finally:
        m.close()


@pytest.mark.s3
def test_s3_holder_sigkilled_mid_stream_forwarders_fail_over(tmp_path, s3_root):
    root, raw = s3_root
    cfg, per = _s3_config(tmp_path)
    adds, deletes = _sigkill_holder(tmp_path, root, cfg, per_worker=per)
    m = _open_after(root, dict(cfg, local_dir=str(tmp_path / "verify")), encrypt=False)
    try:
        _verify(m, adds, deletes, raw)
    finally:
        m.close()


@pytest.mark.s3
def test_s3_forwarding_off_keeps_namespace_busy_error(tmp_path, s3_root):
    root, _raw = s3_root
    cfg, per = _s3_config(tmp_path)
    a = Memory(root, namespace=NS, encrypt=False, config=dict(cfg, **per("A")))
    try:
        out = _child(r'''
import json, sys
from memd.engine.memory import Memory
cfg = json.loads(sys.argv[2])
try:
    Memory(sys.argv[1], namespace="shared", encrypt=False, config=cfg, forwarding="off")
    print("OPENED")
except Exception as e:
    print(type(e).__name__)
''', root, json.dumps(dict(cfg, **per("B"))))
        assert out.stdout.strip() == "NamespaceBusyError", out.stdout + out.stderr[-2000:]
    finally:
        a.close()


@pytest.mark.s3
def test_s3_frozen_holder_resumes_and_forwards(tmp_path, s3_root):
    """The holder frozen (SIGSTOP) past its lease: the forwarder's call
    waiting on it gives up once the lease is stale, takes the namespace
    over (fencing the frozen holder) and goes on. The holder, resumed, has
    its own write in flight refused (LeaseLostError, not acked) - and its
    next ones forwarded to the new writer, though the namespace is its
    facade's own (pinned)."""
    root, raw = s3_root
    ttl = 3.0
    cfg, per = _s3_config(tmp_path, ttl=ttl)
    fleet = Fleet(tmp_path, root, cfg, per_worker=per)
    stop = str(tmp_path / "stop")
    try:
        fleet.spawn("A", 10 ** 6, sleep=0.01, stop=stop, lease_lost_ok=True)
        fleet.wait_ready("A")
        fleet.go()
        fleet.spawn("B", 10 ** 6, sleep=0.01, stop=stop, ryw=True)
        fleet.wait_ready("B")
        deadline = time.monotonic() + 120
        while fleet.acked_adds("B") < 20:
            assert time.monotonic() < deadline, fleet.stderr("B")
            time.sleep(0.01)
        a = fleet.procs["A"]
        os.kill(a.pid, signal.SIGSTOP)
        t_freeze, frozen_b = time.monotonic(), fleet.acked_adds("B")
        # the lease goes stale after the TTL; on MinIO a holder frozen in the
        # middle of a PUT also holds that object's lock for MinIO's ~30 s
        # timeout, and the takeover's fence waits for it (AWS S3 does not lock)
        while fleet.acked_adds("B") < frozen_b + 10:
            assert time.monotonic() - t_freeze < 90, ("the forwarder did not take over", fleet.stderr("B"))
            time.sleep(0.05)
        print(f"\nfrozen holder: the forwarder wrote again {time.monotonic() - t_freeze:.1f} s "
              f"after the freeze (TTL {ttl:.0f} s)")
        os.kill(a.pid, signal.SIGCONT)
        deadline = time.monotonic() + 120
        resumed_a = fleet.acked_adds("A")
        while fleet.acked_adds("A") < resumed_a + 20:
            assert time.monotonic() < deadline, fleet.stderr("A")
            assert a.poll() is None, fleet.stderr("A")
            time.sleep(0.01)
        open(stop, "w").close()
        fleet.finish("A", "B")
    finally:
        for p in fleet.procs.values():
            if p.poll() is None:
                os.kill(p.pid, signal.SIGCONT)
        fleet.kill_all()
    adds, deletes, violations = _acked(fleet, ["A", "B"])
    assert not violations, violations[:5]
    assert fleet.log("A")[-2] == {**fleet.log("A")[-2], "op": "done", "holds": False}, \
        "the resumed holder forwards to the new writer"
    m = _open_after(root, dict(cfg, local_dir=str(tmp_path / "verify")), encrypt=False)
    try:
        _verify(m, adds, deletes, raw)
    finally:
        m.close()
