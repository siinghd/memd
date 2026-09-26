"""Admin store for hosted mode: orgs, namespace ownership, API keys, the
usage ledger and Stripe bookkeeping - one SQLite database under
`<data root>/admin/`, a sibling of `store/` and never inside a tenant
namespace (tenant namespaces are crypto-shredded and exported; billing state
must survive both and must never ride along in a tenant's export).

Durability: WAL journal + synchronous=FULL, so a committed usage event is on
disk before the request that produced it is acknowledged. Every multi-row
change runs inside BEGIN IMMEDIATE, which also serializes writers across
processes (several uvicorn workers can share one admin store).
"""
from __future__ import annotations

import json
import os
import secrets
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

from memd.hosted.plans import GAUGES, Plan
from memd.server.auth import generate_key, hash_secret

SCHEMA = """
CREATE TABLE IF NOT EXISTS org (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL DEFAULT '',
  stripe_customer_id TEXT UNIQUE,
  stripe_subscription_id TEXT,
  plan TEXT NOT NULL DEFAULT 'free',
  status TEXT NOT NULL DEFAULT 'active',
  current_period_end INTEGER,
  grace_until INTEGER,
  -- newest Stripe subscription event applied: re-ordered deliveries must
  -- not roll the plan back to an older state
  last_event_created INTEGER NOT NULL DEFAULT 0,
  -- the oldest listed duplicate subscription (see org_duplicate_subscription),
  -- NULL when none: pushes for the org are held while it is set
  duplicate_subscription_id TEXT,
  -- the one Checkout Session in flight (one subscription per org)
  checkout_session_id TEXT,
  checkout_url TEXT,
  checkout_expires_at INTEGER,
  created INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS namespace (
  ns TEXT PRIMARY KEY,
  org_id TEXT NOT NULL REFERENCES org(id),
  created INTEGER NOT NULL,
  records INTEGER,
  bytes INTEGER,
  measured_at INTEGER
);
CREATE INDEX IF NOT EXISTS namespace_org ON namespace(org_id);
CREATE TABLE IF NOT EXISTS api_key (
  hash TEXT PRIMARY KEY,           -- sha256 of the secret; the key itself is never stored
  key_id TEXT NOT NULL UNIQUE,
  org_id TEXT NOT NULL REFERENCES org(id),
  ns TEXT NOT NULL,
  scopes TEXT NOT NULL DEFAULT 'memory',
  pinned_user TEXT,
  name TEXT NOT NULL DEFAULT '',
  created INTEGER NOT NULL,
  revoked INTEGER
);
CREATE INDEX IF NOT EXISTS api_key_org ON api_key(org_id);
-- append-only usage ledger; the event id is a UUID
CREATE TABLE IF NOT EXISTS usage_event (
  id TEXT PRIMARY KEY,
  org_id TEXT NOT NULL,
  ns TEXT,
  meter TEXT NOT NULL,
  quantity REAL NOT NULL,
  billable REAL NOT NULL DEFAULT 0,  -- the part owed to Stripe (overage / metered usage)
  ts INTEGER NOT NULL,               -- unix seconds
  period TEXT NOT NULL,              -- quota period, 'YYYY-MM' UTC
  push_id TEXT,                      -- the meter_push batch that carries it
  pushed_at INTEGER,
  -- NULL (pending) | sent | reconciling | reconciled | needs_reconcile |
  -- not_billable | no_customer | unmapped | expired
  push_status TEXT
);
CREATE INDEX IF NOT EXISTS usage_unpushed ON usage_event(push_id) WHERE pushed_at IS NULL;
CREATE INDEX IF NOT EXISTS usage_org_period ON usage_event(org_id, period, meter);
CREATE TABLE IF NOT EXISTS usage_rollup (
  org_id TEXT NOT NULL,
  meter TEXT NOT NULL,
  period TEXT NOT NULL,
  quantity REAL NOT NULL DEFAULT 0,
  billable REAL NOT NULL DEFAULT 0,
  PRIMARY KEY (org_id, meter, period)
);
-- in-flight quota reservations: admission reserves under BEGIN IMMEDIATE,
-- the usage commit (or a failure) releases; rows older than the TTL are a
-- crashed request's and stop counting
CREATE TABLE IF NOT EXISTS usage_reservation (
  id TEXT NOT NULL,
  org_id TEXT NOT NULL,
  meter TEXT NOT NULL,
  period TEXT NOT NULL,
  quantity REAL NOT NULL,
  created REAL NOT NULL,
  PRIMARY KEY (id, meter)
);
CREATE INDEX IF NOT EXISTS usage_reservation_org ON usage_reservation(org_id, meter, period);
-- one Stripe meter event per batch; the batch id is the Stripe identifier
-- AND idempotency key, persisted before the first send so every retry
-- (after a crash, a timeout, a 5xx) re-sends the same key - but only while
-- Stripe still remembers it (see MAX_AUTO_PUSH_AGE_S)
CREATE TABLE IF NOT EXISTS meter_push (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL DEFAULT 'regular',  -- regular | reconcile
  org_id TEXT NOT NULL,
  meter TEXT NOT NULL,
  customer TEXT NOT NULL,
  value INTEGER NOT NULL,            -- whole Stripe units sent (see STRIPE_UNIT_SCALE)
  credited INTEGER NOT NULL DEFAULT 0,  -- reconcile: units verified already in Stripe, not re-sent
  ts INTEGER NOT NULL,               -- the meter event timestamp (newest member event)
  first_ts INTEGER NOT NULL,         -- the oldest member event
  created INTEGER NOT NULL,          -- claimed; the first send follows
  pushed_at INTEGER,
  abandoned_at INTEGER,              -- too old to retry under its key: handed to reconciliation
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT
);
CREATE INDEX IF NOT EXISTS meter_push_pending ON meter_push(created) WHERE pushed_at IS NULL;
CREATE INDEX IF NOT EXISTS meter_push_org ON meter_push(org_id, meter, customer, ts);
CREATE TABLE IF NOT EXISTS processed_events (
  id TEXT PRIMARY KEY,               -- Stripe event id: webhook idempotency
  type TEXT NOT NULL,
  created INTEGER,
  org_id TEXT,
  result TEXT,
  processed_at INTEGER NOT NULL
);
-- every live subscription of an org's customer other than the current one:
-- while ANY is listed, the org's metered usage is held (each would bill it)
CREATE TABLE IF NOT EXISTS org_duplicate_subscription (
  org_id TEXT NOT NULL REFERENCES org(id),
  subscription_id TEXT NOT NULL,
  created INTEGER NOT NULL,
  PRIMARY KEY (org_id, subscription_id)
);
CREATE TABLE IF NOT EXISTS billing_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts INTEGER NOT NULL,
  org_id TEXT,
  kind TEXT NOT NULL,
  detail TEXT
);
"""

_ORG_COLS = ("name", "stripe_customer_id", "stripe_subscription_id", "plan", "status",
             "current_period_end", "grace_until", "last_event_created", "duplicate_subscription_id",
             "checkout_session_id", "checkout_url", "checkout_expires_at")
_USAGE_NS = uuid.UUID("6f1b0c5e-2a7d-4c1e-9a53-8d0f4e2b7c11")
SCOPES = frozenset({"memory", "billing", "override"})
# Stripe accepts meter events up to 35 days old; leave a day of margin
STRIPE_MAX_EVENT_AGE_S = 34 * 86400
# Stripe remembers an idempotency key (and a meter event identifier) for
# ~24 h. A usage event older than this is never pushed automatically - a
# retry could land after Stripe forgot the key and bill it twice - it goes
# to reconciliation, which pushes only what Stripe verifiably lacks.
MAX_AUTO_PUSH_AGE_S = 20 * 3600


def stripe_units(quantity: float, scale: int = 1) -> int:
    """Whole Stripe units, rounded up (a fraction is never given away)."""
    return int(-(-round(quantity * scale, 6) // 1))


class OwnershipError(Exception):
    """A namespace already belongs to another org."""


class _Denied(Exception):
    def __init__(self, info: dict):
        super().__init__(info.get("meter", ""))
        self.info = info


def period_of(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m")


def period_bounds(period: str) -> tuple[int, int]:
    y, m = (int(x) for x in period.split("-"))
    start = datetime(y, m, 1, tzinfo=timezone.utc)
    end = datetime(y + (m == 12), m % 12 + 1, 1, tzinfo=timezone.utc)
    return int(start.timestamp()), int(end.timestamp())


def validate_namespace(ns: str) -> str:
    """The engine's namespace grammar ([A-Za-z0-9][A-Za-z0-9_.-]{0,127}), so
    '*' (the operator key) and '_'-prefixed names - '_billing' labels the
    operator-only metrics - can never be bound to a tenant key."""
    from memd.storage.engine import _validate_ns

    if not isinstance(ns, str) or ns == "*":
        raise ValueError("hosted keys are bound to one namespace ('*' is the operator key)")
    return _validate_ns(ns)  # a full match of the grammar (no trailing newline)


def parse_scopes(scopes: str | list[str] | None) -> list[str]:
    if scopes is None:
        return ["memory"]
    items = scopes.replace(",", " ").split() if isinstance(scopes, str) else list(scopes)
    bad = [s for s in items if s not in SCOPES]
    if bad:
        raise ValueError(f"unknown scope(s) {bad!r}; expected a subset of {sorted(SCOPES)}")
    return sorted(set(items)) or ["memory"]


def _private_file(path: str) -> None:
    """Create `path` owner-only BEFORE SQLite opens it: SQLite gives the
    -wal/-shm files the database file's mode, so they are born 0600 too."""
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    os.close(fd)
    for p in (path, path + "-wal", path + "-shm"):
        try:
            os.chmod(p, 0o600)  # also tightens files an older version created
        except OSError:
            pass


class AdminStore:
    def __init__(self, path: str):
        self.path = path
        admin_dir = os.path.dirname(path) or "."
        os.makedirs(admin_dir, mode=0o700, exist_ok=True)
        try:
            os.chmod(admin_dir, 0o700)
        except OSError:
            pass
        _private_file(path)
        self._lock = threading.RLock()
        self._con = sqlite3.connect(path, check_same_thread=False, isolation_level=None, timeout=10.0)
        self._con.row_factory = sqlite3.Row
        self._con.execute("PRAGMA journal_mode=WAL")
        # FULL: a commit is fsynced before it returns - an acknowledged
        # request's usage event survives power loss, not just a process kill
        self._con.execute("PRAGMA synchronous=FULL")
        self._con.execute("PRAGMA foreign_keys=ON")
        self._con.execute("PRAGMA busy_timeout=10000")
        with self._lock:
            self._con.executescript(SCHEMA)
        _private_file(path)  # key hashes, customer ids

    @classmethod
    def for_data_root(cls, data_dir: str) -> "AdminStore":
        return cls(os.path.join(data_dir, "admin", "admin.sqlite3"))

    def close(self) -> None:
        with self._lock:
            self._con.close()

    @contextmanager
    def txn(self) -> Iterator[sqlite3.Connection]:
        """BEGIN IMMEDIATE ... COMMIT; rolled back on any exception."""
        with self._lock:
            self._con.execute("BEGIN IMMEDIATE")
            try:
                yield self._con
            except BaseException:
                self._con.execute("ROLLBACK")
                raise
            self._con.execute("COMMIT")

    def _one(self, sql: str, args: tuple = ()) -> dict | None:
        with self._lock:
            row = self._con.execute(sql, args).fetchone()
        return dict(row) if row is not None else None

    def _all(self, sql: str, args: tuple = ()) -> list[dict]:
        with self._lock:
            return [dict(r) for r in self._con.execute(sql, args).fetchall()]

    # ------------------------------------------------------------------ orgs

    def create_org(self, name: str = "", plan: str = "free", org_id: str | None = None) -> str:
        oid = org_id or f"org_{secrets.token_hex(8)}"
        with self.txn() as con:
            con.execute("INSERT INTO org (id, name, plan, created) VALUES (?, ?, ?, ?)",
                        (oid, name[:200], plan, int(time.time())))
        return oid

    def get_org(self, org_id: str) -> dict | None:
        return self._one("SELECT * FROM org WHERE id = ?", (org_id,))

    def org_by_customer(self, customer_id: str) -> dict | None:
        return self._one("SELECT * FROM org WHERE stripe_customer_id = ?", (customer_id,))

    def list_orgs(self) -> list[dict]:
        return self._all("SELECT * FROM org ORDER BY created, id")

    def update_org(self, org_id: str, con: sqlite3.Connection | None = None, **fields: Any) -> None:
        bad = [k for k in fields if k not in _ORG_COLS]
        if bad:
            raise ValueError(f"unknown org column(s) {bad}")
        if not fields:
            return
        sets = ", ".join(f"{k} = ?" for k in fields)
        args = (*fields.values(), org_id)
        if con is not None:
            con.execute(f"UPDATE org SET {sets} WHERE id = ?", args)
            return
        with self.txn() as c:
            c.execute(f"UPDATE org SET {sets} WHERE id = ?", args)

    # ------------------------------------------------------------ namespaces

    @staticmethod
    def _claim(con: sqlite3.Connection, ns: str, org_id: str) -> None:
        row = con.execute("SELECT org_id FROM namespace WHERE ns = ?", (ns,)).fetchone()
        if row is None:
            con.execute("INSERT INTO namespace (ns, org_id, created) VALUES (?, ?, ?)",
                        (ns, org_id, int(time.time())))
        elif row["org_id"] != org_id:
            raise OwnershipError(f"namespace {ns!r} belongs to another org")

    def claim_namespace(self, ns: str, org_id: str) -> None:
        with self.txn() as con:
            if con.execute("SELECT 1 FROM org WHERE id = ?", (org_id,)).fetchone() is None:
                raise KeyError(f"unknown org {org_id!r}")
            self._claim(con, ns, org_id)

    def ns_owner(self, ns: str) -> str | None:
        row = self._one("SELECT org_id FROM namespace WHERE ns = ?", (ns,))
        return row["org_id"] if row else None

    def org_namespaces(self, org_id: str) -> list[dict]:
        return self._all("SELECT * FROM namespace WHERE org_id = ? ORDER BY ns", (org_id,))

    def set_ns_measure(self, ns: str, records: int, nbytes: int, at: int | None = None) -> None:
        with self.txn() as con:
            con.execute("UPDATE namespace SET records = ?, bytes = ?, measured_at = ? WHERE ns = ?",
                        (int(records), int(nbytes), int(at or time.time()), ns))

    # ------------------------------------------------------------------ keys

    def create_key(self, org_id: str, ns: str, *, name: str = "", scopes: str | list[str] | None = None,
                   pinned_user: str | None = None) -> tuple[str, str]:
        """A new namespace-bound key for `org_id`, claiming `ns` for the org
        in the same transaction. Returns (full_key, key_id); only the hash of
        the secret is stored."""
        validate_namespace(ns)
        sc = " ".join(parse_scopes(scopes))
        full, kid = generate_key(ns)
        h = hash_secret(full.rsplit("_", 1)[1])
        with self.txn() as con:
            if con.execute("SELECT 1 FROM org WHERE id = ?", (org_id,)).fetchone() is None:
                raise KeyError(f"unknown org {org_id!r}")
            self._claim(con, ns, org_id)
            con.execute("INSERT INTO api_key (hash, key_id, org_id, ns, scopes, pinned_user, name, created)"
                        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (h, kid, org_id, ns, sc, pinned_user, (name or f"key-{kid}")[:128], int(time.time())))
        return full, kid

    def import_key(self, org_id: str, ns: str, key_id: str, secret_hash: str, *, name: str = "",
                   scopes: str | list[str] | None = None, pinned_user: str | None = None,
                   created: int | None = None) -> None:
        """Adopt a legacy (keys.toml.json) key: same `memd_<ns>_<kid>_<secret>`
        format and the same secret hash, so the key keeps working unchanged."""
        validate_namespace(ns)
        sc = " ".join(parse_scopes(scopes))
        with self.txn() as con:
            if con.execute("SELECT 1 FROM org WHERE id = ?", (org_id,)).fetchone() is None:
                raise KeyError(f"unknown org {org_id!r}")
            self._claim(con, ns, org_id)
            con.execute("INSERT OR IGNORE INTO api_key (hash, key_id, org_id, ns, scopes, pinned_user, name,"
                        " created) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (secret_hash, key_id, org_id, ns, sc, pinned_user, name[:128],
                         int(created or time.time())))

    def key_for_bearer(self, bearer: str) -> dict | None:
        """The live key record a `memd_<ns>_<kid>_<secret>` bearer names, or
        None. The prefix must match the record's namespace and key id, as in
        the legacy KeyStore."""
        if not bearer or not bearer.startswith("memd_"):
            return None
        head, _, secret = bearer.rpartition("_")
        if not secret or not head:
            return None
        rec = self._one("SELECT * FROM api_key WHERE hash = ? AND revoked IS NULL", (hash_secret(secret),))
        if rec is None or bearer != f"memd_{rec['ns']}_{rec['key_id']}_{secret}":
            return None
        return rec

    def revoke_key(self, key_id: str) -> bool:
        with self.txn() as con:
            cur = con.execute("UPDATE api_key SET revoked = ? WHERE key_id = ? AND revoked IS NULL",
                              (int(time.time()), key_id))
            return cur.rowcount > 0

    def list_keys(self, org_id: str | None = None) -> list[dict]:
        cols = "key_id, org_id, ns, scopes, pinned_user, name, created, revoked"
        if org_id is None:
            return self._all(f"SELECT {cols} FROM api_key ORDER BY created, key_id")
        return self._all(f"SELECT {cols} FROM api_key WHERE org_id = ? ORDER BY created, key_id", (org_id,))

    # ----------------------------------------------------------------- usage

    def record_usage(self, org_id: str, ns: str | None, usage: dict[str, float], plan: Plan,
                     ts: float | None = None, reservation: str | None = None) -> list[str]:
        """Append counter usage to the ledger and bump the rollups - ONE
        transaction, so the quota rollup and the billing ledger can never
        disagree. The billable part of each event is fixed here, against the
        rollup under the write lock: overage above the plan's included
        quantity for metered meters, nothing otherwise."""
        now = time.time() if ts is None else ts
        period = period_of(now)
        ids: list[str] = []
        with self.txn() as con:
            for meter, qty in usage.items():
                if not qty or qty <= 0:
                    continue
                prev_row = con.execute(
                    "SELECT quantity FROM usage_rollup WHERE org_id = ? AND meter = ? AND period = ?",
                    (org_id, meter, period)).fetchone()
                prev = prev_row["quantity"] if prev_row else 0.0
                billable = 0.0
                if meter in plan.metered:
                    included = plan.entitlement(meter).limit or 0
                    billable = max(0.0, prev + qty - included) - max(0.0, prev - included)
                eid = str(uuid.uuid4())
                con.execute("INSERT INTO usage_event (id, org_id, ns, meter, quantity, billable, ts, period)"
                            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            (eid, org_id, ns, meter, float(qty), billable, int(now), period))
                con.execute("INSERT INTO usage_rollup (org_id, meter, period, quantity, billable)"
                            " VALUES (?, ?, ?, ?, ?) ON CONFLICT(org_id, meter, period) DO UPDATE SET"
                            " quantity = quantity + excluded.quantity, billable = billable + excluded.billable",
                            (org_id, meter, period, float(qty), billable))
                ids.append(eid)
            if reservation is not None:
                # same transaction: the rollup grows as the reservation goes
                con.execute("DELETE FROM usage_reservation WHERE id = ?", (reservation,))
        return ids

    # ------------------------------------------------------------ reservations

    def try_reserve(self, org_id: str, period: str, checks: list[tuple[str, float, float, float | None]],
                    *, now: float, ttl_s: float, live: "set[str] | frozenset[str]" = frozenset(),
                    rid: str | None = None) -> tuple[str | None, dict | None]:
        """Atomic check-and-reserve for hard caps. `checks` holds (meter,
        quantity, limit, live_base): the used amount is `live_base` (the
        memories gauge, counted outside SQLite) or the period rollup, plus
        every live reservation. Under BEGIN IMMEDIATE no other request can
        check or reserve in between, so concurrent requests at the boundary
        cannot all pass. Returns (reservation id, None) or (None, denial).

        Expiry reclaims only reservations older than `ttl_s` that are NOT in
        `live` (the ids of requests still running in this process): a slow
        request's quota is never handed out twice, while a crashed request's
        is. `rid` adds rows to an existing reservation."""
        rid, denial, _ = self.reserve(org_id, period, [(m, q, q, lim, base) for m, q, lim, base in checks],
                                      now=now, ttl_s=ttl_s, live=live, rid=rid)
        return rid, denial

    def reserve(self, org_id: str, period: str, checks: list[tuple[str, float, float, float, float | None]],
                *, now: float, ttl_s: float, live: "set[str] | frozenset[str]" = frozenset(),
                rid: str | None = None) -> tuple[str | None, dict | None, dict[str, float]]:
        """try_reserve() generalized: each check is (meter, want, minimum,
        limit, live_base) and reserves as much of `want` as fits, refusing
        (the whole reservation) when less than `minimum` does. Returns
        (reservation id, denial, {meter: granted}). With an existing `rid`,
        a meter's row is RESIZED to the new grant (its own current hold does
        not count against it; a grant of 0 drops the row)."""
        rid = rid or str(uuid.uuid4())
        granted: dict[str, float] = {}
        try:
            with self.txn() as con:
                con.execute("DELETE FROM usage_reservation WHERE created < ? AND id NOT IN"
                            " (SELECT value FROM json_each(?))", (now - ttl_s, json.dumps(sorted(live))))
                for meter, want, minimum, limit, live_base in checks:
                    rperiod = "live" if live_base is not None else period
                    if live_base is not None:
                        used = float(live_base)
                    else:
                        row = con.execute("SELECT quantity FROM usage_rollup WHERE org_id = ? AND meter = ?"
                                          " AND period = ?", (org_id, meter, period)).fetchone()
                        used = float(row["quantity"]) if row else 0.0
                    held = float(con.execute("SELECT COALESCE(SUM(quantity), 0) FROM usage_reservation"
                                             " WHERE org_id = ? AND meter = ? AND period = ? AND id != ?",
                                             (org_id, meter, rperiod, rid)).fetchone()[0])
                    grant = min(float(want), max(0.0, float(limit) - used - held))
                    if grant < float(minimum):
                        # raising rolls back the reservations made so far
                        raise _Denied({"meter": meter, "limit": limit, "used": used + held})
                    granted[meter] = grant
                    if grant > 0:
                        con.execute("INSERT OR REPLACE INTO usage_reservation (id, org_id, meter, period,"
                                    " quantity, created) VALUES (?, ?, ?, ?, ?, ?)",
                                    (rid, org_id, meter, rperiod, grant, now))
                    else:
                        con.execute("DELETE FROM usage_reservation WHERE id = ? AND meter = ?", (rid, meter))
        except _Denied as d:
            return None, d.info, {}
        return rid, None, granted

    def release(self, reservation: str) -> None:
        with self.txn() as con:
            con.execute("DELETE FROM usage_reservation WHERE id = ?", (reservation,))

    def clear_reservations(self) -> int:
        """At server start: memd is one process per data root, so every
        reservation on disk belongs to a request that died with the last one."""
        with self.txn() as con:
            return con.execute("DELETE FROM usage_reservation").rowcount

    def reservations(self, org_id: str | None = None) -> list[dict]:
        if org_id is None:
            return self._all("SELECT * FROM usage_reservation")
        return self._all("SELECT * FROM usage_reservation WHERE org_id = ?", (org_id,))

    def record_gauge(self, org_id: str, meter: str, value: float, plan: Plan, day: str,
                     ts: float | None = None) -> str | None:
        """One snapshot per (org, gauge meter, UTC day). The event id is
        derived from exactly that triple, so re-running the daily snapshot
        (a restart, a second worker) is a no-op rather than a second bill."""
        if meter not in GAUGES:
            raise ValueError(f"{meter!r} is not a gauge meter")
        now = time.time() if ts is None else ts
        period = period_of(now)
        eid = str(uuid.uuid5(_USAGE_NS, f"gauge|{org_id}|{meter}|{day}"))
        billable = float(value) if meter in plan.metered else 0.0
        with self.txn() as con:
            cur = con.execute("INSERT OR IGNORE INTO usage_event (id, org_id, ns, meter, quantity, billable,"
                              " ts, period) VALUES (?, ?, NULL, ?, ?, ?, ?, ?)",
                              (eid, org_id, meter, float(value), billable, int(now), period))
            if cur.rowcount == 0:
                return None
            con.execute("INSERT INTO usage_rollup (org_id, meter, period, quantity, billable)"
                        " VALUES (?, ?, ?, ?, ?) ON CONFLICT(org_id, meter, period) DO UPDATE SET"
                        " quantity = excluded.quantity, billable = excluded.billable",
                        (org_id, meter, period, float(value), billable))
        return eid

    def gauge_recorded(self, org_id: str, meter: str, day: str) -> bool:
        eid = str(uuid.uuid5(_USAGE_NS, f"gauge|{org_id}|{meter}|{day}"))
        return self._one("SELECT 1 AS x FROM usage_event WHERE id = ?", (eid,)) is not None

    def rollup(self, org_id: str, meter: str, period: str) -> float:
        row = self._one("SELECT quantity FROM usage_rollup WHERE org_id = ? AND meter = ? AND period = ?",
                        (org_id, meter, period))
        return float(row["quantity"]) if row else 0.0

    def rollups(self, org_id: str, period: str) -> dict[str, dict]:
        return {r["meter"]: {"quantity": r["quantity"], "billable": r["billable"]}
                for r in self._all("SELECT meter, quantity, billable FROM usage_rollup"
                                   " WHERE org_id = ? AND period = ?", (org_id, period))}

    def events(self, org_id: str | None = None) -> list[dict]:
        if org_id is None:
            return self._all("SELECT * FROM usage_event ORDER BY ts, id")
        return self._all("SELECT * FROM usage_event WHERE org_id = ? ORDER BY ts, id", (org_id,))

    # ------------------------------------------------------------- meter push

    def claim_push_batches(self, event_name_for, *, now: float | None = None,
                           unit_scale: dict[str, int] | None = None, limit: int = 50_000,
                           max_age_s: int = MAX_AUTO_PUSH_AGE_S) -> dict:
        """Move unpushed ledger rows into push batches, in one transaction.

        Billable rows of orgs with a Stripe customer are grouped per (org,
        meter, UTC hour) - a batch never straddles an hour, so its timestamp
        lands it in the right billing period. Each batch's id is a UUIDv5 of
        its member event UUIDs; it is persisted here, BEFORE any send, and
        becomes the Stripe identifier and idempotency key. Rows that owe
        Stripe nothing are closed out with the reason; billable rows older
        than `max_age_s` go to reconciliation instead (needs_reconcile)."""
        now_i = int(time.time() if now is None else now)
        scale = unit_scale or {}
        counts = {"batches": 0, "not_billable": 0, "no_customer": 0, "unmapped": 0, "needs_reconcile": 0}
        with self.txn() as con:
            rows = con.execute(
                "SELECT e.id, e.org_id, e.meter, e.billable, e.ts, o.stripe_customer_id AS customer"
                " FROM usage_event e LEFT JOIN org o ON o.id = e.org_id"
                " WHERE e.pushed_at IS NULL AND e.push_id IS NULL AND e.push_status IS NULL"
                # held: two live subscriptions would each bill the usage
                " AND o.duplicate_subscription_id IS NULL"
                " ORDER BY e.ts, e.id LIMIT ?", (limit,)).fetchall()
            groups: dict[tuple, list] = {}
            for r in rows:
                reason = None
                if not r["billable"] or r["billable"] <= 0:
                    reason = "not_billable"
                elif not r["customer"]:
                    reason = "no_customer"
                elif not event_name_for(r["meter"]):
                    reason = "unmapped"
                if reason is not None:
                    counts[reason] += 1
                    con.execute("UPDATE usage_event SET pushed_at = ?, push_status = ? WHERE id = ?",
                                (now_i, reason, r["id"]))
                    continue
                if now_i - r["ts"] > max_age_s:
                    counts["needs_reconcile"] += 1
                    con.execute("UPDATE usage_event SET push_status = 'needs_reconcile' WHERE id = ?",
                                (r["id"],))
                    continue
                if r["meter"] in GAUGES:
                    key = (r["org_id"], r["meter"], r["customer"], "gauge", r["id"])  # never summed
                else:
                    key = (r["org_id"], r["meter"], r["customer"], r["ts"] // 3600)
                groups.setdefault(key, []).append(r)
            for (org_id, meter, customer, *_), members in groups.items():
                ids = sorted(m["id"] for m in members)
                bid = str(uuid.uuid5(_USAGE_NS, "push|" + ",".join(ids)))
                qty = sum(m["billable"] for m in members)
                value = stripe_units(qty, scale.get(meter, 1))
                ts = max(m["ts"] for m in members)
                first = min(m["ts"] for m in members)
                con.execute("INSERT OR IGNORE INTO meter_push (id, org_id, meter, customer, value, ts, first_ts,"
                            " created) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            (bid, org_id, meter, customer, value, ts, first, now_i))
                con.executemany("UPDATE usage_event SET push_id = ? WHERE id = ?", [(bid, i) for i in ids])
                counts["batches"] += 1
        return counts

    def pending_pushes(self, limit: int = 1000) -> list[dict]:
        return self._all("SELECT p.* FROM meter_push p JOIN org o ON o.id = p.org_id"
                         " WHERE p.pushed_at IS NULL AND p.abandoned_at IS NULL"
                         " AND o.duplicate_subscription_id IS NULL ORDER BY p.created, p.id LIMIT ?", (limit,))

    def mark_pushed(self, batch_id: str, now: float | None = None, status: str = "sent") -> None:
        now_i = int(time.time() if now is None else now)
        with self.txn() as con:
            con.execute("UPDATE meter_push SET pushed_at = ?, attempts = attempts + 1, last_error = NULL"
                        " WHERE id = ?", (now_i, batch_id))
            con.execute("UPDATE usage_event SET pushed_at = ?, push_status = ? WHERE push_id = ?",
                        (now_i, status, batch_id))

    def abandon_batch(self, batch_id: str, now: float | None = None) -> None:
        """A pending batch too old to retry under its key (Stripe may have
        forgotten it, and may or may not hold the first attempt): its events
        go to reconciliation, which asks Stripe what it actually has."""
        now_i = int(time.time() if now is None else now)
        with self.txn() as con:
            con.execute("UPDATE meter_push SET abandoned_at = ? WHERE id = ? AND pushed_at IS NULL",
                        (now_i, batch_id))
            con.execute("UPDATE usage_event SET push_id = NULL, push_status = 'needs_reconcile'"
                        " WHERE push_id = ? AND pushed_at IS NULL", (batch_id,))

    def needs_reconcile_groups(self) -> list[dict]:
        """needs_reconcile rows per (org, meter, customer, period) - per
        event for gauges, which Stripe aggregates as 'last', not 'sum'."""
        rows = self._all(
            "SELECT e.id, e.org_id, e.meter, e.billable, e.ts, e.period, o.stripe_customer_id AS customer"
            " FROM usage_event e LEFT JOIN org o ON o.id = e.org_id"
            " WHERE e.push_status = 'needs_reconcile' AND o.duplicate_subscription_id IS NULL"
            " ORDER BY e.ts, e.id")
        groups: dict[tuple, dict] = {}
        for r in rows:
            key = (r["org_id"], r["meter"], r["customer"], r["period"],
                   r["id"] if r["meter"] in GAUGES else "")
            g = groups.setdefault(key, {"org_id": r["org_id"], "meter": r["meter"], "customer": r["customer"],
                                        "period": r["period"], "ids": [], "billable": 0.0,
                                        "min_ts": r["ts"], "max_ts": r["ts"]})
            g["ids"].append(r["id"])
            g["billable"] += r["billable"]
            g["min_ts"] = min(g["min_ts"], r["ts"])
            g["max_ts"] = max(g["max_ts"], r["ts"])
        return list(groups.values())

    def close_events(self, ids: list[str], status: str, now: float | None = None) -> None:
        now_i = int(time.time() if now is None else now)
        with self.txn() as con:
            con.executemany("UPDATE usage_event SET pushed_at = ?, push_status = ? WHERE id = ?",
                            [(now_i, status, i) for i in ids])

    def create_reconcile_batch(self, org_id: str, meter: str, customer: str, *, value: int, credited: int,
                               ts: int, first_ts: int, ids: list[str], now: float | None = None) -> str:
        """Persist a reconciliation push BEFORE it is sent, under a FRESH id
        (the old keys may be forgotten by Stripe): `value` units to send,
        `credited` units verified as already in Stripe."""
        now_i = int(time.time() if now is None else now)
        bid = str(uuid.uuid4())
        with self.txn() as con:
            con.execute("INSERT INTO meter_push (id, kind, org_id, meter, customer, value, credited, ts, first_ts,"
                        " created) VALUES (?, 'reconcile', ?, ?, ?, ?, ?, ?, ?, ?)",
                        (bid, org_id, meter, customer, int(value), int(credited), int(ts), int(first_ts), now_i))
            con.executemany("UPDATE usage_event SET push_id = ?, push_status = 'reconciling' WHERE id = ?"
                            " AND push_status = 'needs_reconcile'", [(bid, i) for i in ids])
        return bid

    def unsettled(self, org_id: str, meter: str, customer: str, start: int, end: int, settled_before: int) -> bool:
        """A batch in [start, end) that Stripe may not reflect yet: still
        pending, or sent after `settled_before` (meter summaries lag)."""
        row = self._one("SELECT 1 AS x FROM meter_push WHERE org_id = ? AND meter = ? AND customer = ?"
                        " AND ts >= ? AND ts < ? AND abandoned_at IS NULL AND (pushed_at IS NULL"
                        " OR (pushed_at > ? AND value > 0)) LIMIT 1",
                        (org_id, meter, customer, start, end, settled_before))
        return row is not None

    def mark_push_failed(self, batch_id: str, error: str) -> None:
        with self.txn() as con:
            con.execute("UPDATE meter_push SET attempts = attempts + 1, last_error = ? WHERE id = ?",
                        (error[:500], batch_id))

    def unpushed_count(self) -> int:
        row = self._one("SELECT COUNT(*) AS n FROM usage_event WHERE pushed_at IS NULL")
        return int(row["n"]) if row else 0

    def needs_reconcile_count(self) -> int:
        row = self._one("SELECT COUNT(*) AS n FROM usage_event WHERE push_status = 'needs_reconcile'")
        return int(row["n"]) if row else 0

    def settled_units(self, org_id: str, meter: str, customer: str, start: int, end: int) -> int:
        """What Stripe should hold from us for [start, end): every settled
        batch's sent units plus the units reconciliation verified were
        already there. Abandoned batches do not count (reconciliation
        accounts for whatever of them landed)."""
        row = self._one("SELECT COALESCE(SUM(value + credited), 0) AS u FROM meter_push WHERE org_id = ?"
                        " AND meter = ? AND customer = ? AND pushed_at IS NOT NULL AND abandoned_at IS NULL"
                        " AND ts >= ? AND ts < ?", (org_id, meter, customer, start, end))
        return int(row["u"]) if row else 0

    def last_settled_gauge(self, org_id: str, meter: str, customer: str, start: int, end: int) -> int | None:
        row = self._one("SELECT value + credited AS u FROM meter_push WHERE org_id = ? AND meter = ?"
                        " AND customer = ? AND pushed_at IS NOT NULL AND abandoned_at IS NULL"
                        " AND ts >= ? AND ts < ? ORDER BY ts DESC, created DESC LIMIT 1",
                        (org_id, meter, customer, start, end))
        return int(row["u"]) if row else None

    def settled_pairs(self, start: int, end: int) -> list[dict]:
        return self._all("SELECT DISTINCT org_id, meter, customer FROM meter_push WHERE pushed_at IS NOT NULL"
                         " AND abandoned_at IS NULL AND ts >= ? AND ts < ?", (start, end))

    def batches(self, org_id: str | None = None) -> list[dict]:
        if org_id is None:
            return self._all("SELECT * FROM meter_push ORDER BY created, id")
        return self._all("SELECT * FROM meter_push WHERE org_id = ? ORDER BY created, id", (org_id,))

    # --------------------------------------------------------- stripe events

    @staticmethod
    def claim_event(con: sqlite3.Connection, event_id: str, etype: str, created: int | None) -> bool:
        """Inside the webhook's transaction: False when this Stripe event id
        was already processed (a re-delivery). The claim commits together
        with the event's effects, or not at all."""
        try:
            con.execute("INSERT INTO processed_events (id, type, created, processed_at) VALUES (?, ?, ?, ?)",
                        (event_id, etype, created, int(time.time())))
            return True
        except sqlite3.IntegrityError:
            return False

    @staticmethod
    def finish_event(con: sqlite3.Connection, event_id: str, org_id: str | None, result: str) -> None:
        con.execute("UPDATE processed_events SET org_id = ?, result = ? WHERE id = ?",
                    (org_id, result, event_id))

    def processed_event(self, event_id: str) -> dict | None:
        return self._one("SELECT * FROM processed_events WHERE id = ?", (event_id,))

    # --------------------------------------------- duplicate subscriptions

    def duplicates(self, org_id: str) -> list[str]:
        return [r["subscription_id"] for r in self._all(
            "SELECT subscription_id FROM org_duplicate_subscription WHERE org_id = ? ORDER BY created, subscription_id",
            (org_id,))]

    @staticmethod
    def duplicates_in(con: sqlite3.Connection, org_id: str) -> list[str]:
        return [r[0] for r in con.execute(
            "SELECT subscription_id FROM org_duplicate_subscription WHERE org_id = ?"
            " ORDER BY created, subscription_id", (org_id,)).fetchall()]

    @staticmethod
    def set_duplicate(con: sqlite3.Connection, org_id: str, sid: str, present: bool) -> bool:
        """Add or remove one duplicate; keep org.duplicate_subscription_id
        (the hold flag) = the oldest listed one. True when the set changed."""
        if present:
            cur = con.execute("INSERT OR IGNORE INTO org_duplicate_subscription (org_id, subscription_id, created)"
                              " VALUES (?, ?, ?)", (org_id, sid, int(time.time())))
        else:
            cur = con.execute("DELETE FROM org_duplicate_subscription WHERE org_id = ? AND subscription_id = ?",
                              (org_id, sid))
        first = con.execute("SELECT subscription_id FROM org_duplicate_subscription WHERE org_id = ?"
                            " ORDER BY created, subscription_id LIMIT 1", (org_id,)).fetchone()
        con.execute("UPDATE org SET duplicate_subscription_id = ? WHERE id = ?",
                    (first[0] if first else None, org_id))
        return cur.rowcount > 0

    @staticmethod
    def log(con: sqlite3.Connection, org_id: str | None, kind: str, detail: dict | None = None) -> None:
        con.execute("INSERT INTO billing_log (ts, org_id, kind, detail) VALUES (?, ?, ?, ?)",
                    (int(time.time()), org_id, kind, json.dumps(detail or {}, sort_keys=True)))

    def log_entries(self, org_id: str | None = None, kind: str | None = None) -> list[dict]:
        sql, args = "SELECT * FROM billing_log WHERE 1=1", []
        if org_id is not None:
            sql += " AND org_id = ?"
            args.append(org_id)
        if kind is not None:
            sql += " AND kind = ?"
            args.append(kind)
        return self._all(sql + " ORDER BY id", tuple(args))

    def append_log(self, org_id: str | None, kind: str, detail: dict | None = None) -> None:
        with self.txn() as con:
            self.log(con, org_id, kind, detail)
