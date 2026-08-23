"""API keys & scoping capabilities (ADR-3, D7 control #6).

Keys are namespace-scoped by default:
    memd_<namespace>_<secret>
A key may additionally pin a user (all queries forced to that user's scope)
and/or carry `scope_override` (cross-user reads inside the org). Secrets are
stored only as SHA-256 hashes.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import secrets
import threading
import time
from dataclasses import dataclass


@dataclass
class Principal:
    key_id: str
    namespace: str
    pinned_user: str | None = None
    scope_override: bool = False
    rate_limit_per_min: int = 600


def hash_secret(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()


def generate_key(namespace: str) -> tuple[str, str]:
    """Returns (full_key, key_record_id). Full key shown once at creation.
    Secret uses token_hex (no '_'/'-' separators) so 'last segment after _'
    parsing in authenticate() always covers the entire secret."""
    secret = secrets.token_hex(24)
    kid = secrets.token_hex(4)
    return f"memd_{namespace}_{kid}_{secret}", kid


class KeyStore:
    FORCE_REFRESH_S = 5.0  # bounded staleness window for same-tick swaps

    def __init__(self, path: str, admin_key: str | None = None):
        self.path = path
        self._lock = threading.Lock()
        self._keys: dict[str, dict] = {}  # kid -> record
        self._by_hash: dict[str, str] = {}  # secret-segment hash -> kid
        self._by_full_hash: dict[str, str] = {}  # whole-key hash -> kid (opaque keys)
        self._revoked: set[str] = set()  # kids this process dropped; never re-adopt
        self._tombstones: dict[str, dict] = {}  # kid -> revocation record (persisted)
        # content fingerprint of the keys file as we last saw it. mtime is
        # NOT trustworthy here: multiple flushes can land inside one
        # timestamp tick, which made peers skip reloads and clobber fresher
        # files with stale state.
        self._sig: str | None = None
        # fast-path stat signature + last full read (see _load)
        self._stat_sig: tuple | None = None
        self._last_full_read: float = 0.0
        self._load()
        if admin_key:
            self._register_full(admin_key, "admin", namespace="*", scope_override=True)

    def _read_kept(self) -> tuple[list[dict], str] | None:
        """Read + parse the keys file. Returns None when unreadable/corrupt
        or unchanged since our last read (content fingerprint match)."""
        if not os.path.exists(self.path):
            return None
        try:
            with open(self.path, "rb") as f:
                raw = f.read()
            sig = hashlib.sha256(raw).hexdigest()
            if sig == self._sig:
                return None
            recs = json.loads(raw)
            if not isinstance(recs, list):
                raise ValueError("keys file must be a JSON list")
            return recs, sig
        except (OSError, ValueError) as ex:
            from memd.metrics import METRICS

            METRICS.inc("memd_keystore_load_failures_total", reason=type(ex).__name__)
            return None

    def _load(self) -> None:
        """Reload when the file CONTENT changed. Fast path: one os.stat()
        (metadata only) per call - the previous formulation read + SHA-256'd
        the whole keys file on EVERY authenticate(), a constant disk hit on
        the hottest request path. Full read happens when the stat signature
        moves, or at least every FORCE_REFRESH_S so same-tick content swaps
        (mtime collisions) can never hide past a bounded window."""
        try:
            st = os.stat(self.path)
            stat_sig = (st.st_mtime_ns, st.st_size, st.st_ino)
            force_due = (time.monotonic() - self._last_full_read) >= self.FORCE_REFRESH_S
            if stat_sig == self._stat_sig and not force_due:
                return
        except OSError:
            return  # no file yet / transiently gone: keep last-known-good map
        kept = self._read_kept()
        if kept is None:
            return
        self._stat_sig = stat_sig
        self._last_full_read = time.monotonic()
        recs, self._sig = kept
        for rec in recs:
            kid = rec["key_id"]
            if rec.get("revoked"):
                self._tombstones.setdefault(kid, rec)  # keep propagating it
                self._drop_kid(kid)
                continue
            if kid in self._revoked:
                continue
            self._keys[kid] = rec
            self._by_hash[rec["hash"]] = kid
            full_h = rec.get("full_hash")
            if full_h:
                self._by_full_hash[full_h] = kid

    def _flush_locked(self) -> None:
        """Persist atomically AND merge-safely. An exclusive advisory lock
        (flock on a sidecar lockfile) serializes {merge -> write -> rename}
        across ALL KeyStore instances - threads in one process and separate
        processes alike - so concurrent creators can never clobber each
        other's keys. Revocations persist as tombstone records so every
        process learns them on next load (a cached key must not be silently
        rewritten back to disk by a peer that missed the revoke)."""
        lock_path = self.path + ".lock"
        with open(lock_path, "w") as lf:
            fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
            try:
                self._merge_from_disk_locked()
                tmp = f"{self.path}.{os.getpid()}.{secrets.token_hex(4)}.tmp"
                merged = [r for r in self._keys.values() if r["key_id"] not in self._revoked]
                merged.extend(self._tombstones.values())
                blob = json.dumps(merged, indent=1).encode()
                with open(tmp, "wb") as f:
                    f.write(blob)
                os.replace(tmp, self.path)
                self._sig = hashlib.sha256(blob).hexdigest()
                # adopt our own write into the fast-path signature so the
                # next authenticate() doesn't re-read what we just wrote
                try:
                    _st = os.stat(self.path)
                    self._stat_sig = (_st.st_mtime_ns, _st.st_size, _st.st_ino)
                    self._last_full_read = time.monotonic()
                except OSError:
                    pass
                try:
                    os.chmod(self.path, 0o600)  # key ids/namespaces are sensitive-ish
                except OSError:
                    pass
            finally:
                fcntl.flock(lf.fileno(), fcntl.LOCK_UN)

    def _merge_from_disk_locked(self) -> None:
        """Adopt records created by other processes that we don't hold yet,
        and honor revocation tombstones authored anywhere."""
        kept = self._read_kept()
        if kept is None:
            return
        disk, self._sig = kept
        known = set(self._keys)
        for rec in disk:
            kid = rec.get("key_id")
            if not kid:
                continue
            if rec.get("revoked"):
                self._tombstones.setdefault(kid, rec)  # keep propagating it
                self._drop_kid(kid)
                continue
            if kid in known or kid in self._revoked:
                continue
            self._keys[kid] = rec
            self._by_hash[rec.get("hash", "")] = kid
            full_h = rec.get("full_hash")
            if full_h:
                self._by_full_hash[full_h] = kid

    def _drop_kid(self, kid: str) -> None:
        rec = self._keys.pop(kid, None)
        if rec is not None:
            self._by_hash.pop(rec.get("hash", ""), None)
            self._by_full_hash.pop(rec.get("full_hash") or "", None)
        self._revoked.add(kid)
        if len(self._revoked) > 10_000:  # bounded: drop oldest tracking entry
            self._revoked.pop(next(iter(self._revoked)))

    _STRUCTURED_KEY_PARTS = 4  # memd_{namespace}_{kid}_{secret}

    def _register_full(self, full_key: str, name: str, namespace: str, scope_override: bool = False,
                       pinned_user: str | None = None) -> str:
        """Register a caller-supplied key. Structured `memd_ns_kid_secret`
        keys keep the legacy suffix-hash layout; anything else (an operator's
        opaque MEMD_ADMIN_KEY) is hashed whole so any format boots and
        authenticates instead of crashing the server at startup."""
        parts = full_key.split("_")
        structured = (
            len(parts) == self._STRUCTURED_KEY_PARTS
            and all(parts)
            and full_key.startswith("memd_")
        )
        with self._lock:
            if structured:
                kid = parts[2]
                h = hash_secret(parts[3])
            else:
                kid = secrets.token_hex(4)
                h = ""
                self._by_full_hash[hash_secret(full_key)] = kid
            self._keys[kid] = {
                "key_id": kid, "name": name, "hash": h,
                "full_hash": "" if structured else hash_secret(full_key),
                "namespace": namespace,
                "pinned_user": pinned_user, "scope_override": scope_override,
                "created": int(time.time()),
            }
            if h:
                self._by_hash[h] = kid
            self._flush_locked()
        return kid

    def create(self, namespace: str, name: str = "", pinned_user: str | None = None,
               scope_override: bool = False) -> tuple[str, str]:
        full, kid = generate_key(namespace)
        h = hash_secret(full.rsplit("_", 1)[1])
        with self._lock:
            self._keys[kid] = {
                "key_id": kid, "name": (name or f"key-{kid}")[:128], "hash": h, "namespace": namespace,
                "pinned_user": pinned_user, "scope_override": scope_override,
                "created": int(time.time()),
            }
            self._by_hash[h] = kid
            self._flush_locked()
        return full, kid

    def authenticate(self, bearer: str | None) -> Principal | None:
        if not bearer or not bearer.startswith("memd_"):
            # opaque keys (e.g. an operator-supplied admin secret) don't carry
            # the memd_ prefix - resolve via the whole-key hash table
            if bearer:
                kid = self._lookup_full_hash(bearer)
                if kid is not None:
                    return self._principal_for(kid)
            return None
        try:
            secret = bearer.rsplit("_", 1)[1]
        except IndexError:
            return None
        h = hash_secret(secret)
        with self._lock:
            self._load()  # pick up keys created by CLI/other processes
            kid = self._by_hash.get(h)
            if kid is None:
                kid = self._by_full_hash.get(hash_secret(bearer))
            if kid is None:
                return None
            rec = self._keys[kid]
        if not bearer.startswith(f"memd_{rec['namespace']}_{kid}_") and rec["namespace"] != "*":
            return None
        return self._principal_for(kid)

    def _lookup_full_hash(self, bearer: str) -> str | None:
        with self._lock:
            self._load()
            return self._by_full_hash.get(hash_secret(bearer))

    def _principal_for(self, kid: str) -> Principal | None:
        rec = self._keys.get(kid)
        if rec is None:
            return None
        return Principal(
            key_id=kid,
            namespace=rec["namespace"],
            pinned_user=rec.get("pinned_user"),
            scope_override=bool(rec.get("scope_override")),
        )

    def revoke(self, kid: str) -> bool:
        with self._lock:
            rec = self._keys.pop(kid, None)
            if rec is None:
                return False
            self._by_hash.pop(rec.get("hash", ""), None)
            self._by_full_hash.pop(rec.get("full_hash") or "", None)
            # tombstone: no secret material, just the fact of revocation -
            # propagates to every process via the shared file
            self._tombstones[kid] = {
                "key_id": kid,
                "name": rec.get("name", ""),
                "revoked": True,
                "created": rec.get("created", int(time.time())),
                "revoked_at": int(time.time()),
            }
            if len(self._tombstones) > 10_000:  # bounded
                self._tombstones.pop(next(iter(self._tombstones)))
            self._flush_locked()
            return True

    def list_keys(self) -> list[dict]:
        with self._lock:
            return [{k: v for k, v in r.items() if k not in ("hash", "full_hash")}
                    for r in self._keys.values()]


class FailureLimiter:
    """Bounded per-client failure tracker for pre-auth throttling.

    Authentication failures are keyed by client host (the only signal
    available before a valid credential exists): after `max_failures`
    failures within `window_s`, further attempts are rejected until the
    window slides. Successful auth clears the host's count. The map is
    bounded so spoofed/spinning hosts can't grow it without end."""

    def __init__(self, max_failures: int = 30, window_s: float = 60.0, max_hosts: int = 10_000):
        self.max_failures = max_failures
        self.window_s = window_s
        self.max_hosts = max_hosts
        self._events: dict[str, list[float]] = {}
        self._lock = threading.Lock()

    def blocked(self, client: str, now: float | None = None) -> bool:
        now = time.monotonic() if now is None else now
        with self._lock:
            evts = self._events.get(client)
            if not evts:
                return False
            cutoff = now - self.window_s
            evts[:] = [t for t in evts if t > cutoff]
            if not evts:
                self._events.pop(client, None)
                return False
            return len(evts) >= self.max_failures

    def record_failure(self, client: str, now: float | None = None) -> None:
        now = time.monotonic() if now is None else now
        with self._lock:
            if len(self._events) >= self.max_hosts and client not in self._events:
                # drop the oldest host (insertion-ordered dict)
                self._events.pop(next(iter(self._events)), None)
            evts = [t for t in self._events.get(client, []) if t > now - self.window_s]
            evts.append(now)
            self._events[client] = evts

    def record_success(self, client: str) -> None:
        with self._lock:
            self._events.pop(client, None)


class RateLimiter:
    """Per-principal token bucket (D6 noisy-neighbor containment)."""

    def __init__(self, max_buckets: int = 100_000):
        self._buckets: dict[str, list] = {}
        self.max_buckets = max_buckets
        self._lock = threading.Lock()

    def allow(self, key_id: str, limit_per_min: int) -> bool:
        now = time.monotonic()
        with self._lock:
            b = self._buckets.setdefault(key_id, [limit_per_min, now])
            refill = (now - b[1]) * (limit_per_min / 60.0)
            b[0] = min(limit_per_min, b[0] + refill)
            b[1] = now
            if len(self._buckets) > self.max_buckets:
                self._prune_locked(now)
            if b[0] >= 1:
                b[0] -= 1
                return True
            return False

    def forget(self, key_id: str) -> None:
        """Drop state for a revoked/deleted principal (bounded-memory hook)."""
        with self._lock:
            self._buckets.pop(key_id, None)

    def _prune_locked(self, now: float) -> None:
        # stale = untouched for 10 minutes; only called when over cap so the
        # common small-deployment path never pays the scan
        stale = [k for k, (_, ts) in self._buckets.items() if now - ts > 600.0]
        for k in stale:
            del self._buckets[k]
        if len(self._buckets) > self.max_buckets:
            for k in list(self._buckets)[: len(self._buckets) - self.max_buckets]:
                del self._buckets[k]
