"""S3-compatible ObjectStore (AWS S3, Cloudflare R2, MinIO, Ceph).

ADR-2 says object storage is the source of truth from the first byte. Until
now the only implementation was the local filesystem, so every hosted claim in
D2 was unevidenced by construction. This is that backend.

THE ONE HARD PART: S3 has no append.

`ObjectStore.append()` is the WAL's whole contract - "make these bytes durable,
tell me the new total" - and object storage gives you immutable PUTs. Two
designs were possible:

  (a) read-modify-write: GET the log, concatenate, PUT it back. Simple, and
      wrong: it transfers O(WAL) bytes per write ack, which blows both the
      150ms hosted ack SLO and the cost model, and it corrupts under any
      concurrent writer.
  (b) key-sequence parts: each append is its own immutable object
      `<key>/__part-000000000042`, and the logical object is the ordered
      concatenation of the whole object at `<key>` plus its parts.

This implements (b). One append = one PUT = one durable ack, which is exactly
the shape D2's hosted write-ack row cites, and it makes torn frames
structurally impossible: a part either exists whole or does not exist.

`open_log()` is deliberately NOT overridden. The engine's group-commit
machinery (persistent fd, sync-owner election, `_seal_wal_writer_locked`)
exists to amortize fsyncs on a local filesystem and is meaningless here.
Leaving it unimplemented routes writes through the "durable-on-return" path
that pass 1 built and tested for exactly this case, so a PUT returning IS the
durability guarantee.

WHAT THIS DOES NOT GIVE YOU
  - The SQLite derived index stays on LOCAL disk. It is rebuildable by
    contract; pass 22's index snapshot is what makes a cold node cheap.
  - Envelope keys are local files. Data is remote, keys are not, so this is
    "one node with remote durability", not "any node serves any namespace".
    Crypto-shred still works (destroy the local key); a second node cannot
    decrypt. A KMS provider is the missing piece and is not built.
  - Single-writer is enforced by a LEASE (see `try_acquire_owner`), not by the
    local flock, which cannot see other machines.
"""
from __future__ import annotations

import concurrent.futures as _futures
import json
import re
import threading
import time
import zlib

from memd.metrics import METRICS
from memd.storage.objectstore import (AppendConflict, LeaseLostError, ObjectStore,  # noqa: F401
                                      PreconditionFailed, _count_op, adopt_io_tally,
                                      current_io_tally)

# Append parts are SIBLINGS of the logical key, not children.
#
# The obvious scheme is `<key>/__part-000000000042`, and it is a trap: when the
# logical key also exists as a whole object (put-then-append, which the engine
# does), the part becomes a child of an object. MinIO ACCEPTS that PUT and
# returns success, but the object never becomes listable - so `get()` silently
# returned the pre-append bytes and an acked write vanished. Verified against
# a real MinIO; AWS tolerates it, which would have made this a
# works-on-my-endpoint bug.
#
# `<key>.__part-000000000042` sits beside the key instead. No engine key
# contains ".__part-", so this cannot collide with real data.
_PART_SEP = ".__part-"
_PART_RE = re.compile(r"^(?P<key>.+)\.__part-(?P<seq>\d{12})$")
_PART_WIDTH = 12
# S3 multipart: every part but the last must be >= 5MiB.
_MPU_PART_BYTES = 8 * 1024 * 1024
_MPU_THRESHOLD = 8 * 1024 * 1024


def _validate_key(key: str) -> str:
    """Same hygiene as the local store: no traversal, no absolute keys.

    S3 has no filesystem to escape, but engine code builds keys by string
    concatenation and the two backends must reject the same inputs or a test
    that passes on one lies about the other.
    """
    if not key or key.startswith("/") or ".." in key.split("/"):
        raise ValueError(f"invalid object key: {key!r}")
    return key


def _err_response(ex: BaseException) -> dict:
    """The parsed reply a botocore ClientError carries, or {}. A transport
    error (ReadTimeoutError, a connection reset) has none - no `response`,
    or one that is None - and reading a code off that raised
    AttributeError, which replaced the real error."""
    r = getattr(ex, "response", None)
    return r if isinstance(r, dict) else {}


class S3ObjectStore(ObjectStore):
    """Object store over the S3 API.

    Construct directly, or let `Memory("s3://bucket/prefix", ...)` build one.
    Credentials follow the normal boto3 chain when not passed explicitly
    (env, shared config, instance role), so nothing here needs secrets baked in.
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        *,
        endpoint_url: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        region: str | None = None,
        client=None,
        lease_ttl_s: float = 60.0,
        fetch_concurrency: int = 32,
        lease_holder: str | None = None,
    ):
        try:
            import boto3
            from botocore.config import Config
        except ImportError as ex:  # pragma: no cover - dependency guard
            raise RuntimeError(
                "S3ObjectStore needs boto3: pip install 'memd[s3]'") from ex

        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.lease_ttl_s = float(lease_ttl_s)
        self.fetch_concurrency = max(1, int(fetch_concurrency))
        self._client = client or boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=Config(retries={"max_attempts": 5, "mode": "standard"},
                          signature_version="s3v4",
                          # the connection pool must cover the fetch fan-out or
                          # concurrent GETs serialize on connections instead
                          max_pool_connections=max(10, int(fetch_concurrency) + 4)),
        )
        # Control-plane client: lease objects and the cluster registry, with
        # SHORT timeouts and few retries. On MinIO a conditional PUT holds the
        # object's lock until its body arrives, so a peer frozen mid-renewal
        # (SIGSTOP, a VM pause) blocks every reader of that object for
        # MinIO's ~30 s lock timeout. With the data-plane client's 60 s read
        # timeout and 5 retries a router thread waited that out - per request,
        # until the pool was exhausted. A control-plane call instead fails in
        # seconds and is treated as "busy, retry" (AWS S3 does not lock).
        self._ctl = client or boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=Config(retries={"total_max_attempts": 2, "mode": "standard"},
                          signature_version="s3v4", connect_timeout=2, read_timeout=4,
                          max_pool_connections=10),
        )
        # Next part number per logical key. Single-writer per data root (see
        # try_acquire_owner), so an in-process counter is authoritative once
        # seeded; seeding costs one LIST on the first append to a key, not one
        # per append.
        self._next_part: dict[str, int] = {}
        # Namespaces this store believes it owns, and ones it has been FENCED
        # out of. A lease that is never renewed expires under a live writer;
        # a lease that is renewed can still be lost to a partition. Both are
        # handled: a heartbeat keeps the lease fresh, and losing it fences the
        # namespace so writes fail loudly instead of interleaving with the new
        # owner's. See try_acquire_owner.
        self._fenced: set[str] = set()
        # Last SUCCESSFUL heartbeat per namespace, on the MONOTONIC clock.
        # Wall-clock is what the lease body carries and is therefore subject to
        # another machine's skew; "is my own heartbeat fresh?" must not be.
        self._last_beat: dict[str, float] = {}
        self._lease_stop = threading.Event()
        self._lease_thread: threading.Thread | None = None
        # Serialize this process's writes of one namespace's lease object (a
        # renewal - heartbeat, fence check, fresh read - and the release): a
        # renewal in flight while the lease was released left it LIVE after a
        # clean close (see release_owner). Striped by namespace: bounded, and
        # nothing to clean up when a namespace goes.
        self._lease_locks = tuple(threading.RLock() for _ in range(64))
        # running logical size per key, so an append does not re-LIST the log
        self._size_cache: dict[str, int] = {}
        self._seq_lock = threading.Lock()
        self._leases: dict[str, str] = {}
        # ETag of our last write of each held lease: renewals are
        # compare-and-swap on it (None: the endpoint has no conditional PUT)
        self._lease_etag: dict[str, str | None] = {}
        self._took_over: set[str] = set()
        # per append log: one past the highest part this process has SEEN
        # (listed by a read). With _next_part it is the log_bound a commit
        # captures, so the delete after the commit cannot reach parts a
        # successor wrote while this process was paused
        self._seen_hw: dict[str, int] = {}
        self._hw_lock = threading.Lock()
        # Per append log: a part number this process must never number BELOW
        # again - its own earlier counters (kept when a namespace's cached
        # state is dropped) and the high-water mark the namespace's manifest
        # records (set_log_floor). Numbering is strictly monotonic across
        # every tenure of every node, so no part number is ever reused.
        self._floor: dict[str, int] = {}
        # who this store claims leases as; NamespaceStore falls back to
        # host:pid. A cluster node names itself here so the router can map a
        # lease to the node serving it (see memd.server.cluster)
        self.lease_holder = lease_holder

    # ------------------------------------------------------------- plumbing

    def _full(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def _rel(self, full: str) -> str:
        if self.prefix and full.startswith(self.prefix + "/"):
            return full[len(self.prefix) + 1:]
        return full

    def _is_missing(self, ex: Exception) -> bool:
        code = self._code(ex)
        status = (_err_response(ex).get("ResponseMetadata") or {}).get("HTTPStatusCode")
        return code in ("NoSuchKey", "404", "NotFound") or status == 404

    def _fetch_parts(self, segs: list) -> list[bytes]:
        """Read many parts CONCURRENTLY.

        Reading them one at a time makes every whole-log read O(parts) SERIAL
        round trips, and two paths do exactly that: cold open GETs the entire
        WAL (measured 2,002 GetObject calls and 9.6s at ~2k records - 6.4x over
        the 1.5s cold-first-query SLO), and WAL rotation reads every part
        inline on an unlucky caller's write ack (6.2s for one add()). The
        bytes still all arrive - object storage latency is dominated by round
        trips, not bandwidth - so the fix is concurrency, not fewer bytes.
        Memory is unchanged: the caller concatenates them anyway, and the WAL
        is bounded by wal_rotate_bytes.
        """
        if len(segs) <= 1:
            return [self._raw_get(f) or b"" for f, _ in segs]
        workers = min(self.fetch_concurrency, len(segs))
        out: list[bytes] = [b""] * len(segs)
        # Hand the caller's I/O tally to the workers. A pool thread starts
        # with a FRESH context, so count_io()'s ContextVar is invisible there -
        # parallelizing the fetch silently stopped the per-request I/O meter
        # from seeing any of it, which is exactly the accounting this backend
        # exists to make honest. (A copied Context cannot be shared: one
        # Context object may only be entered by one thread at a time.)
        tally = current_io_tally()

        def _one(full: str) -> bytes | None:
            adopt_io_tally(tally)
            return self._raw_get(full)

        with _futures.ThreadPoolExecutor(max_workers=workers,
                                         thread_name_prefix="memd-s3-get") as ex:
            futs = {ex.submit(_one, f): i for i, (f, _) in enumerate(segs)}
            for fut in _futures.as_completed(futs):
                out[futs[fut]] = fut.result() or b""
        return out

    def _raw_get(self, full: str) -> bytes | None:
        _count_op("get_object")   # the per-part fan-out was invisible before
        try:
            return self._client_for(full).get_object(Bucket=self.bucket, Key=full)["Body"].read()
        except Exception as ex:
            if self._is_missing(ex):
                return None
            raise

    def _raw_put(self, full: str, data: bytes) -> None:
        self._client_for(full).put_object(Bucket=self.bucket, Key=full, Body=data)

    def _raw_delete(self, full: str) -> None:
        try:
            self._client_for(full).delete_object(Bucket=self.bucket, Key=full)
        except Exception as ex:
            if not self._is_missing(ex):
                raise

    def _delete_batch(self, batch: list[dict]) -> None:
        """One DeleteObjects per 1000 keys instead of one DeleteObject each.

        `delete()` looped serially - 502 requests and 2.3s to drop a 500-part
        log - while `remove_prefix` in the same file already batched. Same
        physical work, two orders of magnitude fewer round trips.
        """
        if not batch:
            return
        _count_op("delete_batch")
        self._client.delete_objects(Bucket=self.bucket, Delete={"Objects": batch})

    def _client_for(self, full: str):
        """The control-plane client for lease and registry objects."""
        if full.endswith("/.owner") or f"/{_REGISTRY_DIR}/" in f"/{full}":
            return self._ctl
        return self._client

    def _raw_head(self, full: str) -> int | None:
        """Object size, or None when absent."""
        try:
            return int(self._client_for(full).head_object(Bucket=self.bucket, Key=full)["ContentLength"])
        except Exception as ex:
            if self._is_missing(ex):
                return None
            raise

    def _iter_keys(self, full_prefix: str, start_after: str | None = None):
        """Paginated LIST. Every PAGE is a round trip and is counted: the
        per-request I/O accounting reported `append: 1` for a call that was
        really doing a 3-page LIST underneath, which is exactly the kind of
        under-reporting the I/O budget exists to catch."""
        paginator = self._client.get_paginator("list_objects_v2")
        kw = {"Bucket": self.bucket, "Prefix": full_prefix}
        if start_after:
            kw["StartAfter"] = start_after
        for page in paginator.paginate(**kw):
            _count_op("list_page")
            for obj in page.get("Contents", []) or []:
                yield obj["Key"], int(obj["Size"])

    def _seq_hint_key(self, key: str) -> str:
        return f"{self._full(key)}.__seq"

    def _seed_seq(self, key: str) -> tuple[int, int]:
        """(next part number, current logical size) for a cold key.

        A long-lived append log accumulates one object per append, so seeding
        by LISTing every part costs O(parts) round trips ON THE WRITE PATH -
        measured 796.7ms for the first append against a 3000-part ledger
        versus 10.9ms warm, and it grows without bound. A tiny hint object
        records roughly where the log had reached, and the LIST resumes from
        there with StartAfter, so the scan is bounded by how far the hint has
        drifted rather than by the length of the log. The hint is a HINT: it
        is rebuildable, never trusted for correctness, and a missing or stale
        one only costs the old full scan.
        """
        hint_seq, hint_bytes = 0, 0
        raw = self._raw_get(self._seq_hint_key(key))
        if raw:
            try:
                h = json.loads(raw.decode())
                hint_seq = max(0, int(h["seq"]))
                hint_bytes = max(0, int(h["bytes"]))
            except Exception:
                hint_seq, hint_bytes = 0, 0
        if not hint_seq:
            return self._seed_seq_full(key)
        if hint_bytes and self._raw_head(
                f"{self._full(key)}{_PART_SEP}{hint_seq - 1:0{_PART_WIDTH}d}") is None:
            # The part the hint counts up to is gone: the prefix it sums was
            # deleted after it was written (a writer that lost its lease and
            # resumed wrote it late, ADR-12). Rescan - but never let the
            # numbering go back below the hint's.
            seq, size = self._seed_seq_full(key)
            return max(seq, hint_seq), size

        # Resume from the hint. The hint carries BOTH the next sequence number
        # and the logical size AT that point, because knowing only the sequence
        # still forced a full scan to total the bytes - which is what made the
        # first version of this only half a fix (797ms -> 435ms at 3000 parts,
        # still growing). Parts are immutable and monotonic, and every path
        # that could invalidate a prefix (put, delete, truncate, copy-onto)
        # deletes the hint, so resuming is exact rather than approximate.
        start_after = f"{self._full(key)}{_PART_SEP}{hint_seq - 1:0{_PART_WIDTH}d}"
        seq, added = hint_seq, 0
        for full, size in self._iter_keys(self._full(key) + _PART_SEP, start_after):
            m = _PART_RE.match(full)
            if not m:
                continue
            n = int(m.group("seq"))
            if n < hint_seq:
                continue
            seq = max(seq, n + 1)
            added += size
        return seq, hint_bytes + added

    def _seed_seq_full(self, key: str) -> tuple[int, int]:
        parts = self._parts(key)
        seq = 0
        if parts:
            m = _PART_RE.match(parts[-1][0])
            seq = int(m.group("seq")) + 1 if m else len(parts)
        return seq, sum(sz for _, sz in self._segments(key))

    def _iter_under(self, prefix: str):
        """Every physical key belonging to `prefix`, treating it as a PATH
        boundary rather than a string prefix.

        S3 has no directories, so a raw `startswith` LIST made
        `remove_prefix("ns/acme")` also match `ns/acme-eu/...` - i.e.
        destroy_namespace on one tenant CRYPTO-SHREDDED a different tenant
        whose name merely started with the same characters. The local backend
        was never exposed to this because a filesystem prefix IS a directory.
        Two exact scans instead: everything under `<prefix>/`, plus the
        artifacts that belong to `<prefix>` itself as a logical key.
        """
        base = self._full(prefix.rstrip("/"))
        seen: set[str] = set()
        # everything genuinely beneath the directory
        for full, size in self._iter_keys(base + "/"):
            if full not in seen:
                seen.add(full)
                yield full, size
        # ...plus this key's OWN append parts. The scan is a LIST prefix, so it
        # must be filtered by the exact part pattern: `_validate_ns` permits
        # dots, hyphens and underscores, which makes
        # "acme.__part-000000000000" a LEGAL namespace name whose objects sit
        # under `ns/acme.__part-000000000000/`. Matching the prefix alone
        # destroyed that namespace along with `acme` - the first cut of this
        # boundary fix closed the obvious sibling case and left this one open.
        for full, size in self._iter_keys(base + _PART_SEP):
            if _PART_RE.match(full) and full not in seen:
                seen.add(full)
                yield full, size
        # ...and its seq hint, by EXACT key rather than prefix (same reason:
        # "acme.__seq" is also a legal namespace name).
        hint = base + ".__seq"
        hsize = self._raw_head(hint)
        if hsize is not None and hint not in seen:
            seen.add(hint)
            yield hint, hsize
        head = self._raw_head(base)
        if head is not None and base not in seen:
            yield base, head

    def _parts(self, key: str) -> list[tuple[str, int]]:
        """(full key, size) of this logical key's append parts, in order.

        Remembers the highest part seen (see log_bound)."""
        out = []
        for full, size in self._iter_keys(self._full(key) + _PART_SEP):
            if _PART_RE.match(full):
                out.append((full, size))
        out.sort()
        if out:
            m = _PART_RE.match(out[-1][0])
            if m:
                # its own lock: _parts also runs under _seq_lock (seeding)
                with self._hw_lock:
                    self._seen_hw[key] = max(self._seen_hw.get(key, 0), int(m.group("seq")) + 1)
        return out

    def _segments(self, key: str) -> list[tuple[str, int]]:
        """Every physical piece of a logical object, in read order.

        The whole object at `key` comes first (a `put` followed by `append`s is
        a supported sequence - the contract suite pins it), then the parts.
        """
        segs: list[tuple[str, int]] = []
        head = self._raw_head(self._full(key))
        if head is not None:
            segs.append((self._full(key), head))
        segs.extend(self._parts(key))
        return segs

    # ------------------------------------------------------------------ API

    def put(self, key: str, data: bytes) -> None:
        _count_op("put")
        _validate_key(key)
        self._check_fence(key)
        # A put replaces the LOGICAL object, so any append parts it accumulated
        # must go with it - otherwise a later get() would concatenate the new
        # body with stale tail bytes.
        self._delete_batch([{"Key": f} for f, _ in self._parts(key)])
        with self._seq_lock:
            self._next_part.pop(key, None)
            self._size_cache.pop(key, None)
        self._raw_delete(self._seq_hint_key(key))
        self._raw_put(self._full(key), data)

    def put_if_absent(self, key: str, data: bytes) -> bool:
        """Conditional create (If-None-Match: *): exactly one of several
        racing creators wins, the rest get False. Only for whole objects that
        are never appended to (wrapped data keys, the key-custody marker)."""
        _count_op("put_if_absent")
        _validate_key(key)
        self._check_fence(key)
        try:
            self._client.put_object(Bucket=self.bucket, Key=self._full(key), Body=data,
                                    IfNoneMatch="*")
            return True
        except Exception as ex:
            code = self._code(ex)
            if code in ("PreconditionFailed", "412", "ConditionalRequestConflict"):
                return False
            if code in ("NotImplemented", "InvalidRequest", "InvalidArgument"):
                # no conditional writes on this endpoint: best effort
                if self._raw_head(self._full(key)) is not None:
                    return False
                self._raw_put(self._full(key), data)
                return True
            raise

    # ------------------------------------------------ conditional writes

    def get_versioned(self, key: str) -> tuple[bytes, str] | None:
        """(body, ETag) of a whole object (never an append log)."""
        _count_op("get")
        _validate_key(key)
        return self._raw_get_meta(self._full(key))

    def fence(self, namespace: str) -> None:
        """Stop writing `namespace`: drop our lease record WITHOUT touching
        the lease object (it may be someone else's now). Every later
        mutation raises LeaseLostError until the namespace is reopened."""
        self._fence(namespace)

    def put_if_match(self, key: str, data: bytes, version: str | None, *,
                     hint: bool = False, fence: bool = True) -> str:
        """Conditional PUT: If-Match on `version` (the ETag this process last
        observed or wrote), If-None-Match: * when `version` is None. A
        failed precondition means another writer changed the object: the
        namespace is FENCED (unless fence=False: the one caller that expects
        to race, a takeover re-reading the manifest) and PreconditionFailed
        raised. Never retried here."""
        _count_op("put_if_match")
        _validate_key(key)
        self._check_fence(key)
        full = self._full(key)
        try:
            etag = self._raw_cas_put(full, data, if_match=version,
                                     if_none_match=version is None)
        except _NoConditional:
            # an endpoint without conditional writes: best effort, loud metric
            METRICS.inc("memd_s3_unconditional_writes_total",
                        help="conditional writes degraded to plain PUTs (endpoint lacks If-Match)")
            self._raw_put(full, data)
            return ""
        if etag is None:
            METRICS.inc("memd_s3_precondition_failures_total",
                        help="conditional writes that found the object changed (ownership lost)")
            ns = self._ns_of(key)
            if fence and ns is not None:
                self._fence(ns)
            raise PreconditionFailed(
                f"{key!r} was changed by another writer since this process last wrote it; "
                + (f"namespace {ns!r} is fenced" if fence and ns else "not overwritten"))
        return etag

    def list_meta(self, prefix: str) -> list[dict]:
        """Objects under `prefix` with {key, size, etag, age_s} - `age_s`
        on the SERVER's clock (its Date header minus LastModified), so no
        node's clock skew enters it. One LIST, no per-object reads: LIST is
        never blocked by an object a frozen peer holds locked (see _ctl)."""
        from email.utils import parsedate_to_datetime

        _count_op("list")
        out: list[dict] = []
        kw = {"Bucket": self.bucket, "Prefix": self._full(prefix)}
        while True:
            r = self._ctl.list_objects_v2(**kw)
            date = (r.get("ResponseMetadata", {}).get("HTTPHeaders", {}) or {}).get("date")
            now = parsedate_to_datetime(date).timestamp() if date else time.time()
            for o in r.get("Contents", []) or []:
                out.append({"key": self._rel(o["Key"]), "size": int(o["Size"]),
                            "etag": o.get("ETag", ""),
                            # Date is truncated to the second, so this never
                            # reports a write as older than it is
                            "age_s": max(0.0, now - o["LastModified"].timestamp())})
            if not r.get("IsTruncated"):
                return out
            kw["ContinuationToken"] = r["NextContinuationToken"]

    def log_bound(self, key: str) -> int | None:
        with self._seq_lock:
            nxt = max(self._next_part.get(key, 0), self._floor.get(key, 0))
        with self._hw_lock:
            hw = max(nxt, self._seen_hw.get(key, 0))
        return hw if hw else None

    def delete_log(self, key: str, upto: int | None) -> None:
        """Delete the parts of an append log numbered below `upto` (and its
        base object) - never a part numbered at or above it, and never an
        EMPTY part (a successor's takeover fence, which must outlive any
        paused writer: see NamespaceStore._fence_previous_writer). Part
        numbering then continues from `upto` - persisted in the seq hint -
        so no later part can reuse a number a paused writer still means to
        delete or create."""
        if upto is None:
            return self.delete(key)
        _count_op("delete")
        _validate_key(key)
        self._check_fence(key)
        doomed: list[dict] = []
        above = 0
        for full, size in self._parts(key):
            n = int(_PART_RE.match(full).group("seq"))
            if n >= upto:
                above += size
            elif size > 0:
                doomed.append({"Key": full})
        for i in range(0, len(doomed), 1000):
            self._delete_batch(doomed[i:i + 1000])
        self._raw_delete(self._full(key))
        with self._seq_lock:
            self._next_part[key] = max(self._next_part.get(key, 0), upto)
            self._size_cache[key] = above
        try:
            self._raw_put(self._seq_hint_key(key), json.dumps({"seq": upto, "bytes": 0}).encode())
        except Exception:
            self._raw_delete(self._seq_hint_key(key))   # a hint may be absent, never wrong

    def shred(self, key: str) -> None:
        """Delete `key` AND every noncurrent version of it.

        On a versioned bucket a plain DELETE only stacks a delete marker on
        top: the old version - here, a wrapped data key - stays readable to
        anyone with s3:GetObjectVersion, and crypto-shred silently becomes
        "hidden". Remove each version explicitly. When the versions API is
        unavailable (an endpoint without it, a policy denying it) the plain
        delete is enough ONLY for a bucket that is not versioned - otherwise
        this raises rather than report a shred that did not happen."""
        _count_op("shred")
        _validate_key(key)
        self._check_fence(key)
        full = self._full(key)
        try:
            batch: list[dict] = []
            paginator = self._client.get_paginator("list_object_versions")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=full):
                for v in (page.get("Versions") or []) + (page.get("DeleteMarkers") or []):
                    if v.get("Key") == full and v.get("VersionId"):
                        batch.append({"Key": full, "VersionId": v["VersionId"]})
            for i in range(0, len(batch), 1000):
                self._delete_batch(batch[i:i + 1000])
        except Exception as ex:
            code = self._code(ex)
            if code not in ("NotImplemented", "AccessDenied", "MethodNotAllowed"):
                raise
            try:
                status = self._client.get_bucket_versioning(Bucket=self.bucket).get("Status")
            except Exception:
                status = "unknown"
            if status:   # Enabled, Suspended - or we cannot tell
                raise RuntimeError(
                    f"cannot remove old versions of {key!r} (bucket versioning: {status}): "
                    "grant s3:ListBucketVersions and s3:DeleteObjectVersion, or crypto-shred "
                    "leaves the wrapped key readable") from ex
        self._raw_delete(full)

    def get(self, key: str) -> bytes | None:
        _count_op("get")
        _validate_key(key)
        segs = self._segments(key)
        if not segs:
            return None
        if len(segs) == 1:
            return self._raw_get(segs[0][0]) or b""
        return b"".join(self._fetch_parts(segs))

    def delete(self, key: str) -> None:
        _count_op("delete")
        _validate_key(key)
        self._check_fence(key)
        self._delete_batch([{"Key": f} for f, _ in self._parts(key)])
        with self._seq_lock:
            self._next_part.pop(key, None)
            self._size_cache.pop(key, None)
        self._delete_batch([{"Key": self._seq_hint_key(key)},
                            {"Key": self._full(key)}])

    def exists(self, key: str) -> bool:
        _count_op("exists")
        _validate_key(key)
        return bool(self._segments(key))

    def size(self, key: str) -> int:
        _count_op("size")
        _validate_key(key)
        return sum(sz for _, sz in self._segments(key))

    def list(self, prefix: str) -> list[str]:
        """Logical keys under `prefix`.

        Append parts are collapsed to the key that owns them: callers such as
        orphan-segment adoption and `list_namespaces` reason about logical
        objects, and surfacing `wal/__part-000000000007` as a key would be a
        different contract from the local store's.
        """
        _count_op("list")
        out: set[str] = set()
        for full, _ in self._iter_under(prefix):
            if full.endswith(".__seq"):
                continue      # internal hint, not a logical object
            m = _PART_RE.match(full)
            out.add(self._rel(m.group("key") if m else full))
        return sorted(out)

    def append(self, key: str, data: bytes) -> int:
        """One append = one immutable PUT = one durable ack.

        Returns the new logical size. Because each frame is its own object, a
        torn frame cannot exist: the part is either fully written or absent.
        """
        _count_op("append")
        _validate_key(key)
        self._check_fence(key)
        with self._seq_lock:
            seq = self._next_part.get(key)
            if seq is None:
                # first append to this key in this tenure: from the bucket,
                # never below this key's floor (see _floor)
                seq, base = self._seed_seq(key)
                seq = max(seq, self._floor.get(key, 0))
            else:
                base = self._size_cache.get(key)
                if base is None:
                    base = sum(sz for _, sz in self._segments(key))
            self._next_part[key] = seq + 1
        full = f"{self._full(key)}{_PART_SEP}{seq:0{_PART_WIDTH}d}"
        # Conditional create: if this part already exists, another writer owns
        # this log. Fail loudly rather than silently interleaving - two writers
        # on one data root is the CRIT that motivated the owner lease.
        try:
            self._client.put_object(Bucket=self.bucket, Key=full, Body=data,
                                    IfNoneMatch="*")
        except Exception as ex:
            code = self._code(ex)
            if code in ("PreconditionFailed", "412"):
                METRICS.inc("memd_s3_append_conflicts_total",
                            help="append part already existed: another writer holds this log")
                raise AppendConflict(
                    f"append conflict on {key!r}: part {seq} already exists. "
                    "Another process is writing this data root - memd is "
                    "single-writer.") from None
            if code in ("NotImplemented", "InvalidRequest", "InvalidArgument"):
                # endpoint predates conditional writes; fall back (the owner
                # lease is still the primary protection)
                self._raw_put(full, data)
            else:
                raise
        total = base + len(data)
        self._remember_size(key, total)
        # Refresh the hint periodically, not per append: one extra PUT every
        # 64 frames is ~1.5% write overhead and keeps the cold-open scan short.
        if (seq % 64) == 0:
            try:
                self._raw_put(
                    self._seq_hint_key(key),
                    json.dumps({"seq": seq + 1, "bytes": total}).encode())
            except Exception:
                pass  # a hint is rebuildable; losing it only costs a full scan
        return total

    def _remember_size(self, key: str, total: int) -> None:
        self._size_cache[key] = total

    def truncate(self, key: str, size: int) -> None:
        """Cut the logical object back to `size` bytes (torn-tail repair)."""
        _count_op("truncate")
        _validate_key(key)
        self._check_fence(key)
        acc = 0
        for full, sz in self._segments(key):
            if acc >= size:
                self._raw_delete(full)
                continue
            if acc + sz <= size:
                acc += sz
                continue
            keep = size - acc
            blob = self._raw_get(full) or b""
            if keep <= 0:
                self._raw_delete(full)
            else:
                self._raw_put(full, blob[:keep])
            acc = size
        if size == 0:
            # Local truncate(k, 0) leaves a zero-byte FILE: get() is b"" and
            # exists() is True. Deleting everything here made the two backends
            # answer differently for the same call, and the contract suite is
            # explicit that local is the oracle.
            self._raw_put(self._full(key), b"")
        with self._seq_lock:
            self._next_part.pop(key, None)
            self._size_cache.pop(key, None)
        self._raw_delete(self._seq_hint_key(key))

    def copy(self, src: str, dst: str) -> None:
        """Duplicate a logical object without pulling it through this process.

        The naive implementation - `put(dst, get(src))` - is the wrong shape for
        an object store: it buffers the ENTIRE object in RAM (measured 33.9MB
        peak for a 24MB object) and moves every byte twice over the network.
        Its caller is audit-ledger rotation, which seals segments at
        ROTATE_BYTES = 64MB, so that is ~90MB of process memory per rotation on
        a path that is supposed to be bounded maintenance.

        Three cases, in order of cheapness:
          - one whole object: server-side CopyObject. Zero bytes through the
            client, one round trip.
          - a small append log: one GET + one PUT, which is what it was.
          - a large append log: streaming multipart upload. Parts are
            accumulated into >=8MiB buffers and uploaded as they fill, so peak
            memory is one buffer regardless of how long the log is.
        """
        _count_op("copy")
        _validate_key(src)
        _validate_key(dst)
        # copy WRITES dst and reaps dst's parts, so it is a mutating path and
        # must fence like the others. It was the last one missing - the same
        # gap remove_prefix had, and a fenced writer could still overwrite the
        # new owner's data through it.
        self._check_fence(dst)
        segs = self._segments(src)
        if not segs:
            return
        # replacing dst means its own parts AND its seq hint must go, same as
        # put(). Leaving the hint behind made the first append after a
        # copy-over resume from a stale {seq, bytes} and return a wrong size -
        # and that value becomes manifest.wal_size, which replay trusts.
        self._delete_batch([{"Key": f} for f, _ in self._parts(dst)])
        with self._seq_lock:
            self._next_part.pop(dst, None)
            self._size_cache.pop(dst, None)
        self._raw_delete(self._seq_hint_key(dst))

        if len(segs) == 1 and segs[0][0] == self._full(src):
            self._client.copy_object(
                Bucket=self.bucket, Key=self._full(dst),
                CopySource={"Bucket": self.bucket, "Key": self._full(src)})
            return

        total = sum(sz for _, sz in segs)
        if total < _MPU_THRESHOLD:
            self._raw_put(self._full(dst), self.get(src) or b"")
            return

        upload_id = self._client.create_multipart_upload(
            Bucket=self.bucket, Key=self._full(dst))["UploadId"]
        try:
            buf = bytearray()
            pno = 1
            done: list[dict] = []

            def _flush(chunk: bytes) -> None:
                nonlocal pno
                r = self._client.upload_part(
                    Bucket=self.bucket, Key=self._full(dst), UploadId=upload_id,
                    PartNumber=pno, Body=chunk)
                done.append({"ETag": r["ETag"], "PartNumber": pno})
                pno += 1

            for full, _ in segs:
                buf += self._raw_get(full) or b""
                while len(buf) >= _MPU_PART_BYTES:
                    _flush(bytes(buf[:_MPU_PART_BYTES]))
                    del buf[:_MPU_PART_BYTES]
            if buf or not done:
                _flush(bytes(buf))
            self._client.complete_multipart_upload(
                Bucket=self.bucket, Key=self._full(dst), UploadId=upload_id,
                MultipartUpload={"Parts": done})
        except BaseException:
            # never leave a half-finished upload accruing storage charges
            try:
                self._client.abort_multipart_upload(
                    Bucket=self.bucket, Key=self._full(dst), UploadId=upload_id)
            except Exception:
                pass
            raise

    def remove_prefix(self, prefix: str) -> int:
        """Delete everything under `prefix`; returns LOGICAL objects removed."""
        _count_op("remove_prefix")
        self._check_fence(prefix.rstrip("/") + "/x")
        base = self._full(prefix.rstrip("/"))
        logical: set[str] = set()
        batch: list[dict] = []
        for full, _ in self._iter_under(prefix):
            if not full.endswith(".__seq"):
                # the seq hint is internal bookkeeping, not an object the
                # caller ever stored - counting it made remove_prefix report
                # 3 where the local oracle reports 2
                m = _PART_RE.match(full)
                logical.add(m.group("key") if m else full)
            batch.append({"Key": full})
            if len(batch) == 1000:
                self._delete_batch(batch)
                batch = []
        if batch:
            self._delete_batch(batch)
        with self._seq_lock:
            for k in list(self._next_part):
                fk = self._full(k)
                if fk == base or fk.startswith(base + "/") or fk.startswith(base + _PART_SEP):
                    self._next_part.pop(k, None)
                    self._size_cache.pop(k, None)
        return len(logical)

    # -------------------------------------------------------- owner leasing
    #
    # The lease object `ns/<ns>/.owner` holds "<holder>\n<wall-clock stamp>".
    # Every write of it after the first is a COMPARE-AND-SWAP on its ETag
    # (If-Match): renewals, refreshes and stale reclaims alike. Read-then-put
    # left two holes a multi-node fleet hits for real (ADR-12):
    #   - two nodes reclaiming one stale lease both "won" (last PUT wins, the
    #     first keeps writing until its next beat notices), and
    #   - a holder that stalled between reading its lease and renewing it
    #     overwrote the NEW owner's lease and took the namespace back.
    # With CAS exactly one reclaimer wins, and a renewal that finds the ETag
    # moved fences instead of overwriting. Endpoints without conditional
    # writes fall back to the old read-then-put (loud, not impossible).
    #
    # Self-fencing: a holder whose last SUCCESSFUL renewal is older than
    # 2/3 of the TTL (its own monotonic clock) refuses to mutate until a
    # renewal succeeds - another node may reclaim at TTL, so the last third
    # is the margin for clock skew between the two machines.

    def _owner_key(self, namespace: str) -> str:
        return self._full(f"ns/{namespace}/.owner")

    def _ns_of(self, key: str) -> str | None:
        """The namespace a key belongs to, for fence checks."""
        parts = key.split("/")
        return parts[1] if len(parts) >= 2 and parts[0] == "ns" else None

    def _beat_period(self) -> float:
        return max(1.0, self.lease_ttl_s / 3.0)

    def _valid_for(self) -> float:
        """How long after a successful renewal this process may still write."""
        return self.lease_ttl_s * 2.0 / 3.0

    @staticmethod
    def _code(ex: Exception) -> str:
        """The S3 error code of `ex` - "" for an error without a parsed
        reply (see _err_response)."""
        return str((_err_response(ex).get("Error") or {}).get("Code", "") or "")

    def _raw_get_meta(self, full: str) -> tuple[bytes, str] | None:
        """(body, ETag) or None when absent."""
        _count_op("get_object")
        try:
            r = self._client_for(full).get_object(Bucket=self.bucket, Key=full)
            return r["Body"].read(), r.get("ETag", "")
        except Exception as ex:
            if self._is_missing(ex):
                return None
            raise

    def _raw_cas_put(self, full: str, data: bytes, *, if_match: str | None = None,
                     if_none_match: bool = False) -> str | None:
        """Conditional PUT. Returns the new ETag, or None when the condition
        failed (412, or 404 for If-Match on a vanished object). Raises
        _NoConditional on an endpoint that does not support the condition."""
        kw: dict = {}
        if if_match is not None:
            kw["IfMatch"] = if_match
        if if_none_match:
            kw["IfNoneMatch"] = "*"
        try:
            r = self._client_for(full).put_object(Bucket=self.bucket, Key=full, Body=data, **kw)
            return r.get("ETag", "") or ""
        except Exception as ex:
            code = self._code(ex)
            if code in ("PreconditionFailed", "412", "ConditionalRequestConflict") or \
                    (if_match is not None and self._is_missing(ex)):
                return None
            if code in ("NotImplemented", "InvalidRequest", "InvalidArgument"):
                raise _NoConditional() from ex
            raise

    def _lease_body(self, holder: str) -> bytes:
        return f"{holder}\n{time.time()}".encode()

    def _hold(self, namespace: str, holder: str, etag: str | None, t0: float) -> None:
        # Every (re)acquisition starts from the bucket, not from what this
        # process remembered of an EARLIER tenure: other nodes may have
        # appended, folded and deleted since. A stale part counter numbered
        # new parts below existing ones (the log order is the part order) and
        # placed the takeover fence below a paused writer's next part - acked
        # writes were lost across an A -> B -> A handoff.
        self._forget_ns(namespace)
        self._leases[namespace] = holder
        self._lease_etag[namespace] = etag
        self._fenced.discard(namespace)
        self._last_beat[namespace] = t0
        self._start_lease_thread()

    def _fence(self, ns: str) -> None:
        if ns in self._leases or ns not in self._fenced:
            METRICS.inc("memd_s3_owner_lease_lost_total",
                        help="single-writer leases lost to another holder")
        self._fenced.add(ns)
        self._leases.pop(ns, None)
        self._lease_etag.pop(ns, None)
        self._forget_ns(ns)

    def _forget_ns(self, ns: str) -> None:
        """Drop every cached per-key state of namespace `ns` (part counters,
        sizes, seen high-water marks), keeping only the monotonic floor."""
        pre = f"ns/{ns}/"
        with self._seq_lock:
            for k in [k for k in self._next_part if k.startswith(pre)]:
                self._floor[k] = max(self._floor.get(k, 0), self._next_part.pop(k))
            for k in [k for k in self._size_cache if k.startswith(pre)]:
                self._size_cache.pop(k, None)
        with self._hw_lock:
            for k in [k for k in self._seen_hw if k.startswith(pre)]:
                self._floor[k] = max(self._floor.get(k, 0), self._seen_hw.pop(k))

    def set_log_floor(self, key: str, floor: int) -> None:
        """Never number a part of `key` below `floor` (the high-water mark a
        manifest recorded - it survives the parts a fold deleted, which a
        LIST cannot see)."""
        if floor <= 0:
            return
        with self._seq_lock:
            self._floor[key] = max(self._floor.get(key, 0), int(floor))
            if key in self._next_part and self._next_part[key] < floor:
                self._next_part[key] = int(floor)

    def _lease_lock(self, ns: str) -> threading.RLock:
        return self._lease_locks[zlib.crc32(ns.encode()) % len(self._lease_locks)]

    def _renew_one(self, ns: str, holder: str) -> bool | None:
        """One renewal. True: renewed. False: not ours - lost (fenced) or
        released meanwhile. None: transient failure (not renewed, not fenced
        - the next attempt retries)."""
        with self._lease_lock(ns):
            if self._leases.get(ns) != holder:
                # released (or fenced) since the caller looked: renewing now
                # would write a live lease over the released tombstone
                return False
            return self._renew_one_locked(ns, holder)

    def _renew_one_locked(self, ns: str, holder: str) -> bool | None:
        key = self._owner_key(ns)
        t0 = time.monotonic()        # validity counts from BEFORE the request
        body = self._lease_body(holder)
        etag = self._lease_etag.get(ns)
        try:
            if etag:
                try:
                    new = self._raw_cas_put(key, body, if_match=etag)
                except _NoConditional:
                    self._lease_etag[ns] = None
                    return self._renew_one(ns, holder)
                if new is None:
                    # The ETag moved. Usually another holder took the lease -
                    # but a renewal whose response was lost and got retried
                    # also lands here, with OUR body in place: adopt that.
                    cur = self._raw_get_meta(key)
                    if cur is not None and cur[0].decode(errors="replace").split("\n", 1)[0] == holder:
                        self._lease_etag[ns] = cur[1]
                        self._last_beat[ns] = t0
                        return True
                    self._fence(ns)
                    return False
                self._lease_etag[ns] = new
            else:
                # no conditional writes on this endpoint: read, then put
                cur = (self._raw_get(key) or b"").decode(errors="replace")
                who = cur.split("\n", 1)[0] if cur else ""
                if cur and who != holder:
                    self._fence(ns)
                    return False
                self._raw_put(key, body)
            self._last_beat[ns] = t0
            METRICS.inc("memd_s3_owner_lease_renewals_total", help="single-writer lease heartbeats")
            return True
        except Exception:
            # a transient failure must not fence us; the next beat retries,
            # and self-fencing stops writes if it keeps failing
            METRICS.inc("memd_s3_owner_lease_renew_failures_total",
                        help="lease heartbeats that failed")
            return None

    def _check_fence(self, key: str) -> None:
        ns = self._ns_of(key)
        if ns is None:
            return
        if ns not in self._fenced:
            holder = self._leases.get(ns)
            if holder is not None:
                # Only the heartbeat used to evaluate ownership, so a writer
                # that had STALLED - and whose lease another node therefore
                # legitimately reclaimed - kept believing it was the owner
                # until its next beat. append() survives that window because
                # part creation is a conditional PUT, but put/delete/truncate
                # are unconditional: a stalled writer's put() overwrote the new
                # owner's manifest, silently, with no error to either side.
                # If our own heartbeat has gone stale, renew (CAS) before
                # mutating anything. Free when the beat is healthy; one round
                # trip exactly when the process has been stalled, which is
                # when it is dangerous.
                age = time.monotonic() - self._last_beat.get(ns, 0.0)
                if age > self._beat_period():
                    self._renew_one(ns, holder)
                    age = time.monotonic() - self._last_beat.get(ns, 0.0)
                if ns not in self._fenced and age > self._valid_for():
                    METRICS.inc("memd_s3_owner_lease_unconfirmed_total",
                                help="writes refused: the lease could not be renewed in time")
                    raise LeaseLostError(
                        f"single-writer lease on namespace {ns!r} could not be renewed for "
                        f"{age:.1f}s; refusing to write - another node may reclaim it at the "
                        "TTL. Retry once the object store is reachable again.")
        if ns in self._fenced:
            raise LeaseLostError(
                f"lost the single-writer lease on namespace {ns!r}; this "
                "process has been fenced. Another writer owns it - continuing "
                "would interleave two writers on one data root, which silently "
                "destroys acked data. Reopen the namespace to re-acquire.")

    def _renew_leases(self) -> None:
        """Heartbeat: keep held leases fresh, and fence on loss.

        Without this the lease expires under a LIVE writer - reproduced: writer
        A holds a namespace and keeps appending, and after the TTL writer B
        reclaims it and both append to the same log. That is exactly the CRIT
        the lease exists to prevent, delayed by one TTL.

        Renewal alone is not enough either: a partition can let someone else
        take the lease while we still think we hold it. So a renewal whose CAS
        finds a different holder FENCES the namespace, and writes to it then
        fail loudly rather than corrupting the log.
        """
        period = self._beat_period()
        while not self._lease_stop.wait(period):
            for ns, holder in list(self._leases.items()):
                self._renew_one(ns, holder)

    def _start_lease_thread(self) -> None:
        if self._lease_thread is not None:
            return
        self._lease_stop.clear()
        self._lease_thread = threading.Thread(
            target=self._renew_leases, daemon=True, name="memd-s3-lease")
        self._lease_thread.start()

    def try_acquire_owner(self, namespace: str, holder: str) -> bool:
        """See _try_acquire_owner. A control-plane TIMEOUT (the lease object
        is locked by a frozen peer's in-flight write, the endpoint is slow)
        answers "not acquired" - the caller's busy/retry path - instead of an
        opaque transport error."""
        try:
            return self._try_acquire_owner(namespace, holder)
        except Exception as ex:
            from botocore.exceptions import BotoCoreError

            if isinstance(ex, BotoCoreError):
                METRICS.inc("memd_s3_owner_lease_acquire_timeouts_total",
                            help="lease acquisitions that timed out on the control plane")
                return False
            raise

    def _try_acquire_owner(self, namespace: str, holder: str) -> bool:
        """Claim single-writer ownership of a namespace.

        The local backend uses `flock`, which cannot see another machine. Here
        the lock is a leased object created with a conditional PUT: the first
        writer wins, and a lease older than `lease_ttl_s` is reclaimable - by
        a compare-and-swap on its ETag, so of several nodes racing for one
        stale lease exactly one wins - and a crashed node does not wedge the
        namespace forever.

        This is deliberately a lease, not a distributed lock. Correctness
        leans on the TTL and self-fencing (see above) plus conditional part
        creation in append(); a correct multi-writer protocol (manifest CAS
        on ETag) is a larger design and is deferred (ADR-12 item 4).
        """
        key = self._owner_key(namespace)
        t0 = time.monotonic()
        body = self._lease_body(holder)
        now = time.time()
        try:
            etag = self._raw_cas_put(key, body, if_none_match=True)
        except _NoConditional:
            # no conditional writes on this endpoint: best-effort claim
            self._raw_put(key, body)
            self._hold(namespace, holder, None, t0)
            return True
        if etag is not None:
            self._hold(namespace, holder, etag, t0)
            return True
        cur = self._raw_get_meta(key)
        if cur is None:
            # released between our create and our read: try once more
            etag = self._raw_cas_put(key, body, if_none_match=True)
            if etag is not None:
                self._hold(namespace, holder, etag, t0)
                return True
            return False
        existing, cur_etag = cur
        if existing == _RELEASED:
            # cleanly released: free, and no paused writer can be behind it
            try:
                etag = self._raw_cas_put(key, body, if_match=cur_etag)
            except _NoConditional:
                self._raw_put(key, body)
                self._hold(namespace, holder, None, t0)
                return True
            if etag is None:
                return False            # someone else took it first
            self._hold(namespace, holder, etag, t0)
            return True
        try:
            who, ts = existing.decode().split("\n", 1)
            age = now - float(ts)
            if age < -max(5.0, self.lease_ttl_s):
                # Stamped meaningfully in the FUTURE: the holder's clock is
                # ahead (NTP step, VM drift, a bad RTC). A negative age is
                # never greater than the TTL, so such a lease was never stale
                # and a crash behind it made the namespace un-openable for the
                # length of the skew - defeating the whole point of having a
                # TTL. Treat it as stale and say so, loudly enough to diagnose.
                METRICS.inc("memd_s3_owner_lease_clock_skew_total",
                            help="leases stamped in the future (holder clock ahead)")
                age = self.lease_ttl_s + 1
        except Exception:
            who, age = "?", self.lease_ttl_s + 1
        if who == holder or age > self.lease_ttl_s:
            # our own lease (refresh), or one whose holder stopped
            # heartbeating: a live writer renews every ttl/3, so exceeding the
            # TTL means three missed beats, not merely a long-running process
            try:
                etag = self._raw_cas_put(key, self._lease_body(holder), if_match=cur_etag)
            except _NoConditional:
                self._raw_put(key, self._lease_body(holder))   # If-Match unsupported
                self._hold(namespace, holder, None, t0)
                return True
            if etag is None:
                return False            # someone else changed it first
            if who != holder:
                METRICS.inc("memd_s3_owner_lease_reclaimed_total",
                            help="stale single-writer leases reclaimed")
                self._took_over.add(namespace)
            self._hold(namespace, holder, etag, t0)
            return True
        return False

    def took_over(self, namespace: str) -> bool:
        """True (once) when the last acquire of `namespace` reclaimed a lease
        another holder had let go stale - that holder may be merely STALLED
        and resume mid-write. NamespaceStore then burns the next part number
        of each append log (see NamespaceStore._fence_previous_writer)."""
        if namespace in self._took_over:
            self._took_over.discard(namespace)
            return True
        return False

    def read_owner(self, namespace: str) -> dict | None:
        """Who holds `namespace` right now, for routing (never fenced, never
        written): {"holder", "stamp", "age_s", "fresh"} or None when unheld."""
        raw = self._raw_get(self._owner_key(namespace))
        if not raw or raw == _RELEASED:
            return None
        try:
            who, ts = raw.decode().split("\n", 1)
            stamp = float(ts)
        except Exception:
            return {"holder": "?", "stamp": 0.0, "age_s": float("inf"), "fresh": False}
        age = time.time() - stamp
        fresh = -max(5.0, self.lease_ttl_s) <= age <= self.lease_ttl_s
        return {"holder": who, "stamp": stamp, "age_s": age, "fresh": fresh}

    def holds_lease(self, namespace: str, fresh: bool = False) -> bool:
        """We hold `namespace`'s lease (and have not been fenced). `fresh`:
        also prove it - a holder whose heartbeat is stale (a process that
        was paused) renews synchronously first, so a router never serves a
        READ locally from a lease another node may have taken meanwhile."""
        holder = self._leases.get(namespace)
        if holder is None or namespace in self._fenced:
            return False
        if fresh and time.monotonic() - self._last_beat.get(namespace, 0.0) > self._beat_period():
            self._renew_one(namespace, holder)
            return (namespace in self._leases and namespace not in self._fenced
                    and time.monotonic() - self._last_beat.get(namespace, 0.0) <= self._valid_for())
        return True

    def release_owner(self, namespace: str, *, clean: bool = True, discard: bool = False) -> None:
        """Drop our lease - but only if it is still OURS.

        An unconditional delete would remove whichever lease is present,
        including one a different node legitimately reclaimed after we stalled,
        leaving that node writing an unowned namespace that a third process
        could then claim. (S3 has no conditional DELETE that MinIO honours, so
        this is read-then-delete; the loser of that race is FENCED on its
        next renewal - a liveness hiccup, never two writers.)

        Serialized with this process's renewals of the lease (_lease_lock).
        A heartbeat renewal landing between this release dropping the ETag
        and writing the tombstone made the tombstone's compare-and-swap fail
        - silently - and left the lease LIVE after a clean close; one that
        had listed the lease before the release renewed it after, with no
        ETag, by an unconditional put over the tombstone. The next node then
        waited out the TTL and took over a cleanly closed namespace as stale.
        """
        with self._lease_lock(namespace):
            self._release_owner_locked(namespace, clean=clean, discard=discard)

    def _release_owner_locked(self, namespace: str, *, clean: bool, discard: bool) -> None:
        holder = self._leases.pop(namespace, None)
        etag = self._lease_etag.pop(namespace, None)
        self._forget_ns(namespace)
        if holder is None:
            return
        key = self._owner_key(namespace)
        try:
            if discard:
                # the namespace does not exist (a refused open of a name that
                # was never created): leave no lease object behind
                cur = (self._raw_get(key) or b"").decode(errors="replace")
                if cur.split("\n", 1)[0] == holder:
                    self._raw_delete(key)
                return
            if not clean:
                # An ABORTED tenure (a takeover whose fence or manifest claim
                # did not complete): not a clean close, so the next holder
                # must treat it like a stale lease - fence the append logs,
                # claim the manifest - instead of trusting a clean release
                try:
                    self._raw_cas_put(key, _ABORTED, if_match=etag) if etag else \
                        self._raw_put(key, _ABORTED)
                except _NoConditional:
                    self._raw_put(key, _ABORTED)
                return
            if etag:
                # a RELEASED tombstone, compare-and-swapped onto the lease we
                # last wrote: if anyone took it meanwhile, nothing is written
                # (a read-then-delete could delete THEIR lease, letting a
                # third node in beside them)
                try:
                    self._raw_cas_put(key, _RELEASED, if_match=etag)
                    return
                except _NoConditional:
                    pass
            cur = (self._raw_get(key) or b"").decode()
            if cur and cur.split("\n", 1)[0] != holder:
                return                      # someone else owns it now
            self._raw_delete(key)
        finally:
            if not self._leases and self._lease_thread is not None:
                self._lease_stop.set()
                self._lease_thread = None


# the body of a lease its holder released CLEANLY (see release_owner); the
# ONLY lease body a new holder may acquire without fencing and claiming
_RELEASED = b"\n0"
# a tenure that ended without completing its takeover: reads as a stale
# lease (holder "aborted", stamped at the epoch), so the next holder fences
_ABORTED = b"aborted\n0"
# the cluster node registry's directory (memd.server.cluster.NODE_PREFIX)
_REGISTRY_DIR = "_cluster"


class _NoConditional(Exception):
    """The endpoint does not implement conditional PUT headers."""
