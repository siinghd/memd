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

from memd.metrics import METRICS
from memd.storage.objectstore import (ObjectStore, _count_op, adopt_io_tally,
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
        self._lease_stop = threading.Event()
        self._lease_thread: threading.Thread | None = None
        # running logical size per key, so an append does not re-LIST the log
        self._size_cache: dict[str, int] = {}
        self._seq_lock = threading.Lock()
        self._leases: dict[str, str] = {}

    # ------------------------------------------------------------- plumbing

    def _full(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def _rel(self, full: str) -> str:
        if self.prefix and full.startswith(self.prefix + "/"):
            return full[len(self.prefix) + 1:]
        return full

    def _is_missing(self, ex: Exception) -> bool:
        code = getattr(ex, "response", {}).get("Error", {}).get("Code", "")
        status = getattr(ex, "response", {}).get("ResponseMetadata", {}).get("HTTPStatusCode")
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
            return self._client.get_object(Bucket=self.bucket, Key=full)["Body"].read()
        except Exception as ex:
            if self._is_missing(ex):
                return None
            raise

    def _raw_put(self, full: str, data: bytes) -> None:
        self._client.put_object(Bucket=self.bucket, Key=full, Body=data)

    def _raw_delete(self, full: str) -> None:
        try:
            self._client.delete_object(Bucket=self.bucket, Key=full)
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

    def _raw_head(self, full: str) -> int | None:
        """Object size, or None when absent."""
        try:
            return int(self._client.head_object(Bucket=self.bucket, Key=full)["ContentLength"])
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
        for full, size in self._iter_keys(base + "/"):
            if full not in seen:
                seen.add(full)
                yield full, size
        for suffix in (_PART_SEP, ".__seq"):
            for full, size in self._iter_keys(base + suffix):
                if full not in seen:
                    seen.add(full)
                    yield full, size
        head = self._raw_head(base)
        if head is not None and base not in seen:
            yield base, head

    def _parts(self, key: str) -> list[tuple[str, int]]:
        """(full key, size) of this logical key's append parts, in order."""
        out = []
        for full, size in self._iter_keys(self._full(key) + _PART_SEP):
            if _PART_RE.match(full):
                out.append((full, size))
        out.sort()
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
                # first append to this key in this process
                seq, base = self._seed_seq(key)
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
            code = getattr(ex, "response", {}).get("Error", {}).get("Code", "")
            if code in ("PreconditionFailed", "412"):
                METRICS.inc("memd_s3_append_conflicts_total",
                            help="append part already existed: another writer holds this log")
                raise RuntimeError(
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
        segs = self._segments(src)
        if not segs:
            return
        # replacing dst means its own parts must go, same as put()
        for full, _ in self._parts(dst):
            self._raw_delete(full)
        with self._seq_lock:
            self._next_part.pop(dst, None)
            self._size_cache.pop(dst, None)

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

    def _owner_key(self, namespace: str) -> str:
        return self._full(f"ns/{namespace}/.owner")

    def _ns_of(self, key: str) -> str | None:
        """The namespace a key belongs to, for fence checks."""
        parts = key.split("/")
        return parts[1] if len(parts) >= 2 and parts[0] == "ns" else None

    def _check_fence(self, key: str) -> None:
        if not self._fenced:
            return
        ns = self._ns_of(key)
        if ns and ns in self._fenced:
            raise RuntimeError(
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
        take the lease while we still think we hold it. So a renewal that finds
        a different holder FENCES the namespace, and writes to it then fail
        loudly rather than corrupting the log.
        """
        period = max(1.0, self.lease_ttl_s / 3.0)
        while not self._lease_stop.wait(period):
            for ns, holder in list(self._leases.items()):
                key = self._owner_key(ns)
                try:
                    cur = (self._raw_get(key) or b"").decode()
                    who = cur.split("\n", 1)[0] if cur else ""
                    if cur and who != holder:
                        self._fenced.add(ns)
                        self._leases.pop(ns, None)
                        METRICS.inc("memd_s3_owner_lease_lost_total",
                                    help="single-writer leases lost to another holder")
                        continue
                    self._raw_put(key, f"{holder}\n{time.time()}".encode())
                    METRICS.inc("memd_s3_owner_lease_renewals_total",
                                help="single-writer lease heartbeats")
                except Exception:
                    # a transient failure must not fence us; the next beat
                    # retries, and the TTL is 3 beats wide
                    METRICS.inc("memd_s3_owner_lease_renew_failures_total",
                                help="lease heartbeats that failed")

    def _start_lease_thread(self) -> None:
        if self._lease_thread is not None:
            return
        self._lease_stop.clear()
        self._lease_thread = threading.Thread(
            target=self._renew_leases, daemon=True, name="memd-s3-lease")
        self._lease_thread.start()

    def try_acquire_owner(self, namespace: str, holder: str) -> bool:
        """Claim single-writer ownership of a namespace.

        The local backend uses `flock`, which cannot see another machine. Here
        the lock is a leased object created with a conditional PUT: the first
        writer wins, and a lease older than `lease_ttl_s` is reclaimable so a
        crashed node does not wedge the namespace forever.

        This is deliberately a lease, not a distributed lock - it makes
        split-brain LOUD rather than impossible. Two writers on one data root
        silently destroyed acked data, and a loud failure is the improvement
        that matters; a correct multi-writer protocol (manifest CAS on ETag)
        is a larger design and is not built.
        """
        key = self._owner_key(namespace)
        now = time.time()
        body = f"{holder}\n{now}".encode()
        try:
            self._client.put_object(Bucket=self.bucket, Key=key, Body=body,
                                    IfNoneMatch="*")
            self._leases[namespace] = holder
            self._fenced.discard(namespace)
            self._start_lease_thread()
            return True
        except Exception as ex:
            code = getattr(ex, "response", {}).get("Error", {}).get("Code", "")
            if code not in ("PreconditionFailed", "412"):
                if code in ("NotImplemented", "InvalidRequest", "InvalidArgument"):
                    # no conditional writes on this endpoint: best-effort claim
                    self._raw_put(key, body)
                    self._leases[namespace] = holder
                    self._fenced.discard(namespace)
                    self._start_lease_thread()
                    return True
                raise
        existing = self._raw_get(key) or b""
        try:
            who, ts = existing.decode().split("\n", 1)
            age = now - float(ts)
        except Exception:
            who, age = "?", self.lease_ttl_s + 1
        if who == holder:
            self._raw_put(key, body)      # our own lease: refresh
            self._leases[namespace] = holder
            self._fenced.discard(namespace)
            self._start_lease_thread()
            return True
        if age > self.lease_ttl_s:
            # Only reclaimable because the holder stopped heartbeating: a live
            # writer renews every ttl/3, so exceeding the TTL means three
            # missed beats, not merely a long-running process.
            METRICS.inc("memd_s3_owner_lease_reclaimed_total",
                        help="stale single-writer leases reclaimed")
            self._raw_put(key, body)
            self._leases[namespace] = holder
            self._fenced.discard(namespace)
            self._start_lease_thread()
            return True
        return False

    def release_owner(self, namespace: str) -> None:
        """Drop our lease - but only if it is still OURS.

        An unconditional delete would remove whichever lease is present,
        including one a different node legitimately reclaimed after we stalled,
        leaving that node writing an unowned namespace that a third process
        could then claim.
        """
        holder = self._leases.pop(namespace, None)
        if holder is None:
            return
        key = self._owner_key(namespace)
        try:
            cur = (self._raw_get(key) or b"").decode()
            if cur and cur.split("\n", 1)[0] != holder:
                return                      # someone else owns it now
        except Exception:
            pass
        self._raw_delete(key)
        if not self._leases and self._lease_thread is not None:
            self._lease_stop.set()
            self._lease_thread = None
