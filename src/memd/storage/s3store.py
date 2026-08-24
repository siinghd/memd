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

import re
import threading
import time

from memd.metrics import METRICS
from memd.storage.objectstore import ObjectStore, _count_op

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
        self._client = client or boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=Config(retries={"max_attempts": 5, "mode": "standard"},
                          signature_version="s3v4"),
        )
        # Next part number per logical key. Single-writer per data root (see
        # try_acquire_owner), so an in-process counter is authoritative once
        # seeded; seeding costs one LIST on the first append to a key, not one
        # per append.
        self._next_part: dict[str, int] = {}
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

    def _raw_get(self, full: str) -> bytes | None:
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

    def _raw_head(self, full: str) -> int | None:
        """Object size, or None when absent."""
        try:
            return int(self._client.head_object(Bucket=self.bucket, Key=full)["ContentLength"])
        except Exception as ex:
            if self._is_missing(ex):
                return None
            raise

    def _iter_keys(self, full_prefix: str):
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=full_prefix):
            for obj in page.get("Contents", []) or []:
                yield obj["Key"], int(obj["Size"])

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
        # A put replaces the LOGICAL object, so any append parts it accumulated
        # must go with it - otherwise a later get() would concatenate the new
        # body with stale tail bytes.
        for full, _ in self._parts(key):
            self._raw_delete(full)
        with self._seq_lock:
            self._next_part.pop(key, None)
            self._size_cache.pop(key, None)
        self._raw_put(self._full(key), data)

    def get(self, key: str) -> bytes | None:
        _count_op("get")
        _validate_key(key)
        segs = self._segments(key)
        if not segs:
            return None
        if len(segs) == 1:
            return self._raw_get(segs[0][0]) or b""
        chunks = []
        for full, _ in segs:
            blob = self._raw_get(full)
            if blob:
                chunks.append(blob)
        return b"".join(chunks)

    def delete(self, key: str) -> None:
        _count_op("delete")
        _validate_key(key)
        for full, _ in self._parts(key):
            self._raw_delete(full)
        with self._seq_lock:
            self._next_part.pop(key, None)
            self._size_cache.pop(key, None)
        self._raw_delete(self._full(key))

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
        base = self._full(prefix)
        out: set[str] = set()
        for full, _ in self._iter_keys(base):
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
        with self._seq_lock:
            seq = self._next_part.get(key)
            if seq is None:
                # first append to this key in this process: one LIST to find
                # where the log left off, then the counter carries it
                parts = self._parts(key)
                seq = 0
                if parts:
                    m = _PART_RE.match(parts[-1][0])
                    seq = int(m.group("seq")) + 1 if m else len(parts)
                base = sum(sz for _, sz in self._segments(key))
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
        return total

    def _remember_size(self, key: str, total: int) -> None:
        self._size_cache[key] = total

    def truncate(self, key: str, size: int) -> None:
        """Cut the logical object back to `size` bytes (torn-tail repair)."""
        _count_op("truncate")
        _validate_key(key)
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
        with self._seq_lock:
            self._next_part.pop(key, None)
            self._size_cache.pop(key, None)

    def copy(self, src: str, dst: str) -> None:
        _count_op("copy")
        _validate_key(src)
        _validate_key(dst)
        blob = self.get(src)
        if blob is None:
            return
        self.put(dst, blob)

    def remove_prefix(self, prefix: str) -> int:
        """Delete everything under `prefix`; returns LOGICAL objects removed."""
        _count_op("remove_prefix")
        base = self._full(prefix)
        logical: set[str] = set()
        batch: list[dict] = []
        removed = 0
        for full, _ in self._iter_keys(base):
            m = _PART_RE.match(full)
            logical.add(m.group("key") if m else full)
            batch.append({"Key": full})
            if len(batch) == 1000:
                self._client.delete_objects(Bucket=self.bucket, Delete={"Objects": batch})
                batch = []
        if batch:
            self._client.delete_objects(Bucket=self.bucket, Delete={"Objects": batch})
        removed = len(logical)
        with self._seq_lock:
            for k in list(self._next_part):
                if self._full(k).startswith(base):
                    self._next_part.pop(k, None)
                    self._size_cache.pop(k, None)
        return removed

    # -------------------------------------------------------- owner leasing

    def _owner_key(self, namespace: str) -> str:
        return self._full(f"ns/{namespace}/.owner")

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
            return True
        except Exception as ex:
            code = getattr(ex, "response", {}).get("Error", {}).get("Code", "")
            if code not in ("PreconditionFailed", "412"):
                if code in ("NotImplemented", "InvalidRequest", "InvalidArgument"):
                    # no conditional writes on this endpoint: best-effort claim
                    self._raw_put(key, body)
                    self._leases[namespace] = holder
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
            return True
        if age > self.lease_ttl_s:
            METRICS.inc("memd_s3_owner_lease_reclaimed_total",
                        help="stale single-writer leases reclaimed")
            self._raw_put(key, body)
            self._leases[namespace] = holder
            return True
        return False

    def release_owner(self, namespace: str) -> None:
        if self._leases.pop(namespace, None) is None:
            return
        self._raw_delete(self._owner_key(namespace))
