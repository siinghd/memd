"""Object store abstraction (ADR-2).

Object storage is the source of truth from the first byte. The embedded
mode plays object store with the local filesystem; hosted mode points the
same interface at S3/R2/MinIO. Compute stays stateless: everything here is
rebuildable except the segments themselves.
"""
from __future__ import annotations

import contextlib
import contextvars
import os
import secrets
import shutil
import tempfile
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass

from memd.metrics import METRICS

# Per-request I/O attribution. The global memd_store_ops_total counter says
# how many object-store round trips the PROCESS made; it can never answer the
# question the complexity budget actually asks - "how many round trips did
# THIS request cost?" - because concurrent work interleaves into the same
# counter. This context-local tally rides with the request instead.
_req_io: "contextvars.ContextVar[dict | None]" = contextvars.ContextVar("memd_req_io", default=None)


# A put writes a temp file beside its key and renames it into place; a crash
# in between leaves the temp file - with the payload (a segment's records,
# say) - for good. Temp names carry this process's tag, so the namespace's
# owner can delete every one that is not its own (see tmp_is_foreign and
# NamespaceStore._collect_garbage). Random, not the pid: in a container
# every incarnation is pid 1.
TMP_PREFIX = ".tmp-"
_TMP_TAG = f"{TMP_PREFIX}{os.getpid()}x{secrets.token_hex(4)}-"


def tmp_is_foreign(name: str) -> bool:
    """True for a put's temp file that another (so: dead) process left."""
    return name.startswith(TMP_PREFIX) and not name.startswith(_TMP_TAG)


_TALLY_LOCK = threading.Lock()


def _count_op(op: str) -> None:
    """I/O round-trip counting per store operation class - request-level I/O
    budgets are only auditable if the ops themselves are visible."""
    METRICS.inc("memd_store_ops_total", op=op, help="object-store operations by type")
    tally = _req_io.get()
    if tally is not None:
        # A remote backend fans a single logical read out across a thread pool,
        # so this is genuinely concurrent: `tally[op] = tally.get(op, 0) + 1`
        # drops counts under contention, and an undercounting I/O meter is
        # worse than none.
        with _TALLY_LOCK:
            tally[op] = tally.get(op, 0) + 1


def current_io_tally() -> "dict | None":
    """The in-flight request's I/O tally, for workers that need to adopt it."""
    return _req_io.get()


def adopt_io_tally(tally: "dict | None") -> None:
    """Attach this thread to a tally started on another thread."""
    if tally is not None:
        _req_io.set(tally)


@contextlib.contextmanager
def count_io():
    """Count object-store round trips for the enclosed request.

    Yields a dict that fills in as I/O happens: `{"get": 3, "append": 1}`.
    Context-local, so concurrent requests on other threads never bleed in.
    """
    tally: dict[str, int] = {}
    token = _req_io.set(tally)
    try:
        yield tally
    finally:
        try:
            _req_io.reset(token)
        except ValueError:
            # The token belongs to the context it was created in. If the
            # manager is unwound somewhere else (an abandoned ExitStack
            # finalized by GC, a generator closed on another task), resetting
            # is both impossible and unnecessary - clear instead of raising
            # out of a finalizer, which surfaces as an unraisable exception.
            _req_io.set(None)


@dataclass(frozen=True)
class ObjectMeta:
    key: str
    size: int
    exists: bool


class ObjectStore(ABC):
    """Prefix-scoped KV of immutable blobs. Keys are '/'-separated."""

    @abstractmethod
    def put(self, key: str, data: bytes) -> None: ...

    @abstractmethod
    def get(self, key: str) -> bytes | None: ...

    @abstractmethod
    def delete(self, key: str) -> None: ...

    @abstractmethod
    def exists(self, key: str) -> bool: ...

    @abstractmethod
    def list(self, prefix: str) -> list[str]: ...

    @abstractmethod
    def append(self, key: str, data: bytes) -> int:
        """Append to a log-style key; returns new total size. Must be durable on return."""

    def put_hint(self, key: str, data: bytes) -> None:
        """Write a small REBUILDABLE hint object (a cache, a checkpoint).

        Hints carry no durability requirement by construction: every reader
        must detect a stale or missing hint and recompute. Stores may skip
        fsync for these. The default is the durable path, so a store that does
        not override this is simply slower, never wrong.
        """
        self.put(key, data)

    @abstractmethod
    def size(self, key: str) -> int: ...

    @abstractmethod
    def truncate(self, key: str, size: int) -> None:
        """Durably cut a log key back to `size` bytes (torn-tail repair)."""

    def open_log(self, key: str) -> "LogWriter":  # noqa: N805
        """Optional: persistent-handle log writer enabling group commit.
        Stores without one fall back to append() per batch."""
        raise NotImplementedError

    @abstractmethod
    def remove_prefix(self, prefix: str) -> int:
        """Delete everything under prefix (crypto-shred support). Returns count."""

    @abstractmethod
    def copy(self, src: str, dst: str) -> None: ...


class LogWriter(ABC):
    """Persistent-handle append log. write() makes bytes visible (flushed);
    sync() makes them durable. Separating the two lets many writers share
    one fsync (group commit)."""

    @abstractmethod
    def write(self, data: bytes) -> int:
        """Flush to OS; returns end offset."""

    @abstractmethod
    def sync(self) -> None: ...

    @abstractmethod
    def size(self) -> int: ...

    @abstractmethod
    def close(self) -> None: ...


class LocalLogWriter(LogWriter):
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.fd = open(path, "ab")

    def write(self, data: bytes) -> int:
        self.fd.write(data)
        self.fd.flush()  # OS-visible immediately (read-your-writes via any reader)
        return self.fd.tell()

    def sync(self) -> None:
        os.fsync(self.fd.fileno())

    def size(self) -> int:
        return self.fd.tell()

    def close(self) -> None:
        try:
            self.fd.flush()
            os.fsync(self.fd.fileno())
        except OSError:
            pass
        self.fd.close()


class LocalObjectStore(ObjectStore):
    """Filesystem-backed object store. Each key = one file under root.

    Durability: put() writes to a temp file then atomic rename + fsync of
    file and parent dir. append() opens O_APPEND, writes, fsyncs.
    """

    def __init__(self, root: str):
        self.root = os.path.abspath(root)
        os.makedirs(self.root, exist_ok=True)

    def _path(self, key: str) -> str:
        if ".." in key.split("/") or key.startswith("/"):
            raise ValueError(f"invalid object key: {key!r}")
        return os.path.join(self.root, *key.split("/"))

    @staticmethod
    def _fsync_dir(path: str) -> None:
        try:
            fd = os.open(path, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except OSError:
            pass

    def put(self, key: str, data: bytes) -> None:
        _count_op("put")
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), prefix=_TMP_TAG)
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
            self._fsync_dir(os.path.dirname(path))
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

    def put_hint(self, key: str, data: bytes) -> None:
        """Atomic (temp + rename) but NOT fsynced.

        put() costs two fsyncs - one for the file, one for the parent dir. On
        the write path that is ~2.7ms of the 10ms write-ack budget, paid for a
        rebuildable hint. A crash may lose or stale the hint; the reader
        detects that by size and recomputes, so only the rename atomicity
        (never a torn hint) actually matters here.
        """
        _count_op("put_hint")
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), prefix=_TMP_TAG)
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
            os.replace(tmp, path)
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

    def get(self, key: str) -> bytes | None:
        _count_op("get")
        try:
            with open(self._path(key), "rb") as f:
                return f.read()
        except FileNotFoundError:
            return None

    def delete(self, key: str) -> None:
        _count_op("delete")
        try:
            os.unlink(self._path(key))
        except FileNotFoundError:
            pass

    def exists(self, key: str) -> bool:
        _count_op("exists")
        return os.path.exists(self._path(key))

    def list(self, prefix: str) -> list[str]:
        _count_op("list")
        base = self._path(prefix) if prefix else self.root
        if not os.path.isdir(base):
            # The prefix names no directory. It may still be a FILE prefix
            # inside an existing parent ("ns/x/seg-"), so scan that parent -
            # but only that parent. Walking the entire store to answer a
            # prefix whose parent does not even exist turned every
            # destroy_namespace of an absent namespace into an O(all objects)
            # scan.
            parent_key = prefix.rsplit("/", 1)[0] if "/" in prefix else ""
            parent_dir = self._path(parent_key) if parent_key else self.root
            if not os.path.isdir(parent_dir):
                return []
            out = []
            for dirpath, _dirs, files in os.walk(parent_dir):
                rel = os.path.relpath(dirpath, self.root).replace(os.sep, "/")
                for fn in sorted(files):
                    k = f"{rel}/{fn}" if rel != "." else fn
                    if k.startswith(prefix):
                        out.append(k)
            return sorted(out)
        out = []
        for dirpath, _dirs, files in os.walk(base):
            rel = os.path.relpath(dirpath, self.root).replace(os.sep, "/")
            for fn in sorted(files):
                k = f"{rel}/{fn}" if rel != "." else fn
                out.append(k)
        return sorted(out)

    def append(self, key: str, data: bytes) -> int:
        _count_op("append")
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "ab") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
            return f.tell()

    def size(self, key: str) -> int:
        _count_op("size")
        try:
            return os.path.getsize(self._path(key))
        except FileNotFoundError:
            return 0

    def truncate(self, key: str, size: int) -> None:
        _count_op("truncate")
        path = self._path(key)
        with open(path, "r+b") as f:
            f.truncate(size)
            f.flush()
            os.fsync(f.fileno())

    def open_log(self, key: str) -> LocalLogWriter:
        return LocalLogWriter(self._path(key))

    def remove_prefix(self, prefix: str) -> int:
        # A prefix that names neither a directory nor a file has nothing under
        # it. Falling through to list() made destroying an absent (or
        # already-destroyed) namespace walk every object in the store.
        base = self._path(prefix.rstrip("/")) if prefix else self.root
        if not os.path.exists(base):
            return 0
        n = 0
        for k in self.list(prefix):
            self.delete(k)
            n += 1
        # prune empty dirs under the prefix
        base = self._path(prefix.rstrip("/")) if prefix else None
        if base and os.path.isdir(base):
            shutil.rmtree(base, ignore_errors=True)
        return n

    def copy(self, src: str, dst: str) -> None:
        data = self.get(src)
        if data is None:
            raise FileNotFoundError(src)
        self.put(dst, data)
