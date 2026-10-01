"""Object store abstraction.

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


class LeaseLostError(RuntimeError):
    """This process no longer holds (or cannot prove it holds) the writer
    lease of the namespace it tried to mutate. The mutation did not happen;
    whatever the request did before it may have. Retry through the router."""


class AppendConflict(LeaseLostError):
    """An append log's next part already exists: another writer - a
    successor's takeover fence, whose part a resumed stale writer runs into
    - wrote it. The append did not happen (it is never acked)."""


class PreconditionFailed(LeaseLostError):
    """A conditional write found the object changed since this process last
    observed or wrote it: someone else is writing it, so this process has
    lost ownership. Never retried blindly - the caller fences."""


class ReadOnlyError(RuntimeError):
    """A mutation was attempted through a read-only namespace - a read
    replica, which takes no lease and never writes or
    deletes any object. Nothing was changed."""


def content_version(data: bytes) -> str:
    """Version token of an object for stores without native ETags: its
    content hash. Content-identical rewrites compare equal, which is safe -
    every object written conditionally changes on every write (a manifest
    generation, a timestamp, a chain hash)."""
    import hashlib

    return hashlib.sha256(data).hexdigest()


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

    # ------------------------------------------------ conditional writes
    #
    # Every object memd rewrites IN PLACE (the manifest - the commit point -,
    # the audit ledger's checkpoint, the migration report, the key-custody
    # marker, a wrapped key being rotated, a cluster node's registry entry)
    # is written with compare-and-swap: "replace it only if it is still the
    # version I last observed or wrote" (None: "only if it does not exist").
    # A writer that was paused past its lease and resumed then fails instead
    # of overwriting its successor's state. Objects under NEW
    # unique names (segments, snapshots) need no condition: nothing reads
    # them until a conditional manifest commit references them.

    def get_versioned(self, key: str) -> tuple[bytes, str] | None:
        """(content, version token) or None. Only for whole objects that are
        never appended to."""
        data = self.get(key)
        return None if data is None else (data, content_version(data))

    def put_if_match(self, key: str, data: bytes, version: str | None, *,
                     hint: bool = False, fence: bool = True) -> str:
        """Write `key` only if its current version is `version` (None: only
        if it does not exist). Returns the new version; raises
        PreconditionFailed otherwise. `hint`: a rebuildable object (put_hint
        durability). `fence`: a leasing store also fences the namespace on
        failure (see S3ObjectStore) - the default, because a failed
        precondition means ownership was lost.

        The default is check-then-put, correct for a store with one writer
        process; LocalObjectStore serializes it, S3 does it server side."""
        cur = self.get(key)
        if (None if cur is None else content_version(cur)) != version:
            raise PreconditionFailed(f"{key!r} changed since this process last wrote it")
        (self.put_hint if hint else self.put)(key, data)
        return content_version(data)

    def log_bound(self, key: str) -> int | None:
        """Opaque high-water mark of an append log as this process last saw
        it (read or appended), or None when the store has no such notion.
        Passed back to delete_log after a commit."""
        return None

    def delete_log(self, key: str, upto: int | None) -> None:
        """Delete an append log a commit made obsolete - only what existed
        at `upto` (a log_bound taken BEFORE the commit), so a paused writer
        resuming here cannot delete what a successor appended since. Stores
        with a single writer delete it whole."""
        self.delete(key)

    def put_if_absent(self, key: str, data: bytes) -> bool:
        """Create `key` only if nothing is there; False if it already exists.

        Wrapped data keys are minted this way: two nodes opening the same new
        namespace must agree on ONE key, and the loser adopts the winner's.
        The default is check-then-put (not atomic); both real backends
        override it with an atomic create."""
        if self.exists(key):
            return False
        self.put(key, data)
        return True

    def shred(self, key: str) -> None:
        """Delete `key` so that no copy the store keeps stays readable.

        Same as delete() here; a versioned bucket overrides it to remove every
        noncurrent version too - a delete marker over a wrapped data key is
        not a crypto-shred."""
        self.delete(key)

    # ------------------------------------------------ following a log
    #
    # A read replica follows a namespace's append logs: it
    # reads what was appended since it last looked, never the whole log
    # again. A cursor is opaque to it, except that it may move one back
    # over bytes it could not parse yet (a frame still being written).

    # True when a cursor stays valid across a fold deleting the log and new
    # appends recreating it (S3: part numbers are never reused). Otherwise a
    # follower restarts from the beginning after every fold.
    log_cursors_survive_folds = False

    def log_tail(self, key: str, cursor=None) -> tuple[list[tuple[object, bytes]], object, bool]:
        """What was appended to log `key` after `cursor` (None: all of it):
        (chunks, end, reset). `chunks` are (cursor past the chunk, its bytes)
        in log order; `end` is the cursor past everything (also when nothing
        is new); `reset` says the log was replaced or cut back since
        `cursor` - the chunks then start at its beginning and may repeat
        what was read before. Read-only: it never touches this store's write
        bookkeeping. The default reads the whole log (cursor = byte offset)."""
        data = self.get(key) or b""
        off = int(cursor or 0)
        if len(data) < off:
            return ([(len(data), data)] if data else []), len(data), True
        return ([(len(data), data[off:])] if len(data) > off else []), len(data), False

    def log_cursor_back(self, cursor, nbytes: int):
        """`cursor` (the end of a chunk) moved back over its last `nbytes`."""
        return int(cursor) - int(nbytes)


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


_LOCAL_CAS_LOCKS: dict[str, threading.Lock] = {}
_LOCAL_CAS_GUARD = threading.Lock()


def _local_cas_lock(root: str) -> threading.Lock:
    with _LOCAL_CAS_GUARD:
        lk = _LOCAL_CAS_LOCKS.get(root)
        if lk is None:
            lk = _LOCAL_CAS_LOCKS[root] = threading.Lock()
        return lk


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

    def put_if_match(self, key: str, data: bytes, version: str | None, *,
                     hint: bool = False, fence: bool = True) -> str:
        """Compare-and-swap emulated under a lock: a local data root has ONE
        writer process (the per-namespace flock), so serializing the
        compare and the swap inside it is enough. The version is the content
        hash (see content_version)."""
        with _local_cas_lock(self.root):
            try:
                with open(self._path(key), "rb") as f:
                    cur: str | None = content_version(f.read())
            except FileNotFoundError:
                cur = None
            if cur != version:
                _count_op("put_if_match_conflict")
                raise PreconditionFailed(f"{key!r} changed since this process last wrote it")
            (self.put_hint if hint else self.put)(key, data)
            return content_version(data)

    def get_versioned(self, key: str) -> tuple[bytes, str] | None:
        data = self.get(key)
        return None if data is None else (data, content_version(data))

    def put_if_absent(self, key: str, data: bytes) -> bool:
        """Atomic create: the fsynced temp file is hard-LINKED into place,
        which fails if the name exists (rename would silently replace it)."""
        _count_op("put_if_absent")
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), prefix=_TMP_TAG)
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            try:
                os.link(tmp, path)
            except FileExistsError:
                return False
            self._fsync_dir(os.path.dirname(path))
            return True
        finally:
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass

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

    def shred(self, key: str) -> None:
        """Overwrite, fsync, then unlink - as LocalKeyEnvelope.destroy does
        for its key files, so a lazy filesystem does not keep the bytes."""
        _count_op("shred")
        path = self._path(key)
        try:
            size = os.path.getsize(path)
            with open(path, "r+b") as f:
                f.write(secrets.token_bytes(size))
                f.flush()
                os.fsync(f.fileno())
            os.unlink(path)
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

    def log_tail(self, key: str, cursor=None) -> tuple[list[tuple[object, bytes]], object, bool]:
        """A log is one file here; a cursor is (st_dev, st_ino, offset). A
        fold deletes the file and the next append creates a new one: another
        identity (or a shorter file) reads from its beginning, `reset`. An
        inode number the filesystem reuses for the new file is not caught
        here - a follower restarts after every fold anyway (the manifest
        tells it: log_cursors_survive_folds is False). A missing file reads
        as empty with no cursor: whatever is created next is new."""
        _count_op("log_tail")
        try:
            f = open(self._path(key), "rb")
        except FileNotFoundError:
            return [], None, False
        with f:
            st = os.fstat(f.fileno())
            off, reset = 0, False
            if cursor is not None:
                if (cursor[0], cursor[1]) == (st.st_dev, st.st_ino) and cursor[2] <= st.st_size:
                    off = cursor[2]
                else:
                    reset = True
            f.seek(off)
            data = f.read()
        end = (st.st_dev, st.st_ino, off + len(data))
        return ([(end, data)] if data else []), end, reset

    def log_cursor_back(self, cursor, nbytes: int):
        return (cursor[0], cursor[1], cursor[2] - int(nbytes))

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
