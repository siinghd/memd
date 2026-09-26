"""Optional usearch ANN sidecar for the vector lane (`memd[ann]`, decision D6).

The vector lane was an exact flat scan over a cached float32 matrix: perfect
recall, O(N) per query and 4 bytes x dim x N of RAM, with a documented
ceiling around 50K vectors per namespace. This module keeps an HNSW index
(usearch, Apache-2.0; >= 2.25, earlier versions slowed ~90x on ascending
sparse integer keys) beside the SQLite index as a DERIVED view, the way the
tantivy accelerator sits beside FTS5. LanceDB was rejected: its deletes are
markers, with weak physical-purge guarantees (D7).

Selection (`vector_index` / MEMD_VECTOR_INDEX = auto | flat | usearch):
auto uses the sidecar when usearch is importable AND the namespace holds at
least `ann_min_vectors` (default 20000) vectors; below that the exact scan
is exact and fast enough. An explicit "usearch" that cannot be honoured
raises (resolve_vector_index); it never falls back silently.

Contract - SQLite (the vectors and records tables) stays the source of truth:
  - contents: one entry per live record with a vector (not deleted,
    quarantined, invalidated or superseded - the rows the flat matrix loads),
    keyed by the record's SQLite rowid (int64, never reused - see
    NamespaceIndex._note_rowid_hwm). f16 by default (`ann_dtype` i8), cosine,
    connectivity 16.
  - freshness: every change that can alter those contents bumps a counter in
    the SQLite meta table (`vec_wm`) in the same transaction, and queues the
    record's new state here, stamped with it, while the index write lock is
    held. The queue is applied right after that lock is released, by the same
    thread, one applier at a time in counter order, so the write path never
    waits on an HNSW insert. A file on disk is always saved at a COMMITTED
    counter value and says which; it is used only when that value, the SQLite
    file's `vec_uid`, the format, dim, dtype, metric and connectivity all
    match and no hard-delete purge happened since it was built. Anything else
    - missing, corrupt, foreign, behind, purged - is rebuilt from SQLite in
    the background while the exact scan serves. A crash therefore costs a
    rebuild, never a stale answer.
  - builds read SQLite in chunks while writes go on: changes applied from the
    moment the build starts are journaled and replayed onto the new index
    (each entry is a record's full state, so replay is idempotent), which is
    swapped in under the search lock - a search sees the old index or the new
    one, never a half-built one.
  - files: `<ns>.usearch/state.json` names the current `<id>.usearch` file.
    A new file is written under a temp name, fsynced and renamed, then the
    state is switched the same way, then the old file is deleted. A kill at
    any point leaves the old file (still named by the state) or the new one;
    temp files and unnamed files are deleted at open.
  - D7: usearch `remove` only marks an entry; its bytes stay in RAM and in
    the next save. A hard-delete purge therefore deletes the files at once
    and rebuilds from SQLite (where the row is already gone); no save is made
    until that rebuild is done, so no purged vector reaches a file again.
  - queries (NamespaceIndex.search_vector): usearch top-(k x `ann_overfetch`),
    widened once, then the SAME SQL filter and _passes_filter post-check as
    the exact scan, with the survivors re-scored exactly from SQLite. A
    selective filter (at most `ann_exact_max` rows, default 2000), an index
    that is not ready, a window still short after widening, and sweep-size
    limits are answered exactly (counted in fallback_exact_total).
  - concurrency: one applier at a time (the apply lock, which also covers
    saves and swaps); searches take the read side of a lock whose write side
    covers every mutation of the usearch index (its thread contexts are not
    safe for a search running concurrently with an add).
"""
from __future__ import annotations

import contextlib
import importlib.util
import json
import logging
import os
import threading
import time
import uuid
from collections import deque
from typing import TYPE_CHECKING, Any

import numpy as np

from memd.metrics import METRICS

if TYPE_CHECKING:  # pragma: no cover
    from memd.index.sqlite_index import NamespaceIndex

_log = logging.getLogger(__name__)

VECTOR_INDEX_CHOICES = ("auto", "flat", "usearch")
DTYPE_CHOICES = ("f16", "i8")
DEFAULT_MIN_VECTORS = 20_000
DEFAULT_OVERFETCH = 4
DEFAULT_EXACT_MAX = 2_000
CONNECTIVITY = 16
METRIC = "cos"
STATE_FILE = "state.json"
STATE_VERSION = 1
SNAPSHOT_MAGIC = b"MEMDVEC1"
BUILD_CHUNK = 50_000         # rows read (and added) per build step
JOURNAL_CATCHUP = 256        # journal entries left for the final, locked replay
ADD_SLICE = 64               # vectors added per write-lock hold (bounds a search's wait)
SWEEP_LIMIT = 1024           # limits at or above this are sweeps: answered exactly
BACKOFF_BASE_S = 2.0
BACKOFF_MAX_S = 300.0
_RESET = object()            # queue entry: the SQLite index was wiped


def usearch_available() -> bool:
    """Whether the optional `usearch` package is installed, without importing it."""
    return importlib.util.find_spec("usearch") is not None


def requested_vector_index(config: dict | None = None) -> str:
    """config["vector_index"], else env MEMD_VECTOR_INDEX, else "auto"."""
    cfg = config or {}
    choice = str(cfg.get("vector_index") or os.environ.get("MEMD_VECTOR_INDEX")
                 or "auto").strip().lower()
    if choice not in VECTOR_INDEX_CHOICES:
        raise ValueError(f"unknown vector_index {choice!r}; expected one of {list(VECTOR_INDEX_CHOICES)}")
    return choice


def resolve_vector_index(config: dict | None = None) -> str:
    """"flat", "auto" (usearch above the threshold) or "usearch" (always).
    auto without usearch installed is flat. An explicit "usearch" that
    cannot be honoured raises; it never falls back."""
    choice = requested_vector_index(config)
    if choice == "flat":
        return "flat"
    if not usearch_available():
        if choice == "usearch":
            raise ImportError("vector_index 'usearch' was requested but the `usearch` package is not "
                              "importable; install memd[ann] or choose vector_index='flat'")
        return "flat"
    return choice


def vector_index_config(config: dict | None, mode: str) -> dict:
    """The sidecar settings handed to the storage engine (validated here, so
    a bad value fails at Memory() rather than at the first namespace open)."""
    cfg = config or {}
    dtype = str(cfg.get("ann_dtype", "f16")).strip().lower()
    if dtype not in DTYPE_CHOICES:
        raise ValueError(f"ann_dtype must be one of {list(DTYPE_CHOICES)}, not {dtype!r}")
    out = {
        "mode": mode,
        "min_vectors": int(cfg.get("ann_min_vectors", DEFAULT_MIN_VECTORS)),
        "overfetch": int(cfg.get("ann_overfetch", DEFAULT_OVERFETCH)),
        "exact_max": int(cfg.get("ann_exact_max", DEFAULT_EXACT_MAX)),
        "dtype": dtype,
        "build_threads": int(cfg.get("ann_build_threads", _default_build_threads())),
    }
    if out["overfetch"] < 1 or out["min_vectors"] < 0 or out["exact_max"] < 0 or out["build_threads"] < 1:
        raise ValueError(f"invalid ANN settings: {out}")
    return out


def _default_build_threads() -> int:
    return max(1, min(4, (os.cpu_count() or 2) // 2))


class _Aborted(Exception):
    """A build stopped because the sidecar (or its index) closed."""


class _RWLock:
    """Readers (searches) share; a writer (any mutation of the usearch index)
    excludes them. Writer-preferring: writers hold it only for short slices
    (ADD_SLICE vectors), so a steady stream of searches cannot starve them."""

    def __init__(self) -> None:
        self._c = threading.Condition(threading.Lock())
        self._readers = 0
        self._writer = False
        self._waiting = 0

    @contextlib.contextmanager
    def read(self):
        with self._c:
            while self._writer or self._waiting:
                self._c.wait()
            self._readers += 1
        try:
            yield
        finally:
            with self._c:
                self._readers -= 1
                if not self._readers:
                    self._c.notify_all()

    @contextlib.contextmanager
    def write(self):
        with self._c:
            self._waiting += 1
            while self._writer or self._readers:
                self._c.wait()
            self._waiting -= 1
            self._writer = True
        try:
            yield
        finally:
            with self._c:
                self._writer = False
                self._c.notify_all()


def _fsync_dir(path: str) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


def _decode(blob: bytes, dim: int) -> np.ndarray:
    """A stored vector as float32, tolerating pre-v2 float32 blobs."""
    dtype = np.float16 if dim and len(blob) == dim * 2 else np.float32
    return np.frombuffer(blob, dtype=dtype).astype(np.float32)


class UsearchSidecar:
    """The ANN sidecar of one NamespaceIndex (see the module docstring)."""

    def __init__(self, index: "NamespaceIndex", path: str, *, cfg: dict, scrub_seq: int = 0):
        from usearch.index import Index  # ImportError: the store serves the lane flat

        self._Index = Index
        self.index = index
        self.path = path
        self.ns = index._ns_hint
        self.mode = str(cfg.get("mode", "usearch"))
        self.min_vectors = int(cfg.get("min_vectors", DEFAULT_MIN_VECTORS))
        self.overfetch = max(1, int(cfg.get("overfetch", DEFAULT_OVERFETCH)))
        self.exact_max = max(0, int(cfg.get("exact_max", DEFAULT_EXACT_MAX)))
        self.dtype = str(cfg.get("dtype", "f16"))
        self.build_threads = max(1, int(cfg.get("build_threads", _default_build_threads())))
        self._mu = threading.Lock()            # the state below; never held across I/O
        self._apply_lock = threading.Lock()    # one applier / saver / swapper at a time
        self._rw = _RWLock()                   # searches vs mutations of self._ix
        self._done_cv = threading.Condition(self._mu)
        self._ix: Any = None                   # the live usearch index (None: empty)
        self._active = False                   # kept (usearch mode, or auto above the threshold)
        self._ready = False                    # self._ix reflects SQLite (up to the queue)
        self._queue: deque = deque()           # (wm, removes, adds) not yet applied
        self._journal: list | None = None      # entries applied while a build runs
        self._applied_wm = 0
        self._saved_wm: int | None = None
        self._file: str | None = None          # the current file (named by the state)
        self._scrub_known = int(scrub_seq)     # newest purge the store told us about
        self._scrub_built = int(scrub_seq)     # the purge the live index was built after
        self._closed = False
        self._approx_count = 0                 # vectors in SQLite, for the auto threshold
        self._count_checked = 0
        self._build_thread: threading.Thread | None = None
        self._build_want = 0
        self._build_done = 0
        self._build_reason = ""
        self._building = False
        self._stop = threading.Event()
        self._failures = 0
        self._retry_at = 0.0
        self.rebuilds = 0
        self.last_build_ms: float | None = None
        self.fallback_exact = 0
        self.searches = 0
        self.loaded_from = ""                  # "file" | "snapshot" | "build"
        os.makedirs(path, mode=0o700, exist_ok=True)

    # ------------------------------------------------------------ open

    def start(self, fetch_snapshot=None) -> None:
        """Decide whether the sidecar is kept, then load it (from its file,
        else from the published snapshot) or schedule a rebuild."""
        with self.index._read() as c:
            n = int(c.execute("SELECT COUNT(*) FROM vectors").fetchone()[0])
        self._approx_count = self._count_checked = n
        self._clean_temp()
        if self.mode == "auto" and n < self.min_vectors:
            # the exact scan serves; a file kept now would fall behind
            self._discard_files()
            return
        with self._mu:
            self._active = True
        why = self._try_load()
        if why is None:
            self.loaded_from = "file"
            return
        if n == 0:
            # nothing to index: empty and ready (the first add creates it)
            self._discard_files()
            with self._mu:
                self._ready = True
                self._applied_wm = self.index._vec_wm
            return
        if fetch_snapshot is not None and why != "purge":
            try:
                if self._try_snapshot(fetch_snapshot):
                    self.loaded_from = "snapshot"
                    return
            except Exception as e:  # noqa: BLE001 - a snapshot is a cache
                _log.warning("memd: vector snapshot for %r unusable (%s); rebuilding", self.ns, e)
                METRICS.inc("memd_vector_index_snapshot_failures_total",
                            help="vector sidecar snapshots that could not be used (rebuilt instead)",
                            ns=self.ns, detail=type(e).__name__)
        self._discard_files()
        self.request_build(why)

    def _state_path(self) -> str:
        return os.path.join(self.path, STATE_FILE)

    def _read_state(self) -> dict | None:
        try:
            with open(self._state_path()) as f:
                st = json.load(f)
            return st if isinstance(st, dict) else None
        except (OSError, ValueError):
            return None

    def _try_load(self) -> str | None:
        """Load the file the state names; None on success, else why not."""
        st = self._read_state()
        if st is None or not st.get("file"):
            return "missing"
        if (st.get("version") != STATE_VERSION or st.get("uid") != self.index.vec_uid
                or st.get("dtype") != self.dtype or st.get("metric") != METRIC
                or st.get("connectivity") != CONNECTIVITY):
            return "mismatch"
        if int(st.get("scrub_seq", -1)) < self._scrub_known:
            return "purge"
        if int(st.get("wm", -1)) != self.index._vec_wm:
            return "behind"
        fpath = os.path.join(self.path, str(st["file"]))
        if not os.path.exists(fpath):
            return "missing"
        try:
            if os.path.getsize(fpath) != int(st.get("size", -1)):
                return "corrupt"
            ix = self._Index.restore(fpath)
        except Exception:  # noqa: BLE001 - truncated or garbage: rebuild
            return "corrupt"
        if (ix is None or int(ix.ndim) != int(st.get("ndim", -1))
                or len(ix) != int(st.get("count", -1))):
            return "corrupt"
        with self._mu:
            self._ix = ix
            self._ready = True
            self._applied_wm = self._saved_wm = int(st["wm"])
            self._scrub_built = int(st.get("scrub_seq", 0))
            self._file = str(st["file"])
        return None

    def _try_snapshot(self, fetch) -> bool:
        """Install the published sidecar image when it matches this SQLite
        image exactly (same vec_uid and vec_wm) and postdates every purge."""
        blob = fetch(self.index.vec_uid, self.index._vec_wm)
        if not blob:
            return False
        mv = memoryview(blob)
        if bytes(mv[:len(SNAPSHOT_MAGIC)]) != SNAPSHOT_MAGIC:
            raise ValueError("not a memd vector snapshot")
        off = len(SNAPSHOT_MAGIC)
        hlen = int.from_bytes(mv[off:off + 4], "big")
        head = json.loads(bytes(mv[off + 4:off + 4 + hlen]))
        body = mv[off + 4 + hlen:]
        if (head.get("uid") != self.index.vec_uid or int(head.get("wm", -1)) != self.index._vec_wm
                or head.get("dtype") != self.dtype or head.get("metric") != METRIC
                or head.get("connectivity") != CONNECTIVITY
                or int(head.get("scrub_seq", -1)) < self._scrub_known
                or int(head.get("size", -1)) != len(body)):
            raise ValueError("vector snapshot does not match this index image")
        fn = f"{uuid.uuid4().hex}.usearch"
        self._write_file(fn, lambda f: f.write(body))
        del blob, mv, body
        self._write_state(fn, int(head["wm"]), int(head["ndim"]), int(head["count"]),
                          int(head["scrub_seq"]))
        why = self._try_load()
        if why is not None:
            self._discard_files()
            raise ValueError(f"installed vector snapshot unreadable ({why})")
        METRICS.inc("memd_vector_index_snapshots_loaded_total",
                    help="vector sidecars installed from a published snapshot", ns=self.ns)
        return True

    # ------------------------------------------------------------ files

    def _clean_temp(self) -> None:
        """Temp files a killed write left, and files no state names."""
        st = self._read_state() or {}
        keep = {STATE_FILE, str(st.get("file") or "")}
        try:
            names = os.listdir(self.path)
        except OSError:
            return
        for name in names:
            if name not in keep:
                try:
                    os.unlink(os.path.join(self.path, name))
                except OSError:
                    pass

    def _discard_files(self) -> None:
        """Delete every file of the sidecar (the state first: a crash in
        between leaves an unnamed file, which the next open deletes)."""
        try:
            os.unlink(self._state_path())
        except OSError:
            pass
        try:
            names = os.listdir(self.path)
        except OSError:
            names = []
        for name in names:
            try:
                os.unlink(os.path.join(self.path, name))
            except OSError:
                pass
        _fsync_dir(self.path)
        with self._mu:
            self._file = None
            self._saved_wm = None

    def _write_file(self, fn: str, write) -> None:
        tmp = os.path.join(self.path, fn + ".tmp")
        with open(tmp, "wb") as f:
            write(f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, os.path.join(self.path, fn))
        _fsync_dir(self.path)

    def _write_state(self, fn: str, wm: int, ndim: int, count: int, scrub_seq: int) -> None:
        st = {"version": STATE_VERSION, "uid": self.index.vec_uid, "wm": int(wm), "file": fn,
              "size": os.path.getsize(os.path.join(self.path, fn)), "ndim": int(ndim),
              "count": int(count), "dtype": self.dtype, "metric": METRIC,
              "connectivity": CONNECTIVITY, "scrub_seq": int(scrub_seq)}
        tmp = self._state_path() + ".tmp"
        with open(tmp, "w") as f:
            json.dump(st, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._state_path())
        _fsync_dir(self.path)

    def _save_locked(self, wm: int | None = None) -> dict | None:
        """(apply lock held) Save the live index at a COMMITTED counter value:
        `wm` when the caller just committed and read it, else now. Returns
        the state written, or None when nothing may be saved (not ready,
        empty, or a purge whose rebuild has not finished - see D7)."""
        if wm is None:
            with self.index._lock:
                if self.index._closed:
                    return None
                self.index.flush()
                wm = self.index._vec_wm
        self._drain_locked(upto=wm)
        with self._mu:
            ix, ready = self._ix, self._ready
            purged = self._scrub_built < self._scrub_known
            scrub = self._scrub_built
            if self._saved_wm == wm and self._file and not purged:
                return self._read_state()
        if not ready or purged:
            return None
        if ix is None or len(ix) == 0:
            self._discard_files()  # nothing to keep: an empty build is instant
            return None
        t0 = time.monotonic()
        fn = f"{uuid.uuid4().hex}.usearch"
        tmp = os.path.join(self.path, fn + ".tmp")
        try:
            ix.save(tmp)
            with open(tmp, "rb+") as f:
                os.fsync(f.fileno())
            os.replace(tmp, os.path.join(self.path, fn))
            _fsync_dir(self.path)
            self._write_state(fn, wm, int(ix.ndim), len(ix), scrub)
        except BaseException:
            for p in (tmp, os.path.join(self.path, fn)):
                try:
                    os.unlink(p)
                except OSError:
                    pass
            raise
        with self._mu:
            old, self._file, self._saved_wm = self._file, fn, wm
        if old and old != fn:
            try:
                os.unlink(os.path.join(self.path, old))
            except OSError:
                pass
        METRICS.observe("memd_vector_index_save_ms", (time.monotonic() - t0) * 1000,
                        help="vector sidecar save (ms)", ns=self.ns)
        return self._read_state()

    # ------------------------------------------------------------ hooks
    # Called by NamespaceIndex while it holds its write lock, right after
    # the SQL change and its vec_wm bump. They only queue; apply_pending()
    # runs after that lock is released.

    def tracking(self) -> bool:
        """Whether changes are kept (else only counted for the threshold)."""
        return self._active and not self._closed

    def note(self, wm: int, removes: list[int], adds: list[tuple[int, bytes, int]]) -> None:
        with self._mu:
            if self._closed or not self._active:
                return
            self._queue.append((int(wm), removes, adds))

    def note_reset(self, wm: int) -> None:
        """The SQLite index was wiped: every vector is gone."""
        with self._mu:
            if self._closed or not self._active:
                return
            self._queue.append((int(wm), _RESET, None))

    def count_hint(self, n: int) -> None:
        """Vectors written while not kept: the auto threshold watches this."""
        with self._mu:
            self._approx_count += int(n)

    def apply_pending(self) -> None:
        """Apply queued changes, unless another thread is doing so (it
        re-checks the queue after it lets go, so nothing is stranded)."""
        self._maybe_activate()
        while True:
            with self._mu:
                if not self._queue:
                    return
            if not self._apply_lock.acquire(blocking=False):
                return
            try:
                self._drain_locked()
            except Exception as e:  # noqa: BLE001 - damage: rebuild, the exact scan serves
                self._on_damage(e)
            finally:
                self._apply_lock.release()

    @contextlib.contextmanager
    def _exclusive(self):
        """The apply lock; what queued meanwhile is applied on the way out."""
        with self._apply_lock:
            yield
        self.apply_pending()

    frozen = _exclusive  # held across an index snapshot: nothing is applied

    def _drain_locked(self, upto: int | None = None) -> None:
        """(apply lock held) Apply the queue in order, up to `upto`."""
        while True:
            with self._mu:
                batch = []
                while self._queue and (upto is None or self._queue[0][0] <= upto):
                    batch.append(self._queue.popleft())
                if not batch:
                    return
                if self._journal is not None:
                    self._journal.extend(batch)
                live = self._ready
                self._applied_wm = batch[-1][0]
            if live:
                self._apply_entries(batch, live=True)
            # not ready and no build journal: the rows are in SQLite, and the
            # build that makes the index ready reads them there

    def _apply_entries(self, batch: list, *, live: bool, target: Any = None) -> Any:
        """Apply queue entries to the live index (under the search lock) or
        to a build's `target`. Each entry holds each record's full state, so
        the net effect of a batch is its last state per key."""
        final: dict[int, tuple[bytes, int] | None] = {}
        reset = False
        for _wm, removes, adds in batch:
            if removes is _RESET:
                final.clear()
                reset = True
                continue
            for k in removes:
                final[int(k)] = None
            for k, blob, dim in adds:
                final[int(k)] = (blob, int(dim))
        ix = self._ix if live else target
        if reset:
            ix = None
            if live:
                with self._rw.write():
                    self._ix = None
        if not final:
            return ix
        keys = np.fromiter(final.keys(), dtype=np.uint64, count=len(final))
        adds_k = [k for k, v in final.items() if v is not None]
        dim = int(ix.ndim) if ix is not None else (final[adds_k[-1]][1] if adds_k else 0)
        good = [k for k in adds_k if final[k][1] == dim]
        if len(good) < len(adds_k):
            # a model with another dimension: rebuild on the newest one
            self.request_build("dim_change")
        if ix is not None:
            with (self._rw.write() if live else contextlib.nullcontext()):
                ix.remove(keys)
        if not good:
            return ix
        if ix is None:
            ix = self._new_index(dim)
            if live:
                with self._rw.write():
                    self._ix = ix
        mat = np.stack([_decode(final[k][0], dim) for k in good])
        mat = self._cast(mat)
        karr = np.asarray(good, dtype=np.uint64)
        threads = 1 if len(good) < 16 else min(4, self.build_threads)
        for i in range(0, len(good), ADD_SLICE):
            with (self._rw.write() if live else contextlib.nullcontext()):
                ix.add(karr[i:i + ADD_SLICE], mat[i:i + ADD_SLICE], threads=threads)
        return ix

    def _new_index(self, dim: int) -> Any:
        return self._Index(ndim=int(dim), metric=METRIC, dtype=self.dtype, connectivity=CONNECTIVITY)

    def _cast(self, mat: np.ndarray) -> np.ndarray:
        """f16 input for an f16 index (stored as-is); float32 for i8, which
        usearch quantizes from unit floats (f16 input to an i8 index
        silently returns wrong neighbours)."""
        if self.dtype == "f16":
            return np.ascontiguousarray(mat, dtype=np.float16)
        n = np.linalg.norm(mat, axis=1, keepdims=True)
        n[n == 0] = 1.0
        return np.ascontiguousarray(mat / n, dtype=np.float32)

    def _maybe_activate(self) -> None:
        """auto mode: start keeping the sidecar once the namespace crosses
        the threshold (checked exactly at most every 1/8th of it)."""
        if self._active or self.mode != "auto" or self._closed:
            return
        with self._mu:
            due = (self._approx_count >= self.min_vectors
                   and self._approx_count - self._count_checked >= max(1, self.min_vectors // 8))
            if due:
                self._count_checked = self._approx_count
        if not due:
            return
        try:
            with self.index._read() as c:
                n = int(c.execute("SELECT COUNT(*) FROM vectors").fetchone()[0])
        except Exception:  # noqa: BLE001 - closed meanwhile
            return
        with self._mu:
            self._approx_count = self._count_checked = n
            if n < self.min_vectors or self._active or self._closed:
                return
            self._active = True
        METRICS.inc("memd_vector_index_activations_total",
                    help="namespaces that crossed ann_min_vectors (auto: flat -> usearch)", ns=self.ns)
        self.request_build("threshold")

    def _on_damage(self, e: BaseException) -> None:
        _log.warning("memd: usearch sidecar for %r failed (%s: %s); the exact scan serves until "
                     "it is rebuilt", self.ns, type(e).__name__, e)
        METRICS.inc("memd_vector_index_failures_total",
                    help="usearch sidecar operations that failed (rebuilt; the exact scan serves)",
                    ns=self.ns)
        with self._rw.write():
            with self._mu:
                self._ix = None
                self._ready = False
        self._discard_files()
        self.request_build("damaged")

    # ------------------------------------------------------------ builds

    def request_build(self, reason: str) -> int:
        """Schedule a rebuild from SQLite on the sidecar's build thread.
        Returns a ticket for wait_built()."""
        with self._mu:
            if self._closed:
                return 0
            self._build_want += 1
            ticket = self._build_want
            self._build_reason = reason
            if self._build_thread is None:
                self._build_thread = threading.Thread(target=self._build_loop, daemon=True,
                                                      name="memd-ann-build")
                self._build_thread.start()
        return ticket

    def wait_built(self, ticket: int, timeout_s: float | None = None) -> bool:
        deadline = None if timeout_s is None else time.monotonic() + max(0.0, timeout_s)
        with self._done_cv:
            while self._build_done < ticket and not self._closed:
                left = None if deadline is None else deadline - time.monotonic()
                if left is not None and left <= 0:
                    return False
                self._done_cv.wait(timeout=left if left is not None else 1.0)
            return self._build_done >= ticket

    def _build_loop(self) -> None:
        while True:
            with self._mu:
                if self._closed or self._build_done >= self._build_want:
                    self._build_thread = None
                    self._done_cv.notify_all()
                    return
                target, reason = self._build_want, self._build_reason
                wait = self._retry_at - time.monotonic()
            if wait > 0 and self._stop.wait(timeout=wait):
                continue  # closing
            try:
                self._build(reason)
                with self._mu:
                    self._failures = 0
                    self._build_done = max(self._build_done, target)
                    self._done_cv.notify_all()
            except _Aborted:
                continue
            except Exception as e:  # noqa: BLE001 - the exact scan serves; retried with backoff
                with self._mu:
                    self._failures += 1
                    self._retry_at = time.monotonic() + min(
                        BACKOFF_MAX_S, BACKOFF_BASE_S * 2 ** min(self._failures - 1, 16))
                    self._journal = None
                    self._building = False
                METRICS.inc("memd_vector_index_failures_total", ns=self.ns)
                _log.warning("memd: usearch sidecar build for %r failed (%s: %s); retrying",
                             self.ns, type(e).__name__, e)

    def _check_open(self) -> None:
        if self._closed or self.index._closed:
            raise _Aborted()

    def _build(self, reason: str) -> None:
        """Build a fresh index from SQLite while writes go on, then swap it in."""
        t0 = time.monotonic()
        idx = self.index
        with idx._lock:  # no hook is mid-flight: every earlier change is queued
            self._check_open()
            idx.flush()
            with self._mu:
                self._journal = []
                self._building = True
                scrub = self._scrub_known
        try:
            with idx._read() as c:
                row = c.execute("SELECT dim FROM vectors ORDER BY rowid DESC LIMIT 1").fetchone()
            dim = int(row[0]) if row else 0
            new = self._new_index(dim) if dim else None
            last = 0
            while dim:
                self._check_open()
                with idx._read() as c:
                    rows = c.execute(
                        "SELECT r.rowid, v.vec, v.dim FROM records r JOIN vectors v ON v.id = r.id "
                        "WHERE r.rowid > ? AND r.deleted = 0 AND r.quarantined = 0 "
                        "AND r.invalidated_at IS NULL AND r.superseded_by IS NULL "
                        "ORDER BY r.rowid LIMIT ?", (last, BUILD_CHUNK)).fetchall()
                if not rows:
                    break
                last = int(rows[-1][0])
                keep = [r for r in rows if int(r[2]) == dim]
                if keep:
                    mat = self._cast(np.stack([_decode(r[1], dim) for r in keep]))
                    new.add(np.asarray([r[0] for r in keep], dtype=np.uint64), mat,
                            threads=self.build_threads)
                if len(rows) < BUILD_CHUNK:
                    break
            # catch up on what changed meanwhile without holding anything...
            pos = 0
            while True:
                self._check_open()
                with self._mu:
                    batch = list(self._journal[pos:]) if self._journal is not None else []
                if len(batch) <= JOURNAL_CATCHUP:
                    break
                new = self._apply_entries(batch, live=False, target=new)
                pos += len(batch)
            # ...then the rest, and the swap, with the appliers held off
            with self._exclusive():
                self._check_open()
                self._drain_locked()
                with self._mu:
                    batch = list(self._journal[pos:]) if self._journal is not None else []
                new = self._apply_entries(batch, live=False, target=new)
                with self._rw.write():
                    with self._mu:
                        self._ix = new
                        self._ready = True
                        self._journal = None
                        self._building = False
                        self._scrub_built = scrub
                self.rebuilds += 1
                self.last_build_ms = round((time.monotonic() - t0) * 1000, 1)
                self.loaded_from = "build"
                METRICS.inc("memd_vector_index_rebuilds_total", help="usearch sidecar rebuilds",
                            ns=self.ns, reason=reason)
                METRICS.observe("memd_vector_index_build_ms", self.last_build_ms,
                                help="usearch sidecar build from SQLite (ms)", ns=self.ns)
                try:
                    self._save_locked()
                except Exception as e:  # noqa: BLE001 - it serves from RAM; the next open rebuilds
                    _log.warning("memd: saving the usearch sidecar for %r failed (%s)", self.ns, e)
                    self._discard_files()
        finally:
            with self._mu:
                self._journal = None
                self._building = False
        # the exact scan's float32 matrix is dead weight now
        idx.invalidate_vec_cache()

    def purged(self, scrub_seq: int) -> int:
        """A hard-delete purge ran (D7). `remove` only marked the purged
        vectors, so the files may hold their bytes: delete them now, refuse
        saves until the rebuild from SQLite (already without those rows) is
        done, and start it. Returns the build ticket."""
        with self._mu:
            self._scrub_known = max(self._scrub_known, int(scrub_seq))
            active = self._active and not self._closed
        with self._apply_lock:
            self._discard_files()
        if not active:
            return 0
        return self.request_build("purge")

    # ------------------------------------------------------------ queries

    def active(self) -> bool:
        return self._active and not self._closed

    def ready(self) -> bool:
        with self._mu:
            return self._active and self._ready and not self._closed

    def size(self) -> int:
        ix = self._ix
        return len(ix) if ix is not None else 0

    def knn(self, q: np.ndarray, k: int) -> tuple[np.ndarray, np.ndarray] | None:
        """The approximate top-k: (rowids, cosine distances), or None when the
        sidecar cannot answer (the caller falls back to an exact scan)."""
        try:
            with self._rw.read():
                ix = self._ix
                if ix is None:
                    return np.zeros(0, dtype=np.int64), np.zeros(0, dtype=np.float32)
                if int(ix.ndim) != int(q.shape[0]):
                    return None
                m = ix.search(self._cast(q.reshape(1, -1))[0], int(k), threads=1)
            return np.asarray(m.keys, dtype=np.int64), np.asarray(m.distances, dtype=np.float32)
        except Exception as e:  # noqa: BLE001 - never fail the lane
            self._on_damage(e)
            return None

    def note_search(self) -> None:
        self.searches += 1
        METRICS.inc("memd_vector_index_searches_total",
                    help="vector-lane queries served by the usearch sidecar", ns=self.ns)

    def note_fallback(self, reason: str) -> None:
        self.fallback_exact += 1
        METRICS.inc("memd_vector_index_fallback_total",
                    help="vector-lane queries answered exactly instead of by the usearch sidecar",
                    ns=self.ns, reason=reason)

    # ------------------------------------------------------------ snapshots

    def publishable(self, min_vectors: int) -> bool:
        with self._mu:
            return (self._active and self._ready and not self._closed and self._ix is not None
                    and len(self._ix) >= max(1, int(min_vectors))
                    and self._scrub_built >= self._scrub_known)

    def image_at(self, wm: int) -> tuple[Any, dict] | None:
        """(apply lock held via frozen()) The sidecar saved at exactly `wm`
        - the counter of an index image taken under the same freeze - as
        (open file, state), or None. The file handle stays readable after a
        later save replaces the file."""
        st = self._save_locked(wm)
        if not st or int(st.get("wm", -1)) != int(wm):
            return None
        return open(os.path.join(self.path, st["file"]), "rb"), st

    @staticmethod
    def snapshot_payload(f, st: dict) -> bytearray:
        """The published object: magic, header length, header, usearch file."""
        head = json.dumps({k: st[k] for k in ("uid", "wm", "ndim", "count", "dtype", "metric",
                                              "connectivity", "scrub_seq", "size")}).encode()
        pre = SNAPSHOT_MAGIC + len(head).to_bytes(4, "big") + head
        buf = bytearray(len(pre) + int(st["size"]))
        buf[:len(pre)] = pre
        view = memoryview(buf)[len(pre):]
        got = 0
        while got < len(view):
            n = f.readinto(view[got:])
            if not n:
                raise ValueError("vector sidecar file shrank while being published")
            got += n
        return buf

    # ------------------------------------------------------------ lifecycle

    def drain(self, timeout_s: float = 60.0) -> bool:
        """Apply what is queued and wait for a build in flight; True when
        the sidecar is (still) serving and caught up."""
        deadline = time.monotonic() + max(0.0, timeout_s)
        while True:
            with self._apply_lock:  # an applier in flight finishes first
                pass
            self.apply_pending()
            with self._mu:
                ticket = self._build_want
                busy = self._build_done < ticket and not self._closed
                waiting = bool(self._queue)
            if not busy and not waiting:
                return self.ready() or not self.active()
            left = deadline - time.monotonic()
            if left <= 0:
                return False
            if busy:
                self.wait_built(ticket, left)

    def stats(self) -> dict:
        with self._mu:
            ix = self._ix
            return {"kind": "usearch" if self._active else "flat", "mode": self.mode,
                    "ready": bool(self._active and self._ready),
                    "size": len(ix) if ix is not None else 0,
                    "rebuilds": self.rebuilds, "last_build_ms": self.last_build_ms,
                    "fallback_exact_total": self.fallback_exact, "searches": self.searches,
                    "building": self._building, "dtype": self.dtype,
                    "min_vectors": self.min_vectors, "loaded_from": self.loaded_from,
                    "pending": len(self._queue), "failures": self._failures}

    def close(self) -> None:
        """Stop a build in flight and save the index if it moved on."""
        with self._mu:
            if self._closed:
                return
            self._closed = True
            th = self._build_thread
            self._done_cv.notify_all()
        self._stop.set()
        if th is not None:
            th.join(timeout=120)
        try:
            with self._apply_lock:
                self._save_locked()
        except Exception as e:  # noqa: BLE001 - a missing file is rebuilt at open
            _log.warning("memd: saving the usearch sidecar for %r failed (%s); it is rebuilt "
                         "at the next open", self.ns, e)
            self._discard_files()
        with self._rw.write():
            self._ix = None
