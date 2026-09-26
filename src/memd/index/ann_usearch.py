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
  - off the request path: opening a namespace only counts its vectors and
    starts a journal; loading the file (else the published snapshot, else a
    rebuild) runs on the sidecar's worker thread, and the final save of a
    close is handed to a background thread (settle() waits for it: a reopen
    of the same path, a destroy - which cancels it - and engine close).
    usearch holds the GIL for the whole of save() and restore(), so every
    thread of the process pauses for it wherever it runs; up to
    BUFFER_MAX_BYTES the file is read / written by Python (GIL released) and
    usearch only copies memory (measured at 200K x 384 f16, 183 MB: a 93 ms
    pause to save, 108 ms to load, against 175 / 170 ms through a path).
    While the sidecar is loading or rebuilding, a namespace over
    `flat_max_vectors` never loads the exact scan's float32 matrix (see
    NamespaceIndex.search_vector).
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
import hashlib
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
# 2: the state carries a blake2b of the file, checked before usearch reads it
STATE_VERSION = 2
LOADING_MARKER = "loading"   # present while usearch reads a file: a crash there must not loop
SNAPSHOT_MAGIC = b"MEMDVEC2"
MIN_WINDOW = 100             # fewest candidates re-ranked exactly, whatever the limit
REPAIR_EXPANSION = 16        # search depth of the post-build self-check (see _repair)
DEFAULT_EXPANSION_SEARCH = 128
BUILD_CHUNK = 50_000         # rows read (and added) per build step
JOURNAL_CATCHUP = 256        # journal entries left for the final, locked replay
ADD_SLICE = 64               # vectors added per write-lock hold (bounds a search's wait)
SWEEP_LIMIT = 1024           # limits at or above this are sweeps: answered exactly
BACKOFF_BASE_S = 2.0
BACKOFF_MAX_S = 300.0
DEFAULT_FLAT_MAX = 200_000   # above this the exact scan's matrix is never loaded
# save/load through RAM up to this size: usearch then holds the GIL for a
# memory copy instead of the disk I/O (see "off the request path")
BUFFER_MAX_BYTES = 256 << 20
_RESET = object()            # queue entry: the SQLite index was wiped

# Closed sidecars whose background file work (the handed-off final save, a
# write in flight) may not be finished, by absolute path: see settle()
_CLOSING: dict[str, "UsearchSidecar"] = {}
_CLOSING_LOCK = threading.Lock()


def settle(path: str, *, cancel: bool = False, timeout: float | None = None) -> bool:
    """Wait until a closed sidecar of `path` no longer touches its files: its
    final save written (or, with `cancel`, abandoned - a destroy) and any
    write in flight done. A successor's load and a destroy call this first.
    False when `timeout` ran out."""
    key = os.path.abspath(path)
    with _CLOSING_LOCK:
        inst = _CLOSING.get(key)
    if inst is None:
        return True
    if cancel:
        inst._cancel.set()
    if not inst._close_done.wait(timeout):  # its close has handed off (and started) the save
        return False
    th = inst._saver
    if th is not None and th is not threading.current_thread():
        th.join(timeout)
        if th.is_alive():
            return False
    with inst._file_lock:  # a write its worker had in flight when it closed
        pass
    with _CLOSING_LOCK:
        if _CLOSING.get(key) is inst:
            del _CLOSING[key]
    return True


def settle_all(timeout: float | None = None) -> None:
    """settle() every closed sidecar (engine close: final saves land before
    the process may exit)."""
    with _CLOSING_LOCK:
        paths = list(_CLOSING)
    for p in paths:
        settle(p, timeout=timeout)


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
        # search depth floor (HNSW ef; 0 = usearch's own, 64). A search for
        # k candidates always explores at least k.
        "expansion_search": int(cfg.get("ann_expansion_search", DEFAULT_EXPANSION_SEARCH)),
        # while the sidecar is not serving, a namespace with more vectors than
        # this skips the vector lane (selective filters and sweeps are still
        # answered exactly, streamed from SQLite) instead of loading the exact
        # scan's float32 matrix (4 x dim bytes per vector: 1.5 GB at 1M x 384)
        "flat_max": int(cfg.get("flat_max_vectors", DEFAULT_FLAT_MAX)),
    }
    if (out["overfetch"] < 1 or out["min_vectors"] < 0 or out["exact_max"] < 0
            or out["build_threads"] < 1 or out["expansion_search"] < 0 or out["flat_max"] < 0):
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


def _stored(ix: Any, keys: np.ndarray) -> np.ndarray:
    """The vectors stored for `keys`, as unit float32 rows. Read in the
    index's own scalar type: usearch 2.26's get() returns garbage (NaN) when
    asked to convert (f16 -> f32, i8 -> f16)."""
    v = np.asarray(ix.get(keys), dtype=np.float32).reshape(len(keys), -1)
    n = np.linalg.norm(v, axis=1, keepdims=True)
    n[n == 0] = 1.0
    return v / n


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
        self._key = os.path.abspath(path)
        self.ns = index._ns_hint
        self.mode = str(cfg.get("mode", "usearch"))
        self.min_vectors = int(cfg.get("min_vectors", DEFAULT_MIN_VECTORS))
        self.overfetch = max(1, int(cfg.get("overfetch", DEFAULT_OVERFETCH)))
        self.exact_max = max(0, int(cfg.get("exact_max", DEFAULT_EXACT_MAX)))
        self.dtype = str(cfg.get("dtype", "f16"))
        self.build_threads = max(1, int(cfg.get("build_threads", _default_build_threads())))
        self.expansion_search = max(0, int(cfg.get("expansion_search", DEFAULT_EXPANSION_SEARCH)))
        self.min_window = MIN_WINDOW
        self._mu = threading.Lock()            # the state below; never held across I/O
        self._apply_lock = threading.Lock()    # one applier / saver / swapper at a time
        self._file_lock = threading.Lock()     # the files and _file/_saved_wm (after the apply lock)
        self._rw = _RWLock()                   # searches vs mutations of self._ix
        self._done_cv = threading.Condition(self._mu)
        self._ix: Any = None                   # the live usearch index (None: empty)
        self._active = False                   # kept (usearch mode, or auto above the threshold)
        self._ready = False                    # self._ix reflects SQLite (up to the queue)
        self._queue: deque = deque()           # (wm, removes, adds) not yet applied
        self._inflight: list = []              # entries being applied right now (off the queue)
        self._journal: list | None = None      # entries applied while a load or build runs
        self._applied_wm = 0
        self._saved_wm: int | None = None
        self._file: str | None = None          # the current file (named by the state)
        self._scrub_known = int(scrub_seq)     # newest purge the store told us about
        self._scrub_built = int(scrub_seq)     # the purge the live index was built after
        self._closed = False
        self._cancel = threading.Event()       # a destroy: the handed-off final save is dropped
        self._close_done = threading.Event()   # close() returned (its saver, if any, started)
        self._saver: threading.Thread | None = None
        self._bg = 0                           # background threads still running
        self._approx_count = 0                 # vectors in SQLite, for the auto threshold
        self._count_checked = 0
        self._worker: threading.Thread | None = None
        self._open_job: tuple | None = None    # (vec_wm, fetch_snapshot, vectors) for the worker
        self._loading = False
        self._build_want = 0
        self._build_done = 0
        self._build_reason = ""
        self._building = False
        self._stop = threading.Event()
        self._failures = 0
        self._retry_at = 0.0
        self.rebuilds = 0
        self.last_build_ms: float | None = None
        self.last_gil_ms: dict[str, float] = {}  # longest GIL hold of the latest save / load
        self.fallback_exact = 0
        self.searches = 0
        self.loaded_from = ""                  # "file" | "snapshot" | "build"
        os.makedirs(path, mode=0o700, exist_ok=True)

    # ------------------------------------------------------------ open

    def start(self, fetch_snapshot=None) -> None:
        """Decide whether the sidecar is kept and start journaling changes;
        the load itself (its file, else the published snapshot, else a
        rebuild) runs on the worker thread - opening a namespace never waits
        for it. Until it is done the sidecar is not ready (see
        NamespaceIndex.search_vector for what serves meanwhile)."""
        with self.index._read() as c:
            n = int(c.execute("SELECT COUNT(*) FROM vectors").fetchone()[0])
        active = not (self.mode == "auto" and n < self.min_vectors)
        # vec_wm and the journal start together: no change falls in between
        with self.index._lock:
            with self._mu:
                self._approx_count = self._count_checked = n
                self._active = active
                wm = int(self.index._vec_wm)
                if active and n == 0:
                    # nothing to index: empty and ready (the first add creates it)
                    self._ready = True
                    self._applied_wm = wm
                elif active:
                    self._journal = []
                    self._loading = True
                self._open_job = (wm, fetch_snapshot, n)
                self._kick()

    def _open_step(self, job: tuple) -> None:
        """(worker) Load the sidecar at the vec_wm captured by start()."""
        wm, fetch, n = job
        try:
            settle(self.path)  # a predecessor's final save lands first
            if os.path.exists(os.path.join(self.path, LOADING_MARKER)):
                # the last process died while usearch was reading a file: do
                # not hand it (or the snapshot) to usearch again - rebuild
                _log.warning("memd: the usearch sidecar for %r crashed the process while loading; "
                             "rebuilding it from SQLite", self.ns)
                METRICS.inc("memd_vector_index_corrupt_total",
                            help="sidecar files and snapshots rejected as corrupt (rebuilt from SQLite)",
                            ns=self.ns, source="crash_on_load")
                self._discard_files()
                if self._active and n:
                    self.request_build("crash_on_load")
                return
            self._clean_temp()
            if not self._active or n == 0:
                # below the auto threshold (a file kept would fall behind), or empty
                self._discard_files()
                return
            ix, st, why = self._load_file(wm)
            source = "file"
            if ix is None and fetch is not None and why != "purge":
                try:
                    ix, st = self._load_snapshot(fetch, wm)
                    source = "snapshot" if ix is not None else source
                except _Aborted:
                    raise
                except Exception as e:  # noqa: BLE001 - a snapshot is a cache
                    _log.warning("memd: vector snapshot for %r unusable (%s); rebuilding", self.ns, e)
                    METRICS.inc("memd_vector_index_snapshot_failures_total",
                                help="vector sidecar snapshots that could not be used (rebuilt instead)",
                                ns=self.ns, detail=type(e).__name__)
            if ix is None:
                self._discard_files()
                self.request_build(why)  # (before _loading clears: drain() sees the build)
                return
            if source == "snapshot":
                METRICS.inc("memd_vector_index_snapshots_loaded_total",
                            help="vector sidecars installed from a published snapshot", ns=self.ns)
            self._install(ix, scrub=int(st["scrub_seq"]), source=source,
                          saved=(str(st["file"]), wm) if source == "file" else None)
        except _Aborted:
            pass
        except Exception as e:  # noqa: BLE001 - rebuild; the exact scan serves meanwhile
            if not self._closed:
                _log.warning("memd: loading the usearch sidecar for %r failed (%s: %s); rebuilding",
                             self.ns, type(e).__name__, e)
                self._discard_files()
                self.request_build("corrupt")
        finally:
            with self._mu:
                self._loading = False
                self._done_cv.notify_all()

    def _state_path(self) -> str:
        return os.path.join(self.path, STATE_FILE)

    def _read_state(self) -> dict | None:
        try:
            with open(self._state_path()) as f:
                st = json.load(f)
            return st if isinstance(st, dict) else None
        except (OSError, ValueError):
            return None

    def _load_file(self, wm: int) -> tuple[Any, dict | None, str]:
        """(worker) The index in the file the state names, if it is this
        SQLite image's at exactly `wm`: (index, state, "") or (None, None, why not)."""
        with self._file_lock:
            if self._closed:
                raise _Aborted()
            st = self._read_state()
            if st is None or not st.get("file"):
                return None, None, "missing"
            if (st.get("version") != STATE_VERSION or st.get("uid") != self.index.vec_uid
                    or st.get("dtype") != self.dtype or st.get("metric") != METRIC
                    or st.get("connectivity") != CONNECTIVITY):
                return None, None, "mismatch"
            if int(st.get("scrub_seq", -1)) < self._scrub_known:
                return None, None, "purge"
            if int(st.get("wm", -1)) != int(wm):
                return None, None, "behind"
            fpath = os.path.join(self.path, str(st["file"]))
            if not os.path.exists(fpath):
                return None, None, "missing"
            try:
                size = os.path.getsize(fpath)
                if size != int(st.get("size", -1)):
                    return None, None, "corrupt"
                src = self._read_verified(fpath, size, str(st.get("blake2b") or ""))
                if src is None:
                    self._note_corrupt("file")
                    return None, None, "corrupt"
                ix = self._restore(src)
            except Exception:  # noqa: BLE001 - truncated or garbage: rebuild
                return None, None, "corrupt"
        if (ix is None or int(ix.ndim) != int(st.get("ndim", -1))
                or len(ix) != int(st.get("count", -1))):
            return None, None, "corrupt"
        return ix, st, ""

    @staticmethod
    def _digest(data) -> str:
        return hashlib.blake2b(data, digest_size=32).hexdigest()

    @staticmethod
    def _digest_file(path: str) -> str:
        with open(path, "rb") as f:
            return hashlib.file_digest(f, lambda: hashlib.blake2b(digest_size=32)).hexdigest()

    def _read_verified(self, path: str, size: int, want: str):
        """The file's bytes (or, over BUFFER_MAX_BYTES, its path) when its
        blake2b matches the state's; None when it does not. usearch trusts
        what it reads: a file corrupted in place (same size) loaded, and then
        crashed the process inside search, on every restart."""
        if not want:
            return None
        if size <= BUFFER_MAX_BYTES:
            with open(path, "rb") as f:
                data = f.read()  # (GIL released while reading and hashing)
            return data if self._digest(data) == want else None
        return path if self._digest_file(path) == want else None

    def _note_corrupt(self, source: str) -> None:
        _log.warning("memd: the usearch sidecar %s for %r failed its checksum; rebuilding it "
                     "from SQLite", source, self.ns)
        METRICS.inc("memd_vector_index_corrupt_total",
                    help="sidecar files and snapshots rejected as corrupt (rebuilt from SQLite)",
                    ns=self.ns, source=source)

    def _restore(self, src) -> Any:
        """(file lock held) Index.restore of verified bytes (the GIL held for
        a memory copy) or, over BUFFER_MAX_BYTES, of a verified path. The
        loading marker brackets it and a smoke search: if usearch crashes the
        process meanwhile, the next open rebuilds instead of looping."""
        if not self._may_touch_files(False):
            raise _Aborted()
        marker = os.path.join(self.path, LOADING_MARKER)
        with open(marker, "w") as f:
            f.write(str(os.getpid()))
            f.flush()
            os.fsync(f.fileno())
        _fsync_dir(self.path)
        t0 = time.monotonic()
        ix = self._Index.restore(src)
        self._note_gil("load", t0)
        if ix is not None:
            if self.expansion_search:
                ix.expansion_search = self.expansion_search
            self._smoke(ix)
        os.unlink(marker)
        _fsync_dir(self.path)
        return ix

    @staticmethod
    def _smoke(ix: Any) -> None:
        """A few searches for stored vectors: a structure usearch cannot walk
        fails (or crashes, under the loading marker) here, not in a query."""
        n = len(ix)
        if not n:
            return
        keys = np.asarray(ix.keys, dtype=np.uint64)
        pick = keys[np.linspace(0, n - 1, num=min(8, n), dtype=np.int64)]
        ix.search(_stored(ix, pick), 1, threads=1)

    def _load_snapshot(self, fetch, wm: int) -> tuple[Any, dict | None]:
        """(worker) The published sidecar image, when it matches this SQLite
        image exactly (same vec_uid, vec_wm `wm`) and postdates every purge:
        (index, header), or (None, None) when none is published for it.
        Restored from memory: nothing is written locally until a save."""
        if self._closed:
            raise _Aborted()
        got = fetch(self.index.vec_uid, wm)
        if not got:
            return None, None
        blob, expect_uid = got
        mv = memoryview(blob)
        if bytes(mv[:len(SNAPSHOT_MAGIC)]) != SNAPSHOT_MAGIC:
            raise ValueError("not a memd vector snapshot")
        off = len(SNAPSHOT_MAGIC)
        hlen = int.from_bytes(mv[off:off + 4], "big")
        head = json.loads(bytes(mv[off + 4:off + 4 + hlen]))
        body = mv[off + 4 + hlen:]
        if (head.get("uid") != expect_uid or int(head.get("wm", -1)) != int(wm)
                or head.get("dtype") != self.dtype or head.get("metric") != METRIC
                or head.get("connectivity") != CONNECTIVITY
                or int(head.get("scrub_seq", -1)) < self._scrub_known
                or int(head.get("size", -1)) != len(body)):
            raise ValueError("vector snapshot does not match this index image")
        if not head.get("blake2b") or self._digest(body) != head["blake2b"]:
            self._note_corrupt("snapshot")
            raise ValueError("vector snapshot failed its checksum")
        with self._file_lock:
            ix = self._restore(body)
        if (ix is None or int(ix.ndim) != int(head.get("ndim", -1))
                or len(ix) != int(head.get("count", -1))):
            raise ValueError("vector snapshot unreadable")
        return ix, head

    # ------------------------------------------------------------ files
    # Every file operation holds the file lock and is a no-op once the
    # sidecar is closed - except the handed-off final save, which stops only
    # for a destroy (_cancel). settle() waits on the same lock.

    def _may_touch_files(self, final: bool) -> bool:
        return not (self._cancel.is_set() if final else self._closed)

    def _clean_temp(self) -> None:
        """Temp files a killed write left, and files no state names."""
        with self._file_lock:
            if not self._may_touch_files(False):
                return
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

    def _discard_files(self, final: bool = False) -> None:
        """Delete every file of the sidecar (the state first: a crash in
        between leaves an unnamed file, which the next open deletes)."""
        with self._file_lock:
            if not self._may_touch_files(final):
                return
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
            self._file = None
            self._saved_wm = None

    def _write_state(self, fn: str, meta: dict) -> None:
        st = dict(meta, version=STATE_VERSION, file=fn,
                  size=os.path.getsize(os.path.join(self.path, fn)))
        tmp = self._state_path() + ".tmp"
        with open(tmp, "w") as f:
            json.dump(st, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._state_path())
        _fsync_dir(self.path)

    def _meta(self, ix: Any, wm: int, scrub: int) -> dict:
        return {"uid": self.index.vec_uid, "wm": int(wm), "ndim": int(ix.ndim), "count": len(ix),
                "dtype": self.dtype, "metric": METRIC, "connectivity": CONNECTIVITY,
                "scrub_seq": int(scrub)}

    def _note_gil(self, op: str, t0: float) -> None:
        """A usearch call that holds the GIL (every thread paused) ended."""
        ms = round((time.monotonic() - t0) * 1000, 1)
        self.last_gil_ms[op] = ms
        METRICS.observe("memd_vector_index_gil_hold_ms", ms,
                        help="usearch save/restore calls, which hold the GIL (ms)", ns=self.ns, op=op)

    def _serialize(self, ix: Any) -> bytearray | None:
        """ix.save() into memory when it fits BUFFER_MAX_BYTES, else None
        (then it is saved to the file directly, disk I/O inside the GIL hold)."""
        if int(ix.serialized_length) > BUFFER_MAX_BYTES:
            return None
        t0 = time.monotonic()
        buf = ix.save()
        self._note_gil("save", t0)
        return buf

    def _capture_locked(self, wm: int | None = None, *, serialize: bool = True) -> tuple | None:
        """(apply lock held) What a save of the live index at a COMMITTED
        vec_wm needs - `wm` when the caller just committed and read it, else
        now: (bytes or None, index, meta). None when nothing may be saved (not
        ready, or a purge whose rebuild has not finished - D7) or the file is
        already current. An empty index: (None, None, meta) - its files go."""
        if wm is None:
            with self.index._lock:
                if self.index._closed:
                    return None
                self.index.flush()
                wm = self.index._vec_wm
        self._drain_locked(upto=wm)
        with self._mu:
            ix, ready, scrub = self._ix, self._ready, self._scrub_built
            purged = scrub < self._scrub_known
        if not ready or purged:
            return None
        with self._file_lock:
            if self._file and self._saved_wm == wm:
                return None
        if ix is None or len(ix) == 0:
            return None, None, {"wm": int(wm), "scrub_seq": int(scrub), "empty": True}
        return (self._serialize(ix) if serialize else None), ix, self._meta(ix, wm, scrub)

    def _persist(self, cap: tuple, *, final: bool = False) -> bool:
        """Write a capture: the bytes (no lock needed), else the index itself
        (then nothing may mutate it: the apply lock, or a closed sidecar)."""
        buf, ix, meta = cap
        if meta.get("empty"):
            self._discard_files(final)
            return True
        if buf is None and final:
            buf = self._serialize(ix)
        with self._file_lock:
            if not self._may_touch_files(final):
                return False
            if int(meta["scrub_seq"]) < self._scrub_known:
                return False  # a purge since: its vectors never reach a file (D7)
            if self._file and self._saved_wm is not None and self._saved_wm >= int(meta["wm"]):
                return True   # a newer save already landed
            t0 = time.monotonic()
            fn = f"{uuid.uuid4().hex}.usearch"
            tmp = os.path.join(self.path, fn + ".tmp")
            try:
                if buf is not None:
                    digest = self._digest(buf)
                    with open(tmp, "wb") as f:
                        f.write(buf)
                        f.flush()
                        os.fsync(f.fileno())
                else:
                    t1 = time.monotonic()
                    ix.save(tmp)
                    self._note_gil("save", t1)
                    with open(tmp, "rb+") as f:
                        os.fsync(f.fileno())
                    digest = self._digest_file(tmp)
                os.replace(tmp, os.path.join(self.path, fn))
                _fsync_dir(self.path)
                self._write_state(fn, dict(meta, blake2b=digest))
            except BaseException:
                for p in (tmp, os.path.join(self.path, fn)):
                    try:
                        os.unlink(p)
                    except OSError:
                        pass
                raise
            old, self._file, self._saved_wm = self._file, fn, int(meta["wm"])
            if old and old != fn:
                try:
                    os.unlink(os.path.join(self.path, old))
                except OSError:
                    pass
        METRICS.observe("memd_vector_index_save_ms", (time.monotonic() - t0) * 1000,
                        help="vector sidecar save (ms)", ns=self.ns)
        return True

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
                    self._inflight = batch  # searchable (pending_rowids) until applied
            if live:
                try:
                    self._apply_entries(batch, live=True)
                finally:
                    with self._mu:
                        self._inflight = []
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
        for i in range(0, len(good), ADD_SLICE):
            with (self._rw.write() if live else contextlib.nullcontext()):
                # one thread: parallel insertion leaves some nodes poorly
                # linked (see _repair), and these batches are small
                ix.add(karr[i:i + ADD_SLICE], mat[i:i + ADD_SLICE], threads=1)
        return ix

    def _new_index(self, dim: int) -> Any:
        ix = self._Index(ndim=int(dim), metric=METRIC, dtype=self.dtype, connectivity=CONNECTIVITY)
        if self.expansion_search:
            ix.expansion_search = self.expansion_search
        return ix

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

    # ------------------------------------------------------------ worker: open, builds

    def request_build(self, reason: str) -> int:
        """Schedule a rebuild from SQLite on the sidecar's worker thread.
        Returns a ticket for wait_built()."""
        with self._mu:
            if self._closed:
                return 0
            self._build_want += 1
            ticket = self._build_want
            self._build_reason = reason
            self._kick()
        return ticket

    def _kick(self) -> None:
        """(mu held) Start the worker thread unless it is running."""
        if self._worker is None and not self._closed:
            self._bg += 1
            self._worker = threading.Thread(target=self._run, daemon=True, name="memd-ann")
            self._worker.start()

    def wait_built(self, ticket: int, timeout_s: float | None = None) -> bool:
        deadline = None if timeout_s is None else time.monotonic() + max(0.0, timeout_s)
        with self._done_cv:
            while self._build_done < ticket and not self._closed:
                left = None if deadline is None else deadline - time.monotonic()
                if left is not None and left <= 0:
                    return False
                self._done_cv.wait(timeout=left if left is not None else 1.0)
            return self._build_done >= ticket

    def _run(self) -> None:
        """The worker: the open job first, then builds (with backoff)."""
        try:
            while True:
                with self._mu:
                    job, self._open_job = self._open_job, None
                    if self._closed or (job is None and self._build_done >= self._build_want):
                        # the exit decision and _worker=None in one critical
                        # section: a request made after it starts a new worker
                        self._worker = None
                        self._done_cv.notify_all()
                        return
                    target, reason = self._build_want, self._build_reason
                    wait = self._retry_at - time.monotonic()
                if job is not None:
                    self._open_step(job)
                    continue
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
                    if self._closed:
                        continue  # (the index closed under it)
                    with self._mu:
                        self._failures += 1
                        self._retry_at = time.monotonic() + min(
                            BACKOFF_MAX_S, BACKOFF_BASE_S * 2 ** min(self._failures - 1, 16))
                        self._journal = None
                        self._building = False
                    METRICS.inc("memd_vector_index_failures_total", ns=self.ns)
                    _log.warning("memd: usearch sidecar build for %r failed (%s: %s); retrying",
                                 self.ns, type(e).__name__, e)
        finally:
            with self._mu:
                if self._worker is threading.current_thread():
                    self._worker = None
            self._bg_done()

    def _bg_done(self) -> None:
        """A background thread ended: a closed sidecar with none left needs
        no settling any more."""
        with self._mu:
            self._bg -= 1
            gone = self._closed and self._bg <= 0
        if gone:
            with _CLOSING_LOCK:
                if _CLOSING.get(self._key) is self:
                    del _CLOSING[self._key]

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
            if new is not None:
                self._repair(new)
            if self._install(new, scrub=scrub, source="build"):
                self.rebuilds += 1
                self.last_build_ms = round((time.monotonic() - t0) * 1000, 1)
                METRICS.inc("memd_vector_index_rebuilds_total", help="usearch sidecar rebuilds",
                            ns=self.ns, reason=reason)
                METRICS.observe("memd_vector_index_build_ms", self.last_build_ms,
                                help="usearch sidecar build from SQLite (ms)", ns=self.ns)
        finally:
            with self._mu:
                self._journal = None
                self._building = False

    def _repair(self, ix: Any) -> int:
        """(worker, on the private index a build just made) Re-insert, on one
        thread, every node that a search for its own vector does not return.
        Parallel construction leaves a few nodes poorly linked (measured: 60
        of 60K), and which ones differs from build to build: two builds of
        the same rows then answered some queries differently (a true top-10
        row unreachable in one of them). After this pass both find them
        (200/200 identical top-10s, recall 1.0 at 60K clustered). Costs about
        a quarter of the build time. A search answered by a node holding the
        very same vector (a duplicate record: the same text embedded twice)
        counts as found - k=1 returns one of the tied twins, and re-inserting
        every duplicate doubled the build at 30% duplicates. Returns the
        nodes re-inserted."""
        n = len(ix)
        if not n:
            return 0
        keys = np.asarray(ix.keys, dtype=np.uint64)
        ef = ix.expansion_search
        bad: list = []
        try:
            ix.expansion_search = REPAIR_EXPANSION
            for s in range(0, n, BUILD_CHUNK):
                self._check_open()
                ks = keys[s:s + BUILD_CHUNK]
                got = np.asarray(ix.search(_stored(ix, ks), 1, threads=self.build_threads).keys)
                got = got.reshape(len(ks), -1)[:, 0].astype(np.uint64)
                miss = got != ks
                if miss.any():
                    mk, mg = ks[miss], got[miss]
                    twin = np.asarray(ix.contains(mg), dtype=bool).reshape(-1)
                    if twin.any():
                        # compared as stored (the index's own scalar type)
                        twin[twin] = np.all(np.asarray(ix.get(mk[twin])).reshape(int(twin.sum()), -1)
                                            == np.asarray(ix.get(mg[twin])).reshape(int(twin.sum()), -1),
                                            axis=1)
                    bad.extend(mk[~twin].tolist())
            if bad:
                b = np.asarray(bad, dtype=np.uint64)
                v = self._cast(_stored(ix, b))
                ix.remove(b)
                ix.add(b, v, threads=1)
        finally:
            ix.expansion_search = ef
        if bad:
            METRICS.inc("memd_vector_index_repaired_total", len(bad),
                        help="poorly linked HNSW nodes re-inserted after a build", ns=self.ns)
        return len(bad)

    def _install(self, new: Any, *, scrub: int, source: str,
                 saved: tuple[str, int] | None = None) -> bool:
        """(worker) Catch a loaded or built index up with the journal and
        swap it in; a built or snapshot-installed one is then saved. False
        when a purge happened meanwhile (its own rebuild follows)."""
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
        cap = None
        with self._exclusive():
            self._check_open()
            if scrub < self._scrub_known:
                return False
            self._drain_locked()
            with self._mu:
                batch = list(self._journal[pos:]) if self._journal is not None else []
            new = self._apply_entries(batch, live=False, target=new)
            # the exact scan's float32 matrix (loaded while this was not
            # serving) is dead weight from the swap on: freed around it, not
            # after the save below (seconds, at the matrix's full size)
            self.index.invalidate_vec_cache()
            with self._rw.write():
                with self._mu:
                    self._ix = new
                    self._ready = True
                    self._journal = None
                    self._scrub_built = scrub
            self.index.invalidate_vec_cache()  # (a search may have reloaded it meanwhile)
            if saved is not None:
                with self._file_lock:
                    self._file, self._saved_wm = saved
            self.loaded_from = source
            if source != "file":
                try:
                    cap = self._capture_locked()
                    if cap is not None and cap[0] is None and cap[1] is not None:
                        # too large to serialize in memory: saved from the
                        # index itself while the appliers are still held off
                        self._persist(cap)
                        cap = None
                except Exception as e:  # noqa: BLE001 - it serves from RAM; the next open rebuilds
                    _log.warning("memd: saving the usearch sidecar for %r failed (%s)", self.ns, e)
                    self._discard_files()
                    cap = None
        if cap is not None:
            try:
                self._persist(cap)
            except Exception as e:  # noqa: BLE001
                _log.warning("memd: saving the usearch sidecar for %r failed (%s)", self.ns, e)
                self._discard_files()
        return True

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

    def pending_rowids(self) -> list[int]:
        """Rowids whose vectors are queued or being applied but not yet in
        the index (the writer could not take the apply lock - a snapshot
        publish, another writer's batch): searched exactly, so a write is
        visible to the search that follows it (read-your-writes)."""
        with self._mu:
            entries = list(self._inflight) + list(self._queue)
        out: set[int] = set()
        for _wm, _removes, adds in entries:
            if adds:
                out.update(int(k) for k, _b, _d in adds)
        return sorted(out)

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

    def image_at(self, wm: int) -> tuple[bytearray, dict] | None:
        """(apply lock held via frozen()) The sidecar serialized at exactly
        `wm` - the counter of an index image taken under the same freeze - as
        (bytes, meta), or None. Nothing is written locally."""
        self._drain_locked(upto=wm)
        with self._mu:
            ix, ready, scrub = self._ix, self._ready, self._scrub_built
            if not ready or scrub < self._scrub_known or ix is None or len(ix) == 0:
                return None
        t0 = time.monotonic()
        buf = ix.save()
        self._note_gil("save", t0)
        return buf, dict(self._meta(ix, wm, scrub), size=len(buf), blake2b=self._digest(buf))

    @staticmethod
    def snapshot_payload(buf: bytearray, st: dict) -> bytearray:
        """The published object: magic, header length, header, usearch image
        (the header is put in front of `buf` in place)."""
        head = json.dumps({k: st[k] for k in ("uid", "wm", "ndim", "count", "dtype", "metric",
                                              "connectivity", "scrub_seq", "size", "blake2b")}).encode()
        buf[0:0] = SNAPSHOT_MAGIC + len(head).to_bytes(4, "big") + head
        return buf

    # ------------------------------------------------------------ lifecycle

    def drain(self, timeout_s: float = 60.0) -> bool:
        """Apply what is queued and wait for a load or build in flight; True
        when the sidecar is (still) serving and caught up."""
        deadline = time.monotonic() + max(0.0, timeout_s)
        while True:
            with self._apply_lock:  # an applier in flight finishes first
                pass
            self.apply_pending()
            with self._mu:
                busy = not self._closed and (self._open_job is not None or self._loading
                                             or self._build_done < self._build_want)
                waiting = bool(self._queue)
            if not busy and not waiting:
                return self.ready() or not self.active()
            left = deadline - time.monotonic()
            if left <= 0:
                return False
            if busy:
                with self._done_cv:
                    self._done_cv.wait(timeout=min(left, 0.5))

    def stats(self) -> dict:
        with self._mu:
            ix = self._ix
            return {"kind": "usearch" if self._active else "flat", "mode": self.mode,
                    "ready": bool(self._active and self._ready),
                    "size": len(ix) if ix is not None else 0,
                    "rebuilds": self.rebuilds, "last_build_ms": self.last_build_ms,
                    "fallback_exact_total": self.fallback_exact, "searches": self.searches,
                    "building": self._building, "loading": self._loading, "dtype": self.dtype,
                    "min_vectors": self.min_vectors, "loaded_from": self.loaded_from,
                    "pending": len(self._queue), "failures": self._failures,
                    "last_gil_hold_ms": dict(self.last_gil_ms)}

    def close(self) -> None:
        """Stop the worker (a build in flight aborts at its next step) and
        hand the final save - if the index moved on since its file - to a
        background thread: closing never waits on usearch I/O. settle() (a
        reopen, a destroy, engine close) waits for it."""
        with self._mu:
            if self._closed:
                return
            self._closed = True
            self._done_cv.notify_all()
        self._stop.set()
        with _CLOSING_LOCK:
            _CLOSING[self._key] = self
        try:
            self._close()
        finally:
            self._close_done.set()

    def _close(self) -> None:
        cap = None
        try:
            with self._apply_lock:  # an applier or swap in flight finishes first
                cap = self._capture_locked(serialize=False)
        except Exception as e:  # noqa: BLE001 - a missing or stale file is rebuilt at open
            _log.warning("memd: saving the usearch sidecar for %r failed (%s); it is rebuilt "
                         "at the next open", self.ns, e)
        with self._rw.write():
            self._ix = None  # (the saver holds its own reference)
        with self._mu:
            if cap is not None:
                self._bg += 1
                self._saver = threading.Thread(target=self._final_save, args=(cap,), daemon=True,
                                               name="memd-ann-save")
            idle = self._bg <= 0
        if idle:
            with _CLOSING_LOCK:
                if _CLOSING.get(self._key) is self:
                    del _CLOSING[self._key]
        if self._saver is not None:
            # last: usearch holds the GIL while it serializes, so the caller's
            # own remaining work would otherwise wait for it too
            self._saver.start()

    def _final_save(self, cap: tuple) -> None:
        try:
            self._persist(cap, final=True)
        except Exception as e:  # noqa: BLE001 - a missing or stale file is rebuilt at open
            _log.warning("memd: saving the usearch sidecar for %r failed (%s); it is rebuilt "
                         "at the next open", self.ns, e)
            self._discard_files(final=True)
        finally:
            self._bg_done()
