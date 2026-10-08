"""Read replicas: a namespace followed from the bucket, never written.

A namespace has one writer - its leaseholder. A replica opens it WITHOUT the
lease and serves reads from a local derived index it keeps up to date by
following the writer's durable state:

  - bootstrap: verify key custody (never minting a key), install the
    published index snapshot when it covers the snapshot floor, replay the
    segments, then the log tail;
  - refresh (every replica_refresh_s): the manifest (M1), the WAL tail, the
    ops tail, the WAL tail again when the ops ran past every frame, the
    manifest again (M2). A different M2 means a fold may have folded and
    deleted part of what was read: the tail is discarded and read again from
    the new manifest. Events are applied in the one seq order, and only up
    to the HORIZON - the seq every lower event is known to be durable below
    (see _read_tail) - so a delete is never applied before the write it
    follows, nor a write after a delete that follows it;
  - a new lineage (another tenure) or a fold that retired ops above the
    replica's horizon (compact_seq / scrub_seq: the _snapshot_floor rule)
    REBUILDS it - the cache files are deleted, the bootstrap runs again; a
    rotation is caught up by loading its segment (the warm-open catch-up).
    A rebuild across a lineage resolves the data key again (the namespace
    may have been destroyed and created again under its name, with a new
    key), and a rebuild that failed part-way runs again on the next refresh;
  - a namespace whose manifest is gone (crypto-shred) drops its cache.

It never writes or deletes an object: it is handed a ReadOnlyObjectStore and
a ReadOnlyKeyEnvelope, and every write path of NamespaceStore refuses on it
(read_only). The only files it writes are its own local derived caches,
which exist exactly as long as it is open (close deletes them).
"""
from __future__ import annotations

import contextlib
import json
import logging
import os
import threading
import time

from memd.core.schema import MemoryRecord, records_from_jsonl
from memd.metrics import METRICS
from memd.storage.crypto import KeyCustodyError, KeyEnvelope, NullKeyEnvelope, ReadOnlyKeyEnvelope
from memd.storage.engine import (STORE_FORMAT, Manifest, NamespaceStore, StoreFormatError,
                                 _cache_lock_path, _drop_cache_files, _frame_seq, _frame_start,
                                 _frames_with_offsets, _merge_events)
from memd.storage.objectstore import ObjectStore, ReadOnlyError

_log = logging.getLogger(__name__)

DEFAULT_REPLICA_REFRESH_S = 2.0


class ReplicaUnavailableError(RuntimeError):
    """A replica cannot serve this read within the staleness the caller
    accepts (its refresh failed or is too old), or cannot serve the
    namespace at all (key custody, a format it does not read). The read
    should go to the namespace's writer."""


class _Moved(Exception):
    """The manifest changed under a read: retry from the new one."""


class _Rebuilt(Exception):
    """A rebuild ran mid-refresh: read the tail again from its state."""


class ReadOnlyObjectStore(ObjectStore):
    """An ObjectStore that reads through to `inner` and refuses every write.

    No generic attribute forwarding, on purpose: NamespaceStore probes its
    store for capabilities by name (try_acquire_owner, set_log_floor,
    log_bound, ...) and must find none of the writer-side ones here."""

    def __init__(self, inner: ObjectStore):
        self.inner = inner
        self.log_cursors_survive_folds = bool(getattr(inner, "log_cursors_survive_folds", False))

    # ---------------------------------------------------------------- reads
    def get(self, key: str) -> bytes | None:
        return self.inner.get(key)

    def get_versioned(self, key: str):
        return self.inner.get_versioned(key)

    def exists(self, key: str) -> bool:
        return self.inner.exists(key)

    def list(self, prefix: str) -> list[str]:
        return self.inner.list(prefix)

    def size(self, key: str) -> int:
        return self.inner.size(key)

    def log_tail(self, key: str, cursor=None):
        return self.inner.log_tail(key, cursor)

    def log_cursor_back(self, cursor, nbytes: int):
        return self.inner.log_cursor_back(cursor, nbytes)

    # --------------------------------------------------------------- writes
    def _refuse(self, what: str):
        return ReadOnlyError(f"a read replica never {what} an object")

    def put(self, key: str, data: bytes) -> None:
        raise self._refuse("puts")

    def put_hint(self, key: str, data: bytes) -> None:
        raise self._refuse("puts")

    def put_if_match(self, key, data, version, *, hint=False, fence=True) -> str:
        raise self._refuse("puts")

    def put_if_absent(self, key: str, data: bytes) -> bool:
        raise self._refuse("creates")

    def append(self, key: str, data: bytes) -> int:
        raise self._refuse("appends to")

    def delete(self, key: str) -> None:
        raise self._refuse("deletes")

    def delete_log(self, key: str, upto) -> None:
        raise self._refuse("deletes")

    def shred(self, key: str) -> None:
        raise self._refuse("shreds")

    def truncate(self, key: str, size: int) -> None:
        raise self._refuse("truncates")

    def remove_prefix(self, prefix: str) -> int:
        raise self._refuse("deletes")

    def copy(self, src: str, dst: str) -> None:
        raise self._refuse("copies")

    def open_log(self, key: str):
        raise self._refuse("appends to")


class _ServeLock:
    """Reads share the index; a rebuild - which closes it and builds a new
    one in its place - waits for the reads in flight and keeps new ones out.
    Re-entrant for a thread that already reads (a read never waits on a
    rebuild queued behind itself)."""

    def __init__(self):
        self._cv = threading.Condition()
        self._readers = 0
        self._writer = False
        self._waiting = 0
        self._mine = threading.local()

    @contextlib.contextmanager
    def read(self):
        depth = getattr(self._mine, "depth", 0)
        if depth == 0:
            with self._cv:
                while self._writer or self._waiting:
                    self._cv.wait()
                self._readers += 1
        self._mine.depth = depth + 1
        try:
            yield
        finally:
            self._mine.depth = depth
            if depth == 0:
                with self._cv:
                    self._readers -= 1
                    if not self._readers:
                        self._cv.notify_all()

    @contextlib.contextmanager
    def write(self):
        with self._cv:
            self._waiting += 1
            try:
                while self._writer or self._readers:
                    self._cv.wait()
            finally:
                self._waiting -= 1
            self._writer = True
        try:
            yield
        finally:
            with self._cv:
                self._writer = False
                self._cv.notify_all()


class _Tail:
    """One refresh's log reads, not yet applied (see ReplicaStore._read_tail)."""

    __slots__ = ("frames", "ops", "cursors", "seen", "covered", "horizon", "reset")

    def __init__(self, frames, ops, cursors, seen, covered, horizon, reset):
        self.frames = frames      # [(seq, [MemoryRecord])] in log order
        self.ops = ops            # [op dict] in log order
        self.cursors = cursors    # {"wal": cursor, "ops": cursor} past them
        self.seen = seen          # {"wal": newest frame seq, "ops": newest op seq} ever seen
        self.covered = covered    # newest op seq a later WAL read followed
        self.horizon = horizon    # every event at or below it is known
        self.reset = reset        # {"wal": bool, "ops": bool}: a log was re-read from its start


class ReplicaStore(NamespaceStore):
    """A read-only namespace: follows the writer's durable state (see the
    module docstring). Opened by StorageEngine.replica()."""

    read_only = True
    REFRESH_ATTEMPTS = 6
    # a custody refusal mid-refresh resolves the key again at most this often
    KEY_RETRY_S = 30.0

    # a stale read waits at most this long for a refresh (see ensure_fresh)
    REFRESH_WAIT_S = 1.0

    def __init__(self, namespace: str, store: ObjectStore, cache_dir: str,
                 envelope: KeyEnvelope | None = None, *, lexical: dict | None = None,
                 vector_index: dict | None = None, on_applied=None,
                 refresh_wait_s: float | None = None):
        self._serve = _ServeLock()
        self._refresh_lock = threading.Lock()
        self.refresh_wait_s = self.REFRESH_WAIT_S if refresh_wait_s is None else float(refresh_wait_s)
        # refreshes in flight and done, for reads that wait on one (ensure_fresh)
        self._fresh_cv = threading.Condition()
        self._running_since: float | None = None   # monotonic start of the refresh in flight
        self._kicked = False                       # a read's refresh waits for the lock
        self._done = 0                             # refreshes finished (either way)
        self._last_done: tuple[float, BaseException | None] | None = None   # (its start, its error)
        self._lexical_cfg = lexical
        self._on_applied = on_applied
        self._opened = False
        # the applied horizon: every durable event at or below it is in the
        # index, none above (also the index's applied_seq)
        self.applied_seq = 0
        self._synced = False      # built from a manifest (and not dropped since)
        self._lineage = ""        # the tenure it was built in (a pre-lineage build's: "")
        self._key_lineage: str | None = None   # the tenure its data key was proven in
        self._key_retry_at: float | None = None
        self._fresh_index = True  # the index file holds nothing yet
        self.exists = False       # the namespace has a manifest
        self._cursors: dict = {"wal": None, "ops": None}
        self._pending: list[tuple[int, int, object]] = []   # (seq, kind 0 frame / 1 op, event)
        self._seen = {"wal": 0, "ops": 0}
        self._covered = 0
        # events at or below it that a log read returns are folded already
        # (residue a fold has not deleted yet, a log re-read after a reset)
        self._skip_floor = 0
        self.refreshed_at: float | None = None   # monotonic START of the last good refresh
        self.last_read = time.monotonic()
        self.last_error: str | None = None
        self.failures = 0
        self.refreshes = 0
        self.rebuilds = 0
        self.installed_snapshot = False
        # bumped whenever what the index serves changed (a search cache key)
        self.data_epoch = 0
        # every hard delete this replica knows of at or below it is scrubbed
        # from its files
        self.scrubbed_through = 0
        self._scrub_mu = threading.Lock()
        self._scrub_want = 0
        self._scrub_running = False
        self._scrub_on_tail = False   # the first tail after a rebuild: see _apply_tail
        inner_env = envelope or NullKeyEnvelope()
        env = inner_env if isinstance(inner_env, ReadOnlyKeyEnvelope) else ReadOnlyKeyEnvelope(inner_env)
        ro = store if isinstance(store, ReadOnlyObjectStore) else ReadOnlyObjectStore(store)
        super().__init__(namespace, ro, cache_dir, env, lexical=lexical, vector_index=vector_index)
        self._opened = True

    # ------------------------------------------------------------- status

    def age_s(self) -> float | None:
        """Seconds since the start of the last successful refresh (None: no
        refresh has succeeded since the last rebuild began)."""
        at = self.refreshed_at
        return None if at is None else max(0.0, time.monotonic() - at)

    def status(self) -> dict:
        age = self.age_s()
        return {"applied_seq": self.applied_seq, "lineage": self._lineage, "exists": self.exists,
                "age_ms": None if age is None else int(age * 1000), "refreshes": self.refreshes,
                "rebuilds": self.rebuilds, "failures": self.failures, "last_error": self.last_error,
                "pending_events": len(self._pending), "scrubbed_through": self.scrubbed_through,
                "installed_snapshot": self.installed_snapshot}

    def touch(self) -> None:
        self.last_read = time.monotonic()

    @contextlib.contextmanager
    def holding(self):
        """Hold the index (a rebuild waits for it) - for maintenance: a flush,
        derived vectors. Reads use reading()."""
        with self._serve.read():
            if self._closed:
                raise RuntimeError(f"namespace {self.namespace!r} replica was evicted from the open "
                                   "cache; re-resolve it via the engine")
            yield self

    def serving_info(self) -> dict:
        """What a read served now is served from: {"served_by", "applied_seq",
        "age_ms"} (age_ms None: no refresh has completed since the last
        rebuild began - reading() serves nothing then)."""
        age = self.age_s()
        return {"served_by": "replica", "applied_seq": self.applied_seq,
                "age_ms": None if age is None else int(age * 1000)}

    @contextlib.contextmanager
    def reading(self):
        """Hold while reading the index to serve a read: a rebuild waits for
        it. A rebuild that failed part-way (it discarded the index first)
        leaves nothing to serve: ReplicaUnavailableError until one completes -
        also for a read that passed its freshness check before the rebuild
        began.

        A rebuild that completed is not served either until the refresh that
        ran it has applied the log tail too (refreshed_at is None until
        then): the index built from durable data alone can be older than
        what this replica served before it - a hard delete the tail holds,
        applied and served already, not applied yet. A read that waited for
        the rebuild waits for that refresh (at most refresh_wait_s, as in
        ensure_fresh) and is refused if it does not complete in time. Any
        refresh that completes after a rebuild began is newer than the one a
        read checked its freshness against."""
        self.touch()
        waited_since = None
        while True:
            with self.holding():
                if self.exists and not self._synced and self.last_error:
                    # a rebuild that FAILED: refused at once (the router
                    # sends the read to the writer)
                    raise ReplicaUnavailableError(
                        f"namespace {self.namespace!r}: the replica's rebuild did not complete "
                        f"({self.last_error})")
                if (self._synced or not self.exists) and self.refreshed_at is not None:
                    yield self
                    return
            # (the serve lock released: the refresh may rebuild again - the
            # wait is counted from the first time, never started over)
            if waited_since is None:
                waited_since = time.monotonic()
            # a rebuild in progress, or one that has not applied the log
            # tail yet: wait for it, at most refresh_wait_s in all
            self._await_refresh(float("-inf"), waited_since, self.refresh_wait_s,
                                "the replica's rebuild is in progress or has not applied "
                                "the log tail yet")

    def ensure_fresh(self, max_staleness_s: float, wait_s: float | None = None) -> None:
        """Make sure every write the writer acknowledged more than
        `max_staleness_s` before now is applied. Within that bound the
        replica serves as it is, whatever refresh is running. Staler: it
        waits for a refresh - the one in flight if it started within the
        bound, else one it starts in the background - for at most `wait_s`
        (refresh_wait_s, 1 s), and never on a refresh that has already run
        that long (a store or key provider that hangs: the read must not
        queue behind it). ReplicaUnavailableError when none completes in
        time or the one it waited for failed - the router sends the read to
        the writer."""
        t_req = time.monotonic()
        self.touch()
        floor = t_req - max_staleness_s
        at = self.refreshed_at
        if at is not None and at >= floor:
            return
        wait = self.refresh_wait_s if wait_s is None else max(0.0, float(wait_s))
        self._await_refresh(floor, t_req, wait,
                            f"the replica is staler than {int(max_staleness_s * 1000)} ms")

    def _await_refresh(self, floor: float, t_req: float, wait: float, stale: str) -> None:
        """Wait until a refresh that started at or after `floor` has
        completed (refreshed_at >= floor): the one in flight if it did, else
        one queued in the background - until t_req + wait, and never past
        `wait` into a refresh that is running already.
        ReplicaUnavailableError (`stale` says why the read waited) when
        none completes in time, or one it could have used failed."""
        with self._fresh_cv:
            done = self._done
            while True:
                at = self.refreshed_at
                if at is not None and at >= floor:
                    return
                if self._closed:
                    raise ReplicaUnavailableError(f"namespace {self.namespace!r}: the replica was closed")
                last = self._last_done
                if self._done != done and last is not None and last[1] is not None and last[0] >= floor:
                    ex = last[1]
                    raise ReplicaUnavailableError(
                        f"namespace {self.namespace!r}: the replica could not refresh "
                        f"({type(ex).__name__}: {ex})") from ex
                running = self._running_since
                if running is None or running < floor:
                    # nothing in flight that can satisfy this read: queue one
                    self._kick()
                now = time.monotonic()
                deadline = t_req + wait
                if running is not None:
                    deadline = min(deadline, running + wait)
                if now >= deadline:
                    why = (f"a refresh has been running for {int((now - running) * 1000)} ms"
                           if running is not None and now - running >= wait
                           else f"no refresh completed within {int(wait * 1000)} ms")
                    raise ReplicaUnavailableError(f"namespace {self.namespace!r}: {stale} ({why})")
                self._fresh_cv.wait(deadline - now)

    def _kick(self) -> None:
        """(Under _fresh_cv.) Start a refresh in the background for reads
        waiting on one - at most one queued at a time."""
        if self._kicked or self._closed:
            return
        self._kicked = True
        threading.Thread(target=self._kicked_refresh, name=f"memd-replica-read-{self.namespace}",
                         daemon=True).start()

    def _kicked_refresh(self) -> None:
        # (refresh() clears _kicked once it holds the lock: a read arriving
        # after that may queue the next one)
        try:
            self.refresh(_kicked=True)
        except Exception:  # noqa: BLE001 - recorded on the replica; the waiting reads see it
            pass

    # ------------------------------------------------------------ open

    def _open(self) -> None:
        try:  # the image of a snapshot install a crash interrupted
            os.unlink(self.index.path + ".incoming")
        except OSError:
            pass
        if not self.index.created:
            # an earlier replica's file (they are deleted at close; a crash
            # leaves one): a replica is only ever built from durable data
            self._discard_index("a replica starts from durable data")
        self._fresh_index = True
        t0 = time.monotonic()
        self._sync()
        self.refreshed_at = t0
        self.refreshes += 1

    def refresh(self, *, _kicked: bool = False) -> bool:
        """Follow the writer once (see the module docstring). True when the
        index changed. One refresh at a time; a failed one leaves the replica
        as it was (its age grows until one succeeds)."""
        with self._refresh_lock:
            if self._closed:
                raise RuntimeError(f"namespace {self.namespace!r} replica is closed")
            t0 = time.monotonic()
            with self._fresh_cv:
                self._running_since = t0
                if _kicked:
                    self._kicked = False    # the next stale read may queue another
            epoch = self.data_epoch
            err: BaseException | None = None
            try:
                try:
                    self._sync()
                except KeyCustodyError:
                    if not self._resolve_key_again():
                        raise
                    self._sync()
            except BaseException as ex:
                err = ex
                self.failures += 1
                self.last_error = f"{type(ex).__name__}: {ex}"[:300]
                METRICS.inc("memd_replica_refreshes_total", help="replica refreshes by result",
                            ns=self.namespace, result="failed")
                raise
            else:
                self.refreshed_at = t0
                self.refreshes += 1
                self.failures = 0
                self.last_error = None
                METRICS.inc("memd_replica_refreshes_total", ns=self.namespace, result="ok")
                METRICS.observe("memd_replica_refresh_ms", (time.monotonic() - t0) * 1000,
                                help="replica refresh duration (ms)", ns=self.namespace)
                return self.data_epoch != epoch
            finally:
                with self._fresh_cv:
                    self._running_since = None
                    self._done += 1
                    self._last_done = (t0, err)
                    self._fresh_cv.notify_all()

    def _resolve_key_again(self) -> bool:
        """A custody refusal mid-refresh: drop the key in hand and rebuild from
        durable data with the key resolved again from its record - once, and
        at most every KEY_RETRY_S. A stale copy (the namespace destroyed and
        created again under its name) is replaced; a wrong key is refused
        again by the rebuild's _verify_key - never served, never skipped past
        as damage."""
        now = time.monotonic()
        if self._key_retry_at is not None and now - self._key_retry_at < self.KEY_RETRY_S:
            return False
        self._key_retry_at = now
        self._forget_key()
        self._synced = False
        return True

    def _forget_key(self) -> None:
        forget = getattr(self.envelope, "forget", None)
        if callable(forget):
            forget(self.namespace)
        self._key_lineage = None

    def _sync(self) -> None:
        for _attempt in range(self.REFRESH_ATTEMPTS):
            got = self.store.get_versioned(self.manifest_key)
            if got is None:
                self._gone()
                return
            raw, ver = got
            try:
                # not synced: never built, or a rebuild failed part-way (the
                # index was discarded) - it runs again, never a tail on top
                if ver != self._manifest_ver or not self._synced:
                    self._adopt(Manifest.from_dict(json.loads(raw)), ver)
                tail = self._read_tail()
            except _Moved:
                continue
            again = self.store.get_versioned(self.manifest_key)
            if again is None or again[1] != self._manifest_ver:
                continue     # a fold may have folded (and deleted) part of the tail read
            try:
                self._apply_tail(tail)
            except _Rebuilt:
                continue
            return
        raise ReplicaUnavailableError(
            f"namespace {self.namespace!r}: the manifest kept changing during "
            f"{self.REFRESH_ATTEMPTS} refresh attempts")

    # -------------------------------------------------------- manifest side

    def _floor_of(self, m: Manifest) -> int:
        return max(1, m.scrub_seq, m.compact_seq)

    def _adopt(self, m: Manifest, ver) -> None:
        """A new manifest version: rebuild, or catch up across rotations."""
        if m.format < STORE_FORMAT:
            raise StoreFormatError(
                f"namespace {self.namespace!r} is in store format {m.format} and must be migrated "
                "by a writer before a replica can read it (open it on a writer first)")
        if not self._synced:
            why = "bootstrap"
        elif m.lineage != self._lineage:
            why = "another tenure wrote the namespace since"
        elif 0 < self.applied_seq < self._floor_of(m):
            why = "a fold retired ops above the replica's horizon"
        else:
            why = None
        if why is not None:
            self._rebuild(m, ver, why)
            return
        if m.key_check and m.key_check != self.manifest.key_check:
            # the writer stamped (or changed) the key check: it must be ours
            self._verify_key_against(m)
        H = self.applied_seq
        segs = [s for s in m.segments if int(s.get("fold_seq", 0)) > H]
        orphan = self._orphan_checkpoint(m, H)
        if segs or orphan is not None:
            versions, carried, max_fold = self._load_from(m, segs, H)
            if orphan is not None:
                recs, fold, ops, before = orphan
                for rec in recs:
                    versions[rec.id] = (rec, min(fold, before[rec.id] - 1) if rec.id in before else fold)
                carried.extend(ops)
                max_fold = max(max_fold, fold)
            events = _merge_events(carried, [], after=H)
            self._fresh_index = False
            self._replay_into_index(versions, events)
            new_h = max(H, max_fold)
            self.index.flush()
            self.index.set_meta("applied_seq", str(new_h))
            self.applied_seq = new_h
            self._pending = [e for e in self._pending if e[0] > new_h]
            self.data_epoch += 1
            if any(isinstance(ev, dict) and ev.get("op") == "hard_delete" for _s, ev in events):
                self._request_scrub(new_h)
            self._applied([rec for rec, _ in versions.values()])
        self.manifest = m
        self._manifest_ver = ver
        self._skip_floor = max(self._skip_floor, self.applied_seq, m.wal_base_seq)
        if not self.store.log_cursors_survive_folds:
            # a fold deletes the logs and new appends recreate them: a byte
            # offset into the old file means nothing (an inode the
            # filesystem reuses would even look like the same file)
            self._cursors = {"wal": None, "ops": None}
            self._pending = []
        if m.scrub_seq > int(self.index.get_meta("scrubbed_seq") or 0):
            self._request_scrub(self.applied_seq)

    def _orphan_checkpoint(self, m: Manifest, applied: int):
        """The checkpoint the manifest names but its segment list lost (see
        NamespaceStore._adopt_orphan_segments) - read in memory, never
        re-committed: (records, fold, carried ops, before) or None."""
        name = m.checkpoint
        if not name or m.checkpoint_seq <= applied or any(s["name"] == name for s in m.segments):
            return None
        data = self.store.get(f"{self.prefix}/{name}")
        if not data:
            return None
        try:
            recs, fold, ops, before = self._segment_parse(data)
        except KeyCustodyError:
            raise
        except Exception:  # noqa: BLE001 - unreadable: nothing to adopt (as the writer)
            return None
        return (recs, fold, ops, before) if fold == m.checkpoint_seq else None

    def _load_from(self, m: Manifest, segs: list[dict], applied: int):
        missing: list[dict] = []
        got = self._load_checkpoints(applied, where="replica-segment", segments=segs,
                                     missing=missing)
        if missing:
            raise _Moved(f"segment {missing[0]['name']} is gone")
        return got

    def _verify_key_against(self, m: Manifest) -> None:
        prev, self.manifest = self.manifest, m
        try:
            self._key_ok = self._key_proven = False
            self._verify_key()
        finally:
            self.manifest = prev
            self._open_blobs = {}

    def _gone(self) -> None:
        """No manifest: never created, or destroyed (crypto-shred). Whatever
        the replica held is dropped - its files deleted - and it serves an
        empty namespace until a manifest appears."""
        if not self._fresh_index:
            with self._serve.write():
                self._discard_index("the namespace is gone")
                self._fresh_index = True
            self._forget_key()
            self.data_epoch += 1
            METRICS.inc("memd_replica_dropped_total",
                        help="replica caches dropped: the namespace was destroyed", ns=self.namespace)
        self.exists = False
        self._synced = False
        self._lineage = ""
        self.applied_seq = 0
        self.manifest = Manifest()
        self._manifest_ver = None
        self._reset_follow(0)

    def _reset_follow(self, horizon: int) -> None:
        self._cursors = {"wal": None, "ops": None}
        self._pending = []
        self._seen = {"wal": horizon, "ops": horizon}
        self._covered = horizon
        self._skip_floor = horizon

    def _rebuild(self, m: Manifest, ver, why: str) -> None:
        """Delete the cache files and build them again from durable data:
        snapshot (if it covers the floor) + segments; the tail follows."""
        bootstrap = not self._synced
        with self._serve.write():
            # under the serve lock: a read that waited for the rebuild sees
            # its result once the refresh running it has applied the tail
            # too, or that it failed (reading()) - never the discarded index,
            # nor the rebuilt one short of the tail
            self.refreshed_at = None     # nothing is served from it until a refresh completes
            self._synced = False
            self.applied_seq = 0
            self.exists = True
            self._reset_follow(0)
            if not self._fresh_index:
                self._discard_index(why)
                self._fresh_index = True
            with self._scrub_mu:
                self._scrub_want = 0     # the files a scrub was asked for are gone
            self.manifest = m
            self._manifest_ver = ver
            if self._key_lineage is not None and self._key_lineage != m.lineage:
                # another tenure: the namespace may have been destroyed and
                # created again under its name, with a new data key
                self._forget_key()
            self._key_ok = self._key_proven = False
            try:
                self._verify_key()          # KeyCustodyError: refused, never served empty
            except BaseException:
                # a key that failed verification (or could not be resolved)
                # is never kept: the next attempt resolves it again from its
                # record - a key restored meanwhile is picked up at once
                self._forget_key()
                raise
            finally:
                self._open_blobs = {}
            self._key_lineage = m.lineage
            applied = 0
            self.installed_snapshot = False
            self._fresh_index = False
            if m.snapshot_name:
                applied = self._install_index_snapshot()
                self.installed_snapshot = applied > 0
            if self.index.ann is None:
                self._attach_vector_index()
            versions, carried, max_fold = self._load_from(m, list(m.segments), applied)
            orphan = self._orphan_checkpoint(m, applied)
            if orphan is not None:
                recs, fold, ops, before = orphan
                for rec in recs:
                    versions[rec.id] = (rec, min(fold, before[rec.id] - 1) if rec.id in before else fold)
                carried.extend(ops)
                max_fold = max(max_fold, fold)
            self._fresh_index = False
            self._replay_into_index(versions, _merge_events(carried, [], after=applied))
            horizon = max(applied, max_fold)
            self.index.flush()
            self.index.set_meta("applied_seq", str(horizon))
            # durable data holds nothing the writer has purged (scrub_seq) -
            # but the purges still pending are in it: a segment's copy of the
            # record and the delete its header carries (or an installed
            # image that reflects it). Building them in and deleting them
            # again leaves their text in the files (the WAL's page images):
            # scrubbed below, as for a delete applied from the tail.
            self.index.set_meta("scrubbed_seq", str(m.scrub_seq))
            purges = [int(op.get("seq", 0)) for op in carried if op.get("op") == "hard_delete"]
            self.scrubbed_through = min(purges) - 1 if purges else horizon
            self._scrub_on_tail = True
            if self._opened and self.index.lexical is None:
                self._attach_lexical(self._lexical_cfg)
            self.applied_seq = horizon
            self._synced = True
            self._lineage = m.lineage
            self.data_epoch += 1
        if purges:
            self._request_scrub(horizon)
        self._reset_follow(horizon)
        self._skip_floor = max(horizon, m.wal_base_seq)
        if not bootstrap:
            self.rebuilds += 1
            METRICS.inc("memd_replica_rebuilds_total",
                        help="replicas rebuilt from durable data (another tenure, a fold above "
                             "their horizon, an ordering anomaly)", ns=self.namespace)
            _log.info("namespace %s: replica rebuilt (%s)", self.namespace, why)
        self._applied([rec for rec, _ in versions.values()])

    # ------------------------------------------------------------- log side

    def _read_log(self, which: str, cursor):
        """New events of one log after `cursor` -> (events, cursor past
        them, reset). WAL events are (seq, records); ops are op dicts. A
        frame cut short at the end (being written) is left for the next
        read; a COMPLETE frame that does not read is refused, never
        skipped - the writer would refuse it too."""
        key = self.wal_key if which == "wal" else self.ops_key
        chunks, end, reset = self.store.log_tail(key, cursor)
        out: list = []
        for c, data in chunks:
            if not data:
                continue
            if data[:1] == b"{":
                raise StoreFormatError(f"namespace {self.namespace!r}: an unframed (format-1) "
                                       f"{which} log; a writer must migrate it first")
            if which == "ops":
                # the writer's own scan: a record it skipped as damaged (an
                # older binary's torn append, kept with acked ops behind it)
                # is skipped here too - unless it is a COMPLETE frame that
                # does not read, which is refused as the writer refuses it
                damaged: list[tuple[int, int]] = []
                ops, good = self._scan_ops(data, damaged)
                for a, b in damaged + [(good, len(data))]:
                    if a < b:
                        self._refuse_unreadable(data, a, "ops", stop=b)
                out.extend(ops)
                consumed = good
            else:
                consumed = 0
                for fend, payload in _frames_with_offsets(data):
                    try:
                        plain = self._decrypt_frame(payload)
                        recs = records_from_jsonl(plain)
                    except KeyCustodyError as ex:
                        raise self._frame_refusal("WAL", at=_frame_start(fend, payload)) from ex
                    except Exception as ex:  # noqa: BLE001 - a complete frame that does not parse
                        raise self._frame_refusal("WAL", "damaged",
                                                  at=_frame_start(fend, payload)) from ex
                    out.append((_frame_seq(plain), recs))
                    consumed = fend
            if consumed < len(data):
                end = self.store.log_cursor_back(c, len(data) - consumed)
                if end == c:
                    # an object store part is one whole append: one that ends
                    # mid-frame (or holds an op that does not parse) is damage
                    raise self._frame_refusal("WAL" if which == "wal" else "ops", "damaged")
                break
        return out, end, reset

    def _read_tail(self) -> _Tail:
        """The WAL tail, the ops tail, and the WAL tail again only when an op
        ran past every frame - with the horizon they prove.

        The writer assigns a seq and completes its PUT (or write) under the
        namespace lock, so every event below seq s was durable before s was
        even assigned. Mw (the newest frame the first WAL read saw): every op
        below it was durable before that read finished, so the ops read that
        follows sees it. Mo (the newest op): every frame below it was durable
        before the ops read finished, so a WAL read after it sees it. Hence
        horizon = max(Mw, Mo once a WAL read followed it)."""
        cur = dict(self._cursors)
        seen = dict(self._seen)
        covered = self._covered
        reset = {"wal": False, "ops": False}
        frames, cur["wal"], r = self._read_log("wal", cur["wal"])
        reset["wal"] |= r
        seen["wal"] = max([seen["wal"]] + [s for s, _ in frames])
        mw = seen["wal"]
        ops, cur["ops"], r = self._read_log("ops", cur["ops"])
        reset["ops"] |= r
        seen["ops"] = max([seen["ops"]] + [int(o.get("seq", 0)) for o in ops])
        if seen["ops"] > max(mw, covered):
            more, cur["wal"], r = self._read_log("wal", cur["wal"])
            reset["wal"] |= r
            frames += more
            seen["wal"] = max([seen["wal"]] + [s for s, _ in more])
            covered = seen["ops"]
        horizon = max(self.applied_seq, mw, covered)
        return _Tail(frames, ops, cur, seen, covered, horizon, reset)

    def _apply_tail(self, t: _Tail) -> None:
        H = self.applied_seq
        skip = max(self._skip_floor, self.manifest.wal_base_seq)
        pending = list(self._pending)
        if t.reset["wal"]:
            pending = [e for e in pending if e[1] != 0]   # re-read from the start below
        if t.reset["ops"]:
            pending = [e for e in pending if e[1] != 1]
        anomaly = None
        for seq, recs in t.frames:
            if seq <= skip:
                continue
            if seq <= H:
                if not t.reset["wal"]:
                    anomaly = anomaly or seq
                continue
            pending.append((seq, 0, recs))
        for op in t.ops:
            seq = int(op.get("seq", 0))
            if seq <= skip:
                continue
            if seq <= H:
                if not t.reset["ops"]:
                    anomaly = anomaly or seq
                continue
            pending.append((seq, 1, op))
        if anomaly is not None:
            # an event first read with a seq at or below the horizon: the
            # write order the horizon relies on did not hold (an append the
            # writer re-numbered after its response was lost). Rebuild from
            # durable data - which orders it as the writer's own replay does
            METRICS.inc("memd_replica_order_anomalies_total",
                        help="tail events first read below a replica's horizon (rebuilt)",
                        ns=self.namespace)
            self._rebuild(self.manifest, self._manifest_ver,
                          f"an event (seq {anomaly}) first read below the horizon ({H})")
            raise _Rebuilt()
        reflected = []
        if self._scrub_on_tail:
            # the first tail after a rebuild reads the ops log from its start:
            # a hard delete at or below the rebuild's horizon is one its
            # durable data reflected (an installed image's file may hold the
            # text) - scrubbed like one applied now
            self._scrub_on_tail = False
            reflected = [int(op.get("seq", 0)) for op in t.ops
                         if op.get("op") == "hard_delete" and int(op.get("seq", 0)) <= H]
            if reflected:
                self.scrubbed_through = min(self.scrubbed_through, min(reflected) - 1)
        ready = [e for e in pending if e[0] <= t.horizon]
        rest = [e for e in pending if e[0] > t.horizon]
        new_h = max(H, t.horizon)
        if ready:
            frames = [(s, ev) for s, k, ev in ready if k == 0]
            ops = [ev for s, k, ev in ready if k == 1]
            self._fresh_index = False
            self._replay_into_index({}, _merge_events(ops, frames, after=H))
            self.data_epoch += 1
        if new_h != H or ready:
            self.index.flush()
            self.index.set_meta("applied_seq", str(new_h))
        self.applied_seq = new_h
        self._pending = rest
        self._cursors = t.cursors
        self._seen = t.seen
        self._covered = t.covered
        if reflected or any(k == 1 and ev.get("op") == "hard_delete" for _s, k, ev in ready):
            self._request_scrub(new_h)
        if ready:
            self._applied([r for _s, k, ev in ready if k == 0 for r in ev])
        METRICS.set_gauge("memd_replica_applied_seq", float(new_h),
                          help="the seq a replica has applied every event up to", ns=self.namespace)

    def _applied(self, records: list[MemoryRecord]) -> None:
        cb = self._on_applied
        if cb is not None and records and self._opened:
            try:
                cb(self.namespace, self, records)
            except Exception:  # noqa: BLE001 - a hook never fails a refresh
                METRICS.inc("memd_replica_hook_failures_total", ns=self.namespace)

    # -------------------------------------------------------- hard deletes

    def _request_scrub(self, through: int) -> None:
        """Scrub this replica's cache files of what it deleted: applied
        hard deletes removed the rows, but the SQLite file, its WAL, the FTS
        segments, the tantivy copy and the ANN sidecar may still hold their
        bytes. In the background, coalesced: one scrub runs, and one more
        after it if more was applied meanwhile."""
        with self._scrub_mu:
            self._scrub_want = max(self._scrub_want, int(through))
            if self._scrub_running or self._closed:
                return
            self._scrub_running = True
        t = threading.Thread(target=self._scrub_loop, name=f"memd-replica-scrub-{self.namespace}",
                             daemon=True)
        self._scrub_thread = t
        t.start()

    SCRUB_DRAIN_S = 60.0

    def _scrub_loop(self) -> None:
        while True:
            with self._scrub_mu:
                target = self._scrub_want
            idx = self.index
            ok = False
            try:
                ok = self._scrub_once(idx, target)
            except Exception as ex:  # noqa: BLE001 - the next applied delete scrubs again
                if not self._closed:
                    _log.warning("namespace %s: the replica's purge scrub failed (%s)",
                                 self.namespace, ex)
            with self._scrub_mu:
                # a rebuild that replaced the index meanwhile (deleting its
                # files) may have asked for the new one to be scrubbed
                again = self._scrub_want > 0 and (
                    self.index is not idx or (ok and self._scrub_want > target))
                if self._closed or not again:
                    self._scrub_running = False
                    return

    def _scrub_once(self, idx, target: int) -> bool:
        """One scrub of `idx`, the current index (not under the serve lock: a
        rebuild that swaps the index closes this one, which ends the scrub -
        and deletes the files, which is the stronger purge)."""
        if self._closed or idx.closing:
            return False
        self._scrub_begin()
        try:
            if not idx.scrub():
                return False
            ann = idx.ann
            ticket = ann.purged(target) if ann is not None else 0
        finally:
            self._scrub_end()
        lex = idx.lexical
        if lex is not None:
            lex.drain(timeout_s=self.SCRUB_DRAIN_S)
        if ticket and idx.ann is not None:
            idx.ann.wait_built(ticket, timeout_s=self.SCRUB_DRAIN_S)
        with idx._lock:
            if idx._closed or self.index is not idx:
                return False
            if int(idx.get_meta("scrubbed_seq") or 0) < target:
                idx.set_meta("scrubbed_seq", str(target))
            self.scrubbed_through = max(self.scrubbed_through, target)
        METRICS.inc("memd_replica_scrubs_total",
                    help="replica caches scrubbed of hard-deleted text", ns=self.namespace)
        return True

    # ----------------------------------------------------------- reads

    def export_records(self):
        """Folded from durable data, like the writer's - after a refresh, and
        again from the new manifest if a fold deleted a segment under it."""
        with self._refresh_lock:
            for _attempt in range(self.REFRESH_ATTEMPTS):
                self._sync()
                missing: list[dict] = []
                prev = self._load_checkpoints

                def load(*a, **kw):
                    kw.setdefault("missing", missing)
                    return prev(*a, **kw)

                self._load_checkpoints = load
                try:
                    with self._lock:
                        recs = self._visible_records()
                        skipped = list(self.last_export_skipped)
                finally:
                    del self._load_checkpoints
                again = self.store.get_versioned(self.manifest_key)
                if not missing and (again is None) == (self._manifest_ver is None) and (
                        again is None or again[1] == self._manifest_ver):
                    recs.sort(key=lambda r: (r.time.t_ingested, r.id))
                    return recs, skipped
            raise ReplicaUnavailableError(f"namespace {self.namespace!r}: the manifest kept "
                                          "changing during the export")

    def stats(self) -> dict:
        with self.holding():   # (a failed rebuild's state is reported, not refused)
            st = super().stats()
        st["replica"] = self.status()
        return st

    def has_due_deletes(self, now_ms_: int | None = None) -> bool:
        return False   # the writer purges; a replica follows

    # ------------------------------------------------------------ close

    def close(self) -> None:
        """Close and DELETE the cache files: a replica's cache lives exactly
        as long as the replica (nothing it held outlives it)."""
        if getattr(self, "_dropped", False):
            return
        got = self._refresh_lock.acquire(timeout=30.0)
        try:
            self._closed = True
            self._evicted = True
            with self._fresh_cv:
                self._fresh_cv.notify_all()   # reads waiting on a refresh give up
            with self._scrub_mu:
                self._scrub_want = 0
            with self._serve.write():
                self._close_index()
            cache_dir = os.path.dirname(self.index.path)
            try:
                _drop_cache_files(cache_dir, self.namespace)
                # the directory is this engine's alone: its lock file goes too
                with contextlib.suppress(FileNotFoundError):
                    os.unlink(_cache_lock_path(cache_dir, self.namespace))
                self._dropped = True
            except Exception as ex:  # noqa: BLE001 - the engine's directory sweep retries
                _log.warning("namespace %s: could not delete the replica cache (%s)",
                             self.namespace, ex)
        finally:
            if got:
                self._refresh_lock.release()

    def _release_ownership(self, *, clean: bool = True, discard: bool = False) -> None:
        return   # nothing held
