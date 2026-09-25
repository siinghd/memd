"""Durable storage engine (ADR-2): object storage is the source of truth.

Per-namespace layout under the object store:

    {ns}/wal.jsonl      active append log (record batches; fsync'd before ack)
    {ns}/ops.jsonl      mutation ops: tombstone / supersede / quarantine / hard_delete
    {ns}/seg-{ulid}     closed immutable segments (folded state at fold_seq)
    {ns}/manifest.json  {format, gen, seq, segments[], checkpoint, wal_size, ops_size}

Invariants:
  - Segments are immutable; corrections arrive as ops; compaction folds.
  - Write ack = durable append (fsync) - no LLM, no embedding, no index wait.
  - The index (memd.index.NamespaceIndex) is a derived, rebuildable view kept
    on local disk (NVMe-class cache in hosted mode).
  - Hard delete: synchronous tombstone + physical purge guaranteed by forced
    compaction within the deadline (72h default, D7 control #8).
  - Namespace deletion = prefix removal + key destruction (crypto-shred).
  - History has ONE order: manifest.seq numbers every durable event (a WAL
    frame, an op) and open / rebuild / rotate / compact all fold events in
    seq order. A segment stands in for every event at or below its fold_seq
    for the ids it holds; nothing else may be skipped or reordered.
  - One store format per namespace (STORE_FORMAT). An older layout is
    migrated once, on first open, and older binaries refuse the new one.
"""
from __future__ import annotations

import itertools
import json
import logging
import os
import shutil
import socket
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field

from memd.core.schema import MemoryRecord, now_ms, records_from_jsonl, records_to_jsonl, ulid_new
from memd.metrics import METRICS
from memd.index.sqlite_index import NamespaceIndex
from memd.storage.crypto import KeyEnvelope, NullKeyEnvelope
from memd.storage.objectstore import LocalObjectStore, ObjectStore

_log = logging.getLogger(__name__)

HARD_DELETE_DEADLINE_MS = 72 * 3600 * 1000
DEFAULT_WAL_ROTATE_BYTES = 8 * 1024 * 1024
# Object stores make every WAL frame an object, so replay cost is measured
# in round trips, not bytes. 512 frames bounds a cold open's fan-out.
DEFAULT_WAL_ROTATE_FRAMES = 512


@dataclass
class CompactionReport:
    segments_in: int = 0
    segments_out: int = 0
    records_folded: int = 0
    records_purged: int = 0
    hard_deleted_purged: int = 0
    bytes_before: int = 0
    bytes_after: int = 0
    duration_ms: int = 0


# ---------------------------------------------------------------------------
# Store format.
#
# Format 1 (no "format" key) is every layout before this one: WAL frames
# without a seq, a seq counter persisted lazily and rebuilt from the ops log
# after a crash, rotates that dropped ops. Format 2 stamps every frame,
# carries ops in segment headers and records its newest checkpoint. Neither
# direction reads the other safely:
#   - a format-1 binary ignores the ops a format-2 segment header carries
#     (tombstones, hard deletes), serves the records they deleted, and its
#     next compaction makes that durable;
#   - format-1 data read by seq alone resurrects deletes: after a crash the
#     old counter REUSED numbers, so frames and ops do not interleave by seq.
# A format-1 namespace is therefore migrated once, on first open
# (_migrate_legacy), and a format-2 manifest fences older binaries out: they
# parse "version" with int() before anything else, so they stop there with
# this text instead of misreading the store. Downgrades are not supported.
STORE_FORMAT = 2
_FORMAT_FENCE = (
    "memd store format 2: this namespace was upgraded by a newer memd and this "
    "version cannot read it safely (downgrades are not supported - upgrade memd)")


class StoreFormatError(RuntimeError):
    """The namespace uses a store format this build cannot read."""


@dataclass
class Manifest:
    version: int = 0  # generation, bumped on every persist ("gen" on disk)
    seq: int = 0  # monotonic op/batch counter
    segments: list[dict] = field(default_factory=list)  # {name, records, fold_seq}
    wal_size: int = 0
    ops_size: int = 0
    wal_base_seq: int = 0  # seq counter value when the current wal opened
    # Derived-index snapshot: the seq it reflects, and the object holding it.
    # Cold start on a node without the local cache was an O(live records)
    # replay - 593ms at 10K, 2380ms at 40K, crossing the 1.5s cold-first-query
    # SLO at ~25K records - because nothing durable held the FOLDED form.
    snapshot_seq: int = 0
    snapshot_name: str = ""
    format: int = STORE_FORMAT
    # The newest fold (rotate, compaction, migration): the segment it wrote -
    # "" when its output was empty - and its fold_seq. The only evidence on
    # which open re-adopts a segment the list lost (_adopt_orphan_segments).
    checkpoint: str = ""
    checkpoint_seq: int = 0

    def to_dict(self) -> dict:
        return {
            # older binaries int() this first: the fence stops them loudly
            "version": _FORMAT_FENCE if self.format >= 2 else self.version,
            "format": self.format,
            "gen": self.version,
            "seq": self.seq,
            "segments": self.segments,
            "wal_size": self.wal_size,
            "ops_size": self.ops_size,
            "wal_base_seq": self.wal_base_seq,
            "snapshot_seq": self.snapshot_seq,
            "snapshot_name": self.snapshot_name,
            "checkpoint": self.checkpoint,
            "checkpoint_seq": self.checkpoint_seq,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Manifest":
        fmt = int(d.get("format", 1) or 1)
        if fmt > STORE_FORMAT:
            raise StoreFormatError(
                f"namespace store format {fmt} is newer than this memd reads "
                f"({STORE_FORMAT}): upgrade memd")
        return cls(
            version=int(d.get("gen", 0) if fmt >= 2 else d.get("version", 0)),
            seq=int(d.get("seq", 0)),
            segments=list(d.get("segments") or []),
            wal_size=int(d.get("wal_size", 0)),
            ops_size=int(d.get("ops_size", 0)),
            snapshot_seq=int(d.get("snapshot_seq", 0) or 0),
            snapshot_name=str(d.get("snapshot_name", "") or ""),
            wal_base_seq=int(d.get("wal_base_seq", 0)),
            format=fmt,
            checkpoint=str(d.get("checkpoint", "") or ""),
            checkpoint_seq=int(d.get("checkpoint_seq", 0) or 0),
        )


def _frame_encode(payload: bytes) -> bytes:
    return len(payload).to_bytes(4, "big") + payload


def _frame_iter(data: bytes):
    i, n = 0, len(data)
    while i + 4 <= n:
        ln = int.from_bytes(data[i : i + 4], "big")
        if i + 4 + ln > n:
            break  # torn tail write; ignore
        yield data[i + 4 : i + 4 + ln]
        i += 4 + ln


def _frames_with_offsets(data: bytes):
    """Like _frame_iter but yields (end_offset, payload) so callers can
    truncate a log back to the last frame that parsed cleanly."""
    i, n = 0, len(data)
    while i + 4 <= n:
        ln = int.from_bytes(data[i : i + 4], "big")
        if i + 4 + ln > n:
            break
        end = i + 4 + ln
        yield end, data[i + 4 : end]
        i = end


# ---------------------------------------------------------------------------
# One order for history.
#
# Every durable event takes the next manifest.seq: a WAL frame (one append
# batch) or an op (one per op). Recovery used to apply the whole ops log
# FIRST and the WAL records after it, so a record deleted - softly or hard -
# while still in the WAL was re-created by its own frame on the next open
# after a crash: an acked delete undone, and hard-deleted content served
# again. Frames also carried no seq; replay inferred one from position
# (wal_base_seq + index), which ignores the seqs ops consume, so a frame
# written after [frame, op, frame] + restart sat "below" the watermark and was
# never replayed. And a rotate - a partial fold that cannot see older
# segments - dropped every op whose target lived in one, so a cache wipe,
# a rebuild or a second node resurrected those records as well.
#
# Now: frames are stamped with their seq, ops keep theirs, and every consumer
# folds one merged, seq-ordered stream. A segment is a checkpoint: its copy
# of an id reflects every event at or below its fold_seq, so only events
# ABOVE that are applied to the id. Ops a rotate cannot retire travel in the
# new segment's header at their ORIGINAL seq, so their place in history
# never moves; only compaction, which rewrites every copy, retires them.

# The seq rides as an extra key on the frame's first record line, not as a
# header line: MemoryRecord.from_dict ignores unknown keys, so an older binary
# still reads a stamped WAL instead of taking the frame for a torn tail and
# truncating the log behind it (the format fence refuses such a downgrade
# outright now - see STORE_FORMAT).
_WAL_SEQ = b'{"_wal_seq":'


def _wal_stamp(payload: bytes, seq: int) -> bytes:
    """Stamp a records_to_jsonl() payload with the frame's seq."""
    if not payload.startswith(b"{"):
        return payload
    return _WAL_SEQ + str(int(seq)).encode() + b"," + payload[1:]


def _wal_seq(payload: bytes) -> int | None:
    """The seq a frame was stamped with; None for an unstamped (pre-0.2) frame."""
    if not payload.startswith(_WAL_SEQ):
        return None
    try:
        return int(payload[len(_WAL_SEQ):payload.find(b",", len(_WAL_SEQ))])
    except ValueError:
        return None


def _frame_seq(payload: bytes) -> int:
    """A format-2 frame's seq. Format 2 stamps every frame, so an unstamped
    one is format-1 residue its migration already folded: 0, below all."""
    s = _wal_seq(payload)
    return 0 if s is None else s


# Format-1 history (read once, by _migrate_legacy).
#
# An unstamped frame's seq has to be inferred. Filling the gaps the ops leave
# above wal_base_seq is exact only if no number was used twice - and the old
# counter did reuse them: it was persisted lazily and rebuilt from
# max(manifest.seq, ops) after a crash, so ops written after a crash took the
# numbers of frames written before it. Gap-filling [A, B | crash | del B, D,
# del D] (ops 1 and 3) put the frames at 2, 4 and 5, after their own
# tombstones, and resurrected B and D - which the old binary itself hid,
# because its live index had applied every event in real time.
#
# If nothing was reused, every event holds its own seq in (wal_base_seq,
# top], so events = top - wal_base_seq at most. More events than that, or two
# events on one seq, prove reuse: the frame/op interleaving is then unknown
# and the conservative reading is taken - every frame first (in log order),
# then every op (in seq order), so a delete, supersede or quarantine is never
# undone by a write that may have preceded it. When the counts do reconcile
# (a manifest.seq that an earlier open inflated can make them reconcile
# anyway), gap-filling is used, but an unstamped copy still may not undo an
# op on its id that it lands after unless its record was created after that
# op: a record ingested before its delete cannot be the re-add that follows it.


def _legacy_events(frames: list[tuple[int | None, list[MemoryRecord]]], ops: list[dict],
                   base: int, floor: int) -> tuple[list[tuple[int, object]], str, int]:
    """Order a format-1 WAL (frames as (stamp or None, records), in log order)
    and its ops log into one stream for _fold_events -> (events, how, top).

    how is "exact", "guarded" (some copies were held before an op) or
    "conservative" (reuse proven); top is the highest seq the stream holds,
    at least `floor`. Seqs in the result only order it: fold it over an
    empty base."""
    op_seqs = [int(o.get("seq", 0)) for o in ops]
    stamps = [s for s, _ in frames if s is not None]
    used = op_seqs + stamps
    top = max([floor, base] + used)
    fits = (len(set(used)) == len(used) and all(s > base for s in used)
            and len(frames) + len(op_seqs) <= top - base)
    seqs: list[int] = []
    if fits:
        taken = set(used)
        gaps = (s for s in itertools.count(base + 1) if s not in taken)
        for s, _ in frames:
            s = next(gaps) if s is None else s
            if seqs and s <= seqs[-1]:
                fits = False  # a frame can never precede one written before it
                break
            seqs.append(s)
    if not fits:
        ordered = sorted(ops, key=lambda o: int(o.get("seq", 0)))
        return ([(0, recs) for _, recs in frames]
                + [(int(o.get("seq", 0)), o) for o in ordered]), "conservative", top
    stream = sorted([(s, 0, i, frames[i][1]) for i, s in enumerate(seqs)]
                    + [(s, 1, j, o) for j, (s, o) in enumerate(zip(op_seqs, ops))],
                    key=lambda e: (e[0], e[1]))
    events: list[tuple[int, object]] = []
    seen: dict[str, list[tuple[int, dict]]] = {}  # id -> (event index, op) so far
    held: dict[int, list[MemoryRecord]] = {}      # event index -> copies moved before it
    for s, kind, i, ev in stream:
        if kind:
            seen.setdefault(_op_target(ev), []).append((len(events), ev))
            events.append((s, ev))
            continue
        if frames[i][0] is not None:  # stamped: its seq is a fact
            events.append((s, ev))
            continue
        keep = []
        for r in ev:
            at = next((pos for pos, op in seen.get(r.id, ())
                       if _op_at(op) is None or _op_at(op) >= r.time.t_ingested), None)
            if at is None:
                keep.append(r)
            else:
                held.setdefault(at, []).append(r)
        events.append((s, keep))
    if not held:
        return events, "exact", top
    out: list[tuple[int, object]] = []
    for pos, (s, ev) in enumerate(events):
        if pos in held:
            out.append((s, held[pos]))
        out.append((s, ev))
    return out, "guarded", top


def _op_target(op: dict) -> str | None:
    return op.get("id") or op.get("old")


def _op_at(op: dict) -> int | None:
    try:
        return int(op["at"])
    except (KeyError, TypeError, ValueError):
        return None


def _merge_events(
    ops: list[dict], frames: list[tuple[int, list[MemoryRecord]]], after: int = 0
) -> list[tuple[int, object]]:
    """ops (dicts) and WAL frames (record lists) past `after`, as one
    seq-ordered stream. An op can be on disk twice - carried in a segment
    header and left in an ops log a crash did not delete - and counts once."""
    seen: set[int] = set()
    out: list[tuple[int, int, object]] = []
    for op in ops:
        s = int(op.get("seq", 0))
        if s <= after or s in seen:
            continue
        seen.add(s)
        out.append((s, 1, op))
    out.extend((s, 0, recs) for s, recs in frames if s > after)
    out.sort(key=lambda e: (e[0], e[1]))
    return [(s, ev) for s, _, ev in out]


def _fold_events(
    base: dict[str, tuple[MemoryRecord, int]], events: list[tuple[int, object]], now: int, force: bool
) -> tuple[list[MemoryRecord], list[dict], list[str], list[dict], dict[str, int]]:
    """Fold seq-ordered events over segment checkpoints into record state
    (NamespaceStore._replay_into_index applies the same order to the index).

    base maps id -> (record, seq its copy reflects - normally the fold_seq of
    the newest segment holding it).

    Returns (live_records, pending_ops, unquarantined_ids, folded_ops,
    deferred). Pending ops are not-yet-due hard deletes; they must survive
    folds so the forced-compaction deadline (D7) stays enforceable. Their
    target's bytes may stay until then: deferred maps such a kept id to the
    hard delete's seq, and the segment must record that its copy PRECEDES it
    (see _segment_blob) or replay would serve it again. unquarantined_ids
    reports records whose quarantine expired during this fold - callers must
    clear the index flag so decay actually restores visibility."""
    state = {rid: rec for rid, (rec, _) in base.items()}
    held = {rid: fold for rid, (_, fold) in base.items()}
    pending: list[dict] = []
    folded: list[dict] = []
    deferred: dict[str, int] = {}
    unquarantined: list[str] = []
    for seq, ev in events:
        if not isinstance(ev, dict):
            for r in ev:
                if held.get(r.id, -1) < seq:
                    state[r.id] = r
                    deferred.pop(r.id, None)  # a re-add is not under the delete
            continue
        op = ev
        folded.append(op)
        kind = op.get("op")
        rid = _op_target(op)
        due = force or op.get("deadline", 0) <= now
        if kind == "hard_delete" and not due:
            pending.append(op)
        if held.get(rid, -1) >= seq:
            continue  # the checkpoint's copy of this id already reflects it
        target = state.get(rid)
        if kind == "hard_delete":
            # at its place in history, so a LATER re-add survives
            if due:
                state.pop(rid, None)
                deferred.pop(rid, None)
            elif target is not None:
                deferred[rid] = seq
        elif kind == "tombstone" and target is not None:
            target.deleted = True
            if target.time.invalidated_at is None:
                target.time.invalidated_at = op.get("at", now)
        elif kind == "supersede" and target is not None and op.get("new") != op.get("old"):
            # self-supersede would brick the record into invisibility
            target.time.invalidated_at = op.get("at", now)
            target.time.superseded_by = op.get("new")
        elif kind == "quarantine" and target is not None:
            if op.get("flag", True):
                target.meta["quarantined"] = True
                if op.get("expires"):
                    target.meta["quarantine_expires"] = op["expires"]
            else:
                target.meta.pop("quarantined", None)
                target.meta.pop("quarantine_expires", None)
    kept: list[MemoryRecord] = []
    for r in state.values():
        if r.deleted:
            continue
        qexp = r.meta.get("quarantine_expires")
        if qexp is not None and qexp <= now:
            r.meta.pop("quarantined", None)
            r.meta.pop("quarantine_expires", None)
            r.meta["quarantine_expired"] = True
            unquarantined.append(r.id)
        kept.append(r)
    kept_ids = {r.id for r in kept}
    return kept, pending, unquarantined, folded, {k: v for k, v in deferred.items() if k in kept_ids}


def _carry_forward(folded: list[dict], kept: list[MemoryRecord]) -> list[dict]:
    """Ops a ROTATE must keep: it folds only the current WAL, so an op whose
    target is not in its output may still apply to a copy in an older
    segment. Hard deletes always stay - the older bytes must be purged by the
    deadline, and only compaction rewrites them."""
    kept_ids = {r.id for r in kept}
    return [op for op in folded
            if op.get("op") == "hard_delete"
            or (op.get("op") in ("tombstone", "supersede", "quarantine")
                and _op_target(op) not in kept_ids)]


def _held(rid: str, fold: int, before: dict[str, int]) -> int:
    """The seq a segment's copy of rid reflects: its fold, or - for a copy a
    not-yet-due hard delete kept - just before that delete."""
    return min(fold, before[rid] - 1) if rid in before else fold


# ---------------------------------------------------------------------------
# Single-writer enforcement.
#
# StorageEngine assumed it was the only process on a data root: no lock, no
# lease, no leader. Every process kept its OWN in-RAM Manifest and blindly
# put() it, and LocalLogWriter held a long-lived fd on a wal that another
# process's rotate/compact unlinks. Two processes on one root (uvicorn
# --workers 2, two container replicas on one volume - the deployment shape
# 03-architecture.md advertises) silently DESTROY acked data: a writer's
# appends go to an unlinked inode while its manifest put() erases the other
# process's segment reference, leaving an unreachable orphan that
# _adopt_orphan_segments refuses because its fold_seq reads 0.
#
# Until a real multi-writer protocol exists (CAS on the manifest + a lease),
# the honest behaviour is to refuse the second opener. Locks are refcounted
# per PROCESS: opening the same namespace twice in one process is safe - the
# in-process lock hierarchy already covers it - and only a second OS process
# is rejected. Set MEMD_ALLOW_MULTI_PROCESS=1 to opt out (and accept the
# data-loss risk documented above).

_OWNER_FDS: dict[str, list] = {}   # path -> [fd, refcount]
_OWNER_LOCK = threading.Lock()


class NamespaceBusyError(RuntimeError):
    """Another OS process already holds this namespace."""


def _acquire_owner(lock_path: str) -> bool:
    """Exclusive advisory lock, refcounted within this process."""
    try:
        import fcntl
    except ImportError:  # pragma: no cover - non-POSIX
        return False
    with _OWNER_LOCK:
        held = _OWNER_FDS.get(lock_path)
        if held is not None:
            held[1] += 1
            return True
        os.makedirs(os.path.dirname(lock_path), exist_ok=True)
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            raise NamespaceBusyError(
                f"namespace is already open in another process (lock: {lock_path}). "
                "memd is single-writer per data root: a second writer silently "
                "destroys acked data. Run one process per root, or set "
                "MEMD_ALLOW_MULTI_PROCESS=1 to override."
            ) from None
        os.write(fd, str(os.getpid()).encode())
        _OWNER_FDS[lock_path] = [fd, 1]
        return True


def _release_owner(lock_path: str) -> None:
    try:
        import fcntl
    except ImportError:  # pragma: no cover
        return
    with _OWNER_LOCK:
        held = _OWNER_FDS.get(lock_path)
        if held is None:
            return
        held[1] -= 1
        if held[1] > 0:
            return
        _OWNER_FDS.pop(lock_path, None)
        try:
            fcntl.flock(held[0], fcntl.LOCK_UN)
        finally:
            os.close(held[0])


class NamespaceStore:
    """One namespace: WAL + segments + ops + derived index.

    SINGLE WRITER per data root - see _acquire_owner above.
    """

    def __init__(
        self,
        namespace: str,
        store: ObjectStore,
        cache_dir: str,
        envelope: KeyEnvelope | None = None,
        wal_rotate_bytes: int = DEFAULT_WAL_ROTATE_BYTES,
        wal_rotate_frames: int = DEFAULT_WAL_ROTATE_FRAMES,
        lexical: dict | None = None,
    ):
        self.namespace = namespace
        self.store = store
        self.envelope = envelope or NullKeyEnvelope()
        self.wal_rotate_bytes = wal_rotate_bytes
        # On a store with no persistent log handle - i.e. an object store -
        # every WAL frame is its own OBJECT, so the meaningful bound on replay
        # is the frame COUNT, not the byte size. Rotating on bytes alone let a
        # 2000-record namespace accumulate 2000 objects, and cold open then
        # issued 2002 GetObject calls and took 8.4s against a 1.5s SLO. Bytes
        # were never the cost on that backend; round trips were.
        self.wal_rotate_frames = int(wal_rotate_frames)
        self._wal_frames = 0
        self.prefix = f"ns/{namespace}"
        self.wal_key = f"{self.prefix}/wal"
        self.ops_key = f"{self.prefix}/ops"
        self.manifest_key = f"{self.prefix}/manifest.json"
        # Claim the namespace before touching any of its state.
        #
        # Backend-aware on purpose: `flock` cannot see another MACHINE, so on a
        # remote store it protects nothing. A store that knows how to lease
        # ownership (see S3ObjectStore.try_acquire_owner) gets asked; only the
        # local filesystem falls back to flock. Without this the pass-17
        # single-writer protection would silently evaporate the moment anyone
        # pointed memd at S3 - which is exactly the deployment where two
        # writers are most likely.
        self._owner_path = None
        self._owner_lease = None
        if not os.environ.get("MEMD_ALLOW_MULTI_PROCESS"):
            leaser = getattr(store, "try_acquire_owner", None)
            if callable(leaser):
                holder = f"{socket.gethostname()}:{os.getpid()}"
                if not leaser(namespace, holder):
                    raise NamespaceBusyError(
                        f"namespace {namespace!r} is leased by another writer. "
                        "memd is single-writer per data root: a second writer "
                        "silently destroys acked data. Set "
                        "MEMD_ALLOW_MULTI_PROCESS=1 to override.")
                self._owner_lease = namespace
            else:
                root = getattr(store, "root", None)
                if root:
                    lock_path = os.path.join(
                        root, "ns", namespace.replace("/", "__"), ".owner")
                    if _acquire_owner(lock_path):
                        self._owner_path = lock_path
        os.makedirs(cache_dir, exist_ok=True)
        safe = namespace.replace("/", "__")
        self.index = NamespaceIndex(os.path.join(cache_dir, f"{safe}.sqlite"))
        self.index._ns_hint = namespace
        self._lock = threading.RLock()
        self._wlock = threading.Lock()      # serializes WAL writes
        self._sync_lock = threading.Lock()  # one fsync in flight at a time
        self._sync_busy = False
        self._synced_pos = 0
        self._wal_writer = None
        self._log_gen = 0  # bumped on rotate: old-log offsets become void
        self.manifest = Manifest()
        self._manifest_dirty = False
        self._evicted = False
        self._closed = False
        # scheduled physical purges: [(record_id, deadline_ms)] + id set - the
        # D7 hard-delete deadline is only a guarantee if something can SEE when
        # it comes due without rescanning the whole ops log. The set keeps
        # tracking idempotent (a rotate replaying an op must not double-count).
        self._pending_hard: list[tuple[str, int]] = []
        self._pending_hard_ids: set[str] = set()
        self._rotating = False  # re-entrancy guard for rotate-inside-append_ops
        self._replayed_at_open = False
        self._open()
        self._attach_lexical(lexical)

    def _attach_lexical(self, lexical: dict | None) -> None:
        """Optional tantivy accelerator for the bm25 lane (derived, local,
        rebuildable - see memd.index.tantivy_lexical). Attached AFTER replay:
        rows replayed without it may have changed under an old index, so any
        replay at open rebuilds it. It must never stop a namespace opening."""
        path = os.path.splitext(self.index.path)[0] + ".tantivy"
        if not lexical or lexical.get("backend") != "tantivy":
            # opened without it: rows changed now would be missing from an
            # index kept around, and an operator who turned it off should not
            # keep a second copy of the text on disk
            if os.path.isdir(path):
                shutil.rmtree(path, ignore_errors=True)
            return
        try:
            from memd.index.tantivy_lexical import TantivyLexical

            self.index.attach_lexical(TantivyLexical(
                self.index, path,
                commit_ms=int(lexical.get("commit_ms", 500)),
                commit_docs=int(lexical.get("commit_docs", 512)),
                force_rebuild=self._replayed_at_open,
            ))
        except Exception as ex:  # noqa: BLE001 - FTS5 serves the lane
            METRICS.inc("memd_lexical_attach_failures_total",
                        help="namespaces opened without the tantivy accelerator (FTS5 serves)",
                        ns=self.namespace, detail=type(ex).__name__)

    # ---------------------------------------------------------- index snapshot

    SNAPSHOT_MIN_RECORDS = 2_000   # below this, full replay is already fast

    def _snapshot_key(self, name: str) -> str:
        return f"{self.prefix}/{name}"

    def write_index_snapshot(self) -> bool:
        """Persist the FOLDED derived index to the object store.

        Object storage holds the source of truth, but only in raw form: every
        cold start on a node without the local cache had to re-fold all of it,
        O(live records), with no query servable until it finished (593ms at
        10K, 2380ms at 40K, ~12s at 200K, against a 1.5s cold-first-query
        SLO). Nothing durable held the folded form, so every node paid to
        rebuild the same thing.

        The snapshot is a gzipped SQLite image taken with sqlite3's online
        backup API (consistent without blocking writers), envelope-encrypted
        like every other object, and stamped with the seq it reflects.
        It is a CACHE: losing, corrupting or ignoring it costs time, never
        correctness - `_open` falls back to full replay.
        """
        import gzip
        import sqlite3 as _sq
        import tempfile as _tf

        try:
            if self._closed:
                return False
            if self.index.stats().get("records", 0) < self.SNAPSHOT_MIN_RECORDS:
                return False
            self.index.flush()
            fd, tmp = _tf.mkstemp(prefix="memd-snap-", suffix=".sqlite")
            os.close(fd)
            try:
                dst = _sq.connect(tmp)
                try:
                    # sqlite's backup API cannot make progress while the
                    # SOURCE connection holds an open write transaction - it
                    # retries forever - and this index commits LAZILY, so a
                    # transaction is usually open. Commit inside the lock,
                    # immediately before the copy, so nothing can reopen one in
                    # between. (Flushing outside the lock was not enough: the
                    # embed worker reopened a transaction in the gap and the
                    # backup wedged the whole namespace - maintenance thread
                    # holding index._lock, every writer queued behind it.)
                    with self.index._lock:
                        if self.index._closed:
                            return False
                        if self.index._con.in_transaction:
                            self.index._con.commit()
                        self.index._con.backup(dst)
                finally:
                    dst.close()
                with open(tmp, "rb") as f:
                    blob = gzip.compress(f.read(), compresslevel=1)
            finally:
                for suffix in ("", "-wal", "-shm"):
                    try:
                        os.unlink(tmp + suffix)
                    except OSError:
                        pass
            # deliberately NOT named *.sqlite*: it lives in the namespace
            # prefix beside segments, and anything sweeping "sqlite files"
            # (an operator, a cleanup script, a test fixture - this bit me
            # while benchmarking) would delete the durable folded index
            name = f"index-{ulid_new()}.snap"
            # The expensive part (backup + gzip) ran WITHOUT the namespace lock
            # on purpose. Publishing must take it: a destroy racing this would
            # otherwise put() an object under a crypto-SHREDDED namespace and,
            # worse, envelope.encrypt() would mint a FRESH data key for it -
            # resurrecting the namespace and defeating the shred (D7 #9). This
            # is the same hazard the audit ledger has, guarded there in pass 16
            # and reintroduced here.
            with self._lock:
                if self._closed:
                    METRICS.inc("memd_index_snapshot_failures_total",
                                ns=self.namespace, detail="namespace_gone")
                    return False
                payload = self.envelope.encrypt(self.namespace, blob) if self.envelope.enabled else blob
                self.store.put(self._snapshot_key(name), payload)
                old = self.manifest.snapshot_name
                self.manifest.snapshot_name = name
                self.manifest.snapshot_seq = self.manifest.seq
                self._persist_manifest()
            if old and old != name:      # only after the new one is referenced
                try:
                    self.store.delete(self._snapshot_key(old))
                except Exception:
                    pass
            METRICS.inc("memd_index_snapshots_written_total", ns=self.namespace)
            METRICS.observe("memd_index_snapshot_bytes", float(len(payload)),
                            help="index snapshot size (bytes)",
                            buckets=(1e5, 1e6, 1e7, 5e7, 1e8, 5e8), ns=self.namespace)
            return True
        except Exception as ex:  # noqa: BLE001 - a cache write must never fail a caller
            METRICS.inc("memd_index_snapshot_failures_total", ns=self.namespace,
                        detail=type(ex).__name__)
            return False

    def _install_index_snapshot(self) -> int:
        """Materialize the snapshot into the local cache. Returns its seq (0 if
        unusable). Only called when the local cache is EMPTY, so nothing can
        be lost by overwriting it."""
        import gzip

        name = self.manifest.snapshot_name
        if not name or self.manifest.snapshot_seq <= 0:
            return 0
        try:
            blob = self.store.get(self._snapshot_key(name))
            if not blob:
                return 0
            if self.envelope.enabled:
                blob = self.envelope.decrypt(self.namespace, blob)
            raw = gzip.decompress(blob)
            path = self.index.path
            self.index.close()
            tmp = path + ".incoming"
            with open(tmp, "wb") as f:
                f.write(raw)
            for suffix in ("-wal", "-shm"):
                try:
                    os.unlink(path + suffix)
                except OSError:
                    pass
            os.replace(tmp, path)
            self.index = NamespaceIndex(path)
            self.index._ns_hint = self.namespace
            applied = int(self.index.get_meta("applied_seq") or 0)
            seq = min(int(self.manifest.snapshot_seq), applied) if applied else 0
            if seq <= 0:
                return 0
            METRICS.inc("memd_index_snapshots_loaded_total", ns=self.namespace)
            return seq
        except Exception as ex:  # noqa: BLE001 - fall back to full replay
            METRICS.inc("memd_index_snapshot_failures_total", ns=self.namespace,
                        detail=type(ex).__name__)
            try:
                if getattr(self.index, "_closed", False):
                    self.index = NamespaceIndex(self.index.path)
                    self.index._ns_hint = self.namespace
            except Exception:
                pass
            return 0

    # ------------------------------------------------------------------ open/recover

    def _open(self) -> None:
        raw = self.store.get(self.manifest_key)
        if raw:
            self.manifest = Manifest.from_dict(json.loads(raw))
        else:
            self._persist_manifest()
        # repaired before this session can append an op behind a torn record
        ops = self._read_ops(repair=True)
        if self.manifest.format < STORE_FORMAT:
            self._migrate_legacy(ops)
            ops = []
        # Index-applied watermark: the index records the highest manifest.seq it
        # has folded in. Replay covers segments + wal frames + ops with
        # seq > applied - so a fresh/restored cache rebuilds everything, while
        # a surviving cache only catches up on the tail. All idempotent.
        try:
            applied = int(self.index.get_meta("applied_seq") or 0)
        except ValueError:
            applied = 0
        if applied == 0 and self.manifest.snapshot_seq > 0:
            # Fresh local cache (node restore, wiped volume, first open on this
            # machine) and a folded image exists: install it and let the
            # existing watermark logic replay only the tail past it.
            applied = self._install_index_snapshot()
            self._replayed_at_open = True
        versions, carried, max_fold = self._load_checkpoints(applied, where="segment-replay")
        max_fold = self._adopt_orphan_segments(versions, carried, applied, max_fold)
        wal = self.store.get(self.wal_key) or b""
        base = self.manifest.wal_base_seq  # frames at or below it are folded
        tail, good_end, n_frames, last_seq = self._scan_wal(wal, max(applied, base))
        # ONE order: checkpoints, then every op and frame past the watermark
        # by seq (see _fold_events). Applying the ops log before the WAL
        # records re-created records whose delete had been acked.
        events = _merge_events(carried + ops, tail, after=applied)
        self._pending_hard = []
        self._pending_hard_ids = set()
        for o in carried + ops:
            if o.get("op") == "hard_delete" and o.get("id"):
                self._track_pending_hard(o["id"], int(o.get("deadline", 0)))
        # The counter must clear every seq already on disk. The manifest is
        # persisted lazily, so after a crash it can trail the WAL; handing a
        # new frame an old seq would slot it into the past.
        self.manifest.seq = max([self.manifest.seq, applied, max_fold, last_seq]
                                + [int(o.get("seq", 0)) for o in carried + ops])
        if versions or events:
            self._replay_into_index(versions, events)
            self._replayed_at_open = True
        if n_frames and last_seq <= base:
            # every frame is at or below the checkpoint that folded it: the
            # cleanup of a fold or migration that crashed after its commit
            self.store.delete(self.wal_key)
            n_frames = 0
        elif good_end < len(wal):
            self.store.truncate(self.wal_key, good_end)  # torn-tail repair
        self.manifest.ops_size = self.store.size(self.ops_key)
        if not ops and self.manifest.ops_size:
            self.store.delete(self.ops_key)  # the same cleanup, ops side
            self.manifest.ops_size = 0
        # Seed the frame counter from what is actually in the WAL. Starting it
        # at zero on every open meant the frame bound did not survive a
        # RESTART: six cycles of 400 writes left 2,400 WAL parts in the bucket
        # while the in-process counter read 400 each time, so the WAL grew
        # without bound across restarts and cold open went back to being
        # O(records). The bound has to be a property of the log, not of the
        # process that happens to be holding it.
        self._wal_frames = n_frames
        self.manifest.wal_size = self.store.size(self.wal_key)
        # commit replayed rows FIRST, only then advance the watermark -
        # otherwise a crash could persist the watermark while losing rows
        self.index.flush()
        self.index.set_meta("applied_seq", str(self.manifest.seq))
        self._persist_manifest()

    # ------------------------------------------------------------------ format 1 -> 2

    def _migrate_legacy(self, ops: list[dict]) -> None:
        """One-time upgrade of a format-1 namespace (see STORE_FORMAT).

        Folds its WAL + ops log, in the order _legacy_events settles, into
        one new segment exactly as a rotate would (ops it cannot retire ride
        in the header), adds what the old binary's own index proves its
        durable data lost (_legacy_index_evidence), then commits a format-2
        manifest referencing it and deletes the old logs. The manifest put
        is the commit point: a crash before it leaves the format-1 namespace
        as it was (the next open migrates again; an uncommitted segment is
        never adopted), a crash after it leaves only log events at or below
        the new wal_base_seq, which open skips and deletes. The local index
        is rebuilt from the result, so every node serves the same state."""
        t0 = time.monotonic()
        m = self.manifest
        base = m.wal_base_seq
        frames = self._legacy_wal_frames()
        top_fold = max((int(s.get("fold_seq", 0)) for s in m.segments), default=0)
        events, how, top = _legacy_events(frames, ops, base, max(m.seq, top_fold))
        kept, _pending, _unq, folded, deferred = _fold_events({}, events, now_ms(), force=False)
        out = _carry_forward(folded, kept)
        evidence = self._legacy_index_evidence(ops, frames)
        for i, op in enumerate(evidence, 1):
            op["seq"] = top + i
        out.extend(evidence)
        fold = top + len(evidence)
        # the index is derived: reset it before the commit, so no format-1
        # watermark can make a format-2 open skip the new checkpoint
        self.index.wipe()
        self.index.set_meta("applied_seq", "0")
        name = f"seg-{ulid_new()}" if kept or out else ""
        if name:
            self._write_segment(name, kept, fold, out, deferred)
            m.segments.append(self._segment_entry(name, kept, fold, "migrate", out))
        stale_snapshot = m.snapshot_name  # an image of a format-1 index
        m.format = STORE_FORMAT
        m.checkpoint, m.checkpoint_seq = name, fold
        m.seq = m.wal_base_seq = fold
        m.snapshot_name, m.snapshot_seq = "", 0
        m.wal_size = m.ops_size = 0
        self._persist_manifest()  # commit point
        self.store.delete(self.wal_key)
        self.store.delete(self.ops_key)
        if stale_snapshot:
            try:
                self.store.delete(self._snapshot_key(stale_snapshot))
            except Exception:  # noqa: BLE001 - unreferenced now; only costs space
                pass
        METRICS.inc("memd_store_migrations_total",
                    help="namespaces migrated to the current store format",
                    ns=self.namespace, order=how)
        if how != "exact":
            _log.warning(
                "namespace %s: format-1 WAL (%d frames) and ops log (%d ops) do not "
                "reconcile with seq %d over wal_base_seq %d; read %s, so a delete "
                "wins over a write it may have followed", self.namespace, len(frames),
                len(ops), top, base, how)
        if evidence:
            _log.warning(
                "namespace %s: kept %d delete/supersede op(s) the local index had "
                "applied but format-1 durable data had lost", self.namespace, len(evidence))
        _log.info("namespace %s: migrated to store format %d in %d ms",
                  self.namespace, STORE_FORMAT, int((time.monotonic() - t0) * 1000))

    def _legacy_wal_frames(self) -> list[tuple[int | None, list[MemoryRecord]]]:
        """A format-1 WAL as (stamp or None, records) per frame, in log order.
        The first frame that does not parse ends it (torn tail), as a
        format-1 open truncated it."""
        data = self.store.get(self.wal_key) or b""
        raw = [data] if data[:1] == b"{" else [fr for _, fr in _frames_with_offsets(data)]
        out: list[tuple[int | None, list[MemoryRecord]]] = []
        for fr in raw:
            try:
                payload = self._decrypt_frame(fr)
                recs = records_from_jsonl(payload)
            except Exception:  # noqa: BLE001
                break
            out.append((_wal_seq(payload), recs))
        return out

    def _legacy_index_evidence(self, ops: list[dict],
                               frames: list[tuple[int | None, list[MemoryRecord]]]) -> list[dict]:
        """Deletes and supersedes the format-1 index proves and its durable
        data lost.

        A format-1 rotate folded only the WAL, then deleted the ops log: a
        tombstone or supersede whose target already lived in an older
        segment was gone from durable state. The old binary's live index had
        applied it, so its warm open kept hiding the record while a cold
        open or a rebuild served it again. Where this index reflects the
        segment holding an id's newest copy (fold_seq <= its applied_seq)
        and no durable op or frame touches that id, a row it marks deleted
        or superseded is taken at its word - a delete wins. Only such
        restrictions count: a MISSING row proves nothing (an interrupted
        rebuild leaves the watermark over an emptied index)."""
        try:
            applied = int(self.index.get_meta("applied_seq") or 0)
        except ValueError:
            applied = 0
        if applied <= 0 or not self.manifest.segments:
            return []
        versions, carried, _ = self._load_checkpoints(where="migrate-segment")
        touched = ({_op_target(o) for o in carried + ops}
                   | {r.id for _, recs in frames for r in recs})
        seg = {rid: rec for rid, (rec, held) in versions.items()
               if held <= applied and rid not in touched}
        if not seg:
            return []
        with self.index._lock:
            rows = self.index._con.execute(
                "SELECT id, deleted, superseded_by, invalidated_at FROM records "
                "WHERE deleted=1 OR superseded_by IS NOT NULL").fetchall()
        now = now_ms()
        out: list[dict] = []
        for rid, deleted, sup, inv in rows:
            rec = seg.get(rid)
            if rec is None:
                continue
            at = int(inv) if inv is not None else now
            if deleted:
                out.append({"op": "tombstone", "id": rid, "at": at})
            elif sup != rid and not rec.time.superseded_by:
                out.append({"op": "supersede", "old": rid, "new": sup, "at": at})
        return out

    def _read_ops(self, repair: bool = False) -> list[dict]:
        """The ops log, parsed (see _scan_ops). repair=True - open, before
        this session appends anything - also cuts a damaged tail off,
        durably, the way the WAL's torn tail is: an op appended behind a torn
        record was invisible to every reader and lost at the next rotate.
        Format 2: an op at or below wal_base_seq is one a checkpoint already
        folded (left by a fold or migration that crashed after its commit)."""
        data = self.store.get(self.ops_key) or b""
        ops, good_end = self._scan_ops(data)
        if repair and good_end < len(data):
            self.store.truncate(self.ops_key, good_end)
            METRICS.inc("memd_ops_log_repairs_total",
                        help="ops logs whose damaged tail was cut off at open",
                        ns=self.namespace)
            _log.warning("namespace %s: cut %d damaged byte(s) off the ops log tail",
                         self.namespace, len(data) - good_end)
        if self.manifest.format >= STORE_FORMAT:
            base = self.manifest.wal_base_seq
            ops = [o for o in ops if int(o.get("seq", 0)) > base]
        return ops

    def _parse_op(self, data: bytes, i: int) -> tuple[dict, int] | None:
        """The op framed at offset i -> (op, end offset), or None."""
        if i + 4 > len(data):
            return None
        end = i + 4 + int.from_bytes(data[i : i + 4], "big")
        if end == i + 4 or end > len(data):
            return None
        try:
            op = json.loads(self._decrypt_frame(data[i + 4 : end]))
        except ValueError:  # JSONDecodeError and UnicodeDecodeError
            return None
        if not isinstance(op, dict) or not isinstance(op.get("seq"), int):
            return None
        return op, end

    def _scan_ops(self, data: bytes) -> tuple[list[dict], int]:
        """Parse an ops log -> (ops, end of the last op that parsed).

        A record that does not parse - a torn append, garbage - is skipped by
        resynchronizing on the next offset that parses as a LATER op. Older
        versions never repaired this log, so after a torn append they kept
        appending (and acking) ops behind it; stopping at the tear, as the
        length framing alone would, hid those acked deletes and the next
        rotate dropped them. A torn tail has nothing behind it and ends the
        log."""
        if data[:1] == b"{":  # legacy single blob
            try:
                return [json.loads(self._decrypt_frame(data))], len(data)
            except ValueError:
                METRICS.inc("memd_storage_parse_errors_total", where="ops-replay", ns=self.namespace)
                return [], len(data)
        ops: list[dict] = []
        i = good_end = last = 0
        while i < len(data):
            got = self._parse_op(data, i)
            if got is None:
                METRICS.inc("memd_storage_parse_errors_total", where="ops-replay", ns=self.namespace)
                for j in range(i + 1, len(data) - 4):
                    got = self._parse_op(data, j)
                    if got is not None and got[0]["seq"] > last:
                        break
                else:
                    break  # nothing behind it parses: a torn tail
                METRICS.inc("memd_ops_log_salvaged_total",
                            help="ops recovered from behind a damaged ops-log record",
                            ns=self.namespace)
                _log.warning("namespace %s: ops log damaged at byte %d; "
                             "resynchronized at byte %d", self.namespace, i, j)
            op, i = got
            ops.append(op)
            last = max(last, op["seq"])
            good_end = i
        return ops, good_end

    def _load_checkpoints(self, applied: int = 0, where: str = "segment-load"
                          ) -> tuple[dict[str, tuple[MemoryRecord, int]], list[dict], int]:
        """Segment state for a fold -> (versions, carried ops, max fold_seq).

        versions maps id -> (record, fold_seq) for the NEWEST segment holding
        it, over segments an index at `applied` does not reflect yet (all of
        them when applied is 0). carried = ops earlier rotates could not
        retire; the headers of already-reflected segments are read too when
        they hold hard deletes, because those are the purge schedule."""
        versions: dict[str, tuple[MemoryRecord, int]] = {}
        src_fold: dict[str, int] = {}  # newest segment wins, by fold_seq
        carried: list[dict] = []
        max_fold = 0
        for seg in self.manifest.segments:
            data = self.store.get(f"{self.prefix}/{seg['name']}")
            if data is None:
                continue
            fold = int(seg.get("fold_seq", 0))
            max_fold = max(max_fold, fold)
            reflected = applied > 0 and fold <= applied
            if reflected and not seg.get("purges"):
                continue  # already reflected in this index
            # NOTE: segment parsing returns a TUPLE. A site that once iterated
            # it directly raised AttributeError on every segment, was silently
            # swallowed, and fresh-cache recovery recovered NOTHING from
            # segments. Unpack explicitly; parse failures degrade per-segment.
            try:
                recs, _fold_seq, ops, before = self._segment_parse(data, header_only=reflected)
            except Exception as ex:  # noqa: BLE001 - corrupt segment: skip, count, survive
                METRICS.inc("memd_storage_parse_errors_total",
                            where=where, ns=self.namespace,
                            detail=type(ex).__name__)
                continue
            carried.extend(ops)
            for rec in recs:
                if src_fold.get(rec.id, -1) <= fold:
                    src_fold[rec.id] = fold
                    versions[rec.id] = (rec, _held(rec.id, fold, before))
        return versions, carried, max_fold

    def _scan_wal(self, wal: bytes, applied: int
                  ) -> tuple[list[tuple[int, list[MemoryRecord]]], int, int, int]:
        """Frames past the watermark -> (frames, good_end, n_frames, last_seq).

        Seqs grow along the log, so the first frame past `applied` is found by
        bisection: a warm open decrypts O(log n) frames instead of all of them.
        A frame past it that fails to parse ends the log (torn tail)."""
        frames = list(_frames_with_offsets(wal))
        probed: dict[int, int] = {}
        lo, hi = 0, len(frames)
        while lo < hi:
            mid = (lo + hi) // 2
            probed[mid] = _frame_seq(self._decrypt_frame(frames[mid][1]))
            if probed[mid] <= applied:
                lo = mid + 1
            else:
                hi = mid
        good_end = frames[lo - 1][0] if lo else 0
        last_seq = probed[lo - 1] if lo else 0
        n = lo
        out: list[tuple[int, list[MemoryRecord]]] = []
        for i in range(lo, len(frames)):
            end, fr = frames[i]
            try:
                payload = self._decrypt_frame(fr)
                recs = records_from_jsonl(payload)
            except Exception:
                break
            last_seq = _frame_seq(payload)
            out.append((last_seq, recs))
            good_end, n = end, i + 1
        return out, good_end, n, last_seq

    def _wal_events(self, where: str) -> list[tuple[int, list[MemoryRecord]]]:
        """Every parseable WAL frame past wal_base_seq as (seq, records); bad
        frames are counted and skipped (fold paths, not the torn-tail repair)."""
        base = self.manifest.wal_base_seq
        out: list[tuple[int, list[MemoryRecord]]] = []
        for fr in self._read_frames(self.wal_key):
            try:
                payload = self._decrypt_frame(fr)
                recs = records_from_jsonl(payload)
            except Exception:
                METRICS.inc("memd_storage_parse_errors_total", where=where, ns=self.namespace)
                continue
            s = _frame_seq(payload)
            if s > base:
                out.append((s, recs))
        return out

    def _replay_into_index(self, versions: dict[str, tuple[MemoryRecord, int]],
                           events: list[tuple[int, object]]) -> None:
        """The index side of _fold_events: checkpoint copies first, then the
        events strictly in seq order, skipping what a newer checkpoint of the
        same id already reflects. Consecutive frames still go in one
        upsert_batch; ops use the same index calls the write path made."""
        self._apply_to_index([rec for rec, _ in versions.values()], [], from_replay=True)
        held = {rid: fold for rid, (_, fold) in versions.items()}
        recs: list[MemoryRecord] = []
        ops: list[dict] = []
        for seq, ev in events:
            if isinstance(ev, dict):
                if held.get(_op_target(ev), -1) >= seq:
                    continue
                if recs:
                    self._apply_to_index(recs, [], from_replay=True)
                    recs = []
                ops.append(ev)
            else:
                fresh = [r for r in ev if held.get(r.id, -1) < seq]
                if fresh and ops:
                    self._apply_to_index([], ops, from_replay=True)
                    ops = []
                recs.extend(fresh)
        self._apply_to_index(recs, ops, from_replay=True)  # at most one is non-empty

    # ------------------------------------------------------- segment blob format
    #
    # v2 blob (one put() payload):
    #   line 1: {"_seg": {"fold_seq": N, "ops": [...]}}   <- header, enables orphan adoption
    #   lines 2+: resolved record JSONL
    # "ops" (optional) holds the ops a fold could not retire, at their
    # original seqs (see _carry_forward); the manifest entry counts them
    # ("carried") and the hard deletes among them ("purges"). "before"
    # (optional) maps an id whose bytes a not-yet-due hard delete keeps to
    # that delete's seq: the copy precedes it, so replay still applies it.
    # The whole payload is envelope-encrypted when encryption is enabled
    # (segments are the durable source of truth - they must satisfy the same
    # at-rest guarantee as the WAL). Legacy v1 blobs are bare plaintext JSONL;
    # _segment_records falls back for them so old stores keep reading.

    @staticmethod
    def _segment_blob(kept: list[MemoryRecord], fold_seq: int, carried: list[dict] = (),
                      before: dict[str, int] | None = None) -> bytes:
        seg: dict = {"fold_seq": fold_seq}
        if carried:
            seg["ops"] = list(carried)
        if before:
            seg["before"] = dict(before)
        header = json.dumps({"_seg": seg}, separators=(",", ":"))
        body = records_to_jsonl(kept)
        return header.encode() + b"\n" + body if body.strip() else header.encode() + b"\n"

    def _write_segment(self, name: str, kept: list[MemoryRecord], fold_seq: int,
                       carried: list[dict] = (), before: dict[str, int] | None = None) -> bytes:
        data = self._segment_blob(kept, fold_seq, carried, before)
        if self.envelope.enabled:
            data = self.envelope.encrypt(self.namespace, data)
        self.store.put(f"{self.prefix}/{name}", data)
        return data

    @staticmethod
    def _segment_entry(name: str, kept: list, fold_seq: int, reason: str, carried: list[dict]) -> dict:
        entry = {"name": name, "records": len(kept), "fold_seq": fold_seq, "reason": reason}
        if carried:
            entry["carried"] = len(carried)
            purges = sum(1 for op in carried if op.get("op") == "hard_delete")
            if purges:
                entry["purges"] = purges
        return entry

    def _segment_records(self, data: bytes) -> tuple[list[MemoryRecord], int]:
        """Parse a segment blob -> (records, fold_seq). Tolerates legacy
        plaintext blobs and encrypted v2 blobs; fold_seq defaults to 0 when
        absent (legacy), which disables that blob's orphan eligibility."""
        recs, fold_seq, _ops, _before = self._segment_parse(data)
        return recs, fold_seq

    def _segment_parse(self, data: bytes, header_only: bool = False
                       ) -> tuple[list[MemoryRecord], int, list[dict], dict[str, int]]:
        """_segment_records plus the header's carried ops and "before" map."""
        raw = self._decrypt_frame(data)
        first_nl = raw.find(b"\n")
        head = raw[:first_nl] if first_nl != -1 else b""
        fold_seq = 0
        ops: list[dict] = []
        before: dict[str, int] = {}
        recs_data = raw
        if head.startswith(b"{"):
            try:
                h = json.loads(head)
                if isinstance(h.get("_seg"), dict):
                    fold_seq = int(h["_seg"].get("fold_seq", 0))
                    ops = [op for op in h["_seg"].get("ops") or [] if isinstance(op, dict)]
                    before = {str(k): int(v) for k, v in (h["_seg"].get("before") or {}).items()}
                    recs_data = raw[first_nl + 1 :]
            except (ValueError, TypeError, AttributeError):
                pass  # legacy single-record blob starting with '{'
        return ([] if header_only else records_from_jsonl(recs_data)), fold_seq, ops, before

    def _adopt_orphan_segments(self, versions: dict, carried: list[dict], applied: int, max_fold: int) -> int:
        """Re-reference the segment the manifest names as its newest
        checkpoint if the segment list lost it.

        Adoption takes POSITIVE evidence. It used to adopt any unreferenced
        seg-* blob whose fold_seq beat every referenced one - and after a
        compaction whose output was empty nothing is referenced, so a crash
        before that compaction deleted the old segments re-adopted all of
        them on the next cold open and served everything it had deleted.
        Every rotate, compaction and migration commits its checkpoint's name
        in the same manifest put as the segment list ("" when it wrote
        nothing), so an unreferenced blob is either that checkpoint or
        residue: a fold's uncommitted output (the logs it would have
        replaced are still there) or segments a committed fold replaced.
        Residue is never read."""
        name = self.manifest.checkpoint
        if not name or any(s["name"] == name for s in self.manifest.segments):
            return max_fold
        data = self.store.get(f"{self.prefix}/{name}")
        if not data:
            return max_fold
        try:
            recs, fold_seq, ops, before = self._segment_parse(data)
        except Exception:  # noqa: BLE001 - unreadable: nothing to adopt
            return max_fold
        if fold_seq != self.manifest.checkpoint_seq:
            return max_fold  # not the blob the manifest committed
        if fold_seq > applied:  # an index at `applied` already reflects it
            for rec in recs:
                versions[rec.id] = (rec, _held(rec.id, fold_seq, before))
        carried.extend(ops)
        self.manifest.segments.append(
            self._segment_entry(name, recs, fold_seq, "orphan-adopted", ops))
        METRICS.inc("memd_orphan_segments_adopted_total", amount=1, ns=self.namespace)
        self._persist_manifest()
        return max(max_fold, fold_seq)

    def _read_frames(self, key: str, limit_size: int | None = None) -> list[bytes]:
        data = self.store.get(key) or b""
        if limit_size is not None:
            data = data[:limit_size]
        if data[:1] == b"{":
            return [data]  # legacy single blob
        return list(_frame_iter(data))

    def _decrypt_frame(self, frame: bytes) -> bytes:
        if len(frame) > 12 and self.envelope.enabled:
            try:
                return self.envelope.decrypt(self.namespace, frame)
            except Exception:
                return frame
        return frame

    def _apply_to_index(self, records: list[MemoryRecord], ops: list[dict], from_replay: bool = False) -> None:
        items = [(r, None, "") for r in records]
        qflags = {r.id: bool(r.meta.get("quarantined")) for r in records}
        if items:
            self.index.upsert_batch(items, qflags)
        for op in ops:
            kind = op.get("op")
            # ignore malformed self-supersede: it would brick the record
            if kind == "supersede" and op.get("old") == op.get("new"):
                continue
            if kind == "tombstone":
                self.index.tombstone(op["id"], op.get("at", now_ms()))
            elif kind == "hard_delete":
                self.index.hard_delete(op["id"])
            elif kind == "supersede":
                self.index.mark_superseded(op["old"], op["new"], op.get("at", now_ms()))
            elif kind == "quarantine":
                self.index.mark_quarantined(op["id"], bool(op.get("flag", True)))
            elif kind == "set_vector":
                import numpy as np

                vec = np.frombuffer(bytes.fromhex(op["vec_hex"]), dtype=np.float32)
                self.index.set_vector(op["id"], vec, op.get("model", ""))

    def _persist_manifest(self) -> None:
        self.manifest.version += 1
        self.store.put(self.manifest_key, json.dumps(self.manifest.to_dict()).encode())
        self._manifest_dirty = False
        METRICS.inc("memd_manifest_writes_total", ns=self.namespace)

    # ------------------------------------------------------------------ write path

    def _ensure_open(self) -> None:
        if self._closed:
            if getattr(self, "_evicted", False):
                raise RuntimeError(
                    f"namespace {self.namespace!r} was evicted from the open cache; "
                    "re-resolve it via the engine")
            raise RuntimeError(f"namespace {self.namespace!r} destroyed")

    def mark_destroyed(self) -> None:
        """Lifecycle hook: block new operations before teardown begins."""
        self._closed = True

    def append(self, records: list[MemoryRecord]) -> int:
        """Durable append + synchronous index apply (read-your-writes).

        Group commit: the caller's bytes are written (OS-visible) under the
        write lock; durability waits for an fsync that started after those
        bytes, so concurrent appenders share fsyncs instead of serializing.

        The wait happens OUTSIDE the namespace lock deliberately. Holding that
        lock across `_durably_written` meant no two appenders were ever inside
        the sync-owner election at once, so the coalescing this docstring
        describes could never happen and the non-owner branch was dead code -
        measured at 200 fsyncs for 200 appends across 8 writer threads.
        Correctness is unchanged: the frame is already OS-visible before the
        lock is released, `my_gen` makes a concurrent rotate a no-wait (the
        frame is then in an atomically-put segment), and the derived index is
        rebuildable, so a crash between the index upsert and the fsync loses
        only bytes that were never acked.
        """
        if not records:
            return self.manifest.wal_size
        payload = records_to_jsonl(records)
        with self._lock:
            self._ensure_open()
            seq = self.manifest.seq + 1  # the frame's place in history, stamped in it
            frame = _frame_encode(self.envelope.encrypt(self.namespace, _wal_stamp(payload, seq)))
            my_end, my_gen = self._wal_write(frame)
            self._wal_frames += 1
            self.manifest.wal_size = my_end
            self.manifest.seq = seq
            self._manifest_dirty = True
            qflags = {r.id: bool(r.meta.get("quarantined")) for r in records}
            self.index.upsert_batch([(r, None, "") for r in records], qflags)
            size = self.manifest.wal_size
        self._durably_written(my_end, my_gen)   # ack only after fsync
        frames_over = (not self._has_log_writer()
                       and self._wal_frames >= self.wal_rotate_frames)
        if my_end >= self.wal_rotate_bytes or frames_over:
            with self._lock:
                if not self._closed and (
                        self.manifest.wal_size >= self.wal_rotate_bytes
                        or (not self._has_log_writer()
                            and self._wal_frames >= self.wal_rotate_frames)):
                    self.rotate("frames" if frames_over else "size")
                size = self.manifest.wal_size
        return size

    def _wal_write(self, frame: bytes) -> tuple[int, int]:
        """Write a frame OS-visible. Returns (end offset, log generation).
        Writer handle acquired under the write lock: rotation seals the old
        handle under the same lock, so a write can never target a closed fd."""
        with self._wlock:
            if not self._has_log_writer():
                # store has no persistent-handle log: durable append per batch
                size = self.store.append(self.wal_key, frame)
                METRICS.inc("memd_wal_frames_total", ns=self.namespace)
                METRICS.inc("memd_wal_bytes_total", len(frame), ns=self.namespace)
                return size, self._log_gen
            w = self._writer()
            end = w.write(frame)
            self._written_pos = end
            gen = self._log_gen
            METRICS.inc("memd_wal_frames_total", ns=self.namespace)
            METRICS.inc("memd_wal_bytes_total", len(frame), ns=self.namespace)
        return end, gen

    def _has_log_writer(self) -> bool:
        """True when the store OVERRIDES open_log (real group-commit support).
        A bare hasattr() is useless here: the ABC defines open_log, so every
        subclass 'has' it and taking that path raised NotImplementedError
        inside append() - breaking writes on any store without a
        persistent-handle log."""
        return getattr(type(self.store), "open_log", None) is not ObjectStore.open_log

    def _writer(self):
        if not self._has_log_writer():
            raise NotImplementedError
        if self._wal_writer is None:
            self._wal_writer = self.store.open_log(self.wal_key)
            self._written_pos = self._wal_writer.size()
            # bytes from a prior session may never have been fsynced (crash);
            # one sync makes our baseline unambiguous
            if self._written_pos > 0:
                self._wal_writer.sync()
            self._synced_pos = self._written_pos
        return self._wal_writer

    def _durably_written(self, my_end: int, my_gen: int) -> None:
        """Block until an fsync covering my_end completed (group commit:
        whoever holds the sync lock fsyncs up to the latest written pos).
        If the log was rotated meanwhile, our frame was folded into an
        immutable segment written via atomic put() - already durable."""
        if getattr(self.store, "open_log", None) is None or not self._has_log_writer():
            # no persistent-handle log: _wal_write used append(), whose
            # contract is durable-on-return - there is nothing to wait for.
            # (Reaching _writer() here raised NotImplementedError and broke
            # every append on such stores.)
            return
        while True:
            if self._log_gen != my_gen:
                return  # folded into a segment; put() is atomic+durable
            owner = False
            with self._sync_lock:
                if self._log_gen != my_gen:
                    return
                if self._synced_pos >= my_end:
                    return
                if not self._sync_busy:
                    self._sync_busy = True
                    owner = True
            if owner:
                try:
                    # Acquire the writer under _wlock and re-check the
                    # generation there. _seal_wal_writer_locked takes the same
                    # lock, so this is mutually exclusive with a rotate/compact
                    # seal. Without it, a seal landing in the gap would make
                    # _writer() REOPEN the wal handle - and the compaction that
                    # sealed it then unlinks the key underneath, re-creating
                    # the pass-22 ghost-inode data loss by race. The gen check
                    # at the top of the loop is read before this gap and cannot
                    # close it.
                    with self._wlock:
                        if self._log_gen != my_gen:
                            with self._sync_lock:
                                self._sync_busy = False
                            return
                        cur = self._writer()
                        covered_to = self._written_pos
                    # Snapshot the write frontier BEFORE the fsync. Reading
                    # _written_pos AFTER it claimed durability for anything
                    # another appender wrote while the fsync was in flight:
                    # measured 160 violations across 8 writers, up to 3108
                    # bytes marked durable that no fsync ever covered, and the
                    # appender waiting on those bytes returned immediately -
                    # an ACK for data that was not on disk. Bytes written
                    # after this read are also covered by the fsync; not
                    # claiming them is merely conservative.
                    cur.sync()
                except (OSError, ValueError):
                    # writer closed by a concurrent rotate: our frame is in
                    # the folded segment - durable
                    with self._sync_lock:
                        self._sync_busy = False
                    return
                with self._sync_lock:
                    self._synced_pos = max(self._synced_pos, covered_to)
                    self._sync_busy = False
                time.sleep(0)
            else:
                time.sleep(0.0002)

    def _track_pending_hard(self, rid: str, deadline_ms: int) -> None:
        """Idempotently schedule a physical purge (dedup across rotate
        replays so pending counts stay honest)."""
        if rid in self._pending_hard_ids:
            return
        self._pending_hard_ids.add(rid)
        self._pending_hard.append((rid, deadline_ms))

    def append_op(self, op: dict) -> None:
        self.append_ops([op])

    def append_ops(self, ops: list[dict]) -> None:
        """Persist + apply mutation ops in ONE durable store.append (one fsync
        for the whole batch) and one index application per op row.

        The previous per-op path (append_op called in a loop) issued an
        fsync'd append AND a separate index commit PER OP - O(n) round trips
        on the forget/destructive-sweep request path. Frame format is
        unchanged (one op per frame), so replay is identical."""
        if not ops:
            return
        # hygiene: a self-supersede is inert everywhere - refuse to persist it
        ops = [op for op in ops if not (op.get("op") == "supersede" and op.get("old") == op.get("new"))]
        if not ops:
            return
        with self._lock:
            frames = []
            for op in ops:
                self.manifest.seq += 1
                op["seq"] = self.manifest.seq
                payload = json.dumps(op, separators=(",", ":")).encode()
                frames.append(_frame_encode(self.envelope.encrypt(self.namespace, payload)))
            try:
                size = self.store.append(self.ops_key, b"".join(frames))
            except BaseException:
                # A failed append may have left part of its frames behind, and
                # the next op - acked - would land after that torn record.
                self._cut_ops_back(self.manifest.ops_size)
                raise
            self.manifest.ops_size = size
            self._manifest_dirty = True
            # rotate is skipped while a rotation is already folding this log:
            # re-entering would fold mid-replay state. The outer fold re-reads
            # everything we just appended.
            if size >= self.wal_rotate_bytes and not self._rotating:
                self.rotate("ops-size")
            for op in ops:
                if op.get("op") == "hard_delete" and op.get("id"):
                    self._track_pending_hard(op["id"], int(op.get("deadline", 0)))
            METRICS.set_gauge("memd_pending_purges", len(self._pending_hard), ns=self.namespace)
            self._apply_to_index([], ops)

    def _cut_ops_back(self, size: int) -> None:
        """Best effort: truncate the ops log to `size` if a failed append
        grew it (open repairs whatever this cannot)."""
        try:
            if self.store.size(self.ops_key) > size:
                self.store.truncate(self.ops_key, size)
        except Exception:  # noqa: BLE001 - the append's own error is the one to raise
            pass

    def has_due_deletes(self, now_ms_: int | None = None) -> bool:
        """True when at least one scheduled physical purge has passed its
        deadline (drives opportunistic auto-compaction)."""
        now = now_ms_ if now_ms_ is not None else now_ms()
        return any(dl <= now for _, dl in self._pending_hard)

    @property
    def pending_hard_deletes(self) -> int:
        return len(self._pending_hard)

    def rotate(self, reason: str = "manual") -> str:
        """Fold *the current wal* (+pending ops) into a new immutable segment.
        Incremental by design: never rescans older segments (O(wal), not
        O(namespace)) - compaction is the one that merges segments.

        Crash ordering: the manifest that references the new segment is
        persisted BEFORE the wal/ops logs are deleted. A crash between the
        two leaves stale logs (replayed idempotently on open) instead of an
        orphaned segment and lost data."""
        with self._lock:
            self._ensure_open()
            self._seal_wal_writer_locked()
            self._rotating = True
            try:
                return self._rotate_locked(reason)
            finally:
                self._rotating = False

    def _reset_wal_frames(self) -> None:
        self._wal_frames = 0

    def _seal_wal_writer_locked(self) -> None:
        """Close the persistent WAL handle and void its offsets.

        MUST be called by anything that deletes the wal key. Take BOTH locks so
        an in-flight append finishes its write (wlock) and its fsync (sync
        lock) before the fd closes - otherwise writers hit closed-file errors
        and lose frames.

        `compact()` deleted the wal WITHOUT this and silently lost every
        subsequent write: the LogWriter kept its fd on the now-UNLINKED inode,
        so appends went to a ghost file that no reopen could ever see. The
        record was acked, was visible in-process (the derived index had it),
        and vanished on restart. The durability contract says an ack means a
        durable append; this broke it, and no prior pass caught it because the
        SIGKILL crash-consistency suite never compacts mid-stream.
        """
        with self._wlock, self._sync_lock:
            if self._wal_writer is not None:
                try:
                    self._wal_writer.close()
                except Exception:
                    pass
                self._wal_writer = None
            self._log_gen += 1
            self._written_pos = 0
            self._synced_pos = 0

    def _rotate_locked(self, reason: str) -> str:
        """Fold body (caller holds the ns lock and set _rotating)."""
        METRICS.inc("memd_rotations_total", ns=self.namespace, reason=reason)
        ops = self._read_ops()
        frames = self._wal_events("rotate-wal")
        kept, _pending, unq_ids, folded, deferred = _fold_events(
            {}, _merge_events(ops, frames), now_ms(), force=False)
        # quarantine decay: restore index visibility for expired flags
        for rid_ in unq_ids:
            self.index.mark_quarantined(rid_, False)
        # Ops this partial fold cannot retire ride in the new segment's header
        # at their original seqs - written atomically with it, so no crash
        # between deleting the ops log and re-appending them can lose one.
        # (They used to be dropped, or re-appended at NEW seqs afterwards.)
        carried = _carry_forward(folded, kept)
        name = f"seg-{ulid_new()}"
        if kept or carried:
            self._write_segment(name, kept, self.manifest.seq, carried, deferred)
            self.manifest.segments.append(
                self._segment_entry(name, kept, self.manifest.seq, reason, carried))
        self.manifest.checkpoint = name if kept or carried else ""
        self.manifest.checkpoint_seq = self.manifest.seq
        self.index.flush()  # rows durable before advancing watermark
        self.index.set_meta("applied_seq", str(self.manifest.seq))
        self._persist_manifest()
        # truncate logs (object-store friendly: rewrite empty)
        self.store.delete(self.wal_key)
        self.store.delete(self.ops_key)
        self.manifest.wal_size = 0
        self.manifest.ops_size = 0
        self.manifest.wal_base_seq = self.manifest.seq  # next frame = base+1
        self._reset_wal_frames()
        self._synced_pos = 0
        self._written_pos = 0
        self._persist_manifest()
        # every hard delete stays scheduled - due ones included: their older
        # copies are only purged by the compaction the schedule triggers
        for op in carried:
            if op.get("op") == "hard_delete" and op.get("id"):
                self._track_pending_hard(op["id"], int(op.get("deadline", 0)))
        METRICS.set_gauge("memd_pending_purges", len(self._pending_hard), ns=self.namespace)
        return name

    # ------------------------------------------------------------------ maintenance

    def load_all_records(self) -> tuple[list[MemoryRecord], list[dict]]:
        """RAW durable content: every record copy (newest per id) and the ops
        log, with NO op applied - deleted and hard-deleted records included.
        For inspecting what is physically stored; never serve it (export
        serves _visible_records)."""
        segs = {}
        for seg in self.manifest.segments:
            data = self.store.get(f"{self.prefix}/{seg['name']}")
            if data is None:
                continue
            try:
                recs, _ = self._segment_records(data)
            except Exception:
                METRICS.inc("memd_storage_parse_errors_total", where="segment-load", ns=self.namespace)
                continue
            for rec in recs:
                segs[rec.id] = rec
        live: dict[str, MemoryRecord] = dict(segs)
        ops = self._read_ops()
        for fr in self._read_frames(self.wal_key):
            try:
                live.update({r.id: r for r in records_from_jsonl(self._decrypt_frame(fr))})
            except Exception:
                METRICS.inc("memd_storage_parse_errors_total", where="load-wal", ns=self.namespace)
                continue
        return list(live.values()), ops

    def compact(self, force: bool = False) -> CompactionReport:
        """Fold segments+wal+ops into one segment; enforce hard-delete deadlines.

        Crash ordering: the manifest referencing ONLY the new folded segment
        is persisted BEFORE any old file is deleted. A crash between the two
        leaves the old segments in place (harmless: replay is idempotent and
        the next compaction reclaims them) instead of a manifest that points
        at deleted files."""
        t0 = time.monotonic()
        rep = CompactionReport(duration_ms=0)
        with self._lock:
            self._ensure_open()
            if not force and self._compaction_is_noop():
                # Nothing to fold and nothing to apply. The unconditional path
                # read every live record and rewrote the whole live set anyway
                # - O(live) I/O for a guaranteed no-op, and compaction is
                # reachable from an authenticated endpoint and from the
                # maintenance thread.
                METRICS.inc("memd_compactions_skipped_total",
                            help="compactions skipped: provably nothing to do",
                            ns=self.namespace)
                rep.segments_in = rep.segments_out = len(self.manifest.segments)
                rep.duration_ms = int((time.monotonic() - t0) * 1000)
                return rep
            versions, carried, _ = self._load_checkpoints()
            ops = self._read_ops()
            frames = self._wal_events("load-wal")
            rep.bytes_before = sum(self.store.size(f"{self.prefix}/{s['name']}") for s in self.manifest.segments)
            rep.segments_in = len(self.manifest.segments)
            now = now_ms()
            pre_ids = set(versions) | {r.id for _, recs in frames for r in recs}
            kept, pending_ops, unq_ids, folded, deferred = _fold_events(
                versions, _merge_events(carried + ops, frames), now, force=force)
            # quarantine decay: restore index visibility for expired flags
            for rid_ in unq_ids:
                self.index.mark_quarantined(rid_, False)
            purged_ids = pre_ids - {r.id for r in kept}
            rep.records_purged = len(purged_ids)
            rep.hard_deleted_purged = len({op["id"] for op in folded
                                           if op.get("op") == "hard_delete" and op.get("id") in purged_ids})
            # write folded segment; the one fold that sees every copy retires
            # every op except the not-yet-due hard deletes, which keep their
            # original seqs in its header (atomic with it - never re-appended)
            old_names = [s["name"] for s in self.manifest.segments]
            name = f"seg-{ulid_new()}"
            if kept or pending_ops:
                self._write_segment(name, kept, self.manifest.seq, pending_ops, deferred)
            self.manifest.segments = (
                [self._segment_entry(name, kept, self.manifest.seq, "compact", pending_ops)]
                if kept or pending_ops
                else []
            )
            # an empty output is committed explicitly too ("" at this seq): the
            # old segments it replaces must never read as lost checkpoints
            self.manifest.checkpoint = name if kept or pending_ops else ""
            self.manifest.checkpoint_seq = self.manifest.seq
            self.index.flush()  # rows durable before advancing the watermark
            self.index.set_meta("applied_seq", str(self.manifest.seq))
            # commit the new-segment-only view BEFORE deleting anything it
            # replaces - see docstring crash-ordering note
            self._persist_manifest()
            # seal the writer BEFORE unlinking the wal it points at - see
            # _seal_wal_writer_locked for what happens otherwise
            self._seal_wal_writer_locked()
            self.store.delete(self.wal_key)
            self.store.delete(self.ops_key)
            for on in old_names:
                self.store.delete(f"{self.prefix}/{on}")
            self.manifest.wal_size = 0
            self.manifest.ops_size = 0
            self.manifest.wal_base_seq = self.manifest.seq  # next frame = base+1
            self._reset_wal_frames()
            # rebuild pending-purge tracking with only still-pending (not-due) ops
            self._pending_hard = []
            self._pending_hard_ids = set()
            for op in pending_ops:
                if op.get("id"):
                    self._track_pending_hard(op["id"], int(op.get("deadline", 0)))
            self._persist_manifest()
            METRICS.set_gauge("memd_pending_purges", len(self._pending_hard), ns=self.namespace)
            rep.segments_out = len(self.manifest.segments)
            rep.records_folded = len(kept)
            rep.bytes_after = self.store.size(f"{self.prefix}/{name}") if kept or pending_ops else 0
            self.index.invalidate_vec_cache()  # fold dead rows out of the scan matrix
        # Compaction is the natural snapshot point: the index has just been
        # folded and stamped with this manifest.seq, and compaction already
        # costs O(live) and runs off the request path (pass 17), so the
        # snapshot rides along instead of adding a new maintenance schedule.
        #
        # OUTSIDE the namespace lock, deliberately: the snapshot is an
        # O(index bytes) backup + gzip, and taking it under that lock blocked
        # every writer on the namespace for its whole duration (a writer sat
        # 900s on `with self._lock` in append). It reads a consistent image via
        # sqlite's online backup API, so it needs no such exclusion.
        self.write_index_snapshot()
        rep.duration_ms = int((time.monotonic() - t0) * 1000)
        return rep

    def _compaction_is_noop(self) -> bool:
        """True only when folding provably cannot change anything.

        Deliberately conservative - every condition must hold:
          - at most one segment, so there is nothing to merge;
          - an empty WAL and ops log, so there is nothing to apply;
          - no scheduled hard delete, so no purge deadline is waiting;
          - no quarantined rows, so no decay is pending.
        Any doubt falls through to the real compaction.
        """
        try:
            if len(self.manifest.segments) > 1:
                return False
            if self._pending_hard:
                return False
            if self.store.size(self.wal_key) or self.store.size(self.ops_key):
                return False
            if self.index.stats().get("quarantined"):
                return False
        except Exception:
            return False
        return True

    def rebuild_index(self) -> int:
        # lock: same completeness requirement as export - a concurrent
        # rotate/compact must not delete segments mid-replay, and concurrent
        # searches must never observe a wiped (partial) index
        with self._lock:
            self._ensure_open()
            versions, carried, _ = self._load_checkpoints()
            ops = self._read_ops()
            frames = self._wal_events("load-wal")
            self.index.wipe()
            # the same ordered replay as open (applying every op after every
            # record deleted re-adds and resurrected WAL-resident deletes)
            self._replay_into_index(versions, _merge_events(carried + ops, frames))
            self.index.flush()
            self.index.set_meta("applied_seq", str(self.manifest.seq))
        return len(set(versions) | {r.id for _, recs in frames for r in recs})

    # ------------------------------------------------------------------ exports / stats

    def export_jsonl_iter(self):
        """Streaming anti-lock-in export: yields one NDJSON line at a time.

        Why streaming: the previous formulation built the ENTIRE namespace
        as a single bytes blob before the first byte left the process -
        O(namespace) memory on a request path, gigabytes at hosted scale.
        This generator holds O(live records) as objects and O(1) line-sized
        serialization buffers (the residual object residency is inherent to
        in-process fold state; an external sort would be needed to go lower).
        Lock held only for the segment read phase - same completeness
        guarantee as before (a concurrent rotate/compact cannot delete
        segments mid-read), never across caller consumption.

        Exports what a read can see (_visible_records). It used to dump the
        raw stored copies with no op applied: records deleted - soft or hard,
        acked - came back out of every export until a compaction purged them,
        and superseded facts lost their supersedence."""
        with self._lock:
            recs = self._visible_records()
        recs.sort(key=lambda r: (r.time.t_ingested, r.id))
        for r in recs:
            yield json.dumps(r.to_dict(), separators=(",", ":")).encode() + b"\n"

    def _visible_records(self) -> list[MemoryRecord]:
        """Every record a read can see, folded from durable state in the one
        seq order open and compaction use. Soft- and hard-deleted records are
        left out - a hard-deleted one also while its purge is pending and its
        bytes still sit in a segment. Superseded and quarantined records stay
        (history; the flags travel with them)."""
        versions, carried, _ = self._load_checkpoints(where="export-segment")
        frames = self._wal_events("export-wal")
        kept, _pending, _unq, _folded, deferred = _fold_events(
            versions, _merge_events(carried + self._read_ops(), frames), now_ms(), force=False)
        return [r for r in kept if r.id not in deferred]

    def export_jsonl(self) -> bytes:
        # buffered variant for CLI/SDK callers that want one blob
        return b"".join(self.export_jsonl_iter())

    def stats(self) -> dict:
        st = self.index.stats()
        st["segments"] = len(self.manifest.segments)
        st["wal_bytes"] = self.manifest.wal_size
        # real on-disk bytes (was: a record count mislabeled as bytes)
        st["segment_bytes"] = sum(
            self.store.size(f"{self.prefix}/{s['name']}") for s in self.manifest.segments
        )
        return st

    def close(self) -> None:
        with self._lock:
            if self._manifest_dirty:
                self._persist_manifest()
        if self._wal_writer is not None:
            try:
                self._wal_writer.close()
            except Exception:
                pass
            self._wal_writer = None
        # fold the index watermark so the next open only replays the tail
        try:
            self.index.flush()
            self.index.set_meta("applied_seq", str(self.manifest.seq))
        except Exception:
            pass  # closed/deleted index; replay covers it
        self.index.close()
        if self._owner_path:
            _release_owner(self._owner_path)
            self._owner_path = None
        if self._owner_lease is not None:
            try:
                self.store.release_owner(self._owner_lease)
            except Exception:
                pass  # a lease expires on its own; never fail teardown on it
            self._owner_lease = None


class StorageEngine:
    """Manages namespaces over an ObjectStore. Stateless compute: any process
    can open any namespace by replaying the object-store log."""

    def __init__(
        self,
        root: str,
        envelope: KeyEnvelope | None = None,
        store: ObjectStore | None = None,
        cache_dir: str | None = None,
        wal_rotate_bytes: int = DEFAULT_WAL_ROTATE_BYTES,
        wal_rotate_frames: int = DEFAULT_WAL_ROTATE_FRAMES,
        max_open_namespaces: int = 64,
        lexical: dict | None = None,
    ):
        self.root = root
        # {"backend": "fts5"|"tantivy", "commit_ms", "commit_docs"}
        self.lexical = dict(lexical or {})
        self.store = store or LocalObjectStore(root)
        self.envelope = envelope
        self.cache_dir = cache_dir or os.path.join(root, "_cache")
        self.wal_rotate_bytes = wal_rotate_bytes
        self.wal_rotate_frames = wal_rotate_frames
        # LRU-bounded open-namespace table. Every open NamespaceStore pins a
        # SQLite connection + WAL handle + manifest; the tenant mix is
        # "many tiny, heavy tail, mostly idle" (D2), so an unbounded table
        # leaks fds/RAM in proportion to tenants EVER touched, not tenants
        # ACTIVE. Idle stores close cleanly: all state is durable blobs +
        # manifest; reopen replays the tail. Pinned namespaces (the facade's
        # default) are never evicted.
        self._namespaces: "OrderedDict[str, NamespaceStore]" = OrderedDict()
        self._pinned: set[str] = set()
        self.max_open_namespaces = max(1, int(max_open_namespaces))
        self._lock = threading.RLock()

    def pin_namespace(self, ns: str) -> None:
        """Mark a namespace as process-resident (never LRU-evicted). The
        Memory facade pins its default because it holds a direct reference
        that must stay valid across unrelated namespace churn."""
        ns = _validate_ns(ns)
        with self._lock:
            self._pinned.add(ns)
            if ns in self._namespaces:
                self._namespaces.move_to_end(ns)

    def _evict_locked(self) -> None:
        """Close + drop least-recently-used stores past the cap. A store
        whose ns-lock cannot be taken without blocking has an operation in
        flight - skip it this round rather than yank its index out from
        under the caller."""
        while len(self._namespaces) > self.max_open_namespaces:
            evicted_any = False
            for name in list(self._namespaces):  # oldest first (LRU order)
                if len(self._namespaces) <= self.max_open_namespaces:
                    return
                if name in self._pinned:
                    continue  # process-resident: never evicted
                victim = self._namespaces[name]
                if not victim._lock.acquire(blocking=False):
                    continue  # in-flight op on this store; retry next pass
                try:
                    # write-close BEFORE close(): a straggler holding an old
                    # reference must fail fast instead of appending alongside
                    # the store's reopened successor (forked WAL/seq state)
                    victim._closed = True
                    victim._evicted = True
                    try:
                        victim.close()  # flushes manifest + index watermark durably
                    except Exception:
                        METRICS.inc("memd_ns_evict_close_errors_total",
                                    help="errors while closing an evicted namespace store")
                finally:
                    victim._lock.release()
                del self._namespaces[name]
                METRICS.inc("memd_ns_evictions_total",
                            help="open namespace stores closed by the LRU cap")
                evicted_any = True
            if not evicted_any:
                return  # everything pinned or busy: cap exceeded by design

    def namespace(self, ns: str) -> NamespaceStore:
        ns = _validate_ns(ns)
        with self._lock:
            nstore = self._namespaces.get(ns)
            if nstore is None:
                nstore = NamespaceStore(
                    ns, self.store, self.cache_dir, self.envelope, self.wal_rotate_bytes,
                    self.wal_rotate_frames, lexical=self.lexical,
                )
                self._namespaces[ns] = nstore
                self._evict_locked()
            else:
                self._namespaces.move_to_end(ns)
            return nstore

    def peek_namespace(self, ns: str) -> "NamespaceStore | None":
        """Existing open store, or None - NEVER materializes one.

        Derived-lane writers (background embedding) must use this instead of
        namespace(): a by-name lookup that re-creates a popped store can
        resurrect a namespace mid-destroy and replay its not-yet-deleted WAL
        into a ghost index. If the target is gone (destroyed OR LRU-evicted),
        the work is dropped - the vector lane heals via reembed(), records
        were shredded on purpose."""
        ns = _validate_ns(ns)
        with self._lock:
            nstore = self._namespaces.get(ns)
            if nstore is not None:
                self._namespaces.move_to_end(ns)
            return nstore

    def list_namespaces(self) -> list[str]:
        out = []
        for k in self.store.list("ns/"):
            parts = k.split("/")
            if len(parts) >= 3 and parts[2] == "manifest.json":
                out.append(parts[1])
        return sorted(out)

    def destroy_namespace(self, ns: str) -> bool:
        """Crypto-shred: remove prefix + destroy per-namespace key + purge
        the local derived-index cache (a stale index must never resurrect
        shredded data). The namespace is marked closed FIRST so in-flight
        operations fail cleanly instead of hitting torn state."""
        ns = _validate_ns(ns)
        existed = False
        with self._lock:
            nstore = self._namespaces.pop(ns, None)
            if nstore is not None:
                # lifecycle flag blocks NEW operations, then the ns lock
                # serializes teardown against already-in-flight ones (an
                # export that started before destroy completes its read)
                nstore.mark_destroyed()
                with nstore._lock:
                    if self.store.exists(f"ns/{ns}/manifest.json"):
                        existed = True
                    # never persist state after shred (close() would
                    # otherwise rewrite a dirty manifest post-removal)
                    nstore._manifest_dirty = False
            # release handles BEFORE file removal so nothing re-writes them
            if nstore is not None:
                try:
                    nstore.close()
                except Exception:
                    pass
        n = self.store.remove_prefix(f"ns/{ns}")
        # purge derived index cache (rebuildable data tied to the shredded ns)
        safe = ns.replace("/", "__")
        idx_path = os.path.join(self.cache_dir, f"{safe}.sqlite")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(idx_path + suffix)
            except FileNotFoundError:
                pass
        # the tantivy accelerator holds the same text (tokenized): shred it too
        shutil.rmtree(os.path.join(self.cache_dir, f"{safe}.tantivy"), ignore_errors=True)
        if self.envelope is not None:
            self.envelope.destroy(ns)
        return existed or n > 0

    def close(self) -> None:
        with self._lock:
            for ns in self._namespaces.values():
                ns.close()
            self._namespaces.clear()


import re as _re

_NS_RE = _re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")


def _validate_ns(ns: str) -> str:
    if not ns or not _NS_RE.match(ns):
        raise ValueError(f"invalid namespace {ns!r}: must match [A-Za-z0-9][A-Za-z0-9_.-]{{0,127}}")
    return ns
