"""Durable storage engine (ADR-2): object storage is the source of truth.

Per-namespace layout under the object store:

    {ns}/wal.jsonl      active append log (record batches; fsync'd before ack)
    {ns}/ops.jsonl      mutation ops: tombstone / supersede / quarantine / hard_delete
    {ns}/seg-{ulid}     closed immutable segments (folded state at fold_seq)
    {ns}/manifest.json  {version, seq, segments[], wal_size, ops_size}

Invariants:
  - Segments are immutable; corrections arrive as ops; compaction folds.
  - Write ack = durable append (fsync) - no LLM, no embedding, no index wait.
  - The index (memd.index.NamespaceIndex) is a derived, rebuildable view kept
    on local disk (NVMe-class cache in hosted mode).
  - Hard delete: synchronous tombstone + physical purge guaranteed by forced
    compaction within the deadline (72h default, D7 control #8).
  - Namespace deletion = prefix removal + key destruction (crypto-shred).
"""
from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass, field

from memd.core.schema import MemoryRecord, now_ms, records_from_jsonl, records_to_jsonl, ulid_new
from memd.metrics import METRICS
from memd.index.sqlite_index import NamespaceIndex
from memd.storage.crypto import KeyEnvelope, NullKeyEnvelope
from memd.storage.objectstore import LocalObjectStore, ObjectStore

HARD_DELETE_DEADLINE_MS = 72 * 3600 * 1000
DEFAULT_WAL_ROTATE_BYTES = 8 * 1024 * 1024


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


@dataclass
class Manifest:
    version: int = 0
    seq: int = 0  # monotonic op/batch counter
    segments: list[dict] = field(default_factory=list)  # {name, records, fold_seq}
    wal_size: int = 0
    ops_size: int = 0
    wal_base_seq: int = 0  # seq counter value when the current wal opened

    def to_dict(self) -> dict:
        return {
            "version": self.version,
            "seq": self.seq,
            "segments": self.segments,
            "wal_size": self.wal_size,
            "ops_size": self.ops_size,
            "wal_base_seq": self.wal_base_seq,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Manifest":
        return cls(
            version=int(d.get("version", 0)),
            seq=int(d.get("seq", 0)),
            segments=list(d.get("segments") or []),
            wal_size=int(d.get("wal_size", 0)),
            ops_size=int(d.get("ops_size", 0)),
            wal_base_seq=int(d.get("wal_base_seq", 0)),
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


def _apply_ops(
    recs: list[MemoryRecord], ops: list[dict], now: int, force: bool
) -> tuple[list[MemoryRecord], list[dict], list[str]]:
    """Fold mutation ops into record state.

    Returns (live_records, pending_ops, unquarantined_ids). Pending ops are
    not-yet-due hard deletes; they must survive folds so the forced-
    compaction deadline (D7) stays enforceable. unquarantined_ids reports
    records whose quarantine expired during this fold - callers must clear
    the index flag so decay actually restores visibility."""
    by_id = {r.id: r for r in recs}
    hard_ids: set[str] = set()
    pending: list[dict] = []
    unquarantined: list[str] = []
    for op in ops:
        kind = op.get("op")
        target = by_id.get(op.get("id") or op.get("old"))
        if kind == "hard_delete":
            if force or op.get("deadline", 0) <= now:
                hard_ids.add(op["id"])
            else:
                pending.append(op)
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
    for r in recs:
        if r.id in hard_ids:
            continue
        if r.deleted:
            continue
        qexp = r.meta.get("quarantine_expires")
        if qexp is not None and qexp <= now:
            r.meta.pop("quarantined", None)
            r.meta.pop("quarantine_expires", None)
            r.meta["quarantine_expired"] = True
            unquarantined.append(r.id)
        kept.append(r)
    return kept, pending, unquarantined


class NamespaceStore:
    """One namespace: WAL + segments + ops + derived index."""

    def __init__(
        self,
        namespace: str,
        store: ObjectStore,
        cache_dir: str,
        envelope: KeyEnvelope | None = None,
        wal_rotate_bytes: int = DEFAULT_WAL_ROTATE_BYTES,
    ):
        self.namespace = namespace
        self.store = store
        self.envelope = envelope or NullKeyEnvelope()
        self.wal_rotate_bytes = wal_rotate_bytes
        self.prefix = f"ns/{namespace}"
        self.wal_key = f"{self.prefix}/wal"
        self.ops_key = f"{self.prefix}/ops"
        self.manifest_key = f"{self.prefix}/manifest.json"
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
        self.manifest = Manifest()
        self._manifest_dirty = False
        self._closed = False
        # scheduled physical purges: [(record_id, deadline_ms)] + id set - the
        # D7 hard-delete deadline is only a guarantee if something can SEE when
        # it comes due without rescanning the whole ops log. The set keeps
        # tracking idempotent (a rotate replaying an op must not double-count).
        self._pending_hard: list[tuple[str, int]] = []
        self._pending_hard_ids: set[str] = set()
        self._rotating = False  # re-entrancy guard for rotate-inside-append_ops
        self._open()

    # ------------------------------------------------------------------ open/recover

    def _open(self) -> None:
        raw = self.store.get(self.manifest_key)
        if raw:
            self.manifest = Manifest.from_dict(json.loads(raw))
        else:
            self._persist_manifest()
        ops = self._read_ops()
        if ops:
            self.manifest.seq = max(self.manifest.seq, max(o.get("seq", 0) for o in ops))
        # Index-applied watermark: the index records the highest manifest.seq it
        # has folded in. Replay covers segments + wal frames + ops with
        # seq > applied - so a fresh/restored cache rebuilds everything, while
        # a surviving cache only catches up on the tail. All idempotent.
        try:
            applied = int(self.index.get_meta("applied_seq") or 0)
        except ValueError:
            applied = 0
        seg_records: dict[str, MemoryRecord] = {}
        max_fold = 0
        for seg in self.manifest.segments:
            data = self.store.get(f"{self.prefix}/{seg['name']}")
            if data is None:
                continue
            fold = int(seg.get("fold_seq", 0))
            max_fold = max(max_fold, fold)
            if fold <= applied and applied > 0:
                continue  # already reflected in this index
            try:
                for rec in self._segment_records(data):
                    seg_records[rec.id] = rec
            except Exception:
                METRICS.inc("memd_storage_parse_errors_total", where="segment-replay", ns=namespace)
                continue
        max_fold = self._adopt_orphan_segments(seg_records, applied, max_fold)
        pending_ops = [o for o in ops if o.get("seq", 0) > max(applied, max((s.get("fold_seq", 0) for s in self.manifest.segments), default=0))]
        self._pending_hard = [
            (o["id"], int(o.get("deadline", 0))) for o in ops
            if o.get("op") == "hard_delete" and o.get("id")
        ]
        self._pending_hard_ids = {rid for rid, _ in self._pending_hard}
        self._apply_to_index(list(seg_records.values()), pending_ops, from_replay=True)
        wal = self.store.get(self.wal_key) or b""
        base = self.manifest.wal_base_seq
        recs = []
        good_end = 0
        idx = 0
        for end, fr in _frames_with_offsets(wal):
            frame_seq = base + idx + 1
            idx += 1
            if frame_seq <= applied:
                good_end = end
                continue
            try:
                recs.extend(records_from_jsonl(self._decrypt_frame(fr)))
                good_end = end
            except Exception:
                break
        if recs:
            self._apply_to_index(recs, [], from_replay=True)
        if good_end < len(wal):
            self.store.truncate(self.wal_key, good_end)  # torn-tail repair
        self.manifest.wal_size = self.store.size(self.wal_key)
        self.manifest.ops_size = sum(4 + len(f) for f in self._read_frames(self.ops_key))
        # commit replayed rows FIRST, only then advance the watermark -
        # otherwise a crash could persist the watermark while losing rows
        self.index.flush()
        self.index.set_meta("applied_seq", str(self.manifest.seq))
        self._persist_manifest()

    def _read_ops(self) -> list[dict]:
        ops = []
        for fr in self._read_frames(self.ops_key):
            try:
                ops.append(json.loads(self._decrypt_frame(fr)))
            except json.JSONDecodeError:
                METRICS.inc("memd_storage_parse_errors_total", where="ops-replay", ns=self.namespace)
                continue
        return ops

    # ------------------------------------------------------- segment blob format
    #
    # v2 blob (one put() payload):
    #   line 1: {"_seg": {"fold_seq": N}}          <- header, enables orphan adoption
    #   lines 2+: resolved record JSONL
    # The whole payload is envelope-encrypted when encryption is enabled
    # (segments are the durable source of truth - they must satisfy the same
    # at-rest guarantee as the WAL). Legacy v1 blobs are bare plaintext JSONL;
    # _segment_records falls back for them so old stores keep reading.

    @staticmethod
    def _segment_blob(kept: list[MemoryRecord], fold_seq: int) -> bytes:
        header = json.dumps({"_seg": {"fold_seq": fold_seq}}, separators=(",", ":"))
        body = records_to_jsonl(kept)
        return header.encode() + b"\n" + body if body.strip() else header.encode() + b"\n"

    def _write_segment(self, name: str, kept: list[MemoryRecord], fold_seq: int) -> bytes:
        data = self._segment_blob(kept, fold_seq)
        if self.envelope.enabled:
            data = self.envelope.encrypt(self.namespace, data)
        self.store.put(f"{self.prefix}/{name}", data)
        return data

    def _segment_records(self, data: bytes) -> tuple[list[MemoryRecord], int]:
        """Parse a segment blob -> (records, fold_seq). Tolerates legacy
        plaintext blobs and encrypted v2 blobs; fold_seq defaults to 0 when
        absent (legacy), which disables that blob's orphan eligibility."""
        raw = self._decrypt_frame(data)
        first_nl = raw.find(b"\n")
        head = raw[:first_nl] if first_nl != -1 else b""
        fold_seq = 0
        recs_data = raw
        if head.startswith(b"{"):
            try:
                h = json.loads(head)
                if isinstance(h.get("_seg"), dict):
                    fold_seq = int(h["_seg"].get("fold_seq", 0))
                    recs_data = raw[first_nl + 1 :]
            except (ValueError, TypeError):
                pass  # legacy single-record blob starting with '{'
        return records_from_jsonl(recs_data), fold_seq

    def _adopt_orphan_segments(self, seg_records: dict, applied: int, max_fold: int) -> int:
        """Heal segments left on disk but never referenced by a persisted
        manifest (crash inside the old rotate/compact delete window).

        Adoption is conservative: only blobs carrying a v2 header whose
        fold_seq EXCEEDS every referenced segment's fold_seq are adopted -
        those hold the newest resolved state. Older unreferenced files are
        cleanup residue from an interrupted compaction and must be ignored
        (their live content already exists in newer segments; re-folding
        them could resurrect tombstoned/superseded rows)."""
        try:
            all_keys = set(self.store.list(f"{self.prefix}/"))
        except Exception:
            return max_fold
        referenced = {f"{self.prefix}/{s['name']}" for s in self.manifest.segments}
        adopted = 0
        for key in sorted(all_keys):
            base = key.rsplit("/", 1)[-1]
            if key in referenced or not base.startswith("seg-"):
                continue
            data = self.store.get(key)
            if not data:
                continue
            try:
                recs, fold_seq = self._segment_records(data)
            except Exception:
                continue  # unreadable/legacy/foreign blob: leave it alone
            if recs == [] or fold_seq <= max_fold or fold_seq <= applied:
                continue
            for rec in recs:
                seg_records[rec.id] = rec
            self.manifest.segments.append(
                {"name": base, "records": len(recs), "fold_seq": fold_seq, "reason": "orphan-adopted"}
            )
            max_fold = max(max_fold, fold_seq)
            adopted += 1
        if adopted:
            METRICS.inc("memd_orphan_segments_adopted_total", amount=adopted, ns=self.namespace)
            self._persist_manifest()
        return max_fold

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
            raise RuntimeError(f"namespace {self.namespace!r} destroyed")

    def mark_destroyed(self) -> None:
        """Lifecycle hook: block new operations before teardown begins."""
        self._closed = True

    def append(self, records: list[MemoryRecord]) -> int:
        """Durable append + synchronous index apply (read-your-writes).
        Group commit: the caller's bytes are written (OS-visible) under the
        write lock; durability waits for an fsync that started after those
        bytes - concurrent appenders share fsyncs instead of serializing."""
        if not records:
            return self.manifest.wal_size
        payload = records_to_jsonl(records)
        with self._lock:
            self._ensure_open()
            frame = _frame_encode(self.envelope.encrypt(self.namespace, payload))
            my_end, my_gen = self._wal_write(frame)
            self.manifest.wal_size = my_end
            self.manifest.seq += 1
            self._manifest_dirty = True
            self._durably_written(my_end, my_gen)
            qflags = {r.id: bool(r.meta.get("quarantined")) for r in records}
            self.index.upsert_batch([(r, None, "") for r in records], qflags)
            if my_end >= self.wal_rotate_bytes:
                self.rotate("size")
            return self.manifest.wal_size

    def _wal_write(self, frame: bytes) -> tuple[int, int]:
        """Write a frame OS-visible. Returns (end offset, log generation).
        Writer handle acquired under the write lock: rotation seals the old
        handle under the same lock, so a write can never target a closed fd."""
        with self._wlock:
            if not hasattr(self.store, "open_log"):
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

    def _writer(self):
        if getattr(self.store, "open_log", None) is None:
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
                    cur = self._writer()
                    cur.sync()
                except (OSError, ValueError):
                    # writer closed by a concurrent rotate: our frame is in
                    # the folded segment - durable
                    with self._sync_lock:
                        self._sync_busy = False
                    return
                with self._sync_lock:
                    self._synced_pos = max(self._synced_pos, self._written_pos)
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
            size = self.store.append(self.ops_key, b"".join(frames))
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
            # seal the persistent writer: take BOTH locks so an in-flight
            # append finishes its write (wlock) and its fsync (sync lock)
            # before the fd closes - otherwise writers hit closed-file errors
            # and lose frames
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
            self._rotating = True
            try:
                return self._rotate_locked(reason)
            finally:
                self._rotating = False

    def _rotate_locked(self, reason: str) -> str:
        """Fold body (caller holds the ns lock and set _rotating)."""
        METRICS.inc("memd_rotations_total", ns=self.namespace, reason=reason)
        recs: list[MemoryRecord] = []
        for fr in self._read_frames(self.wal_key):
            try:
                recs.extend(records_from_jsonl(self._decrypt_frame(fr)))
            except Exception:
                METRICS.inc("memd_storage_parse_errors_total", where="rotate-wal", ns=self.namespace)
                continue
        ops = self._read_ops()
        kept, pending_ops, unq_ids = _apply_ops(recs, ops, now_ms(), force=False)
        # quarantine decay: restore index visibility for expired flags
        for rid_ in unq_ids:
            self.index.mark_quarantined(rid_, False)
        name = f"seg-{ulid_new()}"
        if kept:
            self._write_segment(name, kept, self.manifest.seq)
            self.manifest.segments.append(
                {"name": name, "records": len(kept), "fold_seq": self.manifest.seq, "reason": reason}
            )
        self.index.flush()  # rows durable before advancing watermark
        self.index.set_meta("applied_seq", str(self.manifest.seq))
        self._persist_manifest()
        # truncate logs (object-store friendly: rewrite empty)
        self.store.delete(self.wal_key)
        self.store.delete(self.ops_key)
        self.manifest.wal_size = 0
        self.manifest.ops_size = 0
        self.manifest.wal_base_seq = self.manifest.seq  # next frame = base+1
        self._synced_pos = 0
        self._written_pos = 0
        self._persist_manifest()
        # rebuild pending-purge tracking with only still-pending (not-due) ops
        self._pending_hard = []
        self._pending_hard_ids = set()
        self.append_ops(pending_ops)  # one fsync; _rotating guard blocks nested folds
        METRICS.set_gauge("memd_pending_purges", len(self._pending_hard), ns=self.namespace)
        return name

    # ------------------------------------------------------------------ maintenance

    def load_all_records(self) -> tuple[list[MemoryRecord], list[dict]]:
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
        ops: list[dict] = []
        for fr in self._read_frames(self.ops_key):
            try:
                ops.append(json.loads(self._decrypt_frame(fr)))
            except json.JSONDecodeError:
                METRICS.inc("memd_storage_parse_errors_total", where="ops-load", ns=self.namespace)
                continue
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
            recs, ops = self.load_all_records()
            rep.bytes_before = sum(self.store.size(f"{self.prefix}/{s['name']}") for s in self.manifest.segments)
            rep.segments_in = len(self.manifest.segments)
            now = now_ms()
            pre_ids = {r.id for r in recs}
            kept, pending_ops, unq_ids = _apply_ops(recs, ops, now, force=force)
            # quarantine decay: restore index visibility for expired flags
            for rid_ in unq_ids:
                self.index.mark_quarantined(rid_, False)
            purged_ids = pre_ids - {r.id for r in kept}
            rep.records_purged = len(purged_ids)
            rep.hard_deleted_purged = sum(1 for op in ops if op.get("op") == "hard_delete" and op.get("id") in purged_ids)
            # write folded segment
            old_names = [s["name"] for s in self.manifest.segments]
            name = f"seg-{ulid_new()}"
            if kept:
                self._write_segment(name, kept, self.manifest.seq)
            self.manifest.segments = (
                [{"name": name, "records": len(kept), "fold_seq": self.manifest.seq, "reason": "compact"}]
                if kept
                else []
            )
            self.index.flush()  # rows durable before advancing the watermark
            self.index.set_meta("applied_seq", str(self.manifest.seq))
            # commit the new-segment-only view BEFORE deleting anything it
            # replaces - see docstring crash-ordering note
            self._persist_manifest()
            self.store.delete(self.wal_key)
            self.store.delete(self.ops_key)
            for on in old_names:
                self.store.delete(f"{self.prefix}/{on}")
            self.manifest.wal_size = 0
            self.manifest.ops_size = 0
            # rebuild pending-purge tracking with only still-pending (not-due)
            # ops, re-persisted in ONE durable append
            self._pending_hard = []
            self._pending_hard_ids = set()
            self.append_ops(pending_ops)
            self._persist_manifest()
            METRICS.set_gauge("memd_pending_purges", len(self._pending_hard), ns=self.namespace)
            rep.segments_out = len(self.manifest.segments)
            rep.records_folded = len(kept)
            rep.bytes_after = self.store.size(f"{self.prefix}/{name}") if kept else 0
            rep.duration_ms = int((time.monotonic() - t0) * 1000)
            self.index.invalidate_vec_cache()  # fold dead rows out of the scan matrix
        return rep

    def rebuild_index(self) -> int:
        # lock: same completeness requirement as export - a concurrent
        # rotate/compact must not delete segments mid-replay, and concurrent
        # searches must never observe a wiped (partial) index
        with self._lock:
            self._ensure_open()
            recs, ops = self.load_all_records()
            self.index.wipe()
            self._apply_to_index(recs, ops)
            self.index.flush()
            self.index.set_meta("applied_seq", str(self.manifest.seq))
        return len(recs)

    # ------------------------------------------------------------------ exports / stats

    def export_jsonl(self) -> bytes:
        # lock: a concurrent rotate/compact must not delete segments mid-read
        # (anti-lock-in export must be complete)
        with self._lock:
            recs, _ = self.load_all_records()
        recs.sort(key=lambda r: (r.time.t_ingested, r.id))
        return records_to_jsonl(recs)

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
    ):
        self.root = root
        self.store = store or LocalObjectStore(root)
        self.envelope = envelope
        self.cache_dir = cache_dir or os.path.join(root, "_cache")
        self.wal_rotate_bytes = wal_rotate_bytes
        self._namespaces: dict[str, NamespaceStore] = {}
        self._lock = threading.RLock()

    def namespace(self, ns: str) -> NamespaceStore:
        ns = _validate_ns(ns)
        with self._lock:
            if ns not in self._namespaces:
                self._namespaces[ns] = NamespaceStore(
                    ns, self.store, self.cache_dir, self.envelope, self.wal_rotate_bytes
                )
            return self._namespaces[ns]

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
