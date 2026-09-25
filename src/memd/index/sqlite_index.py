"""Derived index for one namespace (ADR-5): indexes are rebuildable views.

Backing: SQLite (WAL mode) providing
  - BM25 via FTS5 (tantivy-class sparse retrieval without a second service)
  - flat exact vector scan via a cached numpy matrix (size-adaptive strategy:
    namespaces < ~50K vectors - the vast majority - get perfect recall here;
    IVF-PQ slots behind VectorSearchStrategy later without API change)
  - btree columns for time / entity / scope / validity filtering
  - optionally, a tantivy accelerator for the bm25 lane (memd[fast]; see
    memd.index.tantivy_lexical): a derived view of this index, attached by
    the namespace store; FTS5 stays the synchronous source of truth

Durability contract: the durable append (segments) is the source of truth;
this index is committed synchronously before write-ack to give immediate
read-your-writes, but a lost index commit is always recoverable by replaying
segments (synchronous=NORMAL is therefore correct, not a shortcut).
"""
from __future__ import annotations

import contextlib
import json
import os
import re
import sqlite3
import threading
import time
from dataclasses import dataclass, field

import numpy as np

from memd.core.schema import MemoryRecord, Scope, Source, now_ms
from memd.metrics import METRICS

# entity segments are dot-parts of normalize_entity_key output: [a-z0-9_-]+
_SEG_CHARS = re.compile(r"[^a-z0-9_-]")

# Minimum cosine for the vector lane to report a hit: kills zero-evidence
# matches that are harmless in ranked search but dangerous for unbounded
# sweeps (find_ids). Override with MEMD_MIN_COSINE if an embedding model
# needs a different operating point.
MIN_COSINE = float(os.environ.get("MEMD_MIN_COSINE", "0.02"))

SCHEMA_VERSION = 2

# Vectors are stored as float16. A 384-dim float32 vector costs 1536 bytes on
# disk (2053 B/record measured, including row + index overhead) and was the
# single largest contributor to write amplification - on a 90-byte record the
# vector ALONE was 22.8x the raw bytes against a <=3x SLO. Halving it is free
# in quality terms: these are L2-normalised vectors and cosine is computed in
# float32 after upcast, so the only loss is ~3 decimal digits of mantissa on
# values in [-1, 1].
VEC_DTYPE = np.float16


def _vec_blob(vec: "np.ndarray") -> bytes:
    v = np.asarray(vec, dtype=np.float32).ravel()
    nrm = float(np.linalg.norm(v))
    if nrm > 0:
        v = v / nrm
    return v.astype(VEC_DTYPE).tobytes()


def _vec_from_blob(blob: bytes, dim: int) -> "np.ndarray":
    """Decode one stored vector, tolerating pre-v2 float32 blobs."""
    dtype = VEC_DTYPE if dim and len(blob) == dim * 2 else np.float32
    return np.frombuffer(blob, dtype=dtype).astype(np.float32)


@dataclass(frozen=True)
class IndexFilter:
    scope: Scope | None = None
    kinds: tuple[str, ...] | None = None
    as_of: int | None = None  # None => currently-valid view
    t_event_min: int | None = None
    t_event_max: int | None = None
    entity_keys: tuple[str, ...] | None = None
    sources: tuple[str, ...] | None = None
    include_invalid: bool = False
    include_quarantined: bool = False
    exclude_ids: frozenset[str] = field(default_factory=frozenset)
    now: int | None = None  # boundary for not-yet-valid records (defaults to query time)


@dataclass(frozen=True)
class Hit:
    record: MemoryRecord
    score: float  # lane-specific raw score (bm25 rank, cosine, recency...)
    lane: str  # "bm25" | "vector" | "time" | "entity" | "link"


class NamespaceIndex:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.path = path
        try:
            os.chmod(path, 0o600)  # records may be sensitive; local file only
        except OSError:
            pass
        self._lock = threading.RLock()
        # vector cache: main matrix + bounded overflow block. Per-query cost is
        # O(main) scan + O(overflow); overflow folds into main when it exceeds
        # OVERFLOW_MAX rows, so write bursts never trigger O(total) copies.
        self._main_mat: np.ndarray = np.zeros((0, 0), dtype=np.float32)
        self._main_ids: list[str] = []
        self._ovf_ids: list[str] = []
        self._ovf_vecs: list[np.ndarray] = []
        self._vec_loaded = False
        self.OVERFLOW_MAX = 4096
        self._con = sqlite3.connect(path, check_same_thread=False)
        self._con.row_factory = sqlite3.Row
        # Reads used to share the WRITER connection under one RLock, so a
        # namespace served exactly one query at a time. Measured on 8000
        # records: 26.3 q/s at 1 thread, 52.4 at 2, then DOWN to 49.3 at 4 and
        # 41.5 at 8, with p99 46.9ms -> 288.1ms. SQLite in WAL mode already
        # supports concurrent readers alongside one writer; the lock was the
        # only thing preventing it. Each reader thread gets its own connection.
        self._local = threading.local()
        self._reader_cons: list = []
        self._reader_lock = threading.Lock()
        # close() must not free a connection another thread is executing on -
        # that is a SEGFAULT, not an exception. Before reads moved off the
        # writer lock they were serialized against close() by that same lock;
        # now they need their own barrier. Readers are counted, close() drains
        # them. The counter is held only around the execute, never during it,
        # so concurrency is unaffected.
        self._readers_active = 0
        self._readers_gone = threading.Condition(self._reader_lock)
        self._pending = False  # writes committed lazily, not yet visible cross-connection
        self._lazy_commits = 0
        self._commit_threshold = 64
        self._closed = False
        self._stats_cache: dict | None = None
        self._stats_at = 0.0
        # optional lexical accelerator (TantivyLexical); every mutation below
        # tells it which rows changed BEFORE the change becomes visible
        self.lexical = None
        self._con.execute("PRAGMA journal_mode=WAL")
        self._con.execute("PRAGMA synchronous=NORMAL")
        self._con.execute("PRAGMA temp_store=MEMORY")
        self._con.execute("PRAGMA busy_timeout=5000")
        self._migrate()
        # rowids are never reused (see _note_rowid_hwm): the highest rowid
        # ever handed out, and whether MAX(rowid) may have fallen below it
        row = self._con.execute("SELECT v FROM meta WHERE k='rowid_hwm'").fetchone()
        self._rowid_hwm = int(row[0]) if row else 0
        self._rowid_gap = self._rowid_hwm > 0

    def _migrate(self) -> None:
        c = self._con
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
            CREATE TABLE IF NOT EXISTS records(
              id TEXT PRIMARY KEY,
              kind TEXT NOT NULL,
              content TEXT NOT NULL,
              scope_org TEXT, scope_agent TEXT, scope_user TEXT, scope_session TEXT,
              source INTEGER NOT NULL,
              actor_id TEXT, prov_session TEXT,
              lineage TEXT NOT NULL DEFAULT '[]',
              extractor TEXT,
              t_event INTEGER NOT NULL,
              t_ingested INTEGER NOT NULL,
              valid_from INTEGER,
              invalidated_at INTEGER,
              superseded_by TEXT,
              entity_keys TEXT NOT NULL DEFAULT '[]',
              embedding_version TEXT,
              meta TEXT NOT NULL DEFAULT '{}',
              quarantined INTEGER NOT NULL DEFAULT 0,
              deleted INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS ix_rec_kind ON records(kind);
            CREATE INDEX IF NOT EXISTS ix_rec_tevent ON records(t_event);
            CREATE INDEX IF NOT EXISTS ix_rec_scope ON records(scope_org, scope_user, scope_session);
            -- session-close sweeps filter on scope_session ALONE, which the
            -- composite above cannot serve (its leading columns are
            -- unconstrained), so every segment close scanned the whole
            -- namespace instead of one session's rows
            CREATE INDEX IF NOT EXISTS ix_rec_session ON records(scope_session, kind);
            CREATE INDEX IF NOT EXISTS ix_rec_valid ON records(invalidated_at, deleted);
            CREATE TABLE IF NOT EXISTS vectors(
              id TEXT PRIMARY KEY REFERENCES records(id) ON DELETE CASCADE,
              dim INTEGER NOT NULL,
              model TEXT NOT NULL,
              vec BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS entities(
              entity_key TEXT NOT NULL,
              record_id TEXT NOT NULL REFERENCES records(id) ON DELETE CASCADE,
              PRIMARY KEY(entity_key, record_id)
            );
            CREATE TABLE IF NOT EXISTS entity_segments(
              segment TEXT NOT NULL,
              entity_key TEXT NOT NULL,
              PRIMARY KEY(segment, entity_key)
            );
            CREATE TABLE IF NOT EXISTS links(
              src TEXT NOT NULL,
              dst TEXT NOT NULL,
              link_type TEXT NOT NULL,
              PRIMARY KEY(src, dst, link_type)
            );
            CREATE INDEX IF NOT EXISTS ix_links_dst ON links(dst);
            -- EXTERNAL-CONTENT fts5: the index reads content from `records`
            -- instead of storing its own copy. The previous plain fts5 kept a
            -- second full copy of every record's text - measured at 10.27MB
            -- beside records.content's 10.27MB on a 10K x 800B corpus, i.e.
            -- one third of all durable bytes spent duplicating content that
            -- already exists two feet away. Triggers keep it in sync, which
            -- also deletes the manual fts maintenance the write path carried.
            CREATE VIRTUAL TABLE IF NOT EXISTS fts USING fts5(
              content, content='records', content_rowid='rowid',
              tokenize='porter ascii'
            );
            CREATE TRIGGER IF NOT EXISTS records_ai AFTER INSERT ON records BEGIN
              INSERT INTO fts(rowid, content) VALUES (new.rowid, new.content);
            END;
            CREATE TRIGGER IF NOT EXISTS records_ad AFTER DELETE ON records BEGIN
              INSERT INTO fts(fts, rowid, content) VALUES('delete', old.rowid, old.content);
            END;
            CREATE TRIGGER IF NOT EXISTS records_au AFTER UPDATE ON records BEGIN
              INSERT INTO fts(fts, rowid, content) VALUES('delete', old.rowid, old.content);
              INSERT INTO fts(rowid, content) VALUES (new.rowid, new.content);
            END;
            """
        )
        v = c.execute("SELECT v FROM meta WHERE k='schema_version'").fetchone()
        if v is not None and int(v[0]) < SCHEMA_VERSION:
            # v1 -> v2: the fts table changed shape (content-duplicating ->
            # external-content). It is derived data, so rebuild rather than
            # migrate: drop and repopulate from `records` in one statement.
            c.executescript(
                """
                DROP TRIGGER IF EXISTS records_ai;
                DROP TRIGGER IF EXISTS records_ad;
                DROP TRIGGER IF EXISTS records_au;
                DROP TABLE IF EXISTS fts;
                CREATE VIRTUAL TABLE fts USING fts5(
                  content, content='records', content_rowid='rowid',
                  tokenize='porter ascii'
                );
                CREATE TRIGGER records_ai AFTER INSERT ON records BEGIN
                  INSERT INTO fts(rowid, content) VALUES (new.rowid, new.content);
                END;
                CREATE TRIGGER records_ad AFTER DELETE ON records BEGIN
                  INSERT INTO fts(fts, rowid, content) VALUES('delete', old.rowid, old.content);
                END;
                CREATE TRIGGER records_au AFTER UPDATE ON records BEGIN
                  INSERT INTO fts(fts, rowid, content) VALUES('delete', old.rowid, old.content);
                  INSERT INTO fts(rowid, content) VALUES (new.rowid, new.content);
                END;
                INSERT INTO fts(rowid, content) SELECT rowid, content FROM records;
                """
            )
            # v1 -> v2 also halves stored vectors. Convert in place rather than
            # dropping them: a rebuild would be correct (they are derived) but
            # would cost a full re-embed, which for a BYO-key deployment is
            # real money.
            for _id, _dim, _blob in c.execute("SELECT id, dim, vec FROM vectors").fetchall():
                if _dim and len(_blob) == int(_dim) * 4:
                    c.execute("UPDATE vectors SET vec=? WHERE id=?",
                              (_vec_blob(np.frombuffer(_blob, dtype=np.float32)), _id))
            c.execute("UPDATE meta SET v=? WHERE k='schema_version'", (str(SCHEMA_VERSION),))
        if v is None:
            c.execute("INSERT INTO meta(k,v) VALUES('schema_version',?)", (str(SCHEMA_VERSION),))
        c.commit()

    def attach_lexical(self, lexical) -> None:
        self.lexical = lexical

    def close(self) -> None:
        lex, self.lexical = self.lexical, None
        if lex is not None:
            # its final batch reads this index, so it goes first
            try:
                lex.close()
            except Exception:
                pass
        with self._lock:
            self.flush()
            self._closed = True
            self._con.close()
        with self._reader_lock:
            # _closed is already True, so no NEW reader can start; wait for the
            # ones in flight before freeing their connections
            while self._readers_active:
                if not self._readers_gone.wait(timeout=5.0):
                    break  # never hang teardown on a wedged reader
            cons, self._reader_cons = self._reader_cons, []
        for c in cons:  # per-thread read connections hold their own fds
            try:
                c.close()
            except Exception:
                pass

    # ------------------------------------------------------------------ writes

    def upsert(self, rec: MemoryRecord, vector: np.ndarray | None = None, model: str = "", quarantined: bool = False) -> None:
        self.upsert_batch([(rec, vector, model)], {rec.id: quarantined})

    def upsert_batch(
        self,
        items: list[tuple[MemoryRecord, np.ndarray | None, str]],
        quarantined: dict[str, bool] | None = None,
    ) -> None:
        quarantined = quarantined or {}
        with self._lock:
            if self._closed:
                return
            c = self._con
            try:
                if self.lexical is not None:
                    # an upsert that overwrites an existing id (a native
                    # import keeps ids) changes a row the accelerator may
                    # already hold: its content, scope or flags must be
                    # re-indexed, not just rows above the watermark
                    self._lex_touch(self._existing_ids(c, [rec.id for rec, _, _ in items]))
                rowid = self._next_rowid_locked(c)
                for rec, vec, model in items:
                    p = rec.provenance
                    c.execute(
                        """INSERT INTO records(rowid,id,kind,content,scope_org,scope_agent,scope_user,scope_session,
                             source,actor_id,prov_session,lineage,extractor,t_event,t_ingested,valid_from,
                             invalidated_at,superseded_by,entity_keys,embedding_version,meta,quarantined,deleted)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                           ON CONFLICT(id) DO UPDATE SET
                             kind=excluded.kind, content=excluded.content,
                             scope_org=excluded.scope_org, scope_agent=excluded.scope_agent,
                             scope_user=excluded.scope_user, scope_session=excluded.scope_session,
                             source=excluded.source, actor_id=excluded.actor_id, prov_session=excluded.prov_session,
                             lineage=excluded.lineage, extractor=excluded.extractor,
                             t_event=excluded.t_event, t_ingested=excluded.t_ingested,
                             valid_from=excluded.valid_from, invalidated_at=excluded.invalidated_at,
                             superseded_by=excluded.superseded_by, entity_keys=excluded.entity_keys,
                             embedding_version=excluded.embedding_version, meta=excluded.meta,
                             quarantined=excluded.quarantined, deleted=excluded.deleted""",
                        (
                            rowid, rec.id, rec.kind, rec.content,
                            rec.scope.org, rec.scope.agent, rec.scope.user, rec.scope.session,
                            int(p.source), p.actor_id, p.session_id,
                            json.dumps(p.lineage),
                            json.dumps(p.extractor.to_dict()) if p.extractor else None,
                            rec.time.t_event, rec.time.t_ingested, rec.time.valid_from,
                            rec.time.invalidated_at, rec.time.superseded_by,
                            json.dumps(rec.entity_keys), rec.embedding_version,
                            json.dumps(rec.meta), int(quarantined.get(rec.id, False)), int(rec.deleted),
                        ),
                    )
                    if rowid is not None:
                        rowid += 1  # (an overwrite leaves a gap: harmless)
                    c.execute("DELETE FROM entities WHERE record_id=?", (rec.id,))
                    c.executemany(
                        "INSERT OR IGNORE INTO entities(entity_key, record_id) VALUES(?,?)",
                        [(k, rec.id) for k in rec.entity_keys],
                    )
                    c.execute("DELETE FROM entity_segments WHERE entity_key IN (SELECT entity_key FROM entities WHERE record_id=?)", (rec.id,))
                    segs = {(part, k) for k in rec.entity_keys for part in k.split(".") if part}
                    c.executemany(
                        "INSERT OR IGNORE INTO entity_segments(segment, entity_key) VALUES(?,?)",
                        list(segs),
                    )
                    if rec.kind == "link":
                        lt = rec.meta.get("link_type", "refers_to")
                        dst = rec.meta.get("target")
                        if dst:
                            c.execute("INSERT OR REPLACE INTO links(src,dst,link_type) VALUES(?,?,?)", (rec.id, dst, lt))
                    if vec is not None:
                        c.execute(
                            "INSERT OR REPLACE INTO vectors(id,dim,model,vec) VALUES(?,?,?,?)",
                            (rec.id, int(vec.shape[0]), model, _vec_blob(vec)),
                        )
                # new rows land above the lexical watermark, where the FTS5
                # tail serves them until the accelerator indexes them
                if self.lexical is not None:
                    self.lexical.note_new(len(items))
                # no per-batch commit: lazy via _maybe_commit (replay-safe)
            except Exception:
                self.flush()  # don't leave a broken transaction open
                raise
            finally:
                pass  # vector cache updates flow through set_vector (incremental)
        self._stats_cache = None  # writes invalidate the stats cache
        self._maybe_commit()

    def set_meta(self, k: str, v: str) -> None:
        with self._lock:
            self._con.execute("INSERT OR REPLACE INTO meta(k,v) VALUES(?,?)", (k, v))
            self._con.commit()

    def get_meta(self, k: str) -> str | None:
        with self._lock:
            row = self._con.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
            return row[0] if row else None

    def _invalidate_stats(self) -> None:
        self._stats_cache = None

    def flush(self) -> None:
        with self._lock:
            if self._closed:
                return
            if self._con.in_transaction:
                self._con.commit()
                METRICS.inc("memd_index_commits_total", forced="true")
            self._lazy_commits = 0
            self._pending = False

    @contextlib.contextmanager
    def _read(self):
        """Borrow this thread's read connection for one statement.

        Raises RuntimeError once the index is closed, which is how every other
        lifecycle race in this engine already surfaces (the HTTP layer maps it
        to a clean 503/410 and the destroy-race tests whitelist it)."""
        with self._reader_lock:
            if self._closed:
                raise RuntimeError("index closed")
            self._readers_active += 1
        try:
            yield self._reader()
        finally:
            with self._reader_lock:
                self._readers_active -= 1
                if self._readers_active == 0:
                    self._readers_gone.notify_all()

    def _reader(self) -> sqlite3.Connection:
        """This thread's read connection.

        Read-your-writes is preserved explicitly: the writer commits lazily
        (up to _commit_threshold batches), and uncommitted rows are invisible
        to any OTHER connection - the old design relied on readers sharing the
        writer's connection, which is exactly what serialized them. So a read
        publishes pending work first. That costs one commit after a write
        burst, not one per read, and under WAL + synchronous=NORMAL a commit
        does not fsync.
        """
        if self._pending:
            self.flush()
        con = getattr(self._local, "con", None)
        if con is None:
            con = sqlite3.connect(self.path, check_same_thread=False)
            con.row_factory = sqlite3.Row
            con.execute("PRAGMA busy_timeout=5000")
            self._local.con = con
            with self._reader_lock:
                self._reader_cons.append(con)
        return con

    def _maybe_commit(self) -> None:
        """Lazy commit: readers share this connection (single-process embedded
        engine), so uncommitted rows are visible; a lost commit on crash is
        recoverable by replaying segments. Commit amortizes fsyncs."""
        self._lazy_commits += 1
        self._pending = True
        if self._lazy_commits >= self._commit_threshold:
            METRICS.inc("memd_index_commits_total", forced="false")
            self.flush()

    OVERFLOW_MAX = 4096

    def set_vector(self, record_id: str, vec: np.ndarray, model: str) -> None:
        blob = _vec_blob(vec)
        # the in-RAM cache keeps float32 (BLAS scans it); only the DURABLE
        # copy is halved, so search precision is untouched
        v = np.asarray(vec, dtype=np.float32).ravel()
        _n = float(np.linalg.norm(v))
        if _n > 0:
            v = v / _n
        with self._lock:
            if self._closed:
                METRICS.inc("memd_index_write_after_close_total", ns=self._ns_hint)
                return
            # durable store write always happens; the in-memory cache append
            # is skipped only when the cache has not been built yet (the next
            # search loads everything from sqlite)
            self._con.execute(
                "INSERT OR REPLACE INTO vectors(id,dim,model,vec) VALUES(?,?,?,?)",
                (record_id, int(vec.shape[0]), model, blob),
            )
            self._con.execute("UPDATE records SET embedding_version=? WHERE id=?", (model, record_id))
            if self._vec_loaded:
                dim = self._main_mat.shape[1] if self._main_mat.size else int(v.shape[0])
                if int(v.shape[0]) == dim or self._main_mat.size == 0:
                    # v is already unit-norm; overflow append is O(P), P
                    # bounded by fold threshold - never O(total) per write
                    self._ovf_ids.append(record_id)
                    self._ovf_vecs.append(v)
                    if len(self._ovf_ids) >= self.OVERFLOW_MAX:
                        self._fold_overflow_locked()
                else:
                    self.invalidate_vec_cache()
        self._invalidate_stats()
        self._maybe_commit()

    def _lex_touch(self, ids) -> None:
        if self.lexical is not None:
            self.lexical.touch(list(ids))

    @staticmethod
    def _existing_ids(c: sqlite3.Connection, ids: list[str]) -> list[str]:
        out: list[str] = []
        for i in range(0, len(ids), 500):
            chunk = ids[i:i + 500]
            out += [r[0] for r in c.execute(
                f"SELECT id FROM records WHERE id IN ({','.join('?' * len(chunk))})",  # nosec B608
                chunk).fetchall()]
        return out

    def _note_rowid_hwm(self, c: sqlite3.Connection) -> None:
        """Rowids are never reused. Called BEFORE a physical delete, inside
        its transaction: remember the highest rowid ever handed out.

        SQLite gives a new row MAX(rowid)+1, so deleting the newest rows
        hands their rowids out again. The lexical accelerator indexes by
        rowid order (everything above its watermark is new), and a reused
        rowid lands below the watermark, where nothing rescans: the record
        was missing from the bm25 lane for good. The mark is written in the
        same transaction as the delete, so no crash can separate the two,
        and upsert_batch places new rows above it (AUTOINCREMENT semantics
        without rebuilding the table)."""
        top = c.execute("SELECT MAX(rowid) FROM records").fetchone()[0]
        if top is not None and int(top) > self._rowid_hwm:
            self._rowid_hwm = int(top)
            c.execute("INSERT OR REPLACE INTO meta(k,v) VALUES('rowid_hwm',?)", (str(self._rowid_hwm),))
        self._rowid_gap = self._rowid_hwm > 0

    def _next_rowid_locked(self, c: sqlite3.Connection) -> int | None:
        """The rowid for the next inserted row, or None to let SQLite pick
        (its MAX(rowid)+1 is then already above every rowid ever used)."""
        if not self._rowid_gap:
            return None
        top = c.execute("SELECT MAX(rowid) FROM records").fetchone()[0] or 0
        if int(top) >= self._rowid_hwm:
            self._rowid_gap = False
            return None
        return self._rowid_hwm + 1

    def mark_superseded(self, old_id: str, new_id: str, at_ms: int) -> None:
        with self._lock:
            self._lex_touch([old_id])
            self._con.execute(
                "UPDATE records SET invalidated_at=?, superseded_by=? WHERE id=?",
                (at_ms, new_id, old_id),
            )
            self._con.commit()
            self._invalidate_stats()

    def mark_quarantined(self, record_id: str, flag: bool) -> None:
        with self._lock:
            self._lex_touch([record_id])
            if flag:
                self._con.execute(
                    "UPDATE records SET quarantined=1 WHERE id=?", (record_id,))
            else:
                # clearing MUST scrub the stored meta too: Python-side filter
                # paths (_passes_filter) read meta['quarantined'], and a stale
                # flag there hid decayed records forever even after the
                # column was folded back to 0
                self._con.execute(
                    "UPDATE records SET quarantined=0, "
                    "meta=json_remove(meta, '$.quarantined', '$.quarantine_expires') "
                    "WHERE id=?",
                    (record_id,),
                )
            self._con.commit()
            self._invalidate_stats()

    def tombstone(self, record_id: str, at_ms: int) -> bool:
        with self._lock:
            self._lex_touch([record_id])
            cur = self._con.execute(
                "UPDATE records SET deleted=1, invalidated_at=COALESCE(invalidated_at,?) WHERE id=?",
                (at_ms, record_id),
            )
            self._con.commit()
            self._invalidate_stats()
            return cur.rowcount > 0

    def hard_delete(self, record_id: str) -> bool:
        """Physical removal inside the index (compaction deadline path)."""
        with self._lock:
            self._lex_touch([record_id])
            self._note_rowid_hwm(self._con)
            cur = self._con.execute("DELETE FROM records WHERE id=?", (record_id,))
            self._hard_delete_rows(self._con, record_id)
            self._con.commit()
            self._invalidate_stats()
            return cur.rowcount > 0

    @staticmethod
    def _hard_delete_rows(c: sqlite3.Connection, record_id: str) -> None:
        """Row removals for a hard delete, no commit (shared by the single-id
        path and apply_ops_batch)."""
        # fts is external-content with triggers: deleting the record row
        # removes its index entry, so touching fts here would double-delete
        c.execute("DELETE FROM vectors WHERE id=?", (record_id,))
        c.execute(
            "DELETE FROM entity_segments WHERE entity_key IN "
            "(SELECT entity_key FROM entities WHERE record_id=?)",
            (record_id,),
        )
        c.execute("DELETE FROM entities WHERE record_id=?", (record_id,))
        c.execute("DELETE FROM links WHERE src=? OR dst=?", (record_id, record_id))

    def apply_ops_batch(self, ops: list[dict]) -> None:
        """Apply mutation ops for many records in ONE transaction: one commit,
        one stats invalidation. The batched counterpart of calling
        tombstone()/mark_superseded()/mark_quarantined()/hard_delete() in a
        loop - the destructive-sweep path previously paid a commit per op
        (O(n) fsyncs on the request path). Op semantics match append_op's
        index application exactly; self-supersede is ignored (inert)."""
        if not ops:
            return
        with self._lock:
            if self._closed:
                return
            c = self._con
            self._lex_touch([op.get("id") or op.get("old") for op in ops
                             if (op.get("id") or op.get("old"))
                             and op.get("op") in ("tombstone", "supersede", "quarantine", "hard_delete")])
            try:
                if any(op.get("op") == "hard_delete" for op in ops):
                    self._note_rowid_hwm(c)
                for op in ops:
                    kind = op.get("op")
                    rid = op.get("id") or op.get("old")
                    if not rid:
                        continue
                    if kind == "tombstone":
                        c.execute(
                            "UPDATE records SET deleted=1, invalidated_at=COALESCE(invalidated_at,?) WHERE id=?",
                            (op.get("at", now_ms()), rid),
                        )
                    elif kind == "supersede":
                        new = op.get("new")
                        if new == rid or not new:
                            continue  # malformed/self-supersede would brick the record
                        c.execute(
                            "UPDATE records SET invalidated_at=?, superseded_by=? WHERE id=?",
                            (op.get("at", now_ms()), new, rid),
                        )
                    elif kind == "quarantine":
                        c.execute(
                            "UPDATE records SET quarantined=? WHERE id=?",
                            (int(bool(op.get("flag", True))), rid),
                        )
                    elif kind == "set_vector":
                        import numpy as np

                        vec = np.frombuffer(bytes.fromhex(op["vec_hex"]), dtype=np.float32)
                        c.execute(
                            "INSERT OR REPLACE INTO vectors(id,dim,model,vec) VALUES(?,?,?,?)",
                            (rid, int(vec.shape[0]), op.get("model", ""), _vec_blob(vec)),
                        )
                        c.execute("UPDATE records SET embedding_version=? WHERE id=?", (op.get("model", ""), rid))
                    elif kind == "hard_delete":
                        c.execute("DELETE FROM records WHERE id=?", (rid,))
                        self._hard_delete_rows(c, rid)
                c.commit()
            except Exception:
                self.flush()  # don't leave a broken transaction open
                raise
            self._invalidate_stats()

    # ------------------------------------------------------------------ reads

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> MemoryRecord:
        ex = json.loads(row["extractor"]) if row["extractor"] else None
        from memd.core.schema import ExtractorInfo, Provenance, TimeAxis

        rec = MemoryRecord(
            id=row["id"], namespace="", kind=row["kind"], content=row["content"],
            scope=Scope(org=row["scope_org"], agent=row["scope_agent"], user=row["scope_user"], session=row["scope_session"]),
            provenance=Provenance(
                source=Source(int(row["source"])), actor_id=row["actor_id"], session_id=row["prov_session"],
                lineage=json.loads(row["lineage"]),
                extractor=ExtractorInfo(model=ex["model"], prompt_version=ex["prompt_version"]) if ex else None,
            ),
            time=TimeAxis(t_event=row["t_event"], t_ingested=row["t_ingested"], valid_from=row["valid_from"],
                          invalidated_at=row["invalidated_at"], superseded_by=row["superseded_by"]),
            entity_keys=json.loads(row["entity_keys"]), embedding_version=row["embedding_version"],
            meta=json.loads(row["meta"]), deleted=bool(row["deleted"]),
        )
        if row["quarantined"]:
            rec.meta["quarantined"] = True  # packing fences quarantined rows
        return rec

    def _filter_where(self, f: IndexFilter, args: list) -> str:
        clauses = []
        s = f.scope
        if s is not None:
            # Visibility: a record binds only the scope components it sets;
            # the query must match those or leave them unconstrained.
            # => query at session sees session+user+agent+org records;
            #    query at user sees all that user's sessions + above; etc.
            for col, val in (
                ("scope_org", s.org),
                ("scope_agent", s.agent),
                ("scope_user", s.user),
            ):
                if val is not None:
                    clauses.append(f"({col} IS NULL OR {col} = ?)")
                    args.append(val)
            # session is the private leaf - see Scope.contains. This MUST
            # mirror the Python predicate exactly: the two filters diverging
            # is how the pass-1 and pass-15 leaks happened.
            if s.session is not None:
                clauses.append("(scope_session IS NULL OR scope_session = ?)")
                args.append(s.session)
            if s.user is not None or s.session is not None:
                parts = ["scope_session IS NULL", "scope_user IS NOT NULL"]
                if s.session is not None:
                    parts.append("scope_session = ?")
                    args.append(s.session)
                clauses.append("(" + " OR ".join(parts) + ")")
        if f.kinds:
            clauses.append(f"kind IN ({','.join('?' * len(f.kinds))})")
            args.extend(f.kinds)
        if f.sources:
            vals = [int(Source.parse(x)) for x in f.sources]
            clauses.append(f"source IN ({','.join('?' * len(vals))})")
            args.extend(vals)
        if f.t_event_min is not None:
            clauses.append("t_event >= ?")
            args.append(f.t_event_min)
        if f.t_event_max is not None:
            clauses.append("t_event <= ?")
            args.append(f.t_event_max)
        if not f.include_invalid:
            clauses.append("deleted = 0")
            now = f.now if f.now is not None else time.time() * 1000
            if f.as_of is None:
                clauses.append("invalidated_at IS NULL")
                clauses.append("superseded_by IS NULL")
                # not-yet-valid records stay hidden in the current view
                clauses.append("(valid_from IS NULL OR valid_from <= ?)")
                args.append(now)
            else:
                clauses.append("(invalidated_at IS NULL OR invalidated_at > ?)")
                args.append(f.as_of)
                clauses.append("(valid_from IS NULL OR valid_from <= ?)")
                args.append(f.as_of)
        if not f.include_quarantined:
            clauses.append("quarantined = 0")
        if f.entity_keys:
            clauses.append(
                f"id IN (SELECT record_id FROM entities WHERE entity_key IN ({','.join('?' * len(f.entity_keys))}))"  # nosec B608
            )
            args.extend(f.entity_keys)
        return " AND ".join(clauses) if clauses else "1=1"

    def _fetch(self, where: str, args: list) -> list[MemoryRecord]:
        rows = self._con.execute(f"SELECT * FROM records WHERE {where}", args).fetchall()  # nosec B608
        out = []
        for r in rows:
            rec = self._row_to_record(r)
            rec.namespace = self._ns_hint
            out.append(rec)
        return out

    _ns_hint: str = ""

    def get_by_id(self, record_id: str, include_deleted: bool = False) -> MemoryRecord | None:
        with self._read() as _c:
            row = _c.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        if row is None:
            return None
        if row["deleted"] and not include_deleted:
            return None
        rec = self._row_to_record(row)
        rec.namespace = self._ns_hint
        return rec

    def query_records(self, f: IndexFilter, limit: int = 100) -> list[MemoryRecord]:
        args: list = []
        where = self._filter_where(f, args)
        args.append(limit)
        with self._read() as _c:
            rows = _c.execute(
                f"SELECT * FROM records WHERE {where} ORDER BY t_event DESC LIMIT ?", args  # nosec B608
            ).fetchall()
        out = []
        for r in rows:
            if r["id"] in f.exclude_ids:
                continue
            rec = self._row_to_record(r)
            rec.namespace = self._ns_hint
            out.append(rec)
        return out

    def search_bm25(self, query: str, f: IndexFilter, limit: int = 50) -> list[Hit]:
        """One OR query over the content terms, ranked by FTS5 bm25.

        This replaced an AND-first tier plus a bounded, UNORDERED OR window
        re-ranked in Python by distinct query-term coverage. On natural
        language questions the AND tier almost never fired, so the coverage
        re-rank decided the lane: it had no IDF and no TF, so a record that
        happened to share several common query words outranked the one record
        holding the rare, decisive term (LongMemEval_S ndcg@5 0.739 for the
        lane vs 0.891 for plain bm25 ordering; 0.241 vs 0.884 on _M). It was
        also the dominant CPU cost of search (~130-145ms p50 at 10K-150K
        records): hundreds of rows fetched and string-scanned per query to
        keep ~40.

        bm25 ordering is not free either: FTS5 scores every row on the posting
        lists of the query terms, so cost grows with posting-list length for
        high-document-frequency terms (stopwords are dropped to keep that
        bounded). With the tantivy accelerator attached (memd[fast]), the
        lane is answered by its block-max WAND top-k plus an FTS5 tail, and
        falls back here whenever it cannot answer exactly."""
        METRICS.inc("memd_bm25_queries_total", help="bm25 lane queries (one ranked FTS5 OR query each)")
        lex = self.lexical
        if lex is not None and not self._closed:
            hits = lex.search(query, f, limit)
            if hits is not None:
                return hits
        q_all = _fts_escape(query)
        if not q_all or self._closed:
            return []
        return self._fts5_bm25(" OR ".join(f'"{w}"' for w in q_all.split()), f, limit)

    def _fts5_bm25(self, match_expr: str, f: IndexFilter, limit: int, *,
                   rowid_min: int | None = None, ids: list[str] | None = None,
                   ids_only: bool = False) -> list:
        """The FTS5 bm25 query itself. `rowid_min` / `ids` restrict it to the
        lexical accelerator's tail: rows above its watermark, or rows changed
        since its last commit. `ids_only` returns [(id, score, t_event,
        content key)] (SQL-filtered, not hydrated) for a caller that hydrates
        what it keeps.

        Equal scores are ordered by fusion's key (score, -t_event, content
        hash, id), as the tantivy lane orders them, so the backend never
        changes the order of tied rows. FTS5 alone returned them oldest-first
        (rowid order) while tantivy returned them newest-first."""
        if self._closed:
            return []
        args: list = []
        # Filtering stays IN SQL so the LIMIT counts only eligible rows: a
        # bulk of matching-but-ineligible rows (other scope, tombstoned,
        # superseded, quarantined, not-yet-valid) must never starve eligible
        # records out of the lane.
        filt = self._filter_where(f, args)
        extra = ""
        extra_args: list = []
        if rowid_min is not None:
            extra = " AND fts.rowid > ?"
            extra_args = [int(rowid_min)]
        if ids is not None:
            if not ids:
                return []
            with self._read() as _c:
                rowids = []
                for i in range(0, len(ids), 500):
                    chunk = ids[i:i + 500]
                    rowids += [r[0] for r in _c.execute(
                        f"SELECT rowid FROM records WHERE id IN ({','.join('?' * len(chunk))})",  # nosec B608
                        chunk).fetchall()]
            if not rowids:
                return []
            extra += f" AND fts.rowid IN ({','.join('?' * len(rowids))})"
            extra_args += rowids
        sql = (
            # CROSS JOIN pins the join order: fts drives, records is
            # probed by rowid. Without it the planner drove from `records`
            # (via ix_rec_valid, because the scope/validity predicates sit
            # there) and probed the fts index once PER ROW - 10K probes,
            # turning a 17ms lane into 22 SECONDS at 10K records. The plain
            # fts5 table happened to cost out the other way; external
            # content changed the estimate, not the right answer.
            f"SELECT {'r.id, r.t_event, r.content' if ids_only else 'r.*'}, bm25(fts) AS rank "  # nosec B608
            f"FROM fts CROSS JOIN records r ON r.rowid = fts.rowid "
            f"WHERE fts MATCH ?{extra} AND {filt}"
        )
        qargs = [match_expr] + extra_args + args
        # exclude_ids is applied in Python; over-fetch so it never shrinks the page
        n = limit + len(f.exclude_ids)
        with self._read() as _c:
            rows = _c.execute(sql + " ORDER BY rank, r.t_event DESC LIMIT ?", qargs + [n + 1]).fetchall()
            if n and len(rows) > n and _tie(rows[n]) == _tie(rows[n - 1]):
                # the cut falls inside a group of equal (score, t_event): only
                # the content hash orders it, so the whole group is needed
                b = _tie(rows[n - 1])
                group = _c.execute(sql + " AND r.t_event = ?", qargs + [b[1]]).fetchall()
                rows = [r for r in rows if _tie(r) != b] + [r for r in group if _tie(r) == b]
            else:
                rows = rows[:n]
        rows = _lane_order(rows)
        if ids_only:
            from memd.query.fusion import content_sha

            return [(r["id"], -float(r["rank"]), int(r["t_event"]), int(content_sha(r["content"] or ""), 16))
                    for r in rows if r["id"] not in f.exclude_ids][:limit]
        hits: list[Hit] = []
        for r in rows:
            if r["id"] in f.exclude_ids:
                continue
            rec = self._row_to_record(r)
            rec.namespace = self._ns_hint
            # belt-and-braces: the Python twin of the SQL predicate set
            if not self._passes_filter(rec, f):
                continue
            hits.append(Hit(record=rec, score=-float(r["rank"]), lane="bm25"))
            if len(hits) >= limit:
                break
        return hits

    def session_neighbours(self, ids: list[str], f: IndexFilter,
                           radius: int = 1) -> tuple[dict[str, list[MemoryRecord]], dict[str, int]]:
        """For each record id: up to `radius` raw turns before and after it
        in the SAME session, ordered by (t_event, rowid) - the rowid is the
        ingestion order, which is what orders turns that share a session date.

        Every neighbour satisfies `f` (SQL, then _passes_filter): a session id
        is a caller-chosen string that two users can share, so adjacency alone
        must never pull another scope's row into a packed context.

        Returns ({id: [previous..., next...]}, {record id: rowid}) - the
        positions cover the anchors and their neighbours."""
        out: dict[str, list[MemoryRecord]] = {}
        positions: dict[str, int] = {}
        if not ids or radius <= 0 or self._closed:
            return out, positions
        fargs: list = []
        filt = self._filter_where(f, fargs)
        with self._read() as _c:
            for rid in ids:
                a = _c.execute("SELECT rowid AS _rowid, scope_session, t_event FROM records WHERE id=?",
                               (rid,)).fetchone()
                if a is None:
                    continue
                positions[rid] = int(a["_rowid"])
                if a["scope_session"] is None:
                    continue
                got: list[MemoryRecord] = []
                for cmp, order in (("<", "DESC"), (">", "ASC")):
                    rows = _c.execute(
                        f"SELECT rowid AS _rowid, * FROM records WHERE scope_session = ? "  # nosec B608
                        f"AND kind = 'raw_event' AND (t_event {cmp} ? OR (t_event = ? AND rowid {cmp} ?)) "
                        f"AND {filt} ORDER BY t_event {order}, rowid {order} LIMIT ?",
                        [a["scope_session"], a["t_event"], a["t_event"], a["_rowid"], *fargs, radius],
                    ).fetchall()
                    if order == "DESC":
                        rows = list(reversed(rows))
                    for r in rows:
                        rec = self._row_to_record(r)
                        rec.namespace = self._ns_hint
                        if rec.id in f.exclude_ids or not self._passes_filter(rec, f):
                            continue
                        positions[rec.id] = int(r["_rowid"])
                        got.append(rec)
                out[rid] = got
        return out, positions

    def _fold_overflow_locked(self) -> None:
        """Fold the overflow block into the main matrix (O(main)); called
        only when overflow exceeds OVERFLOW_MAX so amortized cost per vector
        stays O(1)."""
        if not self._ovf_ids:
            self._ovf_vecs = []
            return
        dim = self._main_mat.shape[1] if self._main_mat.size else len(self._ovf_vecs[0])
        add = [v for v in self._ovf_vecs if v.shape[0] == dim]
        if add:
            block = np.stack(add).astype(np.float32)
            m = self._main_mat
            self._main_mat = block if m.size == 0 else np.vstack([m, block])
            self._main_ids.extend(self._ovf_ids)
        self._ovf_ids = []
        self._ovf_vecs = []


    def _load_vectors_locked(self) -> None:
        rows = self._con.execute(
            """SELECT v.id, v.vec, v.dim FROM vectors v JOIN records r ON r.id=v.id
               WHERE r.deleted=0 AND r.quarantined=0
                 AND r.invalidated_at IS NULL AND r.superseded_by IS NULL"""
        ).fetchall()
        ids = [r[0] for r in rows]
        if rows:
            # decode per row so a store holding pre-v2 float32 blobs alongside
            # v2 float16 ones still loads (dim tells us which each is)
            mat = np.stack([_vec_from_blob(r[1], int(r[2])) for r in rows])
            norms = np.linalg.norm(mat, axis=1, keepdims=True)
            norms[norms == 0] = 1.0
            mat = mat / norms
            self._main_mat = mat.astype(np.float32)
            self._main_ids = ids

    _RESTRICTIVE_FILTER_FIELDS = ("scope", "kinds", "sources", "entity_keys",
                                  "t_event_min", "t_event_max", "as_of")

    def _eligible_ids(self, f: IndexFilter) -> set[str] | None:
        """SQL-side eligibility set for predicates the vector matrix cannot
        see (scope/kind/time/entity). Returns None when no restrictive
        predicate is present: deleted/quarantined/superseded rows are already
        excluded at matrix load, so the unfiltered case costs zero extra I/O."""
        if not any(getattr(f, name) is not None for name in self._RESTRICTIVE_FILTER_FIELDS):
            return None
        args: list = []
        where = self._filter_where(f, args)
        with self._read() as _c:
            rows = _c.execute(f"SELECT id FROM records WHERE {where}", args).fetchall()  # nosec B608
        return {r[0] for r in rows}

    def search_vector(self, query_vec: np.ndarray, f: IndexFilter, limit: int = 50) -> list[Hit]:
        """Flat cosine scan over main+overflow blocks. Per-query cost:
        O(V) BLAS dot + O(overflow) merge - never an O(total) rebuild.

        Exactness under filters WITHOUT taxing the common case: candidates
        are taken in widening top-k windows and hydrated in batched chunks.
        A loose filter (typical scoped hybrid search) fills `limit` from the
        first window at the old path's cost. A selective filter (e.g.
        kinds=['fact'] in a raw-heavy namespace) would silently drop valid
        deeper matches under fixed truncation - so after two speculative
        windows we fall back to ONE exact SQL restriction of the score
        vector. Bounded work per request: <= 2 speculative rounds + O(1)
        fallback queries; hydration always chunked (500 ids/query)."""
        q = np.asarray(query_vec, dtype=np.float32).ravel()
        nrm = float(np.linalg.norm(q))
        if nrm > 0:
            q = q / nrm
        with self._lock:
            if self._closed:
                return []
            if not self._vec_loaded:
                self._load_vectors_locked()
                self._vec_loaded = True
            ids: list[str] = list(self._main_ids)
            parts = []
            if self._main_mat.size:
                parts.append(self._main_mat @ q)
            if self._ovf_ids:
                ovf = np.stack(self._ovf_vecs).astype(np.float32)
                ids.extend(self._ovf_ids)
                parts.append(ovf @ q)
        if not ids:
            return []
        scores = np.concatenate(parts) if len(parts) > 1 else parts[0]
        n = int(scores.shape[0])

        def _collect(rows: np.ndarray, seen: set[str], hits: list[Hit]) -> None:
            cand = [(ids[i], float(scores[i])) for i in rows]
            fresh = [(cid, cs) for cid, cs in cand if cid not in seen]
            if not fresh:
                return
            seen.update(cid for cid, _ in fresh)
            recs = {r.id: r for r in self.get_many([cid for cid, _ in fresh])}
            for cid, cscore in fresh:
                if cscore < MIN_COSINE:
                    continue  # no-evidence match: noise in ranked search, poison in sweeps
                if cid in f.exclude_ids:
                    continue
                r = recs.get(cid)
                if r is None or not self._passes_filter(r, f):
                    continue
                hits.append(Hit(record=r, score=cscore, lane="vector"))

        # phase 1: speculative widening windows (cheap when the filter is
        # loose). Sweep-scale limits skip straight to the exact phase: at
        # limit>=1024 the windows would hydrate most of the namespace anyway.
        hits: list[Hit] = []
        seen: set[str] = set()
        if limit < 1024:
            for k in (limit * 4, limit * 16):
                k = min(k, n)
                if k <= 0:
                    break
                top = np.argpartition(-scores, k - 1)[:k] if k < n else np.arange(n)
                top = top[np.argsort(-scores[top])]
                _collect(top, seen, hits)
                if len(hits) >= limit:
                    return hits[:limit]
        # phase 2: exact fallback - one SQL scan restricts scoring to rows
        # whose record satisfies every predicate, then rank within that set
        eligible = self._eligible_ids(f)
        if eligible is not None:
            keep = np.array([i for i, rid in enumerate(ids) if rid in eligible], dtype=np.int64)
            if keep.size == 0:
                return []
            sub_scores = scores[keep]
            m = int(sub_scores.shape[0])
            k = min(limit * 4, m)
            top = np.argpartition(-sub_scores, k - 1)[:k] if k < m else np.arange(m)
            top = top[np.argsort(-sub_scores[top])]
            hits = []
            _collect(keep[top], set(), hits)
        return hits[:limit]

    def _passes_filter(self, rec: MemoryRecord, f: IndexFilter) -> bool:
        """Python-side twin of _filter_where. Every predicate the SQL path
        enforces must be enforced here too: the lanes that hydrate rows before
        (or instead of) SQL filtering - the time lane's recency fetch - reach
        records through THIS function alone, and bm25 re-checks its SQL-
        filtered rows here as belt-and-braces.

        Scope is delegated to Scope.contains rather than re-implemented. The
        open-coded copy that used to live here diverged from it: it applied a
        symmetric "either side unset matches" rule, so after Scope.contains
        learned that a session-bound record is private, the TIME LANE still
        returned another user's session-scoped record to a user-scoped query.
        One predicate, one definition."""
        s = f.scope
        if s is not None and not s.contains(rec.scope):
            return False
        if f.kinds and rec.kind not in f.kinds:
            return False
        if f.sources:
            allowed = {int(Source.parse(x)) for x in f.sources}
            if int(rec.provenance.source) not in allowed:
                return False
        if f.t_event_min is not None and rec.time.t_event < f.t_event_min:
            return False
        if f.t_event_max is not None and rec.time.t_event > f.t_event_max:
            return False
        if f.entity_keys and not (set(f.entity_keys) & set(rec.entity_keys)):
            return False
        if rec.id in f.exclude_ids:
            return False
        # quarantine: single authority for every Python-side filter path
        # (a lane that hydrates rows BEFORE any SQL could exclude them - the
        # old bounded bm25 window did - made an unreviewed record retrievable)
        if not f.include_quarantined and rec.meta.get("quarantined"):
            return False
        if not f.include_invalid:
            if rec.deleted:
                return False
            if f.as_of is None:
                if rec.time.invalidated_at is not None or rec.time.superseded_by is not None:
                    return False
                # not-yet-valid records stay hidden in the current view
                vf = rec.time.valid_from
                if vf is not None and vf > time.time() * 1000:
                    return False
            else:
                if rec.time.invalidated_at is not None and rec.time.invalidated_at <= f.as_of:
                    return False
                if rec.time.valid_from is not None and rec.time.valid_from > f.as_of:
                    return False
        return True

    def expand_links(self, seed_ids: list[str], hop: int = 1, limit: int = 50) -> list[MemoryRecord]:
        if not seed_ids:
            return []
        qs = ",".join("?" * len(seed_ids))
        with self._lock:
            sql = f"SELECT r.* FROM links l JOIN records r ON r.id = l.dst WHERE l.src IN ({qs}) AND r.deleted=0 AND r.quarantined=0 AND r.invalidated_at IS NULL LIMIT ?"  # nosec B608
            rows = self._con.execute(sql, (*seed_ids, limit)).fetchall()
            sql2 = f"SELECT r.* FROM links l JOIN records r ON r.id = l.src WHERE l.dst IN ({qs}) AND r.deleted=0 AND r.quarantined=0 AND r.invalidated_at IS NULL LIMIT ?"  # nosec B608
            rows += self._con.execute(sql2, (*seed_ids, limit)).fetchall()
        seen, out = set(), []
        for r in rows:
            if r["id"] in seen or r["id"] in seed_ids:
                continue
            seen.add(r["id"])
            rec = self._row_to_record(r)
            rec.namespace = self._ns_hint
            out.append(rec)
        return out[:limit]

    def count_missing_embedding(self, model: str) -> int:
        """How many live records lack a current-version vector.

        A COUNT, not a worklist. The vector-health gauge only needs the number,
        and materializing the worklist to take len() of it built ~45,000
        MemoryRecord objects (three json.loads each) on the NAMESPACE OPEN
        path: 1066ms to reopen a 50K namespace versus 54ms once the lane was
        full. The reembed job still builds the real worklist - on the
        maintenance thread, where that cost belongs."""
        with self._read() as _c:
            row = _c.execute(
                "SELECT COUNT(*) FROM records r LEFT JOIN vectors v ON v.id = r.id "
                "WHERE r.deleted=0 AND r.quarantined=0 AND r.invalidated_at IS NULL "
                "AND r.superseded_by IS NULL AND r.kind != 'link' "
                "AND (v.id IS NULL OR r.embedding_version IS NULL OR r.embedding_version != ?)",
                (model,)).fetchone()
        return int(row[0]) if row else 0

    def records_missing_embedding(self, model: str, limit: int = 100_000) -> list[MemoryRecord]:
        """Live, currently-valid records with no vector or an outdated
        embedding_version - the worklist for the re-embedding batch job
        (ADR-8). Quarantined records are excluded: never pre-arm unreviewed
        content for retrieval. Deleted/superseded rows are excluded too:
        embedding budget must not be spent on state no default-view query
        can ever see."""
        with self._lock:
            rows = self._con.execute(
                """SELECT r.* FROM records r LEFT JOIN vectors v ON v.id = r.id
                   WHERE (v.id IS NULL OR (r.embedding_version IS NOT NULL AND r.embedding_version != ?))
                     AND r.quarantined = 0
                     AND r.deleted = 0
                     AND r.superseded_by IS NULL
                   ORDER BY r.t_ingested LIMIT ?""",
                (model, limit),
            ).fetchall()
        out = []
        for r in rows:
            rec = self._row_to_record(r)
            rec.namespace = self._ns_hint
            out.append(rec)
        return out

    def entity_cluster(
        self,
        entity_key: str,
        include_invalid: bool = False,
        scope: Scope | None = None,
    ) -> list[MemoryRecord]:
        """Cluster members for one entity key. Scope-filtered: clusters are
        per-visible-scope - never consolidate across tenants/users."""
        f = IndexFilter(entity_keys=(entity_key,), include_invalid=include_invalid, scope=scope)
        return self.query_records(f, limit=1000)

    def records_of_session(
        self,
        session_id: str,
        limit: int = 1000,
        user_id: str | None = None,
    ) -> list[MemoryRecord]:
        """Exact-session raw records for segment-close extraction. Unlike
        query visibility (which includes ancestor-scope rows), a session
        boundary must sweep ONLY that session's own writes - NEVER
        quarantined ones (extraction is gate 2 of the poisoning defense,
        D7 #4), and - when a user binding is supplied - only rows bound to
        that user (blocks cross-user session-id injection)."""
        with self._lock:
            total = self._con.execute(
                "SELECT COUNT(*) FROM records WHERE scope_session = ? AND kind = 'raw_event' AND deleted = 0",
                (session_id,),
            ).fetchone()[0]
            sql = """SELECT * FROM records WHERE scope_session = ? AND kind = 'raw_event'
                   AND deleted = 0 AND quarantined = 0"""
            args: list = [session_id]
            if user_id:
                sql += " AND (scope_user = ? OR scope_user IS NULL)"
                args.append(user_id)
            sql += " ORDER BY t_event LIMIT ?"
            args.append(limit)
            rows = self._con.execute(sql, args).fetchall()
        if total > limit:
            from memd.metrics import METRICS

            METRICS.inc("memd_session_truncated_total", ns=self._ns_hint)
        out = []
        for r in rows:
            rec = self._row_to_record(r)
            rec.namespace = self._ns_hint
            out.append(rec)
        return out

    def search_time_lane(self, f: IndexFilter, limit: int = 50) -> list[Hit]:
        """Recency fan-out: newest records passing the filter. Gives temporal
        queries a btree-ordered candidate stream (ix_rec_tevent) instead of
        relying on lexical similarity to surface fresh records before the
        packing-stage recency decay ever sees them.

        Performance note: pushing the full predicate set into this query cost
        ~3.5ms on a 3K-row corpus (OR-clauses defeat the ordered index scan);
        fetching a recency window with only cheap predicates and applying the
        exact predicates in Python costs ~0.2ms - same post-filter pattern as
        search_vector, with one widening round so a stale-heavy tail cannot
        silently shrink results."""
        seen: set[str] = set()
        hits: list[Hit] = []

        def fetch(window: int) -> list[sqlite3.Row]:
            if self._closed:
                return []
            with self._read() as _c:
                return _c.execute(
                    "SELECT * FROM records WHERE deleted=0 AND quarantined=0 "
                    "ORDER BY t_event DESC LIMIT ?",
                    (window,),
                ).fetchall()

        for window in (limit * 4, limit * 16):
            rows = fetch(window)
            for r in rows:
                rid = r["id"]
                if rid in seen or rid in f.exclude_ids:
                    continue
                seen.add(rid)
                rec = self._row_to_record(r)
                rec.namespace = self._ns_hint
                if not self._passes_filter(rec, f):
                    continue
                hits.append(Hit(record=rec, score=0.0, lane="time"))
                if len(hits) >= limit:
                    return hits
            if len(rows) < window:
                break  # namespace exhausted; widening cannot add anything
        return hits

    def search_by_entity_tokens(self, tokens: list[str], f: IndexFilter, limit: int = 30) -> list[Hit]:
        """Entity lane: records whose entity-key segments match query tokens.
        Exact btree lookups on the segments table - O(tokens x log E + matches).

        Tokens are normalized to the SEGMENT alphabet ([a-z0-9_-], matching
        normalize_entity_key + dot-splitting). Raw whitespace-splits carry
        punctuation ("acme?", "editor,") that matched no segment and silently
        zeroed the whole lane on most real queries - masked by BM25/vector
        still finding the record."""
        if not tokens:
            return []
        toks: list[str] = []
        seen_toks: set[str] = set()
        for t in tokens[:8]:
            t = _SEG_CHARS.sub("", t.strip().lower())
            if len(t) >= 3 and t not in seen_toks:
                seen_toks.add(t)
                toks.append(t)
        if not toks:
            return []
        qs = ",".join("?" * len(toks))
        filt_args: list = []
        filt = self._filter_where(f, filt_args)
        sql = (
            f"SELECT r.* FROM entity_segments es JOIN entities e ON e.entity_key = es.entity_key "  # nosec B608
            f"JOIN records r ON r.id = e.record_id "
            f"WHERE es.segment IN ({qs}) AND {filt} ORDER BY r.t_event DESC LIMIT ?"
        )
        with self._read() as _c:
            rows = _c.execute(sql, (*toks, *filt_args, limit)).fetchall()
        hits = []
        for r in rows:
            if r["id"] in f.exclude_ids:
                continue
            rec = self._row_to_record(r)
            rec.namespace = self._ns_hint
            hits.append(Hit(record=rec, score=1.0, lane="entity"))
        return hits

    def history(self, record_id: str) -> list[MemoryRecord]:
        """Supersedence chain including the seed: predecessors and successors."""
        chain, seen = [], {record_id}
        frontier = [record_id]
        with self._read() as _con:
            while frontier:
                qs = ",".join("?" * len(frontier))
                sql = f"SELECT * FROM records WHERE superseded_by IN ({qs}) OR id IN (SELECT superseded_by FROM records WHERE id IN ({qs}))"  # nosec B608
                rows = _con.execute(sql, (*frontier, *frontier)).fetchall()
                nxt = []
                for r in rows:
                    rid = r["id"]
                    if rid not in seen:
                        seen.add(rid)
                        nxt.append(rid)
                        rec = self._row_to_record(r)
                        rec.namespace = self._ns_hint
                        chain.append(rec)
                frontier = nxt
            seed_row = self._con.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        if seed_row is not None:
            seed = self._row_to_record(seed_row)
            seed.namespace = self._ns_hint
            chain.append(seed)
        chain.sort(key=lambda r: r.time.t_ingested)
        return chain

    def stats(self) -> dict:
        """Namespace stats with a 1s TTL cache: O(N) COUNT queries amortize to
        ~zero for hot callers (MCP memory_status, REST /stats); any write
        invalidates immediately so numbers stay honest where it matters."""
        with self._lock:
            now = time.monotonic()
            if self._stats_cache is not None and now - self._stats_at < 1.0:
                return dict(self._stats_cache)
            total = self._con.execute("SELECT COUNT(*) FROM records WHERE deleted=0").fetchone()[0]
            tombstones = self._con.execute("SELECT COUNT(*) FROM records WHERE deleted=1").fetchone()[0]
            facts = self._con.execute("SELECT COUNT(*) FROM records WHERE kind='fact' AND deleted=0").fetchone()[0]
            invalid = self._con.execute(
                "SELECT COUNT(*) FROM records WHERE invalidated_at IS NOT NULL AND deleted=0"
            ).fetchone()[0]
            quarantined = self._con.execute("SELECT COUNT(*) FROM records WHERE quarantined=1").fetchone()[0]
            vecs = self._con.execute("SELECT COUNT(*) FROM vectors").fetchone()[0]
            dim_row = self._con.execute("SELECT dim, model FROM vectors LIMIT 1").fetchone()
            links = self._con.execute("SELECT COUNT(*) FROM links").fetchone()[0]
        st = {
            "records": total,
            "tombstones": tombstones,
            "facts": facts,
            "superseded": invalid,
            "quarantined": quarantined,
            "vectors": vecs,
            "vec_dim": dim_row[0] if dim_row else None,
            "embedding_model": dim_row[1] if dim_row else None,
            "links": links,
        }
        with self._lock:
            self._stats_cache = st
            self._stats_at = time.monotonic()
        return dict(st)

    def all_records(self, batch: int = 1000) -> list[MemoryRecord]:
        out, offset = [], 0
        while True:
            with self._lock:
                rows = self._con.execute("SELECT * FROM records ORDER BY id LIMIT ? OFFSET ?", (batch, offset)).fetchall()
            if not rows:
                break
            for r in rows:
                rec = self._row_to_record(r)
                rec.namespace = self._ns_hint
                out.append(rec)
            offset += batch
        return out

    def get_many(self, ids: list[str]) -> list[MemoryRecord]:
        """Direct id lookup: O(k log N) via PK index."""
        out: dict[str, MemoryRecord] = {}
        with self._read() as _con:
            for i in range(0, len(ids), 500):
                chunk = ids[i : i + 500]
                qs = ",".join("?" * len(chunk))
                rows = _con.execute(f"SELECT * FROM records WHERE id IN ({qs})", chunk).fetchall()  # nosec B608
                for r in rows:
                    rec = self._row_to_record(r)
                    rec.namespace = self._ns_hint
                    out[rec.id] = rec
        return [out[i] for i in ids if i in out]

    def wipe(self) -> None:
        with self._lock:
            if self.lexical is not None:
                self.lexical.reset()  # every row is replaced: the watermark is void
            self._note_rowid_hwm(self._con)
            # (executescript COMMITs first, so the mark is durable before the delete)
            self._con.executescript(
                "DELETE FROM vectors; DELETE FROM entities; "
                "DELETE FROM entity_segments; DELETE FROM links; DELETE FROM records;"
            )
            self._con.commit()
            self._main_mat = np.zeros((0, 0), dtype=np.float32)
            self._main_ids = []
            self._ovf_ids = []
            self._ovf_vecs = []
            self._vec_loaded = True

    def invalidate_vec_cache(self) -> None:
        """Fold-away hook after mass invalidation (compaction): next search
        reloads the full matrix from sqlite."""
        with self._lock:
            self._vec_loaded = False
            self._main_mat = np.zeros((0, 0), dtype=np.float32)
            self._main_ids = []
            self._ovf_ids = []
            self._ovf_vecs = []


# Stopwords excluded from FTS terms: OR-ing them made every natural-language
# query match a huge fraction of the corpus (at 50K docs the bm25 lane alone
# cost ~155ms/query - 74% of end-to-end latency). Standard IR stopwords
# ONLY: words fitted to the synthetic test generator ("later follow ups
# agreed discussed session number notes") used to live here too, and they
# silently dropped real query words on real data.
_FTS_STOPWORDS = frozenset(
    "a an and are as at be but by for from has have i in is it its of on or "
    "that the this to we was were will with you your do does did not no yes "
    "so if then than there their they he she his her them about into over "
    "under again further once here when where why how all any both each few "
    "more most other some such only own same too very can just should now".split()
)


def _fts_escape(query: str) -> str:
    """Quoted-term FTS5 expression. Recall strategy lives in search_bm25:
    this only produces clean terms (stopwords dropped, hostile chars stripped)."""
    words = []
    seen: set[str] = set()
    for tok in query.replace('"', " ").split():
        tok = "".join(ch for ch in tok.lower() if ch.isalnum() or ch in "_-")
        if tok and tok not in _FTS_STOPWORDS and tok not in seen:
            seen.add(tok)
            words.append(tok)
    return " ".join(words)


def _tie(row: sqlite3.Row) -> tuple[float, int]:
    return float(row["rank"]), int(row["t_event"])


def _lane_order(rows: list) -> list:
    """FTS5 rows in the bm25 lane's order: (-score, -t_event, content hash,
    id), fusion's tie-break. The hash is computed only inside groups of equal
    (score, t_event), where it can decide something."""
    from collections import Counter

    from memd.query.fusion import content_sha

    groups = Counter(_tie(r) for r in rows)

    def key(r: sqlite3.Row) -> tuple:
        rank, t = _tie(r)
        return (rank, -t, content_sha(r["content"] or "") if groups[(rank, t)] > 1 else "", r["id"])
    return sorted(rows, key=key)
