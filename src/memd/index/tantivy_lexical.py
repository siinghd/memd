"""Optional tantivy lexical accelerator for the bm25 lane (`memd[fast]`).

Evidence (experiment 008, unfiltered index-only OR queries over real
LongMemEval turns, top-60): tantivy p50 0.26 / 0.53 / 1.32 ms vs FTS5 bm25
4.3 / 20.0 / 63.1 ms at 10K / 50K / 150K docs (block-max WAND prunes; FTS5
scores the whole posting-list union), top-10 overlap 0.93-0.95, equal ndcg.

Correctness model - FTS5 stays the synchronous source of truth:
  - the write path is unchanged: a write is acked once it is in SQLite/FTS5.
    This index is a DERIVED, rebuildable view in the local cache dir, fed by
    one background thread per process that batches changes (every
    `commit_ms`, default 500, or `commit_docs`, default 512, whichever comes
    first), commits, and advances a persisted watermark (max rowid indexed).
  - only live rows (not deleted, invalidated or superseded) are indexed,
    keyed by record id, with the filter fields (org/agent/user/session,
    kind, quarantine flag, valid_from, t_event) as a boolean PREFILTER.
  - search = tantivy top-(2k) over committed docs
             UNION FTS5 bm25 over the tail (rowid > watermark, plus any row
             changed since the last commit: deletes, supersession and
             quarantine flips mark their ids pending BEFORE the SQL change).
    Every result is hydrated from SQLite and post-checked with
    _passes_filter; if tantivy's window was full but fewer than k survive,
    the lane falls back to the FTS5 path. Filters the prefilter does not
    model (as_of, include_invalid, sources, entity keys) and sweep-size
    limits go to FTS5 directly.
  - open: a missing, corrupt, foreign (different SQLite file) or uncleanly
    closed index is rebuilt in the background; until it has caught up the
    lane is served by FTS5.
Tail merge: the few tail hits (recent writes) are interleaved with tantivy's
hits by rank (tantivy's first, then the tail's first, ...), since the two
BM25 implementations' raw scores are not on one scale.
"""
from __future__ import annotations

import importlib.util
import json
import logging
import os
import shutil
import threading
import time
import uuid
from typing import TYPE_CHECKING, Any

from memd.metrics import METRICS

if TYPE_CHECKING:  # pragma: no cover
    from memd.index.sqlite_index import Hit, IndexFilter, NamespaceIndex

_log = logging.getLogger(__name__)

LEXICAL_CHOICES = ("auto", "fts5", "tantivy")
STATE_FILE = "memd-lexical.json"
STATE_VERSION = 1
DEFAULT_COMMIT_MS = 500
DEFAULT_COMMIT_DOCS = 512
CATCHUP_ROWS = 8192          # rows scanned per step while catching up
TOUCH_BATCH = 4096           # changed ids re-indexed per step
WRITER_HEAP = 32_000_000     # tantivy needs >= 15MB per writer thread
MAX_TANTIVY_LIMIT = 1000     # larger (sweep) limits go to FTS5
MAX_FAILURES = 5             # consecutive step failures before giving up
_I64_MIN = -(2 ** 63)

_ROW_COLS = ("rowid, id, content, scope_org, scope_agent, scope_user, scope_session, kind, "
             "deleted, invalidated_at, superseded_by, quarantined, valid_from, t_event")


def tantivy_available() -> bool:
    """Whether the optional `tantivy` extra is installed, without importing it."""
    return importlib.util.find_spec("tantivy") is not None


def requested_lexical_backend(config: dict | None = None) -> str:
    """config["lexical_backend"], else env MEMD_LEXICAL_BACKEND, else "auto"."""
    cfg = config or {}
    choice = str(cfg.get("lexical_backend") or os.environ.get("MEMD_LEXICAL_BACKEND")
                 or "auto").strip().lower()
    if choice not in LEXICAL_CHOICES:
        raise ValueError(f"unknown lexical_backend {choice!r}; expected one of {list(LEXICAL_CHOICES)}")
    return choice


def resolve_lexical_backend(config: dict | None = None) -> str:
    """"fts5" or "tantivy". auto = tantivy when importable. An explicit
    "tantivy" that cannot be honoured raises; it never falls back."""
    choice = requested_lexical_backend(config)
    if choice == "auto":
        return "tantivy" if tantivy_available() else "fts5"
    if choice == "tantivy" and not tantivy_available():
        raise ImportError("lexical_backend 'tantivy' was requested but the `tantivy` package is not "
                          "importable; install memd[fast] or choose lexical_backend='fts5'")
    return choice


def _build_schema() -> Any:
    import tantivy

    sb = tantivy.SchemaBuilder()
    sb.add_text_field("id", stored=True, tokenizer_name="raw")
    sb.add_text_field("content", stored=False, tokenizer_name="en_stem")
    for name in ("org", "agent", "user", "session", "kind", "present", "absent"):
        sb.add_text_field(name, stored=False, tokenizer_name="raw")
    for name in ("quarantined", "valid_from", "t_event"):
        sb.add_integer_field(name, indexed=True, fast=True)
    return sb.build()


def _live(row: Any) -> bool:
    return not row["deleted"] and row["invalidated_at"] is None and row["superseded_by"] is None


def _document(row: Any) -> Any:
    import tantivy

    d = tantivy.Document()
    d.add_text("id", row["id"])
    d.add_text("content", row["content"] or "")
    for field, col in (("org", "scope_org"), ("agent", "scope_agent"),
                       ("user", "scope_user"), ("session", "scope_session")):
        v = row[col]
        if v is None:
            d.add_text("absent", field)
        else:
            d.add_text(field, str(v))
            d.add_text("present", field)
    d.add_text("kind", row["kind"])
    d.add_integer("quarantined", int(bool(row["quarantined"])))
    d.add_integer("valid_from", int(row["valid_from"] or 0))
    d.add_integer("t_event", int(row["t_event"]))
    return d


class _Worker:
    """One background thread per process services every accelerator: an
    idle namespace costs a few lock-guarded comparisons per tick, not a thread."""

    _inst: "_Worker | None" = None
    _inst_lock = threading.Lock()

    @classmethod
    def get(cls) -> "_Worker":
        with cls._inst_lock:
            # a forked child inherits the singleton but not its thread
            if cls._inst is None or cls._inst._pid != os.getpid():
                cls._inst = cls()
            return cls._inst

    def __init__(self) -> None:
        self._pid = os.getpid()
        self._accs: list[TantivyLexical] = []
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True, name="memd-lexical")
        self._thread.start()

    def register(self, acc: "TantivyLexical") -> None:
        with self._lock:
            self._accs.append(acc)
        self._wake.set()

    def unregister(self, acc: "TantivyLexical") -> None:
        with self._lock:
            if acc in self._accs:
                self._accs.remove(acc)

    def wake(self) -> None:
        self._wake.set()

    def _run(self) -> None:
        while True:
            self._wake.wait(timeout=0.05)
            self._wake.clear()
            with self._lock:
                accs = list(self._accs)
            now = time.monotonic()
            for acc in accs:
                try:
                    if acc._due(now):
                        acc._run_step(blocking=False)
                except Exception:  # noqa: BLE001 - never kill the shared thread
                    _log.exception("memd: lexical worker tick failed")


class TantivyLexical:
    """The accelerator for one NamespaceIndex (see the module docstring)."""

    def __init__(self, index: "NamespaceIndex", path: str, *, commit_ms: int = DEFAULT_COMMIT_MS,
                 commit_docs: int = DEFAULT_COMMIT_DOCS, force_rebuild: bool = False):
        import tantivy

        self._tv = tantivy
        self.index = index
        self.path = path
        self.ns = index._ns_hint
        self.commit_s = max(0.0, commit_ms / 1000.0)
        self.commit_docs = max(1, int(commit_docs))
        self._lock = threading.Lock()        # state below; never held across I/O
        self._step_lock = threading.Lock()   # one step at a time (worker or drain)
        self._schema = _build_schema()
        self._pending: dict[str, int] = {}   # id -> touch generation, not yet committed
        self._touch_gen = 0
        self._new_rows = 0                   # inserts noted since the last commit
        self._first_dirty = 0.0              # monotonic time of the oldest uncommitted change
        self._w = 0                          # committed watermark (max rowid indexed)
        self._scan_from = 0                  # next scan starts after this rowid (<= _w)
        self._floor_gen = 0
        self._reset_gen = 0
        self._need_clear = False
        self._ready = False
        self._closed = False
        self._failures = 0
        self._disabled = False
        self._retry_at = 0.0
        self._last_step = 0.0
        self.rebuilds = 0
        self._searcher: Any = None
        self._uid = index.get_meta("lex_uid")
        if not self._uid:
            self._uid = uuid.uuid4().hex
            index.set_meta("lex_uid", self._uid)
        state = self._read_state()
        rebuild_why = None
        if force_rebuild:
            rebuild_why = "replayed"
        elif state is None:
            rebuild_why = "missing"
        elif state.get("version") != STATE_VERSION or state.get("uid") != self._uid:
            rebuild_why = "foreign"
        elif not state.get("clean"):
            rebuild_why = "unclean"
        self._index = None
        if rebuild_why is None:
            try:
                self._index = tantivy.Index(self._schema, path=path, reuse=True)
                self._index.reload()
                self._searcher = self._index.searcher()
                self._w = self._scan_from = int(state.get("watermark", 0))
                self._ready = True
            except Exception as e:  # noqa: BLE001 - corrupt index: rebuild
                rebuild_why = "corrupt"
                _log.warning("memd: tantivy index for %r unreadable (%s); rebuilding", self.ns,
                             type(e).__name__)
        if rebuild_why is not None:
            self._index = self._fresh_index()
            if rebuild_why != "missing" or self._has_rows():
                # (a brand-new namespace has nothing to rebuild)
                self.rebuilds += 1
                METRICS.inc("memd_lexical_rebuilds_total", help="tantivy lexical index rebuilds",
                            ns=self.ns, reason=rebuild_why)
        # running from here on: a crash before the next clean close must rebuild
        self._write_state(clean=False)
        _Worker.get().register(self)

    # ------------------------------------------------------------ files

    def _has_rows(self) -> bool:
        with self.index._read() as c:
            return c.execute("SELECT 1 FROM records LIMIT 1").fetchone() is not None

    def _fresh_index(self) -> Any:
        shutil.rmtree(self.path, ignore_errors=True)
        os.makedirs(self.path, mode=0o700, exist_ok=True)
        idx = self._tv.Index(self._schema, path=self.path, reuse=False)
        idx.reload()
        self._searcher = idx.searcher()
        return idx

    def _read_state(self) -> dict | None:
        try:
            with open(os.path.join(self.path, STATE_FILE)) as f:
                st = json.load(f)
            return st if isinstance(st, dict) else None
        except (OSError, ValueError):
            return None

    def _write_state(self, *, clean: bool) -> None:
        tmp = os.path.join(self.path, STATE_FILE + ".tmp")
        try:
            with open(tmp, "w") as f:
                json.dump({"version": STATE_VERSION, "uid": self._uid,
                           "watermark": self._w, "clean": bool(clean)}, f)
            os.replace(tmp, os.path.join(self.path, STATE_FILE))
        except OSError:
            pass  # no state = rebuild on the next open, which is always correct

    # ------------------------------------------------------------ hooks
    # Called by NamespaceIndex while it holds its write lock, BEFORE the SQL
    # change: a search between the two sees the id as pending and serves it
    # from FTS5 (the prefilter never hides a row it cannot yet see).

    def touch(self, ids: "list[str] | tuple[str, ...]") -> None:
        if not ids:
            return
        with self._lock:
            for rid in ids:
                self._touch_gen += 1
                self._pending[rid] = self._touch_gen
            if not self._first_dirty:
                self._first_dirty = time.monotonic()
            big = len(self._pending) >= self.commit_docs
        if big:
            _Worker.get().wake()

    def note_new(self, n: int) -> None:
        """Inserts: new rowids above the watermark (the tail covers them)."""
        with self._lock:
            self._new_rows += n
            if not self._first_dirty:
                self._first_dirty = time.monotonic()
            big = self._new_rows >= self.commit_docs
        if big:
            _Worker.get().wake()

    def lower_floor(self, rowid: int) -> None:
        """A hard delete removed the highest rowid: SQLite will hand it out
        again, below the watermark, so rescan (and tail-serve) from here."""
        with self._lock:
            if rowid < self._scan_from:
                self._scan_from = max(0, int(rowid))
                self._floor_gen += 1
            if not self._first_dirty:
                self._first_dirty = time.monotonic()

    def reset(self) -> None:
        """The SQLite index was wiped and is being repopulated (rowids restart)."""
        with self._lock:
            self._reset_gen += 1
            self._need_clear = True
            self._ready = False
            self._pending.clear()
            self._new_rows = 0
            self._w = self._scan_from = 0
            self._floor_gen += 1
            self._first_dirty = time.monotonic()
        self.rebuilds += 1
        METRICS.inc("memd_lexical_rebuilds_total", help="tantivy lexical index rebuilds",
                    ns=self.ns, reason="wipe")
        _Worker.get().wake()

    # ------------------------------------------------------------ state

    def ready(self) -> bool:
        with self._lock:
            return self._ready and not self._closed and not self._disabled

    def stats(self) -> dict:
        with self._lock:
            return {"backend": "tantivy", "ready": self._ready and not self._disabled,
                    "disabled": self._disabled, "watermark": self._w,
                    "pending": len(self._pending) + self._new_rows,
                    "rebuilds": self.rebuilds}

    def _due(self, now: float) -> bool:
        with self._lock:
            if self._closed or self._disabled or now < self._retry_at:
                return False
            if self._need_clear or not self._ready:
                return True
            work = len(self._pending) + self._new_rows
            if work >= self.commit_docs:
                return True
            if work and now - self._first_dirty >= self.commit_s:
                return True
            # periodic probe for rows nothing announced (cheap indexed seek)
            return now - self._last_step >= max(2.0, self.commit_s)

    # ------------------------------------------------------------ indexer

    def _run_step(self, *, blocking: bool) -> bool:
        """One batch. Returns True when more work is known to remain."""
        if not self._step_lock.acquire(blocking=blocking):
            return True
        try:
            if self._closed or self._disabled:
                return False
            try:
                more = self._step()
                self._failures = 0
                return more
            except Exception as e:  # noqa: BLE001 - the lane falls back to FTS5
                self._on_failure(e)
                return False
        finally:
            self._step_lock.release()

    def _on_failure(self, e: BaseException) -> None:
        self._failures += 1
        METRICS.inc("memd_lexical_index_failures_total",
                    help="tantivy indexer steps that failed (the lane serves from FTS5)",
                    ns=self.ns)
        with self._lock:
            self._ready = False
            self._need_clear = True
            self._w = self._scan_from = 0
            self._pending.clear()
            self._new_rows = 0
            self._retry_at = time.monotonic() + min(60.0, 2.0 * self._failures)
            if self._failures >= MAX_FAILURES:
                self._disabled = True
        if self._disabled:
            _log.warning("memd: tantivy lexical index for %r disabled after %d failures (%s); "
                         "the bm25 lane is served by FTS5", self.ns, self._failures, e)
        else:
            _log.warning("memd: tantivy lexical indexer failed for %r (%s: %s); rebuilding",
                         self.ns, type(e).__name__, e)

    def _read_rows(self, sql: str, args: list) -> list:
        with self.index._read() as c:
            return c.execute(sql, args).fetchall()

    def _step(self) -> bool:
        t0 = time.monotonic()
        with self._lock:
            reset_gen = self._reset_gen
            floor_gen = self._floor_gen
            need_clear = self._need_clear
            scan_from = self._scan_from
            touched = dict(list(self._pending.items())[:TOUCH_BATCH])
            new0 = self._new_rows
            self._last_step = t0
        rows_new = self._read_rows(
            f"SELECT {_ROW_COLS} FROM records WHERE rowid > ? ORDER BY rowid LIMIT ?",  # nosec B608
            [scan_from, CATCHUP_ROWS])
        more = len(rows_new) >= CATCHUP_ROWS
        rows_touched: list = []
        ids = list(touched)
        for i in range(0, len(ids), 500):
            chunk = ids[i:i + 500]
            rows_touched += self._read_rows(
                f"SELECT {_ROW_COLS} FROM records WHERE id IN ({','.join('?' * len(chunk))})",  # nosec B608
                chunk)
        new_w = int(rows_new[-1]["rowid"]) if rows_new else scan_from
        if rows_new or touched or need_clear:
            writer = self._index.writer(heap_size=WRITER_HEAP, num_threads=1)
            try:
                if need_clear:
                    writer.delete_all_documents()
                seen: set[str] = set()
                for rid in touched:
                    writer.delete_documents_by_term("id", rid)
                for row in list(rows_new) + list(rows_touched):
                    rid = row["id"]
                    if rid in seen:
                        continue
                    seen.add(rid)
                    if rid not in touched:
                        # idempotent: a rescan from a lowered floor (or after
                        # a crash) may meet a doc that is already indexed
                        writer.delete_documents_by_term("id", rid)
                    if _live(row):
                        writer.add_document(_document(row))
                writer.commit()
                writer.wait_merging_threads()
            except BaseException:
                try:
                    writer.rollback()
                except Exception:
                    pass
                raise
            finally:
                del writer
            self._index.reload()
            searcher = self._index.searcher()
            METRICS.observe("memd_lexical_commit_ms", (time.monotonic() - t0) * 1000,
                            help="tantivy indexer batch: read + index + commit (ms)", ns=self.ns)
            METRICS.inc("memd_lexical_indexed_total", len(rows_new) + len(rows_touched),
                        help="rows (re)indexed into tantivy", ns=self.ns)
        else:
            searcher = None
        with self._lock:
            if self._reset_gen != reset_gen:
                return True  # wiped mid-step: this batch described the old table
            if searcher is not None:
                self._searcher = searcher
            if need_clear:
                self._need_clear = False
            self._w = new_w
            if self._floor_gen == floor_gen:
                self._scan_from = new_w
            else:
                self._scan_from = min(self._scan_from, new_w)
            for rid, gen in touched.items():
                if self._pending.get(rid) == gen:
                    del self._pending[rid]
            # inserts noted after new0 may already be in rows_new: at worst
            # that costs one empty step, never a missed row (the tail and the
            # periodic probe cover what a count misses)
            self._new_rows = max(0, self._new_rows - (len(rows_new) if more else new0))
            if not more and not self._ready:
                self._ready = True
            left = len(self._pending) + self._new_rows
            self._first_dirty = time.monotonic() if left else 0.0
            more = more or bool(self._pending) or self._scan_from < self._w
        if searcher is not None:
            self._write_state(clean=False)
        METRICS.set_gauge("memd_lexical_lag", float(left),
                          help="changes not yet in the tantivy index (served from FTS5)", ns=self.ns)
        return more

    def drain(self, timeout_s: float = 60.0) -> bool:
        """Index everything visible now; True when caught up."""
        deadline = time.monotonic() + max(0.0, timeout_s)
        while time.monotonic() < deadline:
            if self._closed or self._disabled:
                return False
            if not self._run_step(blocking=True):
                with self._lock:
                    if not self._pending and self._ready:
                        return True
        return False

    def close(self) -> None:
        if self._closed:
            return
        _Worker.get().unregister(self)
        with self._step_lock:
            clean = False
            if not self._disabled:
                try:
                    more = self._step()
                    with self._lock:
                        clean = (not more and self._ready and not self._pending
                                 and not self._need_clear)
                except Exception:  # noqa: BLE001 - an unclean state rebuilds on open
                    clean = False
            self._closed = True
            self._write_state(clean=clean)
            self._searcher = None
            self._index = None

    # ------------------------------------------------------------ search

    def _filter_query(self, f: "IndexFilter", now_ms: float) -> list:
        """The tantivy twin of NamespaceIndex._filter_where (current view).
        It must never be STRICTER than SQL (that would hide rows); a looser
        one is caught by the _passes_filter post-check."""
        Q, Occur, FT = self._tv.Query, self._tv.Occur, self._tv.FieldType
        schema = self._schema

        def term(field: str, value: Any) -> Any:
            return Q.term_query(schema, field, value)

        def anyof(*qs: Any) -> Any:
            return Q.boolean_query([(Occur.Should, q) for q in qs])

        out = []
        s = f.scope
        if s is not None:
            for field, val in (("org", s.org), ("agent", s.agent), ("user", s.user)):
                if val is not None:
                    out.append(anyof(term("absent", field), term(field, str(val))))
            if s.session is not None:
                out.append(anyof(term("absent", "session"), term("session", str(s.session))))
            if s.user is not None or s.session is not None:
                parts = [term("absent", "session"), term("present", "user")]
                if s.session is not None:
                    parts.append(term("session", str(s.session)))
                out.append(anyof(*parts))
        if f.kinds:
            out.append(anyof(*(term("kind", k) for k in f.kinds)))
        if f.t_event_min is not None or f.t_event_max is not None:
            out.append(Q.range_query(schema, "t_event", FT.Integer,
                                     None if f.t_event_min is None else int(f.t_event_min),
                                     None if f.t_event_max is None else int(f.t_event_max)))
        # only live rows are indexed; not-yet-valid ones stay hidden
        out.append(Q.range_query(schema, "valid_from", FT.Integer, None, int(now_ms)))
        if not f.include_quarantined:
            out.append(term("quarantined", 0))
        return out

    def search(self, query: str, f: "IndexFilter", limit: int) -> "list[Hit] | None":
        """bm25-lane hits, or None: serve this query from FTS5."""
        from memd.index.sqlite_index import Hit, _fts_escape

        with self._lock:
            ok = self._ready and not self._closed and not self._disabled
            searcher = self._searcher
            tail_from = min(self._w, self._scan_from)
            pending = set(self._pending)
        if not ok or searcher is None:
            METRICS.inc("memd_lexical_fallback_total", help="bm25-lane queries served by FTS5 instead of tantivy",
                        ns=self.ns, reason="not_ready")
            return None
        if (f.as_of is not None or f.include_invalid or f.sources or f.entity_keys
                or limit > MAX_TANTIVY_LIMIT):
            METRICS.inc("memd_lexical_fallback_total", ns=self.ns, reason="unsupported_filter")
            return None
        q_all = _fts_escape(query)
        if not q_all:
            return []
        terms = [t for t in q_all.split() if any(ch.isalnum() for ch in t)]
        now = f.now if f.now is not None else time.time() * 1000
        fetch = max(1, limit * 2)
        try:
            main_ids: list[tuple[str, float]] = []
            if terms:
                content = self._index.parse_query(" OR ".join(f'"{t}"' for t in terms), ["content"])
                clauses = [(self._tv.Occur.Must, content)]
                clauses += [(self._tv.Occur.Must, self._tv.Query.const_score_query(c, 0.0))
                            for c in self._filter_query(f, now)]
                res = searcher.search(self._tv.Query.boolean_query(clauses), fetch, count=False)
                for score, addr in res.hits:
                    main_ids.append((searcher.doc(addr)["id"][0], float(score)))
        except Exception as e:  # noqa: BLE001 - never fail the lane; FTS5 serves
            METRICS.inc("memd_lexical_fallback_total", ns=self.ns, reason="error")
            _log.warning("memd: tantivy search failed for %r (%s); serving from FTS5",
                         self.ns, type(e).__name__)
            self._on_failure(e)
            return None
        saturated = len(main_ids) >= fetch
        idx = self.index
        # a pending row's committed doc is stale: the tail serves that row
        main = [(rid, score) for rid, score in main_ids if rid not in pending]
        match_expr = " OR ".join(f'"{w}"' for w in q_all.split())
        tail = idx._fts5_bm25(match_expr, f, limit, rowid_min=tail_from, ids_only=True)
        if pending:
            tail += idx._fts5_bm25(match_expr, f, limit, ids=sorted(pending), ids_only=True)
        order: list[tuple[str, float]] = []
        seen: set[str] = set()
        for i in range(max(len(main), len(tail))):
            for src in (main, tail):
                if i < len(src) and src[i][0] not in seen:
                    seen.add(src[i][0])
                    order.append(src[i])
        # hydrate only what is returned (a chunk at a time) and post-check
        # EVERY row against SQLite, the source of truth
        hits: list = []
        pos = 0
        while pos < len(order) and len(hits) < limit:
            chunk = order[pos:pos + (limit - len(hits))]
            pos += len(chunk)
            recs = self._hydrate([rid for rid, _ in chunk])
            for rid, score in chunk:
                rec = recs.get(rid)
                if rec is None or rid in f.exclude_ids or not idx._passes_filter(rec, f):
                    continue
                hits.append(Hit(record=rec, score=score, lane="bm25"))
        if saturated and len(hits) < limit:
            # tantivy's window filled with rows the post-check rejected, so
            # eligible rows may lie beyond it - correctness first
            METRICS.inc("memd_lexical_fallback_total", ns=self.ns, reason="short")
            return None
        tail_ids = {rid for rid, _ in tail}
        n_tail = sum(1 for h in hits if h.record.id in tail_ids)
        if n_tail:
            METRICS.inc("memd_lexical_tail_hits_total", n_tail,
                        help="bm25-lane hits served from the FTS5 tail (not yet in tantivy)", ns=self.ns)
        METRICS.inc("memd_lexical_searches_total", help="bm25-lane queries served by tantivy",
                    ns=self.ns)
        return hits

    def _hydrate(self, ids: list[str]) -> dict:
        if not ids:
            return {}
        idx = self.index
        with idx._read() as c:
            rows = c.execute(f"SELECT * FROM records WHERE id IN ({','.join('?' * len(ids))})",  # nosec B608
                             ids).fetchall()
        out = {}
        for r in rows:
            rec = idx._row_to_record(r)
            rec.namespace = idx._ns_hint
            out[rec.id] = rec
        return out
