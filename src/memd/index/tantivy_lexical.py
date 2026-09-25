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
    The watermark is sound because the SQLite index never reuses a rowid
    (NamespaceIndex._note_rowid_hwm): every row above it is new.
  - only live rows (not deleted, invalidated or superseded) are indexed,
    keyed by record id, with the filter fields (org/agent/user/session,
    kind, quarantine flag, valid_from, t_event) as a boolean PREFILTER.
  - search = tantivy top-(2k) over committed docs
             UNION FTS5 bm25 over the tail (rowid > watermark, plus any row
             changed since the last commit: overwrites, deletes,
             supersession and quarantine flips mark their ids pending
             BEFORE the SQL change).
    Every result is hydrated from SQLite and post-checked with
    _passes_filter; if tantivy's window was full but fewer than k survive,
    the window widens ONCE (x4, within WINDOW_MAX) and, if that is still
    short, the lane falls back to the FTS5 path: on templated data a tie
    group can fill any window, and widening 60 -> 4096 in four steps cost
    47-54ms before FTS5 answered in 8-16ms anyway. Filters the prefilter
    does not model (as_of, include_invalid,
    sources, entity keys), sweep-size limits and query terms too long for
    tantivy's term dictionary go to FTS5 directly.
  - ties: a doc's position among equal scores in tantivy depends on how the
    docs are spread over segments, which depends on commit timing, so
    re-ingesting the same data could return a different top-k. Scores are
    quantized (SCORE_QUANTUM) and ties broken by content, as fusion does
    (score, -t_event, content hash, id) - the key FTS5 serves the lane with
    too, and the one the merge with the tail uses, so neither the backend
    nor whether a row is committed yet changes the order of tied rows. A
    row in a score group that reaches a full window's edge is not served.
  - scores are NOT independent of the commit schedule: tantivy's BM25
    statistics (doc count, doc frequency, average length) include deleted
    and superseded docs until their segments merge, so the same operation
    history committed in a different rhythm can score - and order - two
    near-equal docs differently. Results are deterministic for the same
    operation history AND commit schedule. Re-scoring the window in FTS5
    would remove this, but costs 11 ms p50 / 51 ms p95 per query at 20K docs
    (a 60-row `rowid IN` bm25) against 0.6 ms for the window itself.
  - open: a missing, corrupt, foreign (different SQLite file or an older
    STATE_VERSION) or uncleanly closed index is rebuilt in the background;
    until it has caught up the lane is served by FTS5.
  - failures: an error that means the index is damaged (I/O, missing or
    corrupt files, a tantivy panic) schedules a rebuild into a fresh
    directory; any other error only sends that query (or delays that batch)
    to FTS5. Retries back off exponentially (BACKOFF_BASE_S doubling up to
    BACKOFF_MAX_S) on a failure history that a success does not erase: it
    decays only after FAILURE_DECAY_S without a failure. (Resetting it on
    every successful rebuild meant damage that only shows at search time
    rebuilt every ~2s: 6 rebuilds in 12s.) Nothing disables the accelerator
    for good. stats() counts every rebuild and shows the failure history,
    the lifetime failure count and the backoff.
Tail merge: tail hits are placed among tantivy's by score (ties by the
key above). Both are BM25
(k1 1.2, b 0.75) over the same text, so a rare decisive term scores high in
either; they differ in IDF floor (FTS5 ~0 for terms in over half the rows,
tantivy >= ln 2), stemmer and length quantization, so the merge is
approximate - and it only matters for writes younger than one batch.
Re-scoring tantivy's candidates in FTS5 instead would restore FTS5's cost
(a `rowid IN (...)` bm25 query costs ~2.7 ms per row at 50K). A plain
rank interleave, the first version, put a just-written record holding the
one rare query term below tantivy's best common-term match.
Rows changed since the last commit are served from FTS5 only while they
still pass the filter (a deleted row needs no serving) and are few; many
such rows send the query to FTS5.
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
from memd.query.fusion import content_sha

if TYPE_CHECKING:  # pragma: no cover
    from memd.index.sqlite_index import Hit, IndexFilter, NamespaceIndex

_log = logging.getLogger(__name__)

LEXICAL_CHOICES = ("auto", "fts5", "tantivy")
STATE_FILE = "memd-lexical.json"
# 2: the memd_text tokenizer (no 40-byte token cut), stored t_event and
# content hash, and never-reused rowids. An index of an older version is
# rebuilt ("foreign"): it may also miss rows a reused rowid hid from it.
STATE_VERSION = 2
TOKENIZER = "memd_text"
DEFAULT_COMMIT_MS = 500
DEFAULT_COMMIT_DOCS = 512
CATCHUP_ROWS = 8192          # rows scanned per step while catching up
TOUCH_BATCH = 4096           # changed ids re-indexed per step
WRITER_HEAP = 32_000_000     # tantivy needs >= 15MB per writer thread
MAX_TANTIVY_LIMIT = 1000     # larger (sweep) limits go to FTS5
WINDOW_MAX = 4096            # widest tantivy window before FTS5 answers
WINDOW_WIDENINGS = 1         # x4 widenings of a short window before FTS5 answers
SCORE_QUANTUM = 1e-4         # scores closer than this are ties
# tantivy silently drops a token longer than 65530 bytes (its term size
# limit) from the doc: a query term anywhere near that goes to FTS5
LONG_TERM_BYTES = 32_000
BACKOFF_BASE_S = 2.0         # first retry after a failure; doubles per failure
BACKOFF_MAX_S = 300.0
FAILURE_DECAY_S = 600.0      # the failure history forgets after this long without one
MAX_PENDING_TAIL = 8         # changed-and-eligible rows served per query from FTS5
_I64_MIN = -(2 ** 63)
# substrings of the errors that mean the index itself is damaged (tantivy
# raises ValueError carrying the Rust error text): only these rebuild it
_DAMAGE_MARKERS = ("corrupt", "failed to open", "filedoesnotexist", "ioerror", "io error",
                   "invaliddata", "footer", "checksum", "incompatible", "no such file",
                   "tokenizer")

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
    sb.add_text_field("content", stored=False, tokenizer_name=TOKENIZER)
    for name in ("org", "agent", "user", "session", "kind", "present", "absent"):
        sb.add_text_field(name, stored=False, tokenizer_name="raw")
    for name in ("quarantined", "valid_from"):
        sb.add_integer_field(name, indexed=True, fast=True)
    # stored: the tie-break reads them from the hit (see "ties")
    sb.add_integer_field("t_event", stored=True, indexed=True, fast=True)
    sb.add_unsigned_field("csha", stored=True)
    return sb.build()


def _analyzer() -> Any:
    """tantivy's `en_stem` minus its RemoveLongFilter(40), which silently
    dropped every token of 40+ bytes - a SHA-1 or SHA-256 hex digest, a long
    id, an unspaced CJK sentence - so such records could not be found by
    the very term FTS5 finds them by. Tokens up to tantivy's own term limit
    are indexed; longer query terms go to FTS5 (LONG_TERM_BYTES)."""
    import tantivy

    return (tantivy.TextAnalyzerBuilder(tantivy.Tokenizer.simple())
            .filter(tantivy.Filter.lowercase())
            .filter(tantivy.Filter.stemmer("english"))
            .build())


def _content_key(content: str) -> int:
    """fusion.content_sha as an unsigned int (the same order)."""
    return int(content_sha(content), 16)


def _quant(score: float) -> int:
    return round(score / SCORE_QUANTUM)


def _is_damage(e: BaseException) -> bool:
    """Whether an error means the index itself is damaged (rebuild it)
    rather than that one query or batch failed (retry it)."""
    if isinstance(e, OSError) or type(e).__name__ == "PanicException":
        return True
    msg = str(e).lower()
    return any(m in msg for m in _DAMAGE_MARKERS)


def _live(row: Any) -> bool:
    return not row["deleted"] and row["invalidated_at"] is None and row["superseded_by"] is None


def _document(row: Any) -> Any:
    import tantivy

    d = tantivy.Document()
    d.add_text("id", row["id"])
    d.add_text("content", row["content"] or "")
    d.add_unsigned("csha", _content_key(row["content"] or ""))
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
        self._reset_gen = 0
        self._need_clear = False
        self._ready = False
        self._closed = False
        self._failures = 0                   # failure history: sets the backoff (see FAILURE_DECAY_S)
        self._failures_total = 0
        self._last_failure = 0.0             # monotonic time of the latest failure
        self._failing = False                # no successful step since the latest failure
        self._retry_at = 0.0                 # no indexer step before this (backoff)
        self._last_err_log = 0.0
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
                self._index = self._open_index(reuse=True)
                self._index.reload()
                self._searcher = self._index.searcher()
                self._w = int(state.get("watermark", 0))
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

    def _open_index(self, *, reuse: bool) -> Any:
        idx = self._tv.Index(self._schema, path=self.path, reuse=reuse)
        # custom analyzers are not persisted with the index: register it on
        # every open, before any writer or query parser needs it
        idx.register_tokenizer(TOKENIZER, _analyzer())
        return idx

    def _fresh_index(self) -> Any:
        shutil.rmtree(self.path, ignore_errors=True)
        os.makedirs(self.path, mode=0o700, exist_ok=True)
        idx = self._open_index(reuse=False)
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

    def reset(self) -> None:
        """The SQLite index was wiped and is being repopulated."""
        with self._lock:
            self._reset_gen += 1
            self._need_clear = True
            self._ready = False
            self._pending.clear()
            self._new_rows = 0
            self._w = 0
            self._first_dirty = time.monotonic()
            self.rebuilds += 1
        METRICS.inc("memd_lexical_rebuilds_total", help="tantivy lexical index rebuilds",
                    ns=self.ns, reason="wipe")
        _Worker.get().wake()

    # ------------------------------------------------------------ state

    def ready(self) -> bool:
        with self._lock:
            return self._ready and not self._closed

    def stats(self) -> dict:
        """`disabled`: the indexer is paused, backing off after a failure,
        for another `retry_in_s` (a damaged index is not `ready` meanwhile,
        and FTS5 serves the lane). `rebuilds` counts every rebuild: at open,
        after a wipe, and after damage found while running. `failures` is
        the history the backoff grows with (kept across successful steps,
        forgotten after FAILURE_DECAY_S without one); `failures_total`
        never decreases."""
        with self._lock:
            now = time.monotonic()
            self._decay_failures(now)
            wait = max(0.0, self._retry_at - now) if self._failing else 0.0
            return {"backend": "tantivy", "ready": self._ready, "disabled": wait > 0,
                    "watermark": self._w, "pending": len(self._pending) + self._new_rows,
                    "rebuilds": self.rebuilds, "failures": self._failures,
                    "failures_total": self._failures_total, "retry_in_s": round(wait, 1)}

    def _decay_failures(self, now: float) -> None:
        """(lock held) Forget the failure history after a quiet period."""
        if self._failures and not self._failing and now - self._last_failure >= FAILURE_DECAY_S:
            self._failures = 0

    def _due(self, now: float) -> bool:
        with self._lock:
            if self._closed or now < self._retry_at:
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
            if self._closed:
                return False
            try:
                more = self._step()
                with self._lock:
                    # NOT a reset of the history: damage that only shows at
                    # search time survives a rebuild, and must back off longer
                    self._failing = False
                    self._decay_failures(time.monotonic())
                return more
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException as e:  # noqa: BLE001 - the lane falls back to FTS5
                # (BaseException: a tantivy panic surfaces as pyo3's PanicException)
                self._on_failure(e, damaged=_is_damage(e))
                return False
        finally:
            self._step_lock.release()

    def _on_failure(self, e: BaseException, *, damaged: bool) -> None:
        """Back off exponentially; rebuild (into a fresh directory) only when
        the index itself is damaged. A failed batch that did not damage it
        changed nothing: its rows stay pending / above the watermark, the
        tail keeps serving them, and the retry redoes it."""
        with self._lock:
            now = time.monotonic()
            self._decay_failures(now)
            self._failures += 1
            self._failures_total += 1
            self._last_failure = now
            self._failing = True
            backoff = min(BACKOFF_MAX_S, BACKOFF_BASE_S * 2 ** min(self._failures - 1, 16))
            self._retry_at = now + backoff
            rebuild = damaged and not self._need_clear
            if damaged:
                self._reset_gen += 1  # a batch in flight (search found the damage) is void
                self._ready = False
                self._need_clear = True
                self._w = 0
                self._pending.clear()
                self._new_rows = 0
            if rebuild:
                self.rebuilds += 1
            failures = self._failures
        METRICS.inc("memd_lexical_index_failures_total",
                    help="tantivy indexer steps that failed (the lane serves from FTS5)",
                    ns=self.ns)
        if rebuild:
            METRICS.inc("memd_lexical_rebuilds_total", help="tantivy lexical index rebuilds",
                        ns=self.ns, reason="damaged")
        _log.warning("memd: tantivy lexical index for %r failed (%s: %s); %s in %.0fs "
                     "(failure %d in a row)", self.ns, type(e).__name__, e,
                     "damaged: FTS5 serves the bm25 lane until it is rebuilt, starting"
                     if damaged else "intact: FTS5 serves the unindexed rows, retrying",
                     backoff, failures)

    def _read_rows(self, sql: str, args: list) -> list:
        with self.index._read() as c:
            return c.execute(sql, args).fetchall()

    def _step(self) -> bool:
        t0 = time.monotonic()
        with self._lock:
            reset_gen = self._reset_gen
            need_clear = self._need_clear
            scan_from = self._w
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
            if need_clear:
                # a fresh directory, not delete_all_documents(): the old
                # one may be the damage (searches meanwhile go to FTS5)
                self._index = self._fresh_index()
            writer = self._index.writer(heap_size=WRITER_HEAP, num_threads=1)
            try:
                seen: set[str] = set()
                for rid in touched:
                    writer.delete_documents_by_term("id", rid)
                for row in list(rows_new) + list(rows_touched):
                    rid = row["id"]
                    if rid in seen:
                        continue
                    seen.add(rid)
                    if rid not in touched:
                        # idempotent: a retried batch may meet a doc that
                        # is already indexed
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
            more = more or bool(self._pending)
        if searcher is not None:
            self._write_state(clean=False)
        METRICS.set_gauge("memd_lexical_lag", float(left),
                          help="changes not yet in the tantivy index (served from FTS5)", ns=self.ns)
        return more

    def drain(self, timeout_s: float = 60.0) -> bool:
        """Index everything visible now; True when caught up."""
        deadline = time.monotonic() + max(0.0, timeout_s)
        while time.monotonic() < deadline:
            with self._lock:
                if self._closed or time.monotonic() < self._retry_at:
                    return False  # backing off after a failure: FTS5 serves meanwhile
            if not self._run_step(blocking=True):
                with self._lock:
                    # (a failed step leaves the index ready but not caught up)
                    if not self._pending and self._ready and not self._failing:
                        return True
        return False

    def close(self) -> None:
        if self._closed:
            return
        _Worker.get().unregister(self)
        with self._step_lock:
            clean = False
            try:
                more = self._step()
                with self._lock:
                    clean = (not more and self._ready and not self._pending
                             and not self._need_clear)
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:  # noqa: BLE001 - an unclean state rebuilds on open
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
        from memd.index.sqlite_index import _fts_escape

        with self._lock:
            ok = self._ready and not self._closed
            searcher = self._searcher
            tail_from = self._w
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
        if any(len(t.encode("utf-8")) >= LONG_TERM_BYTES for t in terms):
            METRICS.inc("memd_lexical_fallback_total", ns=self.ns, reason="long_term")
            return None
        now = f.now if f.now is not None else time.time() * 1000
        idx = self.index
        match_expr = " OR ".join(f'"{w}"' for w in q_all.split())
        tail = idx._fts5_bm25(match_expr, f, limit, rowid_min=tail_from, ids_only=True)
        if pending:
            # only changed rows that are eligible NOW need serving (deletes
            # and supersessions - the common changes - never do); a bm25
            # lookup restricted to given rows costs ~2 ms per row in FTS5
            live = self._eligible(sorted(pending), f)
            if len(live) > MAX_PENDING_TAIL:
                METRICS.inc("memd_lexical_fallback_total", ns=self.ns, reason="pending")
                return None
            if live:
                tail += idx._fts5_bm25(match_expr, f, limit, ids=live, ids_only=True)
        fetch = max(1, limit * 2)
        widenings = 0
        recs: dict = {}
        while True:
            try:
                main_ids = self._top(searcher, terms, f, now, fetch)
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException as e:  # noqa: BLE001 - never fail the lane; FTS5 serves
                self._on_search_error(e)
                return None
            hits = self._merge(main_ids, len(main_ids) >= fetch, tail, pending, f, limit, recs)
            if hits is not None:
                break
            if widenings >= WINDOW_WIDENINGS or fetch * 4 > WINDOW_MAX:
                # tantivy's window filled with rows the post-check rejected
                # (or with one tie group), so eligible rows may lie beyond
                # it - correctness first, and FTS5 answers that faster than
                # more windows would (see the module docstring)
                METRICS.inc("memd_lexical_fallback_total", ns=self.ns, reason="short")
                return None
            widenings += 1
            fetch *= 4
        tail_ids = {t[0] for t in tail}
        n_tail = sum(1 for h in hits if h.record.id in tail_ids)
        if n_tail:
            METRICS.inc("memd_lexical_tail_hits_total", n_tail,
                        help="bm25-lane hits served from the FTS5 tail (not yet in tantivy)", ns=self.ns)
        METRICS.inc("memd_lexical_searches_total", help="bm25-lane queries served by tantivy",
                    ns=self.ns)
        return hits

    def _top(self, searcher: Any, terms: list[str], f: "IndexFilter", now: float,
             fetch: int) -> list[tuple[str, float, int, int]]:
        """tantivy's top-`fetch`: [(id, score, t_event, content key)]."""
        if not terms:
            return []
        content = self._index.parse_query(" OR ".join(f'"{t}"' for t in terms), ["content"])
        clauses = [(self._tv.Occur.Must, content)]
        clauses += [(self._tv.Occur.Must, self._tv.Query.const_score_query(c, 0.0))
                    for c in self._filter_query(f, now)]
        res = searcher.search(self._tv.Query.boolean_query(clauses), fetch, count=False)
        out = []
        for score, addr in res.hits:
            d = searcher.doc(addr)
            out.append((d["id"][0], float(score), int(d["t_event"][0]), int(d["csha"][0])))
        return out

    def _merge(self, main_ids: list[tuple[str, float, int, int]], saturated: bool,
               tail: list[tuple[str, float, int, int]], pending: set[str], f: "IndexFilter", limit: int,
               recs: dict) -> "list[Hit] | None":
        """tantivy's window and the FTS5 tail as one list by score, each row
        hydrated and post-checked. None: the window is too narrow to answer."""
        from memd.index.sqlite_index import Hit

        idx = self.index
        # a full window says nothing about the docs past it, except that they
        # score <= its last score: a row scoring at or below that edge could
        # be outranked by (or tie with) one of them, so it is not served
        edge = _quant(main_ids[-1][1]) if saturated and main_ids else None
        # a pending row's committed doc is stale: the tail serves that row
        main = [m for m in main_ids if m[0] not in pending]
        # one list by score (see "Tail merge"), ties by the key fusion uses
        # (score, -t_event, content, id) whichever side a row came from:
        # equal scores keep one order however the docs are spread over
        # segments, whether they are committed yet, and on either backend
        ranked = [(_quant(score), t_event, csha, rid, score)
                  for rid, score, t_event, csha in list(main) + list(tail)]
        ranked.sort(key=lambda t: (-t[0], -t[1], t[2], t[3]))
        order: list[tuple[str, int, float]] = []
        seen: set[str] = set()
        for qscore, _t, _h, rid, score in ranked:
            if rid not in seen:
                seen.add(rid)
                order.append((rid, qscore, score))
        # hydrate only what is returned (a chunk at a time) and post-check
        # EVERY row against SQLite, the source of truth
        hits: list = []
        pos = 0
        while pos < len(order) and len(hits) < limit:
            chunk = order[pos:pos + (limit - len(hits))]
            pos += len(chunk)
            if edge is not None:
                safe = [c for c in chunk if c[1] > edge]
                blocked = len(safe) < len(chunk)
                chunk = safe
            else:
                blocked = False
            want = [rid for rid, _, _ in chunk if rid not in recs]
            if want:
                recs.update(self._hydrate(want))
            for rid, _q, score in chunk:
                rec = recs.get(rid)
                if rec is None or rid in f.exclude_ids or not idx._passes_filter(rec, f):
                    continue
                hits.append(Hit(record=rec, score=score, lane="bm25"))
            if blocked:
                break
        if saturated and len(hits) < limit:
            return None
        return hits

    def _on_search_error(self, e: BaseException) -> None:
        """A query tantivy could not answer goes to FTS5. Only damage
        rebuilds the index; any other error leaves it serving."""
        METRICS.inc("memd_lexical_fallback_total", ns=self.ns, reason="error")
        if _is_damage(e):
            self._on_failure(e, damaged=True)
            return
        now = time.monotonic()
        with self._lock:
            quiet = now - self._last_err_log < 60.0
            if not quiet:
                self._last_err_log = now
        if not quiet:  # one line a minute, not one per query
            _log.warning("memd: tantivy search failed for %r (%s: %s); serving that query from FTS5",
                         self.ns, type(e).__name__, e)

    def _eligible(self, ids: list[str], f: "IndexFilter") -> list[str]:
        """The ids whose rows pass `f` in SQLite right now (PK lookups)."""
        out: list[str] = []
        idx = self.index
        for i in range(0, len(ids), 500):
            chunk = ids[i:i + 500]
            args: list = list(chunk)
            where = idx._filter_where(f, args)
            with idx._read() as c:
                out += [r[0] for r in c.execute(
                    f"SELECT id FROM records WHERE id IN ({','.join('?' * len(chunk))}) AND {where}",  # nosec B608
                    args).fetchall()]
        return out

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
