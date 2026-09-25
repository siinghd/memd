# Changelog

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project's engineering log - every defect with its reproduction and
before/after numbers - is [.ralph/audit-log.md](.ralph/audit-log.md).

## [Unreleased]

### Fixed
- **tantivy lane: a write after hard-deleting the newest rows was never
  indexed.** SQLite reused their rowids below the accelerator's watermark.
  Rowids are now never reused (a high-water mark committed with the delete);
  indexes of the previous version are rebuilt once on open.
- **tantivy lane: tokens of 40+ bytes were dropped** (`en_stem`'s length
  filter): hashes, long ids and unspaced CJK are now indexed; a query term
  near tantivy's 65,530-byte term limit is served by FTS5.
- **tantivy lane: overwriting an existing id** (`memd import --native`) now
  re-indexes it; the old text and scope no longer match.
- **tantivy lane: tied scores are ordered by content** (score, -t_event,
  content hash, id), so re-ingesting the same data gives the same top-k
  however it was split into segments.
- **tantivy failures**: only damage (I/O, missing or corrupt files, a panic)
  rebuilds the index, into a fresh directory, with exponential backoff; other
  errors send that query to FTS5. It is never disabled until restart;
  `stats()["lexical"]` counts every rebuild and shows `failures` and
  `retry_in_s`.
- **Reranker calls are bounded**: at most `max_inflight` (default 8) run at
  once, timed-out ones included; past that a search skips reranking
  (`reason="busy"`), and a call whose deadline passed while queued never
  sends its texts. A `BaseException` from a reranker no longer escapes
  search (KeyboardInterrupt and SystemExit still propagate).

## [0.2.0] - 2026-09-24

First release evaluated on real data: LongMemEval (ICLR 2025), not memd's synthetic
suite. Session retrieval ndcg@5 on LongMemEval_S dev: 0.727 (0.1.0) -> 0.866 (zero-key
default) -> 0.955 (with the Jev reranker). See [BENCHMARKS.md](BENCHMARKS.md).

### Added
- **Reranker plug-in** (`memd.query.rerank`): the top-30 of the bm25 lane
  (plus the vector lane with a real embedder) is reordered by a relevance
  judge. `reranker` / `MEMD_RERANKER` = `auto | none | jev | local`; `auto`
  picks Jev (TypeSafe System One, `pip install "memd[jev]"`) only when
  `TYPESAFE_API_KEY` is set, otherwise none - no key, no egress. With Jev
  active the query and the top-30 candidate texts are sent to TypeSafe's API
  (SECURITY.md). One request per query, one Noul per candidate, chunks of 25
  issued concurrently, 1.5s total deadline. Any error, timeout or malformed
  answer keeps the unreranked order and counts
  `memd_rerank_fallback_total{reason}`; search never fails because of it.
  `local` is a fastembed cross-encoder (default `BAAI/bge-reranker-base`),
  loaded in the background. LongMemEval_S session ndcg@5: bm25 0.891 ->
  bm25 + Jev 0.954 (lab experiment 015).
- **Gated evidence packing**, experimental and opt-in (`pack_mode="gated"`;
  `auto` = ranked with every reranker): the context holds only the
  candidates a calibrated reranker judges relevant (p >= `rerank_gate`,
  default 0.5, else the top 3) plus the turn before and after each in the
  same session, grouped by session under a session-date header,
  budget-capped, with the same provenance fencing. Lab 018: equal QA
  accuracy to top-k packing at 27% fewer tokens over a 100-candidate
  shortlist; over the product's top-30 it drops second evidence sessions
  (lab 020/021, Jev: session ndcg@5 0.906 / recall_all@5 0.803 gated vs
  0.955 / 0.928 ranked), so it is not the default.
- **tantivy lexical accelerator** (`pip install "memd[fast]"`,
  `lexical_backend` = `auto | fts5 | tantivy`). A derived index fed in the
  background; FTS5 stays the synchronous source of truth, so the write ack
  is unchanged and unindexed writes are served from FTS5. Rebuilt in the
  background when missing, corrupt, foreign or not closed cleanly; removed
  on crypto-shred. `bench/lexical_bench.py` measures it filtered, through
  `Memory.search`.
- **Nightly real-data gate**: `bench/lme_gate.py` runs 60 fixed LongMemEval_S
  questions (10 per type, dev split) through `Memory.search` and fails below
  session ndcg@5 0.80 (hash embedder, no reranker);
  `.github/workflows/nightly-gate.yml` runs it nightly and skips offline.
- **S3-compatible object store backend** (`pip install "memd[s3]"`,
  `Memory("s3://bucket/prefix")`) for AWS S3, Cloudflare R2, MinIO and Ceph.
  S3 has no append, so each append is its own immutable object and the logical
  object is their ordered concatenation: one durable write ack = one PUT, and a
  torn frame cannot exist. Single-writer is enforced by a lease rather than a
  file lock, since `flock` cannot see another machine. Exercised in CI against
  a real S3 server. This makes the hosted SLOs in `02-slos.md` evidenced rather
  than aspirational - with the caveat that the measurements are against a
  server on loopback, so the latencies are a floor and the round-trip counts
  (1 PUT per write, 0 reads per warm search) are the durable claim.
- An `ObjectStore` contract suite that every backend must pass, with
  `LocalObjectStore` as the oracle. The interface previously had one
  implementation and therefore no specification.
- CI: tests on 3.11/3.12, the eval gate at three seed/scale configs, bandit,
  a clean-install smoke test, and a container smoke test. Benchmarks run
  nightly as informational artifacts and never gate the build.
- `POST /v1/ns/{ns}/reembed` - rebuilds the vector lane from raw. Previously
  CLI-only, which a hosted operator cannot reach on the node that needs it.
- Derived-index snapshots: compaction publishes a gzipped, encrypted image of
  the folded index, so a cold node restores it instead of re-folding every
  record. Cold open at 40K records: 2111ms -> 922ms.
- Per-namespace rate ceiling (`MEMD_NS_RATE_LIMIT_PER_MIN`), so a tenant can no
  longer multiply its quota by minting keys.
- Process resource gauges (RSS, fds, threads, CPU), per-request object-store
  I/O counts, per-stage search timings, and a bounded vector-health gauge with
  self-heal.

### Fixed - data integrity
- **A crash after an acked delete undid it** (soft or hard, single or batch;
  predates this release): recovery replayed the whole ops log before the WAL
  records, so a record deleted while still in the WAL was re-created by its
  own frame on the next open - hard-deleted content served again. Recovery
  now reproduces the acknowledged history in one order: every WAL frame is
  stamped with its seq (ops already carried theirs), and open, rebuild,
  rotate and compaction all fold one seq-ordered stream, a segment standing
  in for every event at or below its fold_seq. The same order fixes three
  siblings: a rotate dropped ops whose target lived in an older segment (a
  cache wipe, `rebuild_index` or a second node resurrected those records,
  and a due hard delete folded by a rotate was never purged); and frame seqs
  inferred from position hid a frame written after `[frame, op, frame]` and
  a restart from the replay watermark, losing it from the index on a crash.
  Unstamped WAL frames from older versions replay in the right order, and
  older versions can still read a stamped WAL.
- **Two processes on one data root silently destroyed acked data** (8 of 150
  writes lost, measured). A second writer now fails fast with
  `NamespaceBusyError`.
- **A write acked after `compact()` was lost on restart** - compaction unlinked
  the WAL while the log writer held its fd, so later appends went to a ghost
  inode. This predated every prior release.
- **Group commit acked writes that no fsync covered** (160 false acks, up to
  3108 bytes) - the durability watermark was published after the fsync instead
  of captured before it.
- A due hard-delete purge ran a full-namespace compaction inline on the next
  ordinary write: 804ms write ack at 20K records, now 6.9ms on a maintenance
  thread. The D7 purge guarantee is unchanged.
- A clean `close()` discarded queued embeddings (1 of 40 vectors kept, now 40).
- ABBA deadlock between namespace destroy and engine shutdown.
- A segfault when the index closed under a live reader.

### Fixed - retrieval correctness
- **The BM25 lane did not rank by BM25.** For natural-language questions the
  AND tier almost never fired (0 of 376 LongMemEval questions), and the OR
  fallback re-ranked an *unordered* 320-row window by distinct-term coverage,
  with no IDF and no TF. It is now one FTS5 `ORDER BY bm25()` query, filtered in
  SQL. Lane-alone ndcg@5 went from 0.739 to 0.891 on LongMemEval_S and from 0.241 to 0.884
  on LongMemEval_M, and it was also 78-85% of search CPU.
- The time lane returned the newest rows for *every* query. It now runs only on
  recency intent ("recently", "latest", ...) or an explicit time bound (+0.048 ndcg@5).
- The stopword list contained words fitted to the synthetic test generator
  ("session", "number", "notes", "agreed", ...), silently dropping real query
  words. Removed, from both the BM25 and the hash-embedder paths.
- Ranking depended on the wall clock: recency was scored against `now()` and
  then quantized, so memories a few years old collapsed into shared score
  tiers. `now` is now data-relative (or `as_of`), and ordering uses the exact score.
- Retrieval was nondeterministic: re-ingesting identical data changed the
  top-10 for ~23% of queries (ULID/ingest-time tie-breaks). Tie-breaks are now
  content-derived; a separate-process rerun is identical on 126/126 questions.
- `forget()` over-deleted under real embeddings: its fixed 0.35 cosine
  "strong match" floor sits below bge-small's median for *unrelated* strings.
  The floor is now per-embedder (hash 0.35, bge 0.85), with a margin check.
- The embedder silently changed with installed packages. Selection is now
  explicit (`embedder` / `MEMD_EMBEDDER`), reported in `stats()`, and the
  model loads lazily and is shared per process (`Memory()` no longer blocks
  on a model load; an open/close cycle no longer grows RSS by ~67 MB).
- `load_real` did not read the real LongMemEval format (it yielded zero events).
- The BM25 lane discarded its own exact matches when it found fewer than ten,
  and its bounded window counted rows *before* filtering, so eligible records
  were starved out entirely (0 of 5 returned; now 5 of 5). A routine bulk
  delete made every survivor invisible.
- A query-plan inversion made the lexical lane 22 seconds at 10K records.

### Fixed - security
- **A record captured with only a session id was visible to every other user**
  in the namespace. Session is now the private leaf of the scope hierarchy.
- Audit entries were filed under the facade's default namespace, putting one
  tenant's record ids in another's exportable SIEM trail.
- The audit hash chain reset on every reopen under encryption, so `verify()`
  returned False forever after the first restart.
- `/metrics`, `/v1/metrics/json` and `/v1/status` returned the whole fleet's
  telemetry and namespace inventory to any authenticated key.
- A wrong-namespace 403 was booked as an authentication failure, letting any
  valid key lock a chosen client bucket out of the entire API.
- A crypto-shredded namespace could be resurrected - with a fresh data key - by
  an in-flight snapshot or a late audit append.
- Interactive docs and the OpenAPI schema are no longer public by default.
- `kinds` is bounded and validated; the Dockerfile no longer masks a failed
  core install.

### Fixed - observability
- **Every latency quantile in the system was the mean.** Millisecond values
  were recorded into second-scale buckets, so no observation was ever bucketed
  and p50/p95/p99 all collapsed to the average. Durations are now measured,
  named and bucketed in milliseconds throughout.
- `bench/slo_bench.py` graded the retrieval SLO on memd's self-reported latency
  while ~87% of its samples were cache hits replaying a stale value.
- `snapshot()` held the global metrics lock across the whole quantile
  computation, stalling every hot path for the duration of a scrape.

### Changed
- The hash embedder's "vector" lane is no longer fused into ranking
  (`fuse_vector` = `auto | true | false`; `auto` fuses only a real
  embedder). Once bm25 ranked by bm25 it cost ~0.07 ndcg@5 on LongMemEval.
  Hash vectors are still computed: forget() and dedupe use them.
- The hash embedder is now `hash-ngram-384-v2`: patch 2 changed its features
  but not its name, so older vectors were never re-embedded. The open-time
  vector-health check now flags them and the maintenance thread re-embeds.
- The process-wide model cache is keyed by (kind, model): the local
  cross-encoder shares it with the local embedder.
- Local ONNX models (embedder and cross-encoder) run with
  `embed_threads` / `MEMD_EMBED_THREADS` intra-op threads, default
  min(4, cores // 2), instead of every core (throughput collapsed to ~4
  vectors/s at load 35 from oversubscription). Embedding batches are sorted
  by length and text is cut at `embed_max_chars` (2000; 0 = off): a batch
  pads to its longest member, and raw bge-small managed 1.2 turns/s batched
  vs 3.4 one at a time on LongMemEval turns.
- Write amplification: FTS5 no longer stores a second copy of every record and
  vectors are stored at half width. 800B records 7.29x -> 5.02x; 4KB 2.80x.
  `02-slos.md`'s flat 3x bar is amended to a record-size-aware one, with the
  arithmetic - a dense vector is a *fixed* cost per record, so a flat ratio
  stated per raw byte is unreachable below ~4KB at any embedding dimension.
- Audit ledgers are per-namespace, O(1) to open, and bounded in size.
- Index schema v1 -> v2, migrated on open. No re-embed required.

### Known limitations
- Search latency grows with namespace size under FTS5 (p50 15/44/114 ms at
  10K/50K/150K records on real turns); install `memd[fast]` (tantivy: 5.7/7.3/8.8 ms).
- Multi-session aggregation questions ("how many X did I ...") remain the
  weakest category end to end (see BENCHMARKS.md).
- The Jev reranker adds ~1 s per search (network) and sends candidate texts to TypeSafe.
- Single writer per data root, on every backend. `uvicorn --workers N` with
  N>1 does not work.
- With the S3 backend, envelope keys stay local: a second node cannot decrypt
  the bucket. A KMS key provider is not built.
- BM25 tokenization is `ascii`, so CJK is not indexed by the lexical lane.
- Flat vector scan; no IVF/HNSW above the documented ~50K-vector ceiling.

## [0.1.0]
Initial engine: record schema with bitemporal supersedence and trust tiers,
WAL/segment storage with compaction and crypto-shred, hybrid retrieval
(BM25 + vector + entity + time, RRF-fused, budget-packed), async fact
extraction and consolidation, REST/SDK/MCP doors, and a frozen eval harness
with an adversarial ship gate.
