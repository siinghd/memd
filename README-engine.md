# memd — the SQLite of agent memory

Embedded-first agent memory engine: one process, zero external services.
Raw + fact lanes, bitemporal supersedence, provenance/trust tiers, hybrid
retrieval, budget-aware packing. Apache-2.0, capability-complete.

## Ten-minute story (acceptance-tested by `scripts/ten_minute_test.sh`)

```bash
pip install -e .            # Python >= 3.11
```

```python
from memd import Memory

mem = Memory("./my-data")                          # embedded; Memory(api_key=...) = hosted, same API

# two lines in any agent loop:
messages = mem.pack(messages, user_id="u1")        # inject packed, provenance-tagged context
mem.observe(messages, response, user_id="u1")      # capture the turn after your LLM call

# or explicitly:
mem.add("We deploy with `make ship`, never CI", session_id="s1", user_id="u1")
hits = mem.search("how do we deploy?", user_id="u1", budget_tokens=1500)
print(hits.packed_context)                         # ready to inject
```

Kill the process, restart, ask "what did we decide yesterday?" — cross-session
recall works (`scripts/ten_minute_test.sh` proves it from a fresh directory).

## Running it

### 1. Embedded (a library, no server)
Nothing to start. `Memory("./my-data")` owns the directory; see the snippet above.

### 2. REST server
```bash
export MEMD_ADMIN_KEY="$(python -c 'import secrets;print(secrets.token_urlsafe(32))')"
memd serve --http --host 127.0.0.1 --port 8700     # MEMD_DATA=./memd-data by default

# mint a per-tenant key (the admin key is namespace "*"; do not hand it to apps)
memd key create --namespace acme
```
```bash
curl -sX POST localhost:8700/v1/ns/acme/memories \
  -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"content":"we deploy with make ship, never CI","user_id":"u1"}'

curl -sX POST localhost:8700/v1/ns/acme/search \
  -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"query":"how do we deploy?","user_id":"u1"}'
```
`/docs` and `/openapi.json` are **off by default** (they enumerate every route
of an otherwise authenticated API). Set `MEMD_ENABLE_DOCS=1` for development.

### 3. MCP (Claude Desktop, Cursor, any MCP client)
```json
{ "mcpServers": { "memd": { "command": "memd", "args": ["serve", "--mcp"],
                            "env": { "MEMD_DATA": "/abs/path/to/memd-data" } } } }
```

### 4. Docker
```bash
docker build -t memd/memd:0.2.0 .
docker run -d -p 8700:8700 -e MEMD_ADMIN_KEY=... -v memddata:/data memd/memd:0.2.0 serve --http
```
The image runs as **uid 10001**, so a *named volume* (above) works but a
**bind mount does not** unless you either pass `--user "$(id -u):$(id -g)"` or
`chown 10001` the host directory. Both are verified; pick one deliberately.

## Deployment constraint: ONE process per data root

memd is **single-writer per data root** and enforces it with an advisory lock:
a second process opening the same namespace fails fast with
`NamespaceBusyError`. This is not a limitation to work around — two writers on
one root silently destroyed acked data (measured: 8 of 150 writes lost), which
is why the lock exists.

Concretely:
- **`uvicorn --workers N` with N > 1 will not work.** Run one worker.
- Two containers on one volume will not work.
- To scale reads, scale *namespaces* across processes, not processes across one
  namespace. `MEMD_ALLOW_MULTI_PROCESS=1` disables the lock and re-enables the
  data loss; it exists for recovery tooling, not for serving.

A multi-writer protocol (manifest CAS on ETag) is the real fix and is not
built.

## Object storage as the source of truth (S3 / R2 / MinIO)

```bash
pip install "memd[s3]"
```
```python
mem = Memory("s3://my-bucket/memd", config={
    "local_dir": "./.memd-local",          # index cache + envelope keys stay here
    # endpoint/credentials optional: the normal boto3 chain applies
    "s3_endpoint_url": "http://127.0.0.1:9000",
})
```

S3 has no append, and `append()` is the WAL's whole contract. Each append is
its own immutable object (`<key>.__part-000000000042`) and the logical object
is their ordered concatenation — so **one durable write ack = one PUT**, and a
torn frame cannot exist. The read-modify-write alternative would transfer
O(WAL) bytes per ack and was rejected on both latency and cost.

Measured against a real S3 server (MinIO on localhost — see the caveat below):

| | measured | SLO |
|---|---|---|
| durable write ack | p50 **8.8 ms**, p90 **10.1 ms** | p50 ≤ 150 ms, p90 ≤ 300 ms |
| warm retrieval | p50 **26.0 ms**, p99 **71.2 ms** | p50 ≤ 100 ms, p99 ≤ 400 ms |
| cold node (bucket only, no local cache) | open **170 ms** + first query **38 ms** | first query p90 ≤ 1.5 s |
| **object-store round trips per write** | **1 PUT** | O(1) |
| **object-store round trips per warm search** | **0** | O(1) |

The latencies are from a server on loopback and are a floor, not a forecast for
S3-across-a-WAN. The **round-trip counts are not** — they are structural, and
they are the number that decides both the cost model and how a WAN changes
things. A warm search touches object storage zero times because retrieval is
served by the local derived index.

What stays local, and why it matters: the SQLite derived index (rebuildable by
contract — pass 22's snapshot is what makes a cold node cheap) and the envelope
**keys**. Data is remote, keys are not, so this is *one node with remote
durability*, not *any node serves any namespace*. Crypto-shred still works — the
key is local, destroy it and the ciphertext is inert — but a second node cannot
decrypt. A KMS key provider is the missing piece and is not built.

Single-writer is still enforced, by a **lease** rather than a file lock (`flock`
cannot see another machine): the first writer claims `ns/<ns>/.owner` with a
conditional PUT, a second gets `NamespaceBusyError`, and a lease older than the
TTL is reclaimable so a crashed node cannot wedge a namespace forever. It makes
split-brain loud, not impossible.

## Doors (one engine)

| Door | Command | Surface |
|---|---|---|
| Python SDK | `from memd import Memory` | add/search/remember/forget/pack/observe/export |
| REST | `memd serve --http` (:8700) | `/v1/ns/{ns}/events`, `/memories`, `/search`, `/export`, ... |
| MCP | `memd serve --mcp` | exactly 4 tools: memory_search / memory_save / memory_forget / memory_status |

## What's inside

- **Storage** (ADR-2): per-namespace WAL → immutable segments → manifest on an
  object-store interface (local FS embedded; S3/R2 backend is the same
  interface). Durable write ack = fsync'd append; no LLM/embedding on the
  write path. Compaction folds tombstones and enforces hard-delete deadlines.
- **Revisability** (ADR-1): bitemporal records — new fact on an entity key
  supersedes the old cluster-locally. `search(as_of=...)` time-travels;
  `?history=true` walks supersedence chains.
- **Provenance & trust** (D7): every record carries source tier
  (user > agent > tool > web > import), lineage, actor. Untrusted content is
  fenced in packed context; explicit saves inherit session taint; quarantine +
  rate limits catch MINJA-style injection; hash-chained audit log; namespace
  crypto-shred; record-level hard delete with ≤72h physical purge deadline.
- **Retrieval**: rules-based planner (no reflection loop) → fan-out over
  BM25 (SQLite FTS5/porter, ranked by bm25; optionally accelerated by
  tantivy), the entity lane, the time lane on recency intent, and an exact
  flat vector scan with a real embedder (numpy; IVF-PQ slot reserved for
  ≥50K-vector namespaces) → RRF fusion with trust-aware tie-breaks →
  optional rerank of the lexical top-30 → validity filter (current/as_of) →
  lineage-deduped, budget-cut packing that keeps prefix-stable order
  (KV-cache friendly), or, as an experimental opt-in, gated evidence packing.
  The hash embedder's vector lane is not fused (`fuse_vector`, below).
- **Extraction** (ADR-6): async, batched, re-runnable. BYO OpenAI-compatible
  key for LLM extraction/embeddings; heuristic provider keeps facts working
  with zero keys; local ONNX embeddings via the optional fastembed extra.

## Keys & providers (add later, everything works now)

```bash
export MEMD_EMBEDDING_API_KEY=...      # optional: real embeddings
export MEMD_EXTRACTION_API_KEY=...     # optional: LLM fact extraction
```

Without them you get deterministic hash embeddings + pattern extraction:
fully functional, honestly degraded, clearly labeled in `stats()`.

### Retrieval options (`Memory(config={...})` or the env var)

| key / env | values | default |
|---|---|---|
| `reranker` / `MEMD_RERANKER` | `auto` \| `none` \| `jev` \| `local` | `auto`: Jev when a TypeSafe key is set (`TYPESAFE_API_KEY`, or config `typesafe_api_key`) and `typesafe-sdk` is installed (`pip install "memd[jev]"`), else none |
| `jev_model`, `rerank_timeout_s`, `rerank_k` | model pin, deadline, shortlist | `jev-latest`, 1.5s (5s local), 30 |
| `local_rerank_model` | fastembed cross-encoder | `BAAI/bge-reranker-base` |
| `pack_mode` / `MEMD_PACK_MODE` | `auto` \| `ranked` \| `gated` | `auto` = ranked with every reranker; `gated` is an experimental opt-in |
| `rerank_gate` | gated-packing threshold (opt-in mode) | 0.5 |
| `fuse_vector` / `MEMD_FUSE_VECTOR` | `auto` \| `true` \| `false` | `auto`: fuse unless the embedder is the hash embedder |
| `lexical_backend` / `MEMD_LEXICAL_BACKEND` | `auto` \| `fts5` \| `tantivy` | `auto`: tantivy when installed (`pip install "memd[fast]"`) |

- **Reranker.** The top-30 of the bm25 lane (plus the vector lane with a real
  embedder) is reordered by a relevance judge; the rest follows in fused
  order. On LongMemEval_S, Jev reranking took session ndcg@5 from 0.89 to
  0.95. A failed, slow (> `rerank_timeout_s`) or malformed judgement keeps
  the unreranked order and counts `memd_rerank_fallback_total{reason}`;
  search never fails because of it. `stats()["reranker"]` reports name,
  model, calls, fallbacks and p50 latency. **Privacy: with Jev active, the
  query and the top-30 candidate texts of every search are sent to
  TypeSafe's API** (see SECURITY.md). No key, no egress.
- **Gated packing** (`pack_mode="gated"`, experimental opt-in, meant for a
  calibrated reranker such as Jev): the packed context holds only the
  candidates judged relevant (p ≥ `rerank_gate`, else the top 3), each with
  its neighbouring turns, grouped by session under a session-date header,
  still capped by `budget_tokens` and with the same provenance fencing. It
  trades recall for tokens: in the lab it matched top-k QA accuracy at 27%
  fewer tokens over a 100-candidate shortlist, but over the product's top-30
  it drops second evidence sessions (LongMemEval_S session recall_all@5
  0.803 gated vs 0.928 ranked, with Jev). The default packs ranked.
- **tantivy accelerator.** A derived index next to SQLite, fed in the
  background (every 500ms or 512 changes); FTS5 stays the synchronous source
  of truth, so the write ack is unchanged, and writes not yet indexed are
  served from FTS5. It is rebuilt in the background when missing, corrupt or
  not closed cleanly. A search error sends that query to FTS5; only damage
  (I/O, missing or corrupt files) rebuilds it while running, with
  exponential backoff on a failure history that a successful rebuild does
  not erase (it decays after 10 minutes without a failure);
  `stats()["lexical"]` shows `rebuilds`, `failures`, `failures_total` and
  `retry_in_s`. Tied bm25 scores are ordered the same way on both backends
  (score, -t_event, content hash, id). With tantivy the top-k is
  deterministic for the same operation history *and commit schedule*: its
  BM25 statistics count deleted and superseded docs until their segments
  merge, so the same history committed in a different rhythm can order two
  near-equal docs differently.

## Ops

```bash
python bench/slo_bench.py                        # D2 acceptance numbers
python -m memd.harness.run --suite all --gate    # quality+cost gate (D5)
python bench/lme_gate.py                         # real-data gate: LongMemEval_S, 60 q (nightly)
python bench/lexical_bench.py                    # FTS5 vs tantivy, filtered, 10K-150K records
memd export --out backup.jsonl                   # anti-lock-in, symmetric
memd import mem0 --export mem0.json              # migration path
memd migrate --report ./memd-data                # store-format upgrade: preview / what it did (JSON)
memd key create --ns acme [--pin-user u1]        # scoped API keys
```

SLOs measured on this machine (see `bench/slo_bench.py`): durable write ack
p99 ≈ 7ms (target ≤10ms embedded); warm retrieval p50 ≈ 12ms / p99 ≈ 52ms
(targets ≤20/≤100ms); cold restart + first query ≈ 44ms.

Design pack: see `01..09-*.md` in this repo, ADRs in `adr/ADRs.md`. Harness
version hash is stamped into every result file — a result without a hash
doesn't exist.
