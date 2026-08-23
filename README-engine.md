# memd — the SQLite of agent memory

Embedded-first agent memory engine: one process, zero external services.
Raw + fact lanes, bitemporal supersedence, provenance/trust tiers, hybrid
retrieval, budget-aware packing. Apache-2.0, capability-complete.

## Ten-minute story (acceptance-tested in CI)

```bash
pip install -e .            # or: docker run -p 8700:8700 memd/memd
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
  BM25 (SQLite FTS5/porter), exact flat vector scan (numpy; IVF-PQ slot
  reserved for ≥50K-vector namespaces), time/entity lanes → RRF fusion with
  trust-aware tie-breaks → validity filter (current/as_of) → lineage-deduped,
  budget-cut packing that keeps prefix-stable order (KV-cache friendly).
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

## Ops

```bash
python bench/slo_bench.py                        # D2 acceptance numbers
python -m memd.harness.run --suite all --gate    # quality+cost gate (D5)
memd export --out backup.jsonl                   # anti-lock-in, symmetric
memd import mem0 --export mem0.json              # migration path
memd key create --ns acme [--pin-user u1]        # scoped API keys
```

SLOs measured on this machine (see `bench/slo_bench.py`): durable write ack
p99 ≈ 7ms (target ≤10ms embedded); warm retrieval p50 ≈ 12ms / p99 ≈ 52ms
(targets ≤20/≤100ms); cold restart + first query ≈ 44ms.

Design pack: see `01..09-*.md` in this repo, ADRs in `adr/ADRs.md`. Harness
version hash is stamped into every result file — a result without a hash
doesn't exist.
