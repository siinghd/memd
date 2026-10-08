# Concepts

## Records

Everything memd stores is a **record** (`memd.MemoryRecord`): its content, a
kind, a scope, its provenance (source tier, actor, session, lineage) and two
time axes. The log is append-only: a delete or a supersedence is a separate
operation recorded after the record, never an edit of it.

| field | meaning |
|---|---|
| `t_event` | when it happened (defaults to the write time) |
| `t_ingested` | when memd learned it |
| `valid_from` | from when a fact holds (unset: always) |
| `invalidated_at`, `superseded_by` | set when a newer fact replaces it |

## Namespaces

A **namespace** is the unit of isolation. Each one has its own log, segments
and manifest, its own index, its own data key (so it can be crypto-shredded
on its own) and its own audit ledger. API keys are bound to one namespace.

```python
mem = Memory("./data", namespace="acme")      # the facade's default namespace
mem.add("...", namespace="globex")            # every method takes namespace=
```

Names match `[A-Za-z0-9][A-Za-z0-9_.-]{0,127}`; `_`-prefixed names are
reserved. A namespace has **one writer at a time**: a file lock on a local
root, a lease object on an `s3://` root. Another process using the namespace
forwards its writes to that writer ([several processes on one data
root](operations.md#several-processes-on-one-data-root)). That is the scaling
rule too: spread namespaces across processes (see
[multi-node](operations.md#multi-node); reads of one namespace can also be
served by read replicas that follow its writer), never processes across one
namespace.

## Scopes

Inside a namespace a record carries a **scope**: any of `org`, `agent`,
`user`, `session`. A query names the scope it speaks for
(`search(..., user_id="u1")`) and sees records whose scope matches what both
sides set:

- a user query sees all of that user's sessions plus records that name no
  user;
- a session query sees its session and its ancestors, never another session;
- a record bound to a session **and no user** is private to that session: it
  is never returned to another user's query;
- a query that names no one (embedded single-user use, admin sweeps) sees
  everything.

A REST key can be pinned to one user (`memd key create --pin-user u1`): its
requests are scoped to that user, and naming another user is refused (403)
unless the key also has the override capability.

## Kinds and lanes of writing

| kind | written by | |
|---|---|---|
| `raw_event` | `add`, `add_events`, `observe` | the **raw lane**: what was said, verbatim. The durable record. |
| `fact` | `remember`, `close_session` | the **fact lane**: explicit saves, and facts extracted from a session's raw turns |
| `procedure`, `summary`, `pin`, `link` | any write, with `kind=` | the other kinds; search can filter on them (`kinds=[...]`) |

A write is acknowledged once it is durably appended to the namespace's log:
no LLM or embedding call is on the write path. Embedding happens in the
background; a record is searchable by bm25 as soon as the call returns.

**Sessions.** `close_session(session_id)` is the session boundary: the raw
turns go through the extractor (pattern-based by default, an LLM with
`MEMD_EXTRACTION_API_KEY`), the facts are consolidated against what is
already known, and the log rotates into a segment. Extraction is re-runnable
because the raw lane is kept.

The LLM extractor sees each turn's speaker and time, so a fact is
attributed to who said it. A call that fails (provider error, timeout, an
empty, cut-off, oversized or malformed reply) is never retried: that
chunk's turns go through the pattern extractor instead, and
`close_session` returns `extraction_errors` and `raw_failed`. The options:

{% include-markdown "../README-engine.md" start="### Extraction options (`Memory(config={...})` or the env var)" end="- **What the model sees.**" %}

**Supersedence.** A fact written on an entity key
(`remember(..., entity_keys=["user.editor"])`) is consolidated against the
current facts on that key for the same org, agent and user: a near-duplicate
is dropped, a different statement supersedes the old one. The old version is
invalidated, not deleted: `get(id, history=True)` walks the chain and
`search(as_of=ms)` asks what was valid at a past time (a fact with
`valid_from` holds from then on; one without holds from the start). Searches
without `as_of` see only current facts.

## Provenance and trust

Every record has a **source tier**: `user` (5) > `agent` (4) > `tool` (3) >
`web` (2) > `import` (1). Packed context tags each item with its source, and
content from lower tiers is fenced as data, never placed in instruction
position. An explicit `remember` inside a session is capped at the lowest
tier that session has seen (taint), so a session that read a web page cannot
launder it into a `user`-tier fact. Bursts of writes and repeated
near-identical content are **quarantined**: stored, but kept out of search
(unless it asks for `include_quarantined`) until the quarantine expires.

## Retrieval: planner, lanes, fusion, packing

`search()` runs a rules-based planner (no reflection loop), then a fan-out
over **lanes**, fused by reciprocal rank fusion:

| lane | what it ranks | notes |
|---|---|---|
| **bm25** | lexical match, SQLite FTS5 | `memd[fast]` adds a tantivy accelerator; FTS5 stays the source of truth |
| **entity** | records whose entity keys match terms of the query | |
| **time** | the newest records | only for queries with recency intent ("latest", "yesterday") |
| **vector** | embedding similarity | an exact scan by default; with `memd[ann]`, a usearch HNSW sidecar from `ann_min_vectors` (20,000) vectors on. Not fused with the hash embedder (`fuse_vector`) |

An optional **reranker** (Jev with a TypeSafe key, or a local cross-encoder)
reorders the top 30 of the bm25 lane (plus the vector lane with a real
embedder); a failed or slow judgement keeps the fused order. The result is filtered for validity (current, or `as_of`), deduped
by lineage and **packed** into `budget_tokens` (default 12,000), ready to
put in a prompt: `SearchResult.packed_context`. By default it is laid out as
dated session excerpts (each hit with the turns around it, a fact under the
turn it came from, oldest session first); `packing="flat"` gives one
provenance-tagged element per hit in a prefix-stable order. See
[Packing and the budget](#packing-and-the-budget).

Every derived structure (the SQLite index, the tantivy index, the usearch
sidecar, the vectors) is rebuildable from the log by contract. The log and
segments in the object store are the source of truth.

The options, with their defaults:

{% include-markdown "../README-engine.md" start="### Retrieval options (`Memory(config={...})` or the env var)" end="- **Reranker.**" %}

## Packing and the budget

{% include-markdown "../README-engine.md" start="### Packing and the budget" end="## Ops" %}

## Storage and compaction

Per namespace, the object store (a local directory, or S3/R2/MinIO) holds a
framed **WAL** and ops log, immutable **segments**, and a **manifest** that
names them. The log rotates into a segment at a session close or when it
grows past its size or frame limit. **Compaction** folds the segments, the
log and the ops into one segment: it applies tombstones and supersedence and
physically purges hard-deleted records. It runs on a maintenance thread, off
the write path, and on demand with `compact(force=True)`.

## Deleting: soft, hard, forget, shred

| call | effect |
|---|---|
| `delete(id)` | tombstone: gone from reads and search at once |
| `delete(id, hard=True)` | also scheduled for **physical purge** from every file, within a deadline (`hard_delete_deadline_ms`, default 72 h), enforced by compaction |
| `forget(query, ...)` | delete by query. `find_ids` resolves *every* match (no packing budget), and `forget(..., expected=forget_fingerprint(ids))` deletes only if the match set is still the one previewed |
| `destroy_namespace()` | crypto-shred: the namespace's data key is destroyed and its objects removed |

Deletes and forgets are recorded in the namespace's hash-chained **audit
log**, and deleting is always allowed, whatever a hosted plan's state.

## Key custody

Data is encrypted at rest by default with per-namespace data keys (envelope
encryption). The root key that wraps them is held by a **key provider**:
`local` (a file under the local directory, the default), `aws-kms` or
`vault-transit`. Custody **fails closed**: a namespace whose data key is
missing or wrong raises `KeyCustodyError` and nothing is changed. It is
never read as empty and never compacted away. Moving providers, rotating and
what crypto-shred means with a shared KMS key:
[Operations](operations.md#key-custody), [Security](security.md).
