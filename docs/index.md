# memd: the SQLite of agent memory

{% include-markdown "../README-engine.md" start="# memd — the SQLite of agent memory" end="## Ten-minute story" %}

```python
from memd import Memory

mem = Memory("./my-data")    # a directory, not a service
mem.add("We deploy with `make ship`, never CI", user_id="u1", session_id="s1")
print(mem.search("how do we deploy?", user_id="u1").packed_context)
```

## Why embedded-first

- **A library, not a deployment.** `Memory("./my-data")` owns a directory.
  You do not provision a database or run a server, and you need no account or
  API key. Without keys, memd uses local ONNX embeddings (`memd-engine[local-embeddings]`,
  recommended: they give much better recall) or deterministic hash
  embeddings (without the extra). It uses pattern-based fact extraction.
  `stats()` shows which.
- **One engine behind every door.** The Python API, the REST server, the MCP
  server and the TypeScript SDK all reach the same engine with the same
  semantics.
- **Grows without an API change.** Several processes can share one data
  root: one process writes each namespace, and the others forward their
  calls to it. The same `Memory` runs on an `s3://` root. Several server
  processes on one bucket share the namespaces, and read replicas serve
  eventual reads ([Operations](operations.md)).
- **The hard parts are in the open engine** (Apache-2.0). They include
  bitemporal supersedence, provenance and trust tiers, quarantine and a
  hash-chained audit log. They also include hard delete with a physical-purge
  deadline and per-namespace crypto-shred.

## One engine, four doors

{% include-markdown "../README-engine.md" start="## Doors (one engine)" end="## What's inside" %}

## Limits

- **Many processes can use one namespace. One of them writes.** Each
  process can open the same namespace and write to it. One process (the
  holder) writes the log. The other processes send their writes and their
  strong reads to the holder automatically. If the holder stops, another
  process becomes the holder. Thus, the write capacity of one namespace is
  the capacity of one process. To get more write capacity, use more
  namespaces (for example, one namespace for each user or agent). To get
  more read capacity, use read replicas (opt-in eventual reads, with a
  staleness limit). memd does not let several processes write one
  namespace's log at the same time.
- **Fact extraction uses patterns by default.** An optional LLM extractor
  uses your own API key. On 30 LongMemEval_S questions it gave no
  measurable accuracy gain: 0.667 against 0.700 for the pattern extractor
  (difference -0.033, 95% CI [-0.167, +0.067]). It also makes a session
  close slower (1.5 s against 0.08 s at the median). A larger evaluation
  is necessary before we recommend it.
- **The quality evidence comes from one public dataset (LongMemEval_S).**
  The 500-question end-to-end run used the previous defaults (2K flat
  pack). A run on 160 of those questions measured session packing, with
  bge-small, a reranker and relative dates on. The current defaults are
  not yet measured end to end. Details are in [Benchmarks](benchmarks.md).
- **Alpha software** (`Development Status :: 3 - Alpha`); what changed in
  each release is in the [Changelog](changelog.md).
- **Out of scope by design:** an agent framework or runtime, a
  RAG-over-documents platform, a graph database.

## Measured quality

{% include-markdown "../BENCHMARKS.md" start="## Retrieval quality and latency (LongMemEval_S, public `Memory.search`)" end="## Lexical index scale" %}

The measurement method, and what it does not show: [Benchmarks](benchmarks.md).

## Next

- [Quickstart](quickstart.md): install, the ten-minute story, the four ways to run it
- [Concepts](concepts.md): namespaces, scopes, kinds, the retrieval lanes, deletion, keys
- [Examples](examples.md): runnable scripts for every door
- [Operations](operations.md): S3, multi-node, KMS/Vault, backups and recovery
