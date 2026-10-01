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
  No database to provision, no server to run, no account or API key: without
  keys memd runs on deterministic hash embeddings (or local ONNX embeddings
  with `memd[local-embeddings]`) and pattern-based fact extraction, and
  `stats()` says which.
- **One engine behind every door.** The Python API, the REST server, the MCP
  server and the TypeScript SDK all reach the same engine with the same
  semantics.
- **Grows without an API change.** The same `Memory` runs on an `s3://` root,
  and several server processes on one bucket split the namespaces between
  them ([Operations](operations.md)).
- **The hard parts are in the open engine** (Apache-2.0): bitemporal
  supersedence, provenance and trust tiers, quarantine, a hash-chained audit
  log, hard delete with a physical-purge deadline, per-namespace
  crypto-shred.

## One engine, four doors

{% include-markdown "../README-engine.md" start="## Doors (one engine)" end="## What's inside" %}

## What memd is not, yet

Honest limits:

- **One writer per namespace.** A second process on the same namespace fails
  fast with `NamespaceBusyError`; scale by spreading namespaces across
  processes. Several writers inside one namespace and read replicas are not
  built.
- **Fact extraction is pattern-based by default.** An LLM extractor is
  available with your own key; it has not been evaluated.
- **The quality evidence is retrieval on one public dataset.** The
  end-to-end QA numbers are preliminary, and the one-time held-out
  500-question run has not been done ([Benchmarks](benchmarks.md)).
- **Alpha software** (`Development Status :: 3 - Alpha`); what changed in
  each release is in the [Changelog](changelog.md).
- **Out of scope by design:** an agent framework or runtime, a
  RAG-over-documents platform, a graph database.

## Measured quality

{% include-markdown "../BENCHMARKS.md" start="## Retrieval quality and latency (LongMemEval_S, public `Memory.search`)" end="## Lexical index scale" %}

How these were measured, and what they do not show: [Benchmarks](benchmarks.md).

## Next

- [Quickstart](quickstart.md): install, the ten-minute story, the four ways to run it
- [Concepts](concepts.md): namespaces, scopes, kinds, the retrieval lanes, deletion, keys
- [Examples](examples.md): runnable scripts for every door
- [Operations](operations.md): S3, multi-node, KMS/Vault, backups and recovery
