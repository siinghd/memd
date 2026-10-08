# memd

**The SQLite of agent memory.** memd is an embedded-first memory engine for
AI agents: one process, zero external services. Raw + fact lanes, bitemporal
supersedence, provenance and trust tiers, hybrid retrieval and budget-aware
packing, in a library that owns a directory. Apache-2.0.

```python
mem = Memory("./my-data")    # a directory, not a service
```

Status: alpha (0.3.x). See [what it does not do yet](#limits).

## Features

- **Embedded, zero keys.** No database to provision, no server, no account.
  Without API keys memd runs on deterministic hash embeddings (or local ONNX
  embeddings with `memd[local-embeddings]`) and pattern-based fact
  extraction, and `stats()` says which. Bring an OpenAI-compatible key for
  real embeddings or LLM fact extraction.
- **No model call on the write path.** A write is acknowledged once it is
  durably appended to the namespace's log; embedding happens in the
  background, and the record is searchable by BM25 as soon as the call
  returns.
- **Raw and fact lanes.** What was said is kept verbatim; facts are saved
  explicitly (`remember`) or extracted when a session closes, and extraction
  can be re-run because the raw lane is kept.
- **Revisable facts.** A new fact on an entity key supersedes the old one
  without deleting it: `get(id, history=True)` walks the chain and
  `search(as_of=...)` asks what was true at a past time.
- **Provenance and trust.** Every record carries its source tier (user >
  agent > tool > web > import). Untrusted content is fenced as data in the
  packed context, explicit saves inherit a session's taint, and bursts of
  writes or repeated near-identical content are quarantined.
- **Hybrid retrieval.** A rules-based planner fans out over BM25 (SQLite
  FTS5, optionally accelerated by tantivy), entity, time and vector lanes,
  fuses them with reciprocal rank fusion, optionally reranks, and packs the
  result into a token budget in a stable order.
- **Deletion that holds.** Hard delete with a physical-purge deadline,
  forget-by-query with a preview, per-namespace crypto-shred, and a
  hash-chained audit log.
- **Grows without an API change.** The same `Memory` runs on an `s3://`
  root (S3, R2, MinIO), and several server processes on one bucket split the
  namespaces between them, with keys held by AWS KMS or Vault transit.

## Install

Install from PyPI (Python >= 3.11). The package is `memd-engine`; the
import name and the command are `memd`:

```bash
pip install memd-engine
```

or the latest code from this repository:

```bash
pip install "memd-engine @ git+https://github.com/siinghd/memd.git"
```

or from a checkout, with the extras you need:

```bash
git clone https://github.com/siinghd/memd.git && cd memd
pip install -e ".[mcp,s3]"     # extras: mcp, s3, fast, ann, local-embeddings, jev, billing
```

## Quickstart

```python
from memd import Memory

mem = Memory("./my-data")
mem.add("We deploy with `make ship`, never CI", user_id="u1", session_id="s1")
mem.remember("The user prefers dark mode", user_id="u1", entity_keys=["user.theme"])

hits = mem.search("how do we deploy?", user_id="u1", budget_tokens=500)
print(hits.items[0].content)   # We deploy with `make ship`, never CI
print(hits.packed_context)     # provenance-tagged, ready to put in a prompt
mem.close()
```

In an agent loop it is two calls: `messages = mem.pack(messages, user_id="u1")`
before your LLM call, `mem.observe(messages, response, user_id="u1")` after.
Restart the process and ask again: the memory is in `./my-data`.
More in [`examples/`](examples/README.md).

## One engine, four doors

- **Python:** `from memd import Memory`, embedded as above; `Memory(api_key=..., base_url=...)` is the same API over REST.
- **MCP:** `memd serve --mcp` exposes four tools (`memory_search`, `memory_save`, `memory_forget`, `memory_status`) to any MCP client ([setup](examples/mcp/README.md)).
- **HTTP:** `memd serve --http` serves the REST API on port 8700, with per-namespace API keys (`memd key create --namespace acme`).
- **TypeScript:** [`sdk-ts/`](sdk-ts/README.md) is `@memd/client`, a typed REST client for Node >= 18, Bun, Deno and edge runtimes (not on npm yet: build it from `sdk-ts/`).

## Measured quality

Session retrieval on LongMemEval_S through the public `Memory.search`
(dev-fold means ± fold std, from [BENCHMARKS.md](BENCHMARKS.md)):

| memd configuration | ndcg@5 | recall_all@5 | mean search time |
|---|---|---|---|
| v0.1.0 as shipped | 0.727 ± 0.027 | 0.697 | ~130 ms |
| v0.2.0, zero-key default (hash embedder, no reranker) | 0.866 ± 0.017 | 0.835 | ~16 ms |
| v0.2.0 + Jev reranker (`TYPESAFE_API_KEY` set) | 0.955 ± 0.026 | 0.928 | ~1 s (network) |

This is retrieval quality on one public dataset. The end-to-end QA numbers
are preliminary and the one-time held-out 500-question run has not been
done; how these were measured and what they do not show is in
[BENCHMARKS.md](BENCHMARKS.md).

## Limits

- **One writer per namespace.** A second process opening the same namespace
  forwards its writes and strong reads to the process holding it, and takes
  the namespace over when that process goes away - one writer at a time,
  so forwarding adds a hop, not write capacity: scale writes by spreading
  namespaces across processes. Several writers inside one namespace are not
  built; reads can scale out with read replicas, which serve eventually
  consistent reads (opt-in, within a staleness bound).
- **Fact extraction is pattern-based by default.** An LLM extractor is
  available with your own key; it has not been evaluated.
- **Alpha software.** What changed in each release is in the
  [changelog](CHANGELOG.md).

## Documentation

- [README-engine.md](README-engine.md): the full engine guide (running it,
  S3, key custody, multi-node, hosted mode, retrieval options, ops)
- Docs site: <https://siinghd.github.io/memd/> (source in [`docs/`](docs/);
  build it locally with `pip install -e ".[docs]" && mkdocs serve`)
- [BENCHMARKS.md](BENCHMARKS.md): how quality and latency were measured
- [SECURITY.md](SECURITY.md): the threat model, what it does not cover, and
  how to report a vulnerability
- [CONTRIBUTING.md](CONTRIBUTING.md): setup, tests, and what a change needs
- [CHANGELOG.md](CHANGELOG.md): every release

## License

[Apache-2.0](LICENSE).
