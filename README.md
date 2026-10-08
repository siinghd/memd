# memd

[![PyPI](https://img.shields.io/pypi/v/memd-engine?label=PyPI)](https://pypi.org/project/memd-engine/)
[![npm](https://img.shields.io/npm/v/memd-engine?label=npm)](https://www.npmjs.com/package/memd-engine)
[![CI](https://github.com/siinghd/memd/actions/workflows/ci.yml/badge.svg)](https://github.com/siinghd/memd/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/docs-siinghd.github.io%2Fmemd-blue)](https://siinghd.github.io/memd/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

**The SQLite of agent memory.** memd is a memory engine for AI agents. It
runs in your process and keeps its data in one directory. It needs no
database, no server and no account. Apache-2.0.

```python
mem = Memory("./my-data")    # a directory, not a service
```

Status: alpha. The badges show the current release. The [limits](#limits)
section tells what memd does not do.

## Features

- **Embedded, with no keys necessary.** Without API keys, memd uses local
  ONNX embeddings (BAAI/bge-small-en-v1.5, with the `local-embeddings`
  extra) or hash embeddings (without the extra). It extracts facts with
  patterns. `stats()` shows the active mode. An OpenAI-compatible key adds
  API embeddings or LLM fact extraction.
- **No model call on the write path.** memd acknowledges a write when the
  write is durable in the log of the namespace. memd makes the embeddings
  in the background. BM25 search finds the record as soon as the call
  returns.
- **Raw and fact lanes.** memd keeps the text of each turn, word for word
  (the raw lane). Facts go in a second lane. You save a fact with `remember`, or
  memd extracts facts when a session closes. memd keeps the raw lane, thus
  you can run the extraction again.
- **Facts that change.** A new fact on an entity key replaces the old fact,
  but memd does not delete the old fact. `get(id, history=True)` shows the
  chain. `search(as_of=...)` shows what was true at a past time.
- **Provenance and trust.** Each record has a source tier: user > agent >
  tool > web > import. memd puts lower-trust content in a data fence in
  the packed context. An explicit save in a session gets the lowest tier
  that the session saw. memd quarantines bursts of writes and repeated
  near-identical content.
- **Hybrid retrieval, packed as evidence.** A rules-based planner sends the
  query to the BM25, entity, time and vector lanes. Reciprocal rank fusion
  merges the lanes, and an optional reranker can change the order. memd
  packs the result into a token budget (12,000 by default) as dated
  excerpts of past sessions. Each hit comes with the turns around it. A
  fact shows under the turn that it came from.
- **Deletion that holds.** memd has hard delete with a physical-purge
  deadline, forget-by-query with a preview, crypto-shred for each
  namespace, and a hash-chained audit log.

## Measured quality

Session retrieval on LongMemEval_S, through the public `Memory.search`.
The values are dev-fold means ± fold std, from
[BENCHMARKS.md](BENCHMARKS.md). Each row names the measured release.

| memd configuration | ndcg@5 | recall_all@5 | mean search time |
|---|---|---|---|
| v0.1.0 as shipped | 0.727 ± 0.027 | 0.697 | ~130 ms |
| v0.2.0, zero-key default (hash embedder, no reranker) | 0.866 ± 0.017 | 0.835 | ~16 ms |
| v0.2.0 + Jev reranker (`TYPESAFE_API_KEY` set) | 0.955 ± 0.026 | 0.928 | ~1 s (network) |

End-to-end QA on LongMemEval_S: 160 questions, stratified by type, one run.
The reader is DeepSeek V4.1 Flash. The judge is gpt-6-luna-pro.

| context given to the reader | accuracy |
|---|---|
| previous defaults: hash embedder, no reranker, 2K tokens, flat | 0.779 [0.718, 0.838] |
| bge-small + local cross-encoder reranker, 12K tokens, flat | 0.823 |
| bge-small + local cross-encoder reranker, 12K tokens, session layout, relative dates on | 0.875 [0.823, 0.920] |
| the whole history in the prompt (~105K tokens; exploratory) | 0.906 |

The session layout and the 12K budget are now the defaults. The 0.875 run
also used bge-small embeddings, a reranker and relative-date annotations.
The defaults use bge-small only with the `local-embeddings` extra, and no
reranker. The defaults ship the annotations off. Nobody measured the
defaults as shipped, end to end. The previous-defaults row and the
whole-history row use the answers of an earlier run.

These results come from one public dataset, one reader and one judge. For
the setup and the limits, see
[README-engine.md](README-engine.md#packing-and-the-budget) and
[BENCHMARKS.md](BENCHMARKS.md).

## Install

Install from PyPI. memd needs Python 3.11 or later. The package is
`memd-engine`. The import name and the command are `memd`.

```bash
pip install "memd-engine[local-embeddings]"
```

The `local-embeddings` extra runs BAAI/bge-small-en-v1.5 on the CPU,
through fastembed. The model downloads on first use. memd fuses it with
BM25. The extra gives much better recall. On LongMemEval_S session
retrieval, 97.5% of questions had all their evidence sessions in the top
10 (recall_all@10 0.975). With BM25 alone, the result was 92.0%. memd ranks
by BM25 alone without the extra. These are lane-level measurements on 153
questions. The hash embedder's own vector lane scores 0.640, and memd does
not use it for ranking.

`pip install memd-engine` also works, offline and with no model. memd then
uses hash embeddings, and it logs one line about it.

| extra | adds |
|---|---|
| `local-embeddings` | local ONNX embeddings (BAAI/bge-small-en-v1.5 through fastembed), fused with BM25. Recommended. |
| `mcp` | `memd serve --mcp`, the MCP server |
| `s3` | `s3://` data roots and the `aws-kms` key provider (boto3) |
| `fast` | the tantivy accelerator for the BM25 lane |
| `ann` | the usearch HNSW sidecar for the vector lane |
| `jev` | the Jev reranker (active only with `TYPESAFE_API_KEY`) |
| `billing` | Stripe billing for hosted mode |

For TypeScript, install the REST client from npm. It needs a memd server.

```bash
npm install memd-engine
```

To get the latest code from this repository, use one of these commands:

```bash
pip install "memd-engine[local-embeddings] @ git+https://github.com/siinghd/memd.git"

git clone https://github.com/siinghd/memd.git && cd memd
pip install -e ".[local-embeddings,mcp,s3]"
```

## Quickstart

```python
from memd import Memory

mem = Memory("./my-data")
mem.add("We deploy with `make ship`, never CI", user_id="u1", session_id="s1")
mem.remember("The user prefers dark mode", user_id="u1", entity_keys=["user.theme"])

hits = mem.search("how do we deploy?", user_id="u1", budget_tokens=500)
print(hits.items[0].content)   # We deploy with `make ship`, never CI
print(hits.packed_context)     # dated session excerpts, ready to put in a prompt
mem.close()
```

In an agent loop, use two calls. Before your LLM call, call
`messages = mem.pack(messages, user_id="u1")`. After it, call
`mem.observe(messages, response, user_id="u1")`. If you start the process
again, the memory is still in `./my-data`. The
[`examples/`](examples/README.md) directory has more scripts that you can run.

## One engine, four doors

- **Python:** `from memd import Memory`. `Memory(path)` runs the engine in
  your process. `Memory(api_key=..., base_url=...)` gives the same API
  over REST.
- **HTTP:** `memd serve --http` serves the REST API on port 8700. Each
  namespace gets its own API keys (`memd key create --namespace acme`).
- **MCP:** `memd serve --mcp` gives four tools to an MCP client:
  `memory_search`, `memory_save`, `memory_forget` and `memory_status`
  ([setup](examples/mcp/README.md)).
- **TypeScript:** `npm install memd-engine` gives a typed REST client for
  Node 18 or later, Bun, Deno and edge runtimes
  ([SDK guide](sdk-ts/README.md)).

## How memd grows

The same `Memory` API works at each step. Your code does not change.

- **Namespaces.** A namespace is the unit of isolation and of scale. Each
  namespace has its own log, index, data key and audit log. Use one
  namespace for each user, agent or tenant.
- **Several processes on one data root.** One process at a time writes a
  namespace. The other processes send their writes and strong reads to it
  (write forwarding, on by default). If that process stops, another
  process becomes the writer. Thus `uvicorn --workers N`, one
  `memd serve --mcp` for each MCP client, and a script next to a server can
  share one directory. `forwarding="off"` turns this off.
- **Object storage.** The same `Memory` runs on an `s3://` root: AWS S3,
  Cloudflare R2 or another S3-compatible store. CI runs the S3 tests
  against RustFS. A durable write is one PUT. A warm search reads no object.
- **Read replicas.** Reads are strong by default. A search or a get can
  ask for eventual consistency. Then a read replica serves it, within a
  staleness limit.
- **Multi-node.** Several `memd serve --http` processes on one `s3://` root
  share the namespaces. A lease in the bucket gives each namespace one
  writer. A node sends each request to the node that holds the lease. AWS
  KMS or Vault transit wraps the data keys, thus each node can open each
  namespace.
- **Search throughput.** The default search (a 12K session pack) is CPU
  work in Python. In one process, more threads do not give more searches
  per second, because of the GIL. To serve more searches, run more
  processes: `uvicorn --workers N`, more processes on one data root, read
  replicas or more nodes. Retrieval alone (a 2K flat pack) scales with
  threads, because SQLite releases the GIL.

[README-engine.md](README-engine.md#several-processes-on-one-data-root)
gives the details and the measurements.

## Security and deletion

- memd encrypts data at rest by default, with one data key for each
  namespace. A `local` key file, AWS KMS or Vault transit holds the root key.
- Key custody fails closed. If a data key is missing or wrong, memd raises
  `KeyCustodyError` and changes nothing. memd never reads such a namespace
  as empty.
- `delete(id, hard=True)` purges the record from all files within a
  deadline (72 hours by default).
- `find_ids(query)` shows what `forget(query)` will delete. With the
  fingerprint of that preview, `forget` deletes nothing if the matches
  changed.
- `destroy_namespace()` destroys the data key of the namespace
  (crypto-shred).
- A hash-chained audit log records each delete and each forget.
- The fences around lower-trust content mark it as data. They cannot force
  a model to obey that mark.

[SECURITY.md](SECURITY.md) gives the threat model, what it does not cover,
and how to report a vulnerability.

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
  Nobody measured the current defaults end to end.
  [BENCHMARKS.md](BENCHMARKS.md) tells what each run shows and what it
  does not show.
- **Alpha software.** What changed in each release is in the
  [changelog](CHANGELOG.md).
- **Out of scope:** an agent framework or runtime, a platform for RAG over
  documents, and a graph database.

## Documentation

- Docs site: <https://siinghd.github.io/memd/> (quickstart, concepts, API
  reference, operations). The source is in [`docs/`](docs/). To build it
  locally, run `pip install -e ".[docs]" && mkdocs serve`.
- [README-engine.md](README-engine.md): the full engine guide (how to run
  it, several processes, S3, key custody, multi-node, read replicas, hosted
  mode, retrieval options, operations)
- [BENCHMARKS.md](BENCHMARKS.md): how we measured quality and latency
- [SECURITY.md](SECURITY.md): the threat model, what it does not cover,
  and how to report a vulnerability
- [CHANGELOG.md](CHANGELOG.md): each release
- [CONTRIBUTING.md](CONTRIBUTING.md): setup, tests, and what a change needs
- [RELEASING.md](RELEASING.md): how to publish a release

## License

[Apache-2.0](LICENSE).
