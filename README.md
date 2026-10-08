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
  Without API keys, memd uses local ONNX embeddings (BAAI/bge-small-en-v1.5,
  with the `local-embeddings` extra) or deterministic hash embeddings
  (without the extra). It uses pattern-based fact extraction. `stats()`
  shows which. For API embeddings or LLM fact extraction, give an
  OpenAI-compatible key.
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
- **Hybrid retrieval, packed as evidence.** A rules-based planner fans out
  over BM25 (SQLite FTS5, optionally accelerated by tantivy), entity, time
  and vector lanes. Reciprocal rank fusion merges the lanes, and a reranker
  can reorder the result. memd then packs the result into a token budget
  (12,000 by default) as dated excerpts of past sessions. Each hit comes
  with the turns around it. A fact shows under the turn that it came from.
  Each line starts with its speaker.
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
pip install "memd-engine[local-embeddings]"
```

The `local-embeddings` extra runs BAAI/bge-small-en-v1.5 on the CPU, through
fastembed. The model downloads on first use. memd fuses it with BM25. The
extra gives much better recall. On LongMemEval_S session retrieval, 97.5% of
questions had all their evidence sessions in the top 10 (recall_all@10
0.975). With BM25 alone, the result was 92.0%. memd ranks by BM25 alone
without the extra. These are lane-level measurements on 153 questions. The
hash embedder's own vector lane scores 0.640, and memd does not use it for
ranking.

`pip install memd-engine` also works, offline and with no model. memd then
uses hash embeddings, and it logs one line about it.

The latest code from this repository:

```bash
pip install "memd-engine[local-embeddings] @ git+https://github.com/siinghd/memd.git"
```

or from a checkout, with the extras you need:

```bash
git clone https://github.com/siinghd/memd.git && cd memd
pip install -e ".[local-embeddings,mcp,s3]"   # extras: local-embeddings, mcp, s3, fast, ann, jev, billing
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

In an agent loop it is two calls: `messages = mem.pack(messages, user_id="u1")`
before your LLM call, `mem.observe(messages, response, user_id="u1")` after.
Restart the process and ask again: the memory is in `./my-data`.
More in [`examples/`](examples/README.md).

## One engine, four doors

- **Python:** `from memd import Memory`, embedded as above; `Memory(api_key=..., base_url=...)` is the same API over REST.
- **MCP:** `memd serve --mcp` exposes four tools (`memory_search`, `memory_save`, `memory_forget`, `memory_status`) to any MCP client ([setup](examples/mcp/README.md)).
- **HTTP:** `memd serve --http` serves the REST API on port 8700, with per-namespace API keys (`memd key create --namespace acme`).
- **TypeScript:** [`sdk-ts/`](sdk-ts/README.md) is `memd-engine`, a typed REST client for Node >= 18, Bun, Deno and edge runtimes (not on npm yet: build it from `sdk-ts/`).

## Measured quality

Session retrieval on LongMemEval_S through the public `Memory.search`
(dev-fold means ± fold std, from [BENCHMARKS.md](BENCHMARKS.md)):

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

These results come from one public dataset, one reader and one judge. The
one-time held-out 500-question run is not done. For the setup and the
limits, see [README-engine.md](README-engine.md#packing-and-the-budget) and
[BENCHMARKS.md](BENCHMARKS.md).

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
