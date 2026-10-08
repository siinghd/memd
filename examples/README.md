# Examples

Small, runnable scripts against memd's public APIs. Each one checks its own
results and exits non-zero if memd misbehaves; `tests/test_examples.py` runs
them in CI (the TypeScript ones in the sdk-ts workflow, which has Node).

| file | shows | needs |
|---|---|---|
| [`01_quickstart.py`](01_quickstart.py) | embedded engine: `add`, `remember`, `search`, `forget` (preview, then confirm), hard delete purged from disk | `pip install memd-engine` |
| [`02_sessions_and_facts.py`](02_sessions_and_facts.py) | `observe`, `close_session` fact extraction, supersedence on an entity key, `history`, `as_of` time travel, restart | - |
| [`03_mcp_server.py`](03_mcp_server.py) + [`mcp/`](mcp/README.md) | `memd serve --mcp` for Claude Code / Claude Desktop: config snippets, and a client calling all four tools | `memd[mcp]` |
| [`04_http_server_sdk.py`](04_http_server_sdk.py) | `memd serve --http` with the Python `HostedMemory` SDK and `Memory(api_key=...)`; typed errors | - |
| [`ts/`](#typescript) | the TypeScript SDK `@memd/client` against a server: a quickstart, sessions and facts, search options, an HTTP API and a Next.js route with memory per user, an agent loop, eventual reads, retries and typed errors, export streaming | Node >= 20 |
| [`06_s3_minio.py`](06_s3_minio.py) | an `s3://` data root on MinIO: cold reopen from the bucket, key custody failing closed, crypto-shred | `memd[s3]`, an S3 API |

```bash
pip install -e ".[mcp,s3]"          # from a checkout; or: pip install "memd-engine[mcp,s3]"
python examples/01_quickstart.py
python examples/02_sessions_and_facts.py
python examples/03_mcp_server.py
python examples/04_http_server_sdk.py
```

The Python examples write to a fresh temp directory unless you pass one
(`python examples/01_quickstart.py ./my-data`). Without API keys memd uses
its zero-key defaults (hash or local embeddings, pattern-based fact
extraction); set `MEMD_EMBEDDER=hash` for the fastest, fully deterministic
run.

`04_http_server_sdk.py` starts a throwaway server through
[`local_server.py`](local_server.py) unless `MEMD_URL` and `MEMD_API_KEY`
point at one you run (`memd serve --http`, then
`memd key create --ns demo` for the key).

## TypeScript

Strict TypeScript (ESM, Node >= 20) over the public API of `@memd/client`,
run with `tsx`. Each file's first line says what it shows and the second
how to run it.

```bash
(cd sdk-ts && npm ci && npm run build)   # the examples install the SDK from ../../sdk-ts
cd examples/ts && npm install
python ../local_server.py -- npx tsx 01_quickstart.ts
python ../local_server.py -- node quickstart.mjs   # plain JavaScript, no build step
npm run typecheck                                  # tsc --noEmit over every example
```

| file | shows |
|---|---|
| [`01_quickstart.ts`](ts/01_quickstart.ts) | `remember`, `search`, `get`, `forget` (preview, then confirm), hard `delete` |
| [`02_sessions_and_facts.ts`](ts/02_sessions_and_facts.ts) | a conversation session (`observe`), `closeSession` fact extraction, recall in the next session, supersedence on an entity key, `history`, `as_of` |
| [`03_search_options.ts`](ts/03_search_options.ts) | scope filters (`user_id`, `agent_id`, ...), per-call `namespace`, `kinds`, `budget_tokens` and `truncated`, `findIds`, `as_of` and a `t_event` window, the server's reranker |
| [`04_http_api.ts`](ts/04_http_api.ts) | a `node:http` API giving an agent memory per user, one namespace each (the operator key stays on the server): input validation, memd errors mapped to HTTP statuses, cancellation, account deletion by crypto-shred. Run under `local_server.py --admin` |
| [`05_nextjs_route.ts`](ts/05_nextjs_route.ts) + [`nextjs/app/api/memory/route.ts`](ts/nextjs/app/api/memory/route.ts) | a Next.js App Router route handler (`GET` / `POST` / `DELETE`) on standard `Request` / `Response`: one namespace, a `user_id` scope per user, validation, `req.signal`; the driver calls it without Next.js |
| [`06_agent_loop.ts`](ts/06_agent_loop.ts) | a tool-calling agent loop (OpenAI-style function calling) whose `remember_fact` / `search_memory` tools are memd, every turn captured with `observe`, recall across sessions; a stub model stands in for the LLM so it runs offline |
| [`07_eventual_reads.ts`](ts/07_eventual_reads.ts) | `consistency: "eventual"` with `maxStalenessMs` (client-wide and per call), `lastRead` and the `X-Memd-Served-By` / `-Replica-Seq` / `-Replica-Age-Ms` headers through a wrapped `fetch`, strong reads for read-your-writes. One server always answers `leader`; a cluster node that does not own the namespace answers `replica` |
| [`08_robust_client.ts`](ts/08_robust_client.ts) | retries and backoff (`retries`, `retryBaseDelayMs`, `Retry-After` on a real 429), per-attempt `timeoutMs` and an overall `AbortSignal` deadline, writes made safe to retry with an idempotency key in `meta`, typed errors (`ValidationError`, `AuthenticationError`, `PermissionDeniedError`, `ForgetPreviewMismatchError`, ...). Faults are injected through the `fetch` option |
| [`09_export_stream.ts`](ts/09_export_stream.ts) | `exportStream` piped to a JSONL file with backpressure, the `lastExportSkippedFrames` completeness signal, reading the file back, stopping a stream early |
| [`quickstart.mjs`](ts/quickstart.mjs) | the same client from plain JavaScript: `pack` / `observe` around an LLM call, typed errors |

`local_server.py` passes `MEMD_URL`, `MEMD_API_KEY` and `MEMD_NAMESPACE` to
the command it runs (with `--admin`, `MEMD_ADMIN_KEY` too); against your
own server, set them and run the same commands without it. Outside this
repository, depend on the published package instead:
`npm install @memd/client`. `tests/test_examples.py` type-checks and runs
every one of them when Node and the install are there, and skips them
otherwise.

## S3

```bash
docker run -d -p 9000:9000 -e RUSTFS_ACCESS_KEY=minioadmin \
  -e RUSTFS_SECRET_KEY=minioadmin rustfs/rustfs:1.0.1
MEMD_S3_ENDPOINT=http://127.0.0.1:9000 python examples/06_s3_minio.py
```

Credentials come from `MEMD_S3_ACCESS_KEY` / `MEMD_S3_SECRET_KEY` (the
example defaults them to `minioadmin`, which the server above is started
with); the bucket (`MEMD_EXAMPLE_BUCKET`, default `memd-examples`) is created
if missing, and each run uses a fresh prefix.
