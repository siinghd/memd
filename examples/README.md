# Examples

Small, runnable scripts against memd's public APIs. Each one checks its own
results and exits non-zero if memd misbehaves; `tests/test_examples.py` runs
the Python ones in CI.

| file | shows | needs |
|---|---|---|
| [`01_quickstart.py`](01_quickstart.py) | embedded engine: `add`, `remember`, `search`, `forget` (preview, then confirm), hard delete purged from disk | `pip install memd-engine` |
| [`02_sessions_and_facts.py`](02_sessions_and_facts.py) | `observe`, `close_session` fact extraction, supersedence on an entity key, `history`, `as_of` time travel, restart | - |
| [`03_mcp_server.py`](03_mcp_server.py) + [`mcp/`](mcp/README.md) | `memd serve --mcp` for Claude Code / Claude Desktop: config snippets, and a client calling all four tools | `memd[mcp]` |
| [`04_http_server_sdk.py`](04_http_server_sdk.py) | `memd serve --http` with the Python `HostedMemory` SDK and `Memory(api_key=...)`; typed errors | - |
| [`05_ts/`](05_ts/quickstart.mjs) | the TypeScript SDK `@memd/client` against a server | Node >= 18 |
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

```bash
(cd sdk-ts && npm ci && npm run build)   # the example installs the SDK from ../../sdk-ts
(cd examples/05_ts && npm install)
python examples/local_server.py -- node examples/05_ts/quickstart.mjs
```

`local_server.py` passes `MEMD_URL`, `MEMD_API_KEY` and `MEMD_NAMESPACE` to
the command it runs; against your own server, set them and run
`npm start` in `examples/05_ts`. Outside this repository, depend on the
published package instead: `npm install @memd/client`.

## S3 (MinIO)

```bash
docker run -d -p 9000:9000 -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
MEMD_S3_ENDPOINT=http://127.0.0.1:9000 python examples/06_s3_minio.py
```

Credentials come from `MEMD_S3_ACCESS_KEY` / `MEMD_S3_SECRET_KEY` (the
example defaults them to MinIO's `minioadmin`); the bucket
(`MEMD_EXAMPLE_BUCKET`, default `memd-examples`) is created if missing, and
each run uses a fresh prefix.
