# Quickstart

## Install

```bash
pip install memd                       # Python >= 3.11
pip install "memd[mcp,s3]"             # extras: mcp, s3, fast, ann, local-embeddings, jev
```

!!! note "Not on PyPI yet"
    Until the first release is published, install from a checkout:
    `pip install -e ".[mcp,s3]"`. The [Examples](examples.md) run from one.

| extra | adds |
|---|---|
| `mcp` | `memd serve --mcp`, the MCP server |
| `s3` | `s3://` data roots (boto3) |
| `fast` | the tantivy accelerator for the bm25 lane |
| `ann` | the usearch HNSW sidecar for the vector lane |
| `local-embeddings` | local ONNX embeddings (fastembed) |
| `jev` | the Jev reranker (active only with `TYPESAFE_API_KEY`) |
| `billing` | Stripe billing for hosted mode |

## The ten-minute story

{% include-markdown "../README-engine.md" start="## Ten-minute story (acceptance-tested by `scripts/ten_minute_test.sh`)" end="## Running it" %}

## Running it

{% include-markdown "../README-engine.md" start="## Running it" end="## Deployment constraint: ONE process per data root" %}

!!! warning "One process per data root"
    A data directory belongs to one process at a time: a second one opening
    the same namespace fails fast with `NamespaceBusyError`, and
    `uvicorn --workers N` with N > 1 does not work. See
    [Operations](operations.md#one-process-per-data-root).

## Next

- [Concepts](concepts.md): what a namespace, a scope, a kind and a lane are
- [Examples](examples.md): sessions and facts, MCP, the REST door, TypeScript, S3
- [Python API](reference/python.md), [HTTP API](reference/http.md),
  [TypeScript SDK](reference/typescript.md)
