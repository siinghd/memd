# Quickstart

## Install

```bash
pip install "memd-engine[local-embeddings]"           # Python >= 3.11; recommended
pip install "memd-engine[local-embeddings,mcp,s3]"    # extras: local-embeddings, mcp, s3, fast, ann, jev
```

Plain `pip install memd-engine` works too, offline and with no model: memd
then uses hash embeddings and ranks by its lexical lanes alone (it logs one
line saying so). The `local-embeddings` extra materially improves recall:
on LongMemEval_S session retrieval, 97.5% of questions had every evidence
session in the top 10 with bge-small fused with BM25, against 92.0% with
BM25 alone (lane-level measurements, 153 questions).

!!! note "Not on PyPI yet"
    Until the first release is published, install from a checkout:
    `pip install -e ".[local-embeddings,mcp,s3]"`. The [Examples](examples.md) run from one.

| extra | adds |
|---|---|
| `mcp` | `memd serve --mcp`, the MCP server |
| `s3` | `s3://` data roots (boto3) |
| `fast` | the tantivy accelerator for the bm25 lane |
| `ann` | the usearch HNSW sidecar for the vector lane |
| `local-embeddings` | local ONNX embeddings: BAAI/bge-small-en-v1.5 via fastembed, fused with BM25 (recommended) |
| `jev` | the Jev reranker (active only with `TYPESAFE_API_KEY`) |
| `billing` | Stripe billing for hosted mode |

## The ten-minute story

{% include-markdown "../README-engine.md" start="## Ten-minute story (acceptance-tested by `scripts/ten_minute_test.sh`)" end="## Running it" %}

## Running it

{% include-markdown "../README-engine.md" start="## Running it" end="## Several processes on one data root" %}

!!! note "Several processes on one data root"
    A namespace is written by one process at a time. A second process
    opening it forwards its writes and strong reads to the one holding it,
    and takes the namespace over when that one goes away
    (`forwarding="off"` raises `NamespaceBusyError` instead). See
    [Operations](operations.md#several-processes-on-one-data-root).

## Next

- [Concepts](concepts.md): what a namespace, a scope, a kind and a lane are
- [Examples](examples.md): sessions and facts, MCP, the REST door, TypeScript, S3
- [Python API](reference/python.md), [HTTP API](reference/http.md),
  [TypeScript SDK](reference/typescript.md)
