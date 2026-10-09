# Quickstart

## Install

```bash
pip install "memd-engine[local-embeddings]"           # Python >= 3.11. Recommended.
pip install "memd-engine[local-embeddings,mcp,s3]"    # extras: local-embeddings, mcp, s3, fast, ann, jev, billing
npm install memd-engine                               # the TypeScript REST client
```

The package is `memd-engine` on PyPI and on npm. The Python import name and
the command are `memd`.

`pip install memd-engine` also works, offline and with no model. memd then
uses hash embeddings and ranks by its lexical lanes only. It logs one line
about it. The `local-embeddings` extra gives much better recall. On
LongMemEval_S session retrieval, bge-small fused with BM25 put all evidence
sessions in the top 10 for 97.5% of questions. BM25 alone did this for
92.0%. These are lane-level measurements on 153 questions.

!!! note "From a checkout"
    To get the latest code, install from a checkout:
    `pip install -e ".[local-embeddings,mcp,s3]"`. The [Examples](examples.md) run from a checkout.

| extra | adds |
|---|---|
| `mcp` | `memd serve --mcp`, the MCP server |
| `s3` | `s3://` data roots and the `aws-kms` key provider (boto3) |
| `fast` | the tantivy accelerator for the bm25 lane |
| `ann` | the usearch HNSW sidecar for the vector lane |
| `local-embeddings` | local ONNX embeddings: BAAI/bge-small-en-v1.5 through fastembed, fused with BM25 (recommended) |
| `jev` | the Jev reranker (active only with `TYPESAFE_API_KEY`) |
| `billing` | Stripe billing for hosted mode |

## The ten-minute story

{% include-markdown "../README-engine.md" start="## Ten-minute story (acceptance-tested by `scripts/ten_minute_test.sh`)" end="## Running it" %}

## Running it

{% include-markdown "../README-engine.md" start="## Running it" end="## Several processes on one data root" %}

!!! note "Several processes on one data root"
    One process at a time writes a namespace. A second process that
    opens it forwards its writes and strong reads to the process that holds
    it. It takes the namespace over when that one goes away
    (`forwarding="off"` raises `NamespaceBusyError` instead). See
    [Operations](operations.md#several-processes-on-one-data-root).

## Next

- [Concepts](concepts.md): what a namespace, a scope, a kind and a lane are
- [Examples](examples.md): sessions and facts, MCP, the REST door, TypeScript, S3
- [Python API](reference/python.md), [HTTP API](reference/http.md),
  [TypeScript SDK](reference/typescript.md)
