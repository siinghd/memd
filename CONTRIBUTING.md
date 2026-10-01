# Contributing to memd

## The short version

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev,mcp]"
make test          # pytest, ~4 min
make gate          # eval gate: accuracy, cost bar, adversarial zero-regression
make ten-min       # fresh dir -> cross-session recall, the D4 onboarding story
```

All three must pass before a change lands. CI runs exactly these on 3.11 and
3.12 (against MinIO, so the S3 tests run too), plus bandit, a clean-install
smoke test, a container smoke test, the TypeScript SDK and the docs build.

```bash
MEMD_TEST_S3_ENDPOINT=http://127.0.0.1:9000 make test   # with MinIO (see tests/test_s3_backend.py)
python examples/01_quickstart.py                        # examples/ are run by tests/test_examples.py
pip install -e ".[docs]" && mkdocs build --strict       # the docs site (mkdocs.yml); mkdocs serve to preview
```

The docs pages include README-engine.md, SECURITY.md, BENCHMARKS.md and
CHANGELOG.md between their headings: rename a heading there and the strict
build names the page to fix. Publishing is described in
[RELEASING.md](RELEASING.md).

## What this project asks of a change that others don't

**Reproduce before you fix.** Every defect in `.ralph/audit-log.md` was
demonstrated with a runnable probe *before* a line changed, and the probe's
output is in the log. "Looks wrong" is not a finding; a failing script is.
This is not ceremony - it is the only thing separating a fix from a guess, and
the log records several cases where a plausible reading of the code turned out
to be wrong.

**Write the test so it would fail on the old code.** A test that passes both
before and after guards nothing. Two examples from the log, both caught late:
a lockout test used 25 attempts against a 30-failure threshold, and an index
test allowed *any* index so it passed on the tree that had no index at all.
When in doubt, check out the parent commit and run your test against it.

**Measure old versus new, interleaved.** Benchmarks on a shared machine drift.
Run the arms alternately (`BASE, NEW, BASE, NEW, ...`) and compare medians, or
you will report the machine's mood as your improvement. The log contains an
apparent 25% p99 regression that turned out to be a measurement artifact, and
a real one that only an interleaved run separated from noise.

**State complexity in O() with the variable named**, and count I/O separately
from arithmetic. "Fast" is not a claim. Round trips to disk or network must be
O(1) per request - `count_io()` in `storage/objectstore.py` exists to assert
exactly that, and there are tests that do.

**If it is a rewrite, say so and say why a patch was wrong.** Several
subsystems here were replaced rather than patched (the histogram core, the
audit ledger's ownership model, the BM25 OR tier) because the patch would have
left the other half of the defect in place. That reasoning belongs in the
commit message.

## Layout

| Path | What lives there |
|---|---|
| `src/memd/core/` | The record model: bitemporal fields, scope, provenance, trust tiers |
| `src/memd/storage/` | WAL, segments, manifest, compaction, audit ledger, envelope crypto |
| `src/memd/index/` | The SQLite derived index - **rebuildable by contract**, never the source of truth |
| `src/memd/query/` | Planner, RRF fusion, budget-aware packing |
| `src/memd/pipeline/` | Extraction, embedding, consolidation (async, re-runnable) |
| `src/memd/server/` | REST, MCP, auth |
| `src/memd/harness/` | The eval gate - suites, adversarial probes, scoring |
| `bench/` | SLO and scale benchmarks (informational, never a CI gate) |
| `.ralph/audit-log.md` | The engineering log: every defect found, fixed, deferred, with before/after numbers |

## Invariants you should not break without an ADR

- **Object storage + WAL/segments are the source of truth.** The SQLite index
  is a cache. Anything that makes the index authoritative is a design change.
- **Single writer per data root**, enforced by an advisory lock. Two writers
  silently destroyed acked data (8 of 150 lost, measured) - that is why the
  lock exists.
- **No LLM or embedding call on the write path.** A durable ack is an fsync'd
  append and nothing else.
- **Per-write maintenance is O(entity cluster), never O(namespace).**
  Namespace-scale work belongs on the maintenance thread.
- **Untrusted content is fenced in packed context** and never rendered in
  instruction position.
- **Durations are measured, named and bucketed in milliseconds** (`_ms`). Mixing
  units is what previously made every latency quantile in the system wrong.

## Reporting a security issue

See [SECURITY.md](SECURITY.md).
