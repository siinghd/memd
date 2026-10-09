# Contributing to memd

## The short version

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev,mcp]"
make test          # pytest (about 16 min in CI, with the S3 tests)
make gate          # eval gate: accuracy, cost bar, adversarial zero-regression
make ten-min       # fresh dir -> cross-session recall, the ten-minute story
```

All three must pass before a change is merged. CI (continuous integration)
runs exactly these on Python 3.11 and 3.12, against RustFS (an S3 server), so
the S3 tests also run. CI also runs bandit, a clean-install smoke test, a
container smoke test, the TypeScript SDK (software development kit) and the
docs build.

```bash
MEMD_TEST_S3_ENDPOINT=http://127.0.0.1:9000 make test   # with an S3 server (see tests/test_s3_backend.py)
python examples/01_quickstart.py                        # examples/ are run by tests/test_examples.py
pip install -e ".[docs]" && mkdocs build --strict       # the docs site (mkdocs.yml); mkdocs serve to preview
```

The docs pages include README-engine.md, SECURITY.md, BENCHMARKS.md and
CHANGELOG.md between their headings. If you rename a heading in these files,
the strict build shows the page that you must correct.
[RELEASING.md](RELEASING.md) tells how to publish a release.

## What this project asks of a change that others don't

**Reproduce before you fix.** Show the defect with a probe that you can run,
*before* a line changes. Put the probe and its output in the pull request.
"Looks wrong" is not a finding; a script that fails is a finding. This rule is
necessary: it is the only difference between a fix and a guess. In this
project, a reading of the code that seemed correct was wrong several times.

**Write the test so it would fail on the old code.** A test that passes both
before and after the change protects nothing. This project found two such
tests late. A lockout test used 25 attempts against a 30-failure threshold.
An index test accepted *any* index, so it passed on the tree that had no index
at all. If you are not sure, check out the parent commit. Then run your test
against it.

**Measure old versus new, interleaved.** On a shared machine, benchmark
results drift. Run the arms alternately (`BASE, NEW, BASE, NEW, ...`). Then
compare the medians. If you do not, you can report noise from the machine as
your improvement. Put the numbers from before and after in the pull request.
In this project, an apparent 25% p99 regression was only a measurement
artifact, and only an interleaved run separated a real one from noise.

**State complexity in O() with the variable named.** Count I/O (input/output)
separately from arithmetic. "Fast" is not a claim. Round trips to disk or
network must be O(1) for each request. `count_io()` in `storage/objectstore.py`
exists to assert exactly that, and some tests use it for this.

**If it is a rewrite, say so and say why a patch was wrong.** This project
replaced several subsystems and did not patch them: the histogram core, the
ownership model of the audit ledger, and the BM25 OR tier. A patch would have
left the other half of the defect in place. Put that reasoning in the commit
message.

## Writing the docs

The docs are in ASD-STE100 Simplified Technical English (STE): short
sentences, the active voice, and one meaning for each word. Before you
send a change to a `.md` file, run the checker:

```bash
python scripts/ste_check.py README-engine.md docs/   # or no argument: all the docs
```

It reports long sentences, passive-voice candidates, `-ing` words and some
words that STE does not approve (with the approved word). The word list is
a small subset, because the official STE dictionary is not in this
repository. The report is for information: CI runs it, and a problem that
it reports does not stop a merge. Correct the problems in the text that you
changed.

## Layout

| Path | What lives there |
|---|---|
| `src/memd/core/` | The record model: bitemporal fields, scope, provenance, trust tiers |
| `src/memd/storage/` | WAL (write-ahead log), segments, manifest, compaction, audit ledger, envelope crypto |
| `src/memd/index/` | The SQLite derived index: **rebuildable by contract**, never the source of truth |
| `src/memd/query/` | Planner, RRF (reciprocal rank fusion), budget-aware packing |
| `src/memd/pipeline/` | Extraction, embedding, consolidation (async, can run again) |
| `src/memd/server/` | REST, MCP (Model Context Protocol), authentication |
| `src/memd/harness/` | The eval gate: suites, adversarial probes, scoring |
| `bench/` | SLO (service level objective) and scale benchmarks (for information only, never a CI gate) |

## Invariants you should not break without a design discussion

- **Object storage + WAL/segments are the source of truth.** The SQLite index
  is a cache. Anything that makes the index authoritative is a design change.
- **Single writer per namespace**, enforced by a lock file (a local root) or
  a lease (an `s3://` root). Two writers destroyed acknowledged data without
  an error (measured: 8 of 150 lost). That is why the lock exists. Other
  processes send their calls to the writer (write forwarding). They never
  write the log of the namespace themselves.
- **No LLM or embedding call on the write path.** The write path does not
  call an LLM (large language model) or an embedding model. A durable ack
  (acknowledgment) is an append with fsync, and nothing more.
- **Per-write maintenance is O(entity cluster), never O(namespace).**
  Do namespace-scale work on the maintenance thread.
- **Untrusted content is fenced in packed context**, and memd never renders
  it in the instruction position.
- **Durations are measured, named and bucketed in milliseconds** (`_ms`).
  Mixed units previously made every latency quantile in the system wrong.

## Reporting a security issue

Refer to [SECURITY.md](SECURITY.md).
