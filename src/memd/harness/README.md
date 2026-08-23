# memd eval harness (D5)

Rule: **no optimization ships without a number moving here.** The harness is
the authority over the SLO table in `../02-slos.md`.

## Principles (enforced by code, not convention)
- **Frozen everything**: pinned answer/judge models + prompts + retrieval
  configs + dataset revisions, all version-locked. Every result carries
  `HARNESS_VERSION` (content hash of this directory). A result without a hash
  doesn't exist.
- **Quality and cost side by side, always**: every run emits accuracy/recall
  AND $/1K queries, tokens/query, write COGS, p50/p95 latencies.
  A quality win that regresses cost >20% fails the gate (`--gate`).
- **One adapter interface** (`add_events / search / answer`) so systems under
  test are comparable: `memd`, `full-context`, `plain-rag`, and any
  installed third-party adapter.

## Suites
| suite | file | gates |
|---|---|---|
| longmemeval-synthetic | `suites/longmemeval_synthetic.py` | single-hop/multi-hop/temporal/knowledge-update recall vs full-context baseline |
| halumem-ops | `suites/halumem_ops.py` | operation-level extraction/update correctness (update >=0.95 latest-fact target) |
| adversarial | `suites/adversarial.py` | MINJA-style injection quarantine, stale-fact, contradiction pairs, cross-tenant leakage - ship-blocking |

`longmemeval-synthetic` is generated deterministically from a seed so runs are
comparable without network access; when the real LongMemEval dataset is
available locally, point `MEMD_LME_DATA` at it to run the real one.

## Usage
```bash
python -m memd.harness.run --suite all --gate        # CI gate mode
python -m memd.harness.run --suite adversarial       # one suite
```
