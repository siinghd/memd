# memd eval harness

Rule: **an optimization ships only if it changes a number here.** The harness is
the authority for the quality and cost targets of memd.

## Principles (enforced by code, not convention)
- **Everything is frozen**: the answer and judge models, the prompts, the
  retrieval configs and the dataset revisions are pinned and version-locked.
  Every result contains `HARNESS_VERSION`, a content hash of every `*.py` file
  in this directory and its subdirectories. Other files, for example this
  README, do not change it. A result without a hash is not a valid result.
- **Quality and cost side by side, always**: every run reports accuracy/recall
  AND $/1K queries, tokens/query, write COGS (cost of goods sold) and p50/p95
  latencies. A quality improvement that makes cost >20% worse fails the gate
  (`--gate`).
- **One adapter interface** (`add_events / search / answer`): all systems under
  test use it, so you can compare them. These systems are `memd`,
  `full-context`, `plain-rag` and any installed third-party adapter.

## Suites
| suite | file | gates |
|---|---|---|
| longmemeval-synthetic | `suites/longmemeval_synthetic.py` | single-hop/multi-hop/temporal/knowledge-update recall compared with the full-context baseline |
| halumem-ops | `suites/halumem_ops.py` | operation-level extraction/update correctness (update >=0.95 latest-fact target) |
| adversarial | `suites/adversarial.py` | MINJA-style injection quarantine, stale-fact, contradiction pairs, cross-tenant leakage. A failure blocks the release |

The harness generates `longmemeval-synthetic` deterministically from a seed.
Thus, you can compare runs without network access. To run the real
LongMemEval dataset, put it on the local disk and set `MEMD_LME_DATA` to it.

## Usage
```bash
python -m memd.harness.run --suite all --gate        # CI gate mode
python -m memd.harness.run --suite adversarial       # one suite
```
