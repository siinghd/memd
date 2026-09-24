# memd benchmarks (v0.2.0)

All numbers below come from logged, pre-registered experiments in the memd research lab (a separate repository, not bundled here)
(`experiments/NNN-*`, each with a PLAN written before running, a negative control, 3 disjoint question
folds, and a hostile review). Nothing here is from memd's own synthetic suite.

## Data and harness

- **Dataset:** [LongMemEval](https://arxiv.org/abs/2410.10813) (ICLR 2025), cleaned release
  (`xiaowu0162/longmemeval-cleaned`). `_S`: ~48 sessions / ~490 turns per question. `_M`: ~500 sessions / ~5K turns per question.
- **Split:** questions with index % 5 == 0 are held out; the rest (376 non-abstention questions in `_S`) form three
  dev folds. The numbers below are dev-fold means ± fold std unless stated otherwise.
- **Calibration:** the harness's BM25 baseline reproduces the paper's Table 9 BM25 row within 0.02 on
  recall_all@5, ndcg@5, recall_all@10 and ndcg@10 (exp 004).
- **Metrics:** session-level ndcg_any@5 and recall_all@5. Sessions are ranked by the first appearance of any of their turns in
  memd's returned items (`Memory.search`, public API).

## Retrieval quality and latency (LongMemEval_S, public `Memory.search`)

| memd configuration | ndcg@5 | recall_all@5 | mean search time | exp |
|---|---|---|---|---|
| v0.1.0 as shipped | 0.727 ± 0.027 | 0.697 | ~130 ms | 000, 012 |
| v0.2.0, zero-key default (hash embedder, no reranker) | 0.866 ± 0.017 | 0.835 | ~16 ms | 020 |
| v0.2.0 + Jev reranker (`TYPESAFE_API_KEY` set) | **0.955 ± 0.026** | **0.928** | ~1 s (network) | 021 |

Random-order and wrong-query controls score ndcg@5 ≤ 0.13 in every experiment.

At `_M` scale (120 dev questions, ~5K turns per user), v0.1.0 search collapsed to ndcg@5 0.41 (its lexical lane
re-ranked an unordered 320-row window by term coverage without IDF). A bm25()-ranked lane on the same index (the lane v0.2.0 ships) scores 0.88 (exp 006, lane-level measurement).

## Lexical index scale (optional `memd[fast]` = tantivy accelerator)

Filtered, user-scoped search through `Memory.search`, 200 real questions over real LongMemEval turns:

| records | FTS5 p50 / p99 | tantivy p50 / p99 |
|---|---|---|
| 10K | 15.1 / 68.6 ms | 5.7 / 33.7 ms |
| 50K | 44.0 / 95.5 ms | 7.3 / 36.6 ms |
| 150K | 114.3 / 309.0 ms | 8.8 / 38.5 ms |

The single-record write ack is unchanged (FTS5 remains the synchronous source of truth; tantivy is an async
derived accelerator). The 60-question real-data gate scores 0.874 (tantivy) vs 0.867 (FTS5). Measured on a
loaded, shared 8-core host (`bench/lexical_bench.py`).

## End-to-end QA (preliminary)

Stratified 120-question sample (all 6 question types + abstention). Reader `openai/gpt-6-luna`, judge
`openai/gpt-6-luna-pro` with the official LongMemEval per-type judge prompts; Jev used as co-judge
(kappa 0.97 against the LLM-judge consensus, exp 017).

| context given to the reader | accuracy | mean context tokens | exp |
|---|---|---|---|
| memd v0.2 packed context (no reranker; lab build before the patch-3 fusion change) | 0.692 ± 0.029 | 3.8K | 018 |
| + Jev rerank, top-k | 0.783 ± 0.014 | 3.1K | 018 |
| + Jev probability-gated packing (100-candidate shortlist) | 0.792 ± 0.014 | 2.3K | 018 |

Not yet run (blocked on API credits): the full-context reference and the wrong-context control for this
sample, and the one-time held-out 500-question run. Multi-session aggregation ("how many X…") is the weakest
category (0.55 with Jev vs ~0.9 with the full history on an earlier sample).

## How this compares

Vendors report 84–96% QA accuracy on LongMemEval_S with different readers and judges, mostly self-reported
and in some cases disputed. Our QA numbers use cheap current models and a small sample. Compare directionally,
not to the decimal. We publish the harness, the controls and the failures on purpose.

## Reproduce

- Real-data gate: `python bench/lme_gate.py` (downloads LongMemEval_S; 60 fixed dev questions; fails below
  ndcg@5 0.80). It also runs nightly in CI.
- Lexical scale: `python bench/lexical_bench.py`.
