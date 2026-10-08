# memd benchmarks (v0.2.0)

All numbers below come from logged, pre-registered experiments kept in a separate research repository (not bundled here)
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

## Vector index scale (optional `memd[ann]` = usearch sidecar)

The vector lane as `Memory.search` calls it (user-scoped filter over 8 users, limit = the planner's
`candidate_k`), 200 queries, 384-d vectors; recall@10 against the lane's exact answer
(`bench/ann_bench.py`, each size in its own process):

| vectors | recall@10 | usearch lane p50 / p99 | exact flat lane p50 / p99 | build from SQLite | peak RSS |
|---|---|---|---|---|---|
| 50K synthetic | 1.000 | 14.3 / 54.7 ms | 37.3 / 84.3 ms | 8.0 s | 0.44 GB |
| 200K synthetic | 0.999 | 14.1 / 31.9 ms | 54.4 / 123.5 ms | 31.4 s | 1.04 GB (0.47 GB before the flat matrix loads) |
| 1M synthetic | 0.993 | 19.4 / 74.2 ms | not run: its float32 matrix alone is 1.5 GB | 315 s | 1.36 GB |
| 50K LongMemEval turns, hash embedder | 0.966 | 20.4 / 61.2 ms | 48.4 / 135.3 ms | 18.8 s | 0.52 GB |
| 199K LongMemEval turns, hash embedder | 0.938 | 25.6 / 77.1 ms | 63.5 / 125.6 ms | 91.9 s | 1.31 GB |

Synthetic = unit vectors around 256 random centers (dense, like a real embedder's). The hash
embedder's sparse n-gram vectors are hard for HNSW: neither `ann_expansion_search` 512 (0.943) nor
`ann_overfetch` 8 (0.936, 2x the latency) lifts the filtered 199K recall, and ties are not the
cause (tie-aware recall is the same). The hash lane is not fused into ranking by default
(`fuse_vector`), and sweeps (`find_ids`) are always exact.

The write path does not wait on the sidecar: `add_events` (100 events) ack p50 / p99 was
18.8 / 57.8, 20.8 / 36.3 and 19.1 / 44.9 ms with it at 50K / 200K / 1M, against 23.8 / 51.0,
24.8 / 37.4 and 20.5 / 54.2 ms with the exact scan, while the embed worker fed each batch's
vectors into it. The sidecar file is 46 / 183 / 917 MB (f16). Builds use 4 threads. Save and load
run on background threads, but usearch holds the GIL throughout: at 200K the longest stall any
thread saw was 100 ms per save and 105 ms per load (the final save took 236 ms in the
background). Measured on a loaded, shared 8-core ARM host (Neoverse-N1).

## End-to-end QA: session packing and the 12K budget

The current search defaults (session packing, 12,000 tokens) come from this run. 160 LongMemEval_S questions in a
fixed stratified order (multi-session and temporal-reasoning over-sampled twice, abstention included), results
re-weighted to the dataset's type mix; one run. Reader `deepseek/deepseek-v4.1-flash` (one provider pinned, a
pinned fallback served 61 of 480 calls), judge `openai/gpt-6-luna-pro` with the official per-type judge prompts.
Retrieval for every row but the first: bge-small embeddings (`local-embeddings`) and a local cross-encoder
reranker (`Xenova/ms-marco-MiniLM-L-6-v2`, not memd's default `local_rerank_model`).

| context given to the reader | accuracy [95% CI] | multi-session (n 52) | temporal (n 54) | reader prompt tokens | evidence sessions in context |
|---|---|---|---|---|---|
| previous defaults: hash embedder, no reranker, 2K flat | 0.779 [0.718, 0.838] | 0.577 | 0.796 | 2.0K | 0.858 |
| bge-small + reranker, 2K flat | 0.789 [0.726, 0.847] | 0.654 | 0.759 | 2.0K | 0.912 |
| bge-small + reranker, 12K flat | 0.823 [0.763, 0.879] | 0.750 | 0.833 | 10.6K | 0.985 |
| bge-small + reranker, 12K sessions | **0.875 [0.823, 0.920]** | 0.769 | 0.870 | 10.4K | 0.971 |
| the whole history, no retrieval | 0.906 [0.858, 0.949] | 0.904 | 0.944 | ~105K | 1 |

Paired (re-weighted): 12K sessions vs previous defaults +0.095 [+0.034, +0.156], McNemar p = 0.0015 (22 / 5
discordant); 12K sessions vs 12K flat +0.052 [-0.010, +0.113]; 12K flat vs 2K flat +0.034 [-0.012, +0.081];
embedder + reranker at 2K vs previous defaults +0.009 [-0.043, +0.061]. The session pack is still below the whole
history (unweighted paired -0.056 [-0.106, -0.006], p = 0.049). Per-type values are raw means.

What this does not show: the new defaults with the hash embedder and no reranker (not run end to end); more than
one seed, reader or judge; the held-out 500 questions. Unlike the retrieval table above, it is one sample with no
fold split, and it has not been independently reviewed. The measured session pack had relative-date annotations
on (`pack_resolve_dates`); memd ships them off, because on the 20-question pilot the pack without them scored no
worse (0.85 vs 0.75; not tested at power). Abstention fell with more context (0.857 to 0.714, n = 7). Search
latency in this run is dominated by the cross-encoder on a loaded CPU (seconds per search); the packing itself is
measured in README-engine.md.

## Embedders (session retrieval, lane level)

LongMemEval_S, 153 questions (the order above, abstention excluded), lanes measured outside `Memory.search` (BM25
over stemmed turns and memd's RRF approximated), re-weighted to the type mix:

| ranking | recall_all@10 [95% CI] | recall_all@5 | ndcg@10 | multi-session recall_all@10 |
|---|---|---|---|---|
| hash embedder's vector lane (not fused by memd) | 0.640 [0.566, 0.711] | 0.545 | 0.639 | 0.385 |
| BM25 over turns (what memd ranks by with the hash embedder) | 0.920 [0.878, 0.955] | 0.818 | 0.894 | 0.827 |
| bge-small vector lane (`local-embeddings`) | 0.980 [0.960, 0.995] | 0.911 | 0.942 | 0.962 |
| RRF(BM25, bge-small) (memd with `local-embeddings`) | 0.975 [0.950, 0.995] | 0.920 | 0.953 | not reported |

Fused vs BM25 alone: +0.056 [+0.021, +0.095] recall_all@10 (9 / 0 discordant). One sample, no fold split, not
independently reviewed. On a 19-question subset
EmbeddingGemma 2 matched bge-small (recall_all@10 1.000 for both; no measurable difference) at 2.4-4.6x the CPU
time per text.

## End-to-end QA (preliminary)

Stratified 120-question sample (all 6 question types + abstention). Reader `openai/gpt-6-luna`, judge
`openai/gpt-6-luna-pro` with the official LongMemEval per-type judge prompts; Jev used as co-judge
(kappa 0.97 against the LLM-judge consensus, exp 017).

| context given to the reader | accuracy | mean context tokens | exp |
|---|---|---|---|
| memd v0.2 packed context (no reranker; a pre-release build, before a later fusion change) | 0.692 ± 0.029 | 3.8K | 018 |
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
