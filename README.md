# Agent Memory System — Design Pack + Implementation

Generated 2026-08-13 from web research; **implemented 2026-08-21** (see
[README-engine.md](README-engine.md) and [adr/ADRs.md](adr/ADRs.md)).

**Thesis in three lines:** Object storage is the source of truth; RAM/NVMe are stateless caches (not RAM→FS→S3 demotion). Raw interaction data is the durable record; LLM extraction is an async, re-runnable derived index. The market wedge is capability-complete, Apache-2.0, embedded-first memory — the ground every incumbent just vacated.

## Implementation status (v0.2.0)

| Deliverable | Status | Where |
|---|---|---|
| Record schema, bitemporal supersedence, trust tiers (ADR-1/4) | done | `src/memd/core/schema.py` |
| WAL/segments/manifest storage, compaction, crypto-shred (ADR-2) | done; S3/R2 backend, single writer | `src/memd/storage/` |
| Retrieval: FTS5-bm25 lane, gated time lane, optional dense lane, optional reranker (Jev/local), budget packing | done | `src/memd/index/`, `src/memd/query/` |
| Optional tantivy lexical accelerator (`memd[fast]`) | done | `src/memd/index/` |
| Fact lane: extraction, entity clusters, consolidation (ADR-6) | done (regex extractor by default; LLM extractor unevaluated) | `src/memd/pipeline/` |
| Security controls D7: quarantine, taint, audit, hard delete, keys | done | `src/memd/storage/{audit,crypto}.py`, `server/auth.py` |
| REST / SDK / MCP three doors (D4) | done | `server/http.py`, `sdk/`, `server/mcp_server.py`, `cli.py` |
| Real-data evaluation (LongMemEval), nightly gate | done | [BENCHMARKS.md](BENCHMARKS.md), `bench/lme_gate.py` |
| Multi-writer, KMS keys, TS SDK, hosted metering | not built | see CHANGELOG "Known limitations" |

**Quality, measured on real data** (LongMemEval_S dev split, public `Memory.search`; details and caveats in
[BENCHMARKS.md](BENCHMARKS.md)): session retrieval ndcg@5 **0.866** with zero keys and zero network
(0.1.0 scored 0.727), **0.955** with the optional Jev reranker. The synthetic suite under `src/memd/harness/`
remains a regression and adversarial gate; it is *not* evidence of quality (its data is templated and
full-context also scores 1.0 on it).

| File | Deliverable |
|---|---|
| [ANSWERS-10Q.md](ANSWERS-10Q.md) | D0 — the 10 pinned interrogation answers + full source list |
| [01-wedge.md](01-wedge.md) | Market & licensing wedge; what we will not build |
| [02-slos.md](02-slos.md) | Workload spec & SLOs (acceptance criteria for everything else) |
| [03-architecture.md](03-architecture.md) | Data model, storage, indexes, write/read paths, maintenance, diagram |
| [04-interfaces.md](04-interfaces.md) | SDK / REST / MCP, scoping, 10-minute story, Mem0/Letta migration |
| [05-eval.md](05-eval.md) | Harness-first evaluation; adversarial suite as ship gate |
| [06-economics.md](06-economics.md) | Multi-tenancy, unit costs, scale table, implied pricing |
| [07-security.md](07-security.md) | ASI06-mapped controls; MINJA/AgentPoison-informed |
| [08-roadmap.md](08-roadmap.md) | Phases 0–3 with entry/exit/kill conditions |
| [09-opencore-adrs.md](09-opencore-adrs.md) | Apache-2.0 vs commercial line; ADRs by reversal cost |
| [adr/ADRs.md](adr/ADRs.md) | ADRs as-implemented (accepted/amended) |
| [research/](research/README.md) | 1:1 verbatim research logs with provenance tiers |
