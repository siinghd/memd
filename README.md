# Agent Memory System — Design Pack + Implementation

Generated 2026-08-13 from web research; **implemented 2026-08-21** (see
[README-engine.md](README-engine.md) and [adr/ADRs.md](adr/ADRs.md)).

**Thesis in three lines:** Object storage is the source of truth; RAM/NVMe are stateless caches (not RAM→FS→S3 demotion). Raw interaction data is the durable record; LLM extraction is an async, re-runnable derived index. The market wedge is capability-complete, Apache-2.0, embedded-first memory — the ground every incumbent just vacated.

## Implementation status (Phase 0 + Phase 1 complete)

| Deliverable | Status | Where |
|---|---|---|
| Record schema, bitemporal supersedence, trust tiers (ADR-1/4) | done | `src/memd/core/schema.py` |
| WAL/segments/manifest storage, compaction, crypto-shred (ADR-2) | done | `src/memd/storage/` |
| Hybrid retrieval: BM25 + flat vector + time/entity + RRF + packing (ADR-5) | done | `src/memd/index/`, `src/memd/query/` |
| Fact lane: extraction, entity clusters, consolidation (ADR-6) | done | `src/memd/pipeline/` |
| Security controls D7: quarantine, taint, audit, hard delete, keys | done | `src/memd/storage/{audit,crypto}.py`, `server/auth.py` |
| REST / SDK / MCP three doors (D4) | done | `server/http.py`, `sdk/`, `server/mcp_server.py`, `cli.py` |
| Eval harness with frozen hash + adversarial gate (D5) | done | `src/memd/harness/` |
| SLO acceptance numbers (D2) | passing | `bench/slo_bench.py` |
| Dockerfile + 10-minute CI story | done | `Dockerfile`, `scripts/ten_minute_test.sh` |

Gate results (`make gate`, harness_version stamped): memd = full-context
accuracy at **7.5% of full-context tokens**, beats plain-RAG by +17pts,
HaluMem-style update ops **1.0** (target ≥0.95), adversarial probes 6/6.

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
