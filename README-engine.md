# memd — the SQLite of agent memory

Embedded-first agent memory engine: one process, zero external services.
Raw + fact lanes, bitemporal supersedence, provenance/trust tiers, hybrid
retrieval, budget-aware packing. Apache-2.0, capability-complete.

## Ten-minute story (acceptance-tested by `scripts/ten_minute_test.sh`)

```bash
pip install -e .            # Python >= 3.11
```

```python
from memd import Memory

mem = Memory("./my-data")                          # embedded; Memory(api_key=...) = hosted, same API

# two lines in any agent loop:
messages = mem.pack(messages, user_id="u1")        # inject packed, provenance-tagged context
mem.observe(messages, response, user_id="u1")      # capture the turn after your LLM call

# or explicitly:
mem.add("We deploy with `make ship`, never CI", session_id="s1", user_id="u1")
hits = mem.search("how do we deploy?", user_id="u1", budget_tokens=1500)
print(hits.packed_context)                         # ready to inject
```

Kill the process, restart, ask "what did we decide yesterday?" — cross-session
recall works (`scripts/ten_minute_test.sh` proves it from a fresh directory).

## Running it

### 1. Embedded (a library, no server)
Nothing to start. `Memory("./my-data")` owns the directory; see the snippet above.

### 2. REST server
```bash
export MEMD_ADMIN_KEY="$(python -c 'import secrets;print(secrets.token_urlsafe(32))')"
memd serve --http --host 127.0.0.1 --port 8700     # MEMD_DATA=./memd-data by default

# mint a per-tenant key (the admin key is namespace "*"; do not hand it to apps)
memd key create --namespace acme
```
```bash
curl -sX POST localhost:8700/v1/ns/acme/memories \
  -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"content":"we deploy with make ship, never CI","user_id":"u1"}'

curl -sX POST localhost:8700/v1/ns/acme/search \
  -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"query":"how do we deploy?","user_id":"u1"}'
```
`/docs` and `/openapi.json` are **off by default** (they enumerate every route
of an otherwise authenticated API). Set `MEMD_ENABLE_DOCS=1` for development.

### 3. MCP (Claude Desktop, Cursor, any MCP client)
```json
{ "mcpServers": { "memd": { "command": "memd", "args": ["serve", "--mcp"],
                            "env": { "MEMD_DATA": "/abs/path/to/memd-data" } } } }
```

### 4. Docker
```bash
docker build -t memd/memd:0.2.0 .
docker run -d -p 8700:8700 -e MEMD_ADMIN_KEY=... -v memddata:/data memd/memd:0.2.0 serve --http
```
The default image does not include the Stripe SDK (~26 MB installed; only
hosted billing uses it). For `serve --http --hosted` with billing, build the
variant: `docker build --build-arg MEMD_BILLING=1 -t memd/memd:0.2.0-hosted .`
(the build fails if the billing install does).
The image runs as **uid 10001**, so a *named volume* (above) works but a
**bind mount does not** unless you either pass `--user "$(id -u):$(id -g)"` or
`chown 10001` the host directory. Both are verified; pick one deliberately.

## Deployment constraint: ONE process per data root

memd is **single-writer per data root** and enforces it with an advisory lock:
a second process opening the same namespace fails fast with
`NamespaceBusyError`. This is not a limitation to work around — two writers on
one root silently destroyed acked data (measured: 8 of 150 writes lost), which
is why the lock exists.

Concretely:
- **`uvicorn --workers N` with N > 1 will not work.** Run one worker.
- Two containers on one volume will not work.
- To scale reads, scale *namespaces* across processes, not processes across one
  namespace. `MEMD_ALLOW_MULTI_PROCESS=1` disables the lock and re-enables the
  data loss; it exists for recovery tooling, not for serving.

A multi-writer protocol (manifest CAS on ETag) is the real fix and is not
built.

## Object storage as the source of truth (S3 / R2 / MinIO)

```bash
pip install "memd[s3]"
```
```python
mem = Memory("s3://my-bucket/memd", config={
    "local_dir": "./.memd-local",          # index cache + envelope keys stay here
    # endpoint/credentials optional: the normal boto3 chain applies
    "s3_endpoint_url": "http://127.0.0.1:9000",
})
```

S3 has no append, and `append()` is the WAL's whole contract. Each append is
its own immutable object (`<key>.__part-000000000042`) and the logical object
is their ordered concatenation — so **one durable write ack = one PUT**, and a
torn frame cannot exist. The read-modify-write alternative would transfer
O(WAL) bytes per ack and was rejected on both latency and cost.

Measured against a real S3 server (MinIO on localhost — see the caveat below):

| | measured | SLO |
|---|---|---|
| durable write ack | p50 **8.8 ms**, p90 **10.1 ms** | p50 ≤ 150 ms, p90 ≤ 300 ms |
| warm retrieval | p50 **26.0 ms**, p99 **71.2 ms** | p50 ≤ 100 ms, p99 ≤ 400 ms |
| cold node (bucket only, no local cache) | open **170 ms** + first query **38 ms** | first query p90 ≤ 1.5 s |
| **object-store round trips per write** | **1 PUT** | O(1) |
| **object-store round trips per warm search** | **0** | O(1) |

The latencies are from a server on loopback and are a floor, not a forecast for
S3-across-a-WAN. The **round-trip counts are not** — they are structural, and
they are the number that decides both the cost model and how a WAN changes
things. A warm search touches object storage zero times because retrieval is
served by the local derived index.

What stays local, and why it matters: the SQLite derived index (rebuildable by
contract — pass 22's snapshot is what makes a cold node cheap) and the envelope
**keys**. Data is remote, keys are not, so this is *one node with remote
durability*, not *any node serves any namespace*. Crypto-shred still works — the
key is local, destroy it and the ciphertext is inert — but a second node cannot
decrypt. A KMS key provider is the missing piece and is not built.

Single-writer is still enforced, by a **lease** rather than a file lock (`flock`
cannot see another machine): the first writer claims `ns/<ns>/.owner` with a
conditional PUT, a second gets `NamespaceBusyError`, and a lease older than the
TTL is reclaimable so a crashed node cannot wedge a namespace forever. It makes
split-brain loud, not impossible.

## Hosted mode & billing

**Off by default.** Embedded memd and a plain `memd serve --http` behave exactly
as described above and never import `stripe`. Hosted mode adds tenancy, usage
metering, plan entitlements and Stripe billing for running memd as a service:

```bash
pip install "memd[billing]"                      # the Stripe SDK, imported lazily
export MEMD_HOSTED=1                             # or: memd serve --http --hosted
memd org create --name acme                      # -> {"org": "org_..."}; plan "free"
memd key create --hosted --org org_... --ns acme --scopes memory,billing
memd serve --http --hosted
```

**Tenancy: org -> namespaces -> API keys.** An org is the billing unit. It owns
namespaces (a namespace is claimed by the first org that mints a key into it
and can never be claimed by another), and every key belongs to one org and is
bound to one of its namespaces. Scopes are exact and combine only
explicitly (`--scopes memory,override`):

| scope | grants |
|---|---|
| `memory` | the data routes `/v1/ns/{ns}/...` and the namespace's operational views `/v1/status`, `/metrics`, `/v1/metrics/json` |
| `billing` | `/v1/billing/checkout`, `/portal`, `/usage` - nothing else |
| `override` | only the cross-user capability of a `memory` key (reads across users, `include_deleted`, namespace crypto-shred); no route by itself |
| operator key (`MEMD_ADMIN_KEY`) | every namespace, every metric series; no org, never metered |

Namespace names follow the engine's grammar, matched in full
(`[A-Za-z0-9][A-Za-z0-9_.-]{0,127}`, no trailing newline); `_`-prefixed
names are reserved.
Keys keep the `memd_<ns>_<kid>_<secret>` format; only a SHA-256 hash of the
secret is stored. The state lives in an admin SQLite database at
`<data root>/admin/admin.sqlite3` - beside the tenant store, never inside a
tenant namespace, so it is neither exported with nor crypto-shredded by a
tenant. `MEMD_ADMIN_KEY` stays the operator key: it spans namespaces, has no
org and is never metered. Self-hosted keys (`keys.toml.json`) are not honoured
in hosted mode; adopt them into an org with
`memd key migrate --hosted --org org_...` (the key strings keep working).

**Plans** (defaults from [06-economics.md](06-economics.md); config, not code -
override any value with a JSON file at `MEMD_PLANS_PATH`):

| meter | free (hard caps) | dev, $29/mo | scale (usage-based) |
|---|---|---|---|
| `memories_stored` (live gauge) | 50K | 500K (hard) | unlimited |
| `searches` / month | 10K | 250K (hard) | metered |
| `extractions_our_key` / month | 10K | 100K included, then metered ($8/100K) | metered |
| `reranked_searches` / month | 1K | 10K included, then metered | metered |
| `writes` / month | - | - | metered |
| `stored_gb` (daily gauge) | - | - | metered |

Enforcement happens before the operation: over a hard cap the request gets
`402 {"code": "quota_exceeded", "meter": ..., "limit": ..., "used": ...}`; on a
paid plan a soft limit never refuses - the part above the included quantity is
recorded as billable overage. Hard caps hold under concurrency: the check and
a *reservation* of the requested quantity happen in one `BEGIN IMMEDIATE`
transaction against the rollup plus every in-flight reservation; the usage
commit releases the reservation in the same transaction, a failed operation
releases it; a reservation never expires while its request is still running
(however long it takes), and one left by a crashed process stops counting
after 10 minutes. 50 concurrent requests at a cap of 20 admit exactly 20.
Nothing is ever recorded beyond what was reserved. Quota periods are calendar
months (UTC). **A spent `reranked_searches` quota never refuses a search**:
the search is served unreranked (`memd_rerank_fallback_total{reason="quota"}`)
and only `searches` is metered; searches are limited by the `searches` cap
alone. A session close is a write: at the memories cap it answers 402. While
its extractor runs it holds room for one memory only - writes alongside it
are not starved - and once the facts are extracted it reserves exactly
min(facts extracted, remaining headroom); anything beyond is not written
(`facts_capped` in the response). With extraction on our key under a
hard cap, the session's raw records are extracted up to the remaining
allowance and the rest stay raw-only - searchable, never extracted
(`raw_skipped`); with no allowance left the close answers 402. After
`invoice.payment_failed` the org has a 7-day grace period
(`MEMD_BILLING_GRACE_DAYS`); after it the org is **read-only**: writes and
session extraction answer `402 {"code": "payment_required"}`, searches, reads
and exports keep working. **Deletes (`DELETE /memories/{id}`, `forget`,
namespace crypto-shred) are always allowed**, whatever the plan or payment
state, and no data is ever dropped for non-payment.

**How metering works.** Each metered request writes a usage event (a UUID,
the org, the namespace, the meter, the quantity) to an append-only ledger in
the admin store, together with the period rollup that quota checks read, in
one transaction, committed with `synchronous=FULL` *after* the operation
succeeded and *before* the response is sent: every acknowledged operation is
billed, and an operation the client never saw acknowledged (a crash in
between) is not. Hourly (`MEMD_BILLING_PUSH_INTERVAL_S`), a background job
claims the unpushed rows into batches (per org, meter and hour), persists
each batch id - a UUIDv5 of its member event UUIDs - and sends it as a
[Stripe Billing Meter Event](https://docs.stripe.com/api/billing/meter-event/create)
with that id as both `identifier` and idempotency key; a crash anywhere
re-sends the same key, so Stripe records each batch once. Only the billable
part is pushed (overage on dev, everything metered on scale).
**Stripe only remembers an idempotency key for about 24 hours**, so usage
older than 20 hours (`MEMD_BILLING_MAX_PUSH_AGE_S`, capped at 20 h) is never
pushed automatically, and a batch that has been pending that long is
abandoned rather than retried: after an outage, a re-send could otherwise
bill a batch Stripe already has. Those events become `needs_reconcile`, and
every job tick settles them against Stripe's meter event summary for their
quota period (up to their last hour): what Stripe should hold (every settled
batch in that window plus these events) minus what it reports is the
verified missing quantity. Zero means the earlier attempts landed (nothing is
sent); up to the events' own units is pushed once, under a fresh idempotency
key recorded before the send. Anything else - Stripe holding more than the
ledger, or lacking more than these events - is not guessed at: drift alert,
nothing sent, the events stay `needs_reconcile` for a human. A window with a
batch still pending or sent within the last hour (`MEMD_BILLING_SETTLE_S`;
summaries lag) is retried on a later tick. The gauges
(`memories_stored`, `stored_gb`) are snapshotted once per UTC day; `stored_gb`
is sent in milli-GB so small tenants are not rounded up to a whole GB (price
that meter per 1/1000 GB and give it the `last` aggregation). A daily
drift report compares what the ledger settled for the quota period to date
with Stripe's meter event summaries and raises `memd_billing_drift_alerts_total{meter}` (and a
`drift_alert` row in the admin store's `billing_log`) when they differ by
more than `MEMD_BILLING_DRIFT_TOLERANCE` (1%). Billing metrics carry
`ns="_billing"`, so only operator keys see them on `/metrics`.

**Routes** (billing-scoped key; the org is always the key's own):

| route | does |
|---|---|
| `POST /v1/billing/checkout` `{"plan": "dev"\|"scale", "interval": "month"\|"year"}` | a Stripe Checkout Session (subscription: the flat price plus the plan's metered prices, open for 1 h); returns `{url, id}`. One subscription per org: `409 already_subscribed` while one is live, `409 checkout_pending` (with the open session's `url`) while a checkout is open |
| `POST /v1/billing/portal` | a Billing Portal session; returns `{url}` |
| `GET /v1/billing/usage` | current-period usage per meter: used, limit, hard, metered, billable; plan, status, grace, read-only |
| `POST /v1/billing/webhook` | Stripe's webhook endpoint (no bearer: the `Stripe-Signature` is the authentication) |

The webhook is verified with `stripe.Webhook.construct_event` (HMAC-SHA256 over
the raw body, timestamp tolerance 300 s) and is idempotent by Stripe event id:
the `processed_events` row commits in the same transaction as the event's
effect; bodies over 1 MiB (counted as read, chunked or not) are refused.
Handled:
- `checkout.session.completed`: only `mode=subscription` sessions with a
  subscription; the subscription is **re-read from Stripe** and *its* status
  applies (a payment-mode session never grants a plan nor clears past_due).
- `customer.subscription.created/updated/deleted`: `active`/`trialing` grant
  the plan the subscription's prices bill; `past_due` keeps it and starts the
  grace clock; `incomplete`, `incomplete_expired`, `unpaid`, `paused` and
  `canceled` grant nothing (free plan). A per-org cursor on the event's
  `created` ignores re-ordered older events, so a late
  `checkout.session.completed` cannot resurrect a deleted subscription.
- One subscription per org: an event for a subscription that is not the
  org's current one never changes the plan (canceling it does not downgrade
  the org); every *other live* subscription is listed as a duplicate
  (`memd_billing_duplicate_subscriptions_total`, a `duplicate_subscription`
  log row) and the org's metered usage is **held** - not pushed, since each
  would bill it - while ANY duplicate is listed. When the current
  subscription ends, the duplicates are re-read from Stripe, each on its
  own, and the first one still live is promoted to current (its invoices
  count from then on); dead ones - and ones Stripe no longer has (404) -
  leave the list; one that cannot be read right now stays listed (and held)
  until its own next event. None of this ever fails the webhook: the
  cancellation always takes effect. Each subscription also keeps its own
  event cursor and a tombstone once it ends, so a late or re-ordered event
  can neither re-list an ended subscription nor undo a newer one.
- `invoice.payment_failed` (starts the grace period once) and
  `invoice.payment_succeeded` count only for the org's current
  subscription; `checkout.session.expired` closes the open checkout.
- An org is bound to a Stripe customer only when it has none yet and the
  customer was created by memd's checkout for that org (its
  `metadata.memd_org_id`); anything else is logged and ignored.
- Events for customers memd does not know are acknowledged (200) and counted
  (`memd_billing_webhook_unmapped_total`); a Stripe read that fails answers
  500 so Stripe retries.

**Configuration** (environment):

| variable | meaning |
|---|---|
| `MEMD_HOSTED` | `1` enables hosted mode (same as `serve --hosted`) |
| `MEMD_STRIPE_SECRET_KEY` | Stripe secret key. `sk_live_`/`rk_live_` keys are **refused** unless `MEMD_ALLOW_LIVE_BILLING=1` (exactly `1`); a key or webhook secret containing whitespace is refused |
| `MEMD_STRIPE_WEBHOOK_SECRET` | the webhook endpoint's signing secret (`whsec_...`) |
| `MEMD_STRIPE_PRICE_DEV_MONTHLY`, `..._DEV_YEARLY` | flat subscription prices (`MEMD_STRIPE_PRICE_<PLAN>_MONTHLY/_YEARLY`) |
| `MEMD_STRIPE_PRICE_<PLAN>_<METER>` | metered prices, e.g. `MEMD_STRIPE_PRICE_DEV_EXTRACTIONS_OUR_KEY`, `MEMD_STRIPE_PRICE_SCALE_STORED_GB`; every metered meter of a plan needs one |
| `MEMD_STRIPE_METER_<METER>` | the Stripe meter's `event_name` (default `memd_<meter>`) |
| `MEMD_STRIPE_METER_ID_<METER>` | the meter id for reconciliation (default: looked up by event name) |
| `MEMD_PUBLIC_URL`, `MEMD_BILLING_SUCCESS_URL`, `MEMD_BILLING_CANCEL_URL`, `MEMD_BILLING_RETURN_URL` | Checkout/Portal redirect targets |
| `MEMD_PLANS_PATH` | JSON plan overrides |
| `MEMD_BILLING_GRACE_DAYS`, `MEMD_BILLING_PUSH_INTERVAL_S`, `MEMD_BILLING_DRIFT_TOLERANCE`, `MEMD_STRIPE_WEBHOOK_TOLERANCE_S` | 7, 3600, 0.01, 300 (must be > 0: 0 would disable the replay check) |
| `MEMD_STRIPE_API_BASE` | point the Stripe client elsewhere (stripe-mock in tests) |
| `MEMD_BILLING_MAX_PUSH_AGE_S`, `MEMD_BILLING_SETTLE_S`, `MEMD_STRIPE_MAX_NETWORK_RETRIES` | 72000 (the maximum), 3600, 2 |
| `MEMD_BILLING_JOBS` | `0` disables the in-process push/snapshot/reconcile loop |

Without `MEMD_STRIPE_SECRET_KEY`, hosted mode still enforces tenancy and
quotas (orgs keep the plan set with `memd org set-plan`) and the billing
routes answer `503 billing_not_configured`.

**Privacy.** The ledger records counts - org, namespace, meter, quantity,
time - never memory content, queries or user ids. What reaches Stripe is the
customer id, the meter's event name, a number and a timestamp; the org's
name goes into the Stripe customer record once, at first checkout.
`extractions_our_key` counts only when extraction runs on the operator's LLM
key; the local heuristic extractor is never billed.

## Doors (one engine)

| Door | Command | Surface |
|---|---|---|
| Python SDK | `from memd import Memory` | add/search/remember/forget/pack/observe/export |
| REST | `memd serve --http` (:8700) | `/v1/ns/{ns}/events`, `/memories`, `/search`, `/export`, ... |
| MCP | `memd serve --mcp` | exactly 4 tools: memory_search / memory_save / memory_forget / memory_status |
| TypeScript SDK | `npm install @memd/client` ([sdk-ts/](sdk-ts/README.md)) | the REST door, typed, for Node ≥ 18, Bun, Deno and edge runtimes (Workers, Vercel Edge) |

## What's inside

- **Storage** (ADR-2): per-namespace WAL → immutable segments → manifest on an
  object-store interface (local FS embedded; S3/R2 backend is the same
  interface). Durable write ack = fsync'd append; no LLM/embedding on the
  write path. Compaction folds tombstones and enforces hard-delete deadlines.
- **Revisability** (ADR-1): bitemporal records — new fact on an entity key
  supersedes the old cluster-locally. `search(as_of=...)` time-travels;
  `?history=true` walks supersedence chains.
- **Provenance & trust** (D7): every record carries source tier
  (user > agent > tool > web > import), lineage, actor. Untrusted content is
  fenced in packed context; explicit saves inherit session taint; quarantine +
  rate limits catch MINJA-style injection; hash-chained audit log; namespace
  crypto-shred; record-level hard delete with ≤72h physical purge deadline.
- **Retrieval**: rules-based planner (no reflection loop) → fan-out over
  BM25 (SQLite FTS5/porter, ranked by bm25; optionally accelerated by
  tantivy), the entity lane, the time lane on recency intent, and an exact
  flat vector scan with a real embedder (numpy; IVF-PQ slot reserved for
  ≥50K-vector namespaces) → RRF fusion with trust-aware tie-breaks →
  optional rerank of the lexical top-30 → validity filter (current/as_of) →
  lineage-deduped, budget-cut packing that keeps prefix-stable order
  (KV-cache friendly), or, as an experimental opt-in, gated evidence packing.
  The hash embedder's vector lane is not fused (`fuse_vector`, below).
- **Extraction** (ADR-6): async, batched, re-runnable. BYO OpenAI-compatible
  key for LLM extraction/embeddings; heuristic provider keeps facts working
  with zero keys; local ONNX embeddings via the optional fastembed extra.

## Keys & providers (add later, everything works now)

```bash
export MEMD_EMBEDDING_API_KEY=...      # optional: real embeddings
export MEMD_EXTRACTION_API_KEY=...     # optional: LLM fact extraction
```

Without them you get deterministic hash embeddings + pattern extraction:
fully functional, honestly degraded, clearly labeled in `stats()`.

### Retrieval options (`Memory(config={...})` or the env var)

| key / env | values | default |
|---|---|---|
| `reranker` / `MEMD_RERANKER` | `auto` \| `none` \| `jev` \| `local` | `auto`: Jev when a TypeSafe key is set (`TYPESAFE_API_KEY`, or config `typesafe_api_key`) and `typesafe-sdk` is installed (`pip install "memd[jev]"`), else none |
| `jev_model`, `rerank_timeout_s`, `rerank_k` | model pin, deadline, shortlist | `jev-latest`, 1.5s (5s local), 30 |
| `local_rerank_model` | fastembed cross-encoder | `BAAI/bge-reranker-base` |
| `pack_mode` / `MEMD_PACK_MODE` | `auto` \| `ranked` \| `gated` | `auto` = ranked with every reranker; `gated` is an experimental opt-in |
| `rerank_gate` | gated-packing threshold (opt-in mode) | 0.5 |
| `fuse_vector` / `MEMD_FUSE_VECTOR` | `auto` \| `true` \| `false` | `auto`: fuse unless the embedder is the hash embedder |
| `lexical_backend` / `MEMD_LEXICAL_BACKEND` | `auto` \| `fts5` \| `tantivy` | `auto`: tantivy when installed (`pip install "memd[fast]"`) |

- **Reranker.** The top-30 of the bm25 lane (plus the vector lane with a real
  embedder) is reordered by a relevance judge; the rest follows in fused
  order. On LongMemEval_S, Jev reranking took session ndcg@5 from 0.89 to
  0.95. A failed, slow (> `rerank_timeout_s`) or malformed judgement keeps
  the unreranked order and counts `memd_rerank_fallback_total{reason}`;
  search never fails because of it. `stats()["reranker"]` reports name,
  model, calls, fallbacks and p50 latency. **Privacy: with Jev active, the
  query and the top-30 candidate texts of every search are sent to
  TypeSafe's API** (see SECURITY.md). No key, no egress.
- **Gated packing** (`pack_mode="gated"`, experimental opt-in, meant for a
  calibrated reranker such as Jev): the packed context holds only the
  candidates judged relevant (p ≥ `rerank_gate`, else the top 3), each with
  its neighbouring turns, grouped by session under a session-date header,
  still capped by `budget_tokens` and with the same provenance fencing. It
  trades recall for tokens: in the lab it matched top-k QA accuracy at 27%
  fewer tokens over a 100-candidate shortlist, but over the product's top-30
  it drops second evidence sessions (LongMemEval_S session recall_all@5
  0.803 gated vs 0.928 ranked, with Jev). The default packs ranked.
- **tantivy accelerator.** A derived index next to SQLite, fed in the
  background (every 500ms or 512 changes); FTS5 stays the synchronous source
  of truth, so the write ack is unchanged, and writes not yet indexed are
  served from FTS5. It is rebuilt in the background when missing, corrupt or
  not closed cleanly. A search error sends that query to FTS5; only damage
  (I/O, missing or corrupt files) rebuilds it while running, with
  exponential backoff on a failure history that a successful rebuild does
  not erase (it decays after 10 minutes without a failure);
  `stats()["lexical"]` shows `rebuilds`, `failures`, `failures_total` and
  `retry_in_s`. Tied bm25 scores are ordered the same way on both backends
  (score, -t_event, content hash, id). With tantivy the top-k is
  deterministic for the same operation history *and commit schedule*: its
  BM25 statistics count deleted and superseded docs until their segments
  merge, so the same history committed in a different rhythm can order two
  near-equal docs differently.

## Ops

```bash
python bench/slo_bench.py                        # D2 acceptance numbers
python -m memd.harness.run --suite all --gate    # quality+cost gate (D5)
python bench/lme_gate.py                         # real-data gate: LongMemEval_S, 60 q (nightly)
python bench/lexical_bench.py                    # FTS5 vs tantivy, filtered, 10K-150K records
memd export --out backup.jsonl                   # anti-lock-in, symmetric
memd import mem0 --export mem0.json              # migration path
memd migrate --report ./memd-data                # store-format upgrade: preview / what it did (JSON)
memd key create --ns acme [--pin-user u1]        # scoped API keys
```

SLOs measured on this machine (see `bench/slo_bench.py`): durable write ack
p99 ≈ 7ms (target ≤10ms embedded); warm retrieval p50 ≈ 12ms / p99 ≈ 52ms
(targets ≤20/≤100ms); cold restart + first query ≈ 44ms.

Design pack: see `01..09-*.md` in this repo, ADRs in `adr/ADRs.md`. Harness
version hash is stamped into every result file — a result without a hash
doesn't exist.
