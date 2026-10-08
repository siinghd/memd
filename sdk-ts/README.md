# memd-engine

The official TypeScript client for the [memd](../README-engine.md) REST API.

- Zero runtime dependencies. It uses the platform's `fetch`.
- Runs on Node ≥ 18, Bun, Deno, Cloudflare Workers, Vercel Edge and browsers. The core uses only web-standard APIs.
- Ships ESM and CommonJS builds with full `.d.ts` types. The request and response models match the server's exactly, in its snake_case.
- Maps each HTTP status to a typed error, applies timeouts, and retries idempotent calls only.
- Uses the same method names and semantics as the Python SDK (`memd.sdk.HostedMemory`).

```bash
npm install memd-engine
```

You need a running server (`memd serve --http`) and a key for your namespace:

```bash
memd key create --namespace acme     # prints {"key": "memd_acme_…"}: store it now
```

The admin key (`MEMD_ADMIN_KEY`) can reach every namespace. Don't ship it to apps. Give each app a namespace key.

## Quickstart: Node

```ts
import { MemdClient } from "memd-engine";

const memd = new MemdClient({
  baseUrl: "http://localhost:8700",
  apiKey: process.env.MEMD_API_KEY!,
  namespace: "acme",
});

// two lines in any agent loop (OpenAI-style messages)
const withMemory = await memd.pack(messages, { user_id: "u1" }); // inject packed context
const reply = await callYourLLM(withMemory);
await memd.observe(messages, reply, { user_id: "u1", session_id: "s1" }); // capture the turn

// or explicitly
await memd.add("We deploy with `make ship`, never CI", { user_id: "u1", session_id: "s1" });
await memd.remember("The user prefers dark mode", { user_id: "u1", entity_keys: ["user.theme"] });
const hits = await memd.search("how do we deploy?", { user_id: "u1", budget_tokens: 1500 });
console.log(hits.packed_context); // dated session excerpts, ready to inject
```

## Quickstart: Next.js route handler

`app/api/chat/route.ts`. The client has no connection state, so a module-level instance is fine on the Node and Edge runtimes.

```ts
import { MemdClient, RateLimitError } from "memd-engine";
import OpenAI from "openai";

export const runtime = "edge"; // or "nodejs": the same code works on both

const memd = new MemdClient({
  baseUrl: process.env.MEMD_URL!,
  apiKey: process.env.MEMD_API_KEY!,
  namespace: "acme",
  timeoutMs: 5_000,
});
const openai = new OpenAI();

export async function POST(req: Request) {
  const { messages, userId, sessionId } = await req.json();

  const packed = await memd.pack(messages, { user_id: userId, signal: req.signal });
  const completion = await openai.chat.completions.create({ model: "gpt-4o-mini", messages: packed });
  const reply = completion.choices[0]!.message;

  try {
    await memd.observe(messages, reply, { user_id: userId, session_id: sessionId });
  } catch (err) {
    if (!(err instanceof RateLimitError)) throw err; // memory capture is best-effort here
  }
  return Response.json({ reply: reply.content });
}
```

## Quickstart: Cloudflare Workers

```ts
import { MemdClient } from "memd-engine";

interface Env {
  MEMD_URL: string;
  MEMD_API_KEY: string; // wrangler secret put MEMD_API_KEY
}

export default {
  async fetch(req: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const memd = new MemdClient({ baseUrl: env.MEMD_URL, apiKey: env.MEMD_API_KEY, namespace: "acme" });
    const { query, userId } = await req.json<{ query: string; userId: string }>();

    const res = await memd.search(query, { user_id: userId, budget_tokens: 1000 });
    // write-behind: don't make the user wait on capture
    ctx.waitUntil(memd.add(query, { user_id: userId, role: "user" }));
    return Response.json({ context: res.packed_context, hits: res.items.length });
  },
};
```

## Examples

Runnable, type-checked examples in [`examples/ts`](../examples/README.md#typescript), each against a throwaway local server:

| example | shows |
|---|---|
| [`01_quickstart.ts`](../examples/ts/01_quickstart.ts) | `remember`, `search`, `get`, `forget` (preview, then confirm), hard `delete` |
| [`02_sessions_and_facts.ts`](../examples/ts/02_sessions_and_facts.ts) | `observe`, `closeSession` fact extraction, supersedence, `history`, `as_of` |
| [`03_search_options.ts`](../examples/ts/03_search_options.ts) | scope filters, namespaces, `kinds`, budgets, time, the server's reranker |
| [`04_http_api.ts`](../examples/ts/04_http_api.ts) | a `node:http` API with memory per user (a namespace each), validation, error mapping |
| [`05_nextjs_route.ts`](../examples/ts/05_nextjs_route.ts) | a Next.js route handler ([`route.ts`](../examples/ts/nextjs/app/api/memory/route.ts)) on standard `Request` / `Response` |
| [`06_agent_loop.ts`](../examples/ts/06_agent_loop.ts) | a tool-calling agent loop with memd as its memory (a stub model, runs offline) |
| [`07_eventual_reads.ts`](../examples/ts/07_eventual_reads.ts) | `consistency: "eventual"`, `maxStalenessMs`, `lastRead` and the staleness headers |
| [`08_robust_client.ts`](../examples/ts/08_robust_client.ts) | retries and backoff, timeouts and deadlines, writes safe to retry, typed errors |
| [`09_export_stream.ts`](../examples/ts/09_export_stream.ts) | `exportStream` to a JSONL file, `lastExportSkippedFrames` |

```bash
(cd sdk-ts && npm ci && npm run build)   # the examples install the SDK from ../../sdk-ts
cd examples/ts && npm install
python ../local_server.py -- npx tsx 01_quickstart.ts
```

## API

Every method accepts per-call options: `namespace` (overrides the client's), `signal`, `timeoutMs` and `retries`.

| Method | Route | Returns |
|---|---|---|
| `add(content, opts?)` | `POST /v1/ns/{ns}/events` | `string[]` (ids) |
| `addEvents(events, opts?)` | `POST /v1/ns/{ns}/events` (1 to 1000) | `string[]` |
| `remember(content, opts?)` | `POST /v1/ns/{ns}/memories` | `string` (id) |
| `search(query, opts?)` | `POST /v1/ns/{ns}/search` | `SearchResult` |
| `pack(messages, opts?)` | search on the last user message | messages plus one system message |
| `observe(messages, response, opts?)` | `POST /v1/ns/{ns}/events` | `string[]` |
| `get(id, { history?, include_deleted? })` | `GET /v1/ns/{ns}/memories/{id}` | `MemoryRecord \| null` |
| `delete(id, { hard? })` | `DELETE /v1/ns/{ns}/memories/{id}` | `boolean` (`false` on 404) |
| `findIds(query, opts?)` | `POST /v1/ns/{ns}/find_ids` | `string[]` |
| `forget(query, opts?)` | `POST /v1/ns/{ns}/forget` | `ForgetPreview`, or `string[]` with `confirm` |
| `export(opts?)` | `POST /v1/ns/{ns}/export` | `MemoryRecord[]` |
| `exportJsonl(opts?)` | same | raw NDJSON `string` |
| `exportStream(opts?)` | same, streamed | `AsyncGenerator<MemoryRecord>` |
| `stats(opts?)` | `GET /v1/ns/{ns}/stats` | `NamespaceStats` |
| `closeSession(sid, opts?)` | `POST /v1/ns/{ns}/sessions/{sid}/close` | `CloseSessionResult` |
| `compact({ force? })` | `POST /v1/ns/{ns}/compact` | `CompactionReport` |
| `reembed(opts?)` | `POST /v1/ns/{ns}/reembed` | `ReembedResult` |
| `destroyNamespace(ns?)` | `DELETE /v1/ns/{ns}` (admin key) | `boolean` (`false` if it does not exist) |
| `status(opts?)` | `GET /v1/status` | `ServerStatus` |
| `health(opts?)` | `GET /health` | `HealthResult` |

Request fields keep the server's names (`user_id`, `session_id`, `budget_tokens`, `entity_keys`, …). `null` and `undefined` both mean "use the server default".

`search` and `pack` pack their results into `budget_tokens` (server default 12,000) as `packed_context`. The layout is the server's `packing` setting. A call can give `packing`:

- `"sessions"` (the default): dated excerpts of past conversations. Each hit comes with the turns around it. A fact shows under the turn that it came from.
- `"flat"`: one provenance-tagged `<memory>` element for each hit, in rank order.

For the old footprint, give `{ budget_tokens: 2000, packing: "flat" }`. With session packing, `items` also lists the turns around each hit (`lanes` `["neighbour"]` or `["source"]`). Each item carries the text as `packed_context` shows it: a long turn's item carries an excerpt. To get the full record, use `get(id)`.

Reads are strong by default (served by the namespace's writer). `consistency: "eventual"` (with an optional `maxStalenessMs`), in the client options or per `search` / `pack` / `get` call, lets a read replica serve them; `client.lastRead` then says who served the last one: `{ servedBy: "leader" | "replica", appliedSeq, ageMs }` (the replica's applied seq and age, `null` for the leader).

An export is complete unless the server had to leave out a damaged WAL frame: it then exports everything it can read and says how many frames it left out in the `X-Memd-Export-Skipped-Frames` response header. After `export`, `exportJsonl` or `exportStream` (before its first record), `client.lastExportSkippedFrames` holds that count; `0` means complete.

### pack / observe

These take the same messages as the Python SDK: OpenAI-style `{ role, content }`. `content` may be a string or an array of parts, and text parts are used.

- `pack` searches with the last `user` message. When anything matches, it inserts one `{ role: "system", content: packed_context }` after your leading system messages. Otherwise it returns the messages unchanged. The input array is never mutated. A user message longer than the server's 10,000-character query cap is trimmed for the search.
- `observe` stores every message that has text, plus `response` (a string or a message object) as `assistant`, in one durable batch.

### Deleting

- `delete(id)` soft-deletes: the record disappears from every read at once, `history` included, and is physically purged at the next compaction (≤ 72 h). `delete(id, { hard: true })` purges it, and it also works on a record that is already soft-deleted.
- Only an admin key can read a deleted record, with `get(id, { include_deleted: true })`. A namespace key gets `PermissionDeniedError`.
- `forget` works in two phases. The preview says what would be deleted. Pass that preview back as `confirm`, with the same query and filters, to delete exactly that set:

```ts
const preview = await memd.forget("wifi password", { user_id: "u1", kinds: ["fact"] });
console.log(preview.count, preview.will_delete);
const deleted = await memd.forget("wifi password", { user_id: "u1", kinds: ["fact"], confirm: preview });
```

  The preview's `fingerprint` travels with the confirm. If the matches changed in between, the server deletes nothing and the client throws `ForgetPreviewMismatchError` (409, `preview_mismatch`): preview again. `confirm: true` deletes without this check.

## Errors

Every failure is a `MemdError` with `status`, `code` and `message`. The message is the server's `detail`, and `code` is the server's machine-readable `code`. The class follows the status:

| Status | Class | Typical `code` |
|---|---|---|
| 400 | `BadRequestError` | `validation_error` |
| 401 | `AuthenticationError` | `unauthorized` |
| 403 | `PermissionDeniedError` | `forbidden` |
| 404 | `NotFoundError` | `not_found` (`get`, `delete` and `destroyNamespace` return `null`/`false` instead) |
| 409 | `ConflictError`; `ForgetPreviewMismatchError` for a stale forget | `conflict`, `preview_mismatch` |
| 410 | `GoneError` | `namespace_destroyed` |
| 413 | `PayloadTooLargeError` | `payload_too_large` |
| 422 | `ValidationError` (`.issues`: pydantic's list) | `validation_error` |
| 429 | `RateLimitError` (`.retryAfter`, in seconds, from `Retry-After`) | `rate_limited` |
| 5xx | `ServerError` | `internal_error`, `unavailable` |
| none | `NetworkError`, `RequestTimeoutError` | `network_error`, `timeout` |
| none | `RequestAbortedError` (your signal fired) | `aborted` |

A body without a `code`, such as one from an older server or a proxy, gets the server's default code for its status.

```ts
import { PermissionDeniedError, RateLimitError, ValidationError } from "memd-engine";

try {
  await memd.search(q, { namespace: "someone-else" });
} catch (err) {
  if (err instanceof PermissionDeniedError) {/* key is scoped to another namespace */}
  else if (err instanceof ValidationError) console.log(err.issues);
  else if (err instanceof RateLimitError) console.log(err.retryAfter);
  else throw err;
}
```

## Timeouts, retries, cancellation

```ts
new MemdClient({
  apiKey,
  timeoutMs: 30_000,      // per attempt; 0 disables it
  retries: 2,             // extra attempts for idempotent calls
  retryBaseDelayMs: 250,  // exponential backoff: 250, 500, 1000 … with jitter
  retryMaxDelayMs: 8_000,
  fetch: myFetch,         // inject for tests, proxies or instrumentation
  headers: { "x-gateway-token": "…" },
});
```

- Retries happen on 429, 500, 502, 503 and 504, on timeouts, and on network errors. memd sends `Retry-After` with its 429s, and the client waits that long, up to 60 s. Otherwise it uses its backoff.
- Only idempotent calls retry: `get`, `search`, `pack`, `findIds`, `forget` without `confirm`, the exports, `stats`, `status`, `health` and record `delete`. A retried `delete` whose first attempt landed returns `false`.
- Writes never retry: `add`, `addEvents`, `remember`, `observe`, confirmed `forget`, `closeSession`, `compact`, `reembed` and `destroyNamespace`. The API has no idempotency key, so a retried write could store twice. Handle `RateLimitError` on writes yourself. memd rejects a request with 429 before running it, so retrying after a 429 is safe.
- Pass `signal` to cancel. The client throws `RequestAbortedError` and never retries after an abort.

## Development

```bash
npm ci
npm run typecheck      # the core compiles without Node types: no Node-only APIs
npm test               # unit tests (mocked fetch)
npm run build          # dist/: ESM + CJS + .d.ts
npm run smoke          # load the built package via require() and import
MEMD_PYTHON=python3 npm run test:integration   # starts a real server from ../src
```
