// Robust @memd/client usage: retries and backoff on 503/429, per-attempt timeouts and overall deadlines, writes made safe to retry, typed error handling.
// Run (in examples/ts): python ../local_server.py -- npx tsx 08_robust_client.ts   (or set MEMD_URL / MEMD_API_KEY / MEMD_NAMESPACE yourself)
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import {
  AuthenticationError,
  ConflictError,
  ForgetPreviewMismatchError,
  MemdClient,
  MemdError,
  NetworkError,
  PermissionDeniedError,
  RateLimitError,
  RequestAbortedError,
  RequestTimeoutError,
  ServerError,
  ValidationError,
  type AddOptions,
  type FetchLike,
} from "@memd/client";

const { MEMD_URL, MEMD_API_KEY, MEMD_NAMESPACE = "default" } = process.env;
if (!MEMD_URL || !MEMD_API_KEY) {
  console.error("set MEMD_URL and MEMD_API_KEY, or run under examples/local_server.py");
  process.exit(2);
}

// --- SIMULATED FAULTS, for this demo only --------------------------------------
// A real server rarely fails on cue, so this fetch (the client's `fetch` option)
// injects faults on request: a 503, a hung server, or a response lost after the
// server did the work. Everything else goes to the real server. It also logs
// every HTTP attempt, so the retries are visible.
type Fault = "unavailable" | "hang" | "lost-response";
const faults: Array<{ route: RegExp; fault: Fault }> = [];
const attempts: string[] = [];
const faultyFetch: FetchLike = async (url, init) => {
  const route = `${init.method} ${new URL(url).pathname.replace(/^\/v1\/ns\/[^/]+/, "")}`;
  attempts.push(route);
  const i = faults.findIndex((f) => f.route.test(route));
  const fault = i >= 0 ? faults.splice(i, 1)[0]?.fault : undefined;
  if (fault === "unavailable") {
    return Response.json({ detail: "namespace re-opening; retry", code: "unavailable" }, { status: 503 });
  }
  if (fault === "hang") {
    await sleep(60_000, undefined, { signal: init.signal ?? undefined }); // until the client gives up
  }
  const res = await fetch(url, init);
  if (fault === "lost-response") {
    await res.body?.cancel();
    throw new TypeError("fetch failed: connection reset"); // the server did the work; we never hear back
  }
  return res;
};
// --- end of the simulation ---------------------------------------------------------

const memd = new MemdClient({
  baseUrl: MEMD_URL,
  apiKey: MEMD_API_KEY,
  namespace: MEMD_NAMESPACE,
  timeoutMs: 5_000, // per attempt; 0 disables it
  retries: 3, // extra attempts, for idempotent calls only
  retryBaseDelayMs: 100, // backoff 100, 200, 400 ms ... with jitter, or the server's Retry-After
  retryMaxDelayMs: 2_000,
  fetch: faultyFetch,
});

function section(title: string): void {
  attempts.length = 0;
  faults.length = 0;
  console.log(`\n--- ${title}`);
}

await memd.add("We deploy with `make ship`, never from CI", { user_id: "u1" });

// --- 1. reads retry by themselves -----------------------------------------------
// Idempotent calls (search, pack, get, findIds, forget previews, exports, stats,
// delete) retry on 429, 500, 502, 503, 504, timeouts and network errors.
section("a read through two 503s");
faults.push({ route: /search/, fault: "unavailable" }, { route: /search/, fault: "unavailable" });
const hits = await memd.search("how do we deploy?", { user_id: "u1" });
console.log("attempts:", attempts);
assert.equal(attempts.length, 3);
assert.match(hits.items[0]?.content ?? "", /make ship/);

// --- 2. timeouts and deadlines --------------------------------------------------
section("a hung attempt, timed out and retried");
faults.push({ route: /search/, fault: "hang" });
const started = Date.now();
await memd.search("how do we deploy?", { user_id: "u1", timeoutMs: 300 }); // per attempt, this call only
console.log(`attempts: ${attempts.length}, ${Date.now() - started} ms`);
assert.equal(attempts.length, 2);

section("an overall deadline across the retries");
// timeoutMs bounds each attempt; a signal bounds the whole call, retries included.
// An aborted call is never retried.
faults.push(...Array.from({ length: 10 }, () => ({ route: /search/, fault: "hang" as const })));
try {
  await memd.search("how do we deploy?", { user_id: "u1", timeoutMs: 300, signal: AbortSignal.timeout(1_000) });
  assert.fail("the deadline passes first");
} catch (err) {
  assert.ok(err instanceof RequestAbortedError, String(err));
  console.log(`${err.name} after ${attempts.length} attempts`);
}

// --- 3. writes are never retried: make them safe to retry --------------------------
// The API has no idempotency key, so the client never retries add, remember,
// observe, closeSession or a confirmed forget: a write whose response was lost may
// have landed, and sending it again would store it twice.
section("a naive retry after a lost response");
faults.push({ route: /events$/, fault: "lost-response" });
const naive = "The release train leaves on Thursdays";
try {
  await memd.add(naive, { user_id: "u1" });
} catch (err) {
  assert.ok(err instanceof NetworkError);
  await memd.add(naive, { user_id: "u1" }); // "it failed, try again"
}
const copies = async (text: string) =>
  (await memd.export()).filter((r) => r.content === text && r.scope.user === "u1").length;
console.log("copies stored:", await copies(naive));
assert.equal(await copies(naive), 2);

/**
 * add() that is safe to retry. The event carries a key of our own in `meta`;
 * before writing again after an unknown outcome, look for that key.
 */
async function addOnce(content: string, options: AddOptions = {}, idempotencyKey: string = randomUUID()): Promise<string> {
  const meta = { ...options.meta, idempotency_key: idempotencyKey };
  for (let attempt = 1; ; attempt++) {
    try {
      const [id] = await memd.add(content, { ...options, meta });
      assert.ok(id);
      return id;
    } catch (err) {
      if (attempt >= 4) throw err;
      if (err instanceof RateLimitError) {
        // a 429 is refused before the request runs: nothing was stored
        await sleep((err.retryAfter ?? 1) * 1000);
      } else if (err instanceof NetworkError || err instanceof ServerError) {
        // no answer, or a 5xx: the write may have landed. Look before writing again.
        const landed = await findByKey(content, options, idempotencyKey);
        if (landed) return landed;
        await sleep(100 * 2 ** attempt);
      } else {
        throw err; // any other 4xx: sending it again cannot help
      }
    }
  }
}

async function findByKey(content: string, options: AddOptions, key: string): Promise<string | undefined> {
  // findIds sees every match, unbounded by any budget; get() returns the meta
  const ids = await memd.findIds(content, { user_id: options.user_id, session_id: options.session_id, kinds: ["raw_event"] });
  for (const id of ids) {
    const record = await memd.get(id, { consistency: "strong" });
    if (record?.meta["idempotency_key"] === key) return id;
  }
  return undefined;
}

section("addOnce after a lost response");
faults.push({ route: /events$/, fault: "lost-response" });
const once = "Hotfixes go out from the release branch";
const id = await addOnce(once, { user_id: "u1" });
console.log("attempts:", attempts);
console.log("copies stored:", await copies(once));
assert.equal(await copies(once), 1);
assert.equal((await memd.get(id))?.content, once);

// --- 4. typed errors ------------------------------------------------------------
// Every failure is a MemdError (status, code, message); the class says what to do.
function advice(err: unknown): string {
  if (err instanceof ValidationError) return `fix the request: ${err.issues.map((i) => i.loc.join(".")).join(", ")}`;
  if (err instanceof AuthenticationError) return "the key is wrong or revoked: do not retry";
  if (err instanceof PermissionDeniedError) return "this key may not do that (another namespace, user or an admin call)";
  if (err instanceof ForgetPreviewMismatchError) return "the matches changed since the preview: nothing deleted, preview again";
  if (err instanceof ConflictError) return "conflicting state";
  if (err instanceof RateLimitError) return `back off ${err.retryAfter ?? "a few"} s`;
  if (err instanceof RequestTimeoutError) return "timed out: retry, or raise timeoutMs";
  if (err instanceof RequestAbortedError) return "cancelled by the caller";
  if (err instanceof NetworkError) return "no response: retry reads, check writes before retrying";
  if (err instanceof ServerError) return "server-side failure: retry later";
  if (err instanceof MemdError) return `unexpected ${err.status} ${err.code}`;
  return "not a memd error";
}
async function attempt(what: string, call: () => Promise<unknown>): Promise<MemdError> {
  try {
    await call();
  } catch (err) {
    assert.ok(err instanceof MemdError, String(err));
    console.log(`${what}: ${err.name} ${err.status} ${err.code} -> ${advice(err)}`);
    return err;
  }
  throw new Error(`${what}: expected an error`);
}

section("typed errors");
assert.ok((await attempt("budget below 64", () => memd.search("deploy", { budget_tokens: 10 }))) instanceof ValidationError);
const wrongKey = new MemdClient({ baseUrl: MEMD_URL, apiKey: "memd_nope_0000_0000", namespace: MEMD_NAMESPACE });
assert.ok((await attempt("wrong key", () => wrongKey.search("deploy"))) instanceof AuthenticationError);
assert.ok((await attempt("other namespace", () => memd.search("deploy", { namespace: "not-mine" }))) instanceof PermissionDeniedError);
assert.equal(await memd.get("01NOSUCHRECORD000000000000"), null); // get and delete answer a 404 with null / false
// a confirm whose preview went stale deletes nothing
const preview = await memd.forget("release branch", { user_id: "u1" });
await memd.add("Release branch builds are signed", { user_id: "u1" });
const stale = await attempt("stale forget", () => memd.forget("release branch", { user_id: "u1", confirm: preview }));
assert.ok(stale instanceof ForgetPreviewMismatchError);
assert.equal(await copies(once), 1, "nothing was deleted");

// --- 5. a real 429 ------------------------------------------------------------------
// findIds is an O(namespace) sweep, budgeted at 10 calls a minute per key. memd
// answers a 429 with Retry-After, and the client waits that long (up to 60 s)
// before retrying an idempotent call.
section("the server's own rate limit");
let limited: RateLimitError | undefined;
for (let i = 0; i < 20 && !limited; i++) {
  try {
    await memd.findIds("deploy", { user_id: "u1", retries: 0 }); // no retries: see the 429 itself
  } catch (err) {
    if (!(err instanceof RateLimitError)) throw err;
    limited = err;
  }
}
assert.ok(limited, "the budget runs out");
console.log(`429 on call ${attempts.length}: retry after ${limited.retryAfter} s`);
const waited = Date.now();
await memd.findIds("deploy", { user_id: "u1", retries: 1 }); // waits out Retry-After, then succeeds
console.log(`with a retry: succeeded after ${((Date.now() - waited) / 1000).toFixed(1)} s`);
console.log("ok");
