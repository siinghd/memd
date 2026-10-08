// Loads the BUILT package through its exports map (package self-reference),
// as an ESM consumer would. Plain node, no test runner: CI runs it on every
// supported Node version, including 18.
import assert from "node:assert/strict";
import { KINDS, MemdClient, MemdError, RateLimitError } from "memd-engine";

const replies = [
  new Response(JSON.stringify({ detail: "rate limit exceeded" }), { status: 429, headers: { "retry-after": "0" } }),
  new Response(JSON.stringify({ ok: true, version: "smoke" }), { status: 200 }),
];
const client = new MemdClient({ apiKey: "k", baseUrl: "http://smoke.test", fetch: async () => replies.shift() });
assert.deepEqual(await client.health(), { ok: true, version: "smoke" });

const failing = new MemdClient({
  apiKey: "k",
  retries: 0,
  fetch: async () => new Response(JSON.stringify({ detail: "slow down" }), { status: 429, headers: { "retry-after": "5" } }),
});
const err = await failing.stats().catch((e) => e);
assert.ok(err instanceof RateLimitError && err instanceof MemdError);
assert.equal(err.retryAfter, 5);
assert.equal(KINDS.length, 6);
console.log(`esm ok (node ${process.version})`);
