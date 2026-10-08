// Quickstart for memd-engine: remember, search, get, forget (preview, then confirm) and a hard delete.
// Run (in examples/ts): python ../local_server.py -- npx tsx 01_quickstart.ts   (or set MEMD_URL / MEMD_API_KEY / MEMD_NAMESPACE yourself)
import assert from "node:assert/strict";
import { MemdClient } from "memd-engine";

const { MEMD_URL, MEMD_API_KEY, MEMD_NAMESPACE = "default" } = process.env;
if (!MEMD_URL || !MEMD_API_KEY) {
  console.error("set MEMD_URL and MEMD_API_KEY, or run under examples/local_server.py");
  process.exit(2);
}

// One client per process is enough: it holds no connection state.
const memd = new MemdClient({ baseUrl: MEMD_URL, apiKey: MEMD_API_KEY, namespace: MEMD_NAMESPACE });
console.log("server:", await memd.health());

// The raw lane: what was said, verbatim. Searchable as soon as the call returns.
await memd.add("We deploy with `make ship`, never from CI", { user_id: "u1", session_id: "s1" });
await memd.add("The staging database is postgres 16", { user_id: "u1", session_id: "s1" });
// The explicit lane: "remember this", a fact on an entity key.
const themeId = await memd.remember("The user prefers dark mode", { user_id: "u1", entity_keys: ["user.theme"] });

// search: hybrid retrieval (bm25, vectors, entities), packed into a token budget.
const hits = await memd.search("how do we deploy?", { user_id: "u1", budget_tokens: 500 });
const top = hits.items[0];
assert.ok(top, "search found something");
assert.match(top.content, /make ship/, "the deploy note ranks first");
console.log(`top hit: ${top.content} (score ${top.score}, lanes ${top.lanes.join("+")})`);
console.log(hits.packed_context); // provenance-tagged, ready to put in a prompt

// get: one record by id, with its scope, provenance and time axes; null when there is none.
const theme = await memd.get(themeId);
assert.ok(theme, "the fact is stored");
console.log("get:", theme.content, theme.kind, theme.scope, theme.entity_keys);

// forget by query, in two phases. The preview says what WOULD be deleted ...
const preview = await memd.forget("staging database", { user_id: "u1" });
console.log("forget preview:", preview.count, "record(s):", preview.will_delete.map((r) => r.content));
// ... and passing it back as `confirm` (same query, same filters) deletes exactly that set.
// If the matches changed in between, the server deletes nothing and this throws
// ForgetPreviewMismatchError: preview again.
const forgotten = await memd.forget("staging database", { user_id: "u1", confirm: preview });
assert.equal(forgotten.length, preview.count);
for (const id of forgotten) assert.equal(await memd.get(id), null, "a forgotten record is gone");

// delete by id. Soft by default: gone from every read at once, physically purged at the
// next compaction. hard: true purges it as well.
const secretId = (await memd.add("my door code is 4417-ZEBRA", { user_id: "u1" }))[0];
assert.ok(secretId);
assert.equal(await memd.delete(secretId, { hard: true }), true);
assert.equal(await memd.get(secretId), null);
assert.equal((await memd.search("door code", { user_id: "u1" })).items.length, 0);
// delete() returns false when there is nothing to delete (also after a retried delete that landed)
assert.equal(await memd.delete(secretId), false);
console.log("hard-deleted:", secretId);

const stats = await memd.stats();
console.log(`namespace ${stats.namespace}: ${stats.records} records, ${stats.facts} facts`);
console.log("ok");
