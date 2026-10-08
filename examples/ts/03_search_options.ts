// Search options with @memd/client: scope filters, namespaces, kinds, budgets and limits, time (as_of and t_event), the server's reranker.
// Run (in examples/ts): python ../local_server.py -- npx tsx 03_search_options.ts   (or set MEMD_URL / MEMD_API_KEY / MEMD_NAMESPACE yourself)
import assert from "node:assert/strict";
import { setTimeout as sleep } from "node:timers/promises";
import { KINDS, MemdClient, PermissionDeniedError, type Kind, type SearchResult } from "@memd/client";

const { MEMD_URL, MEMD_API_KEY, MEMD_NAMESPACE = "default" } = process.env;
if (!MEMD_URL || !MEMD_API_KEY) {
  console.error("set MEMD_URL and MEMD_API_KEY, or run under examples/local_server.py");
  process.exit(2);
}

const memd = new MemdClient({ baseUrl: MEMD_URL, apiKey: MEMD_API_KEY, namespace: MEMD_NAMESPACE });
const contents = (res: SearchResult): string[] => res.items.map((h) => h.content);

// --- seed --------------------------------------------------------------------
// addEvents: up to 1000 raw-lane events in one durable append.
await memd.addEvents([
  { content: "We deploy with `make ship` from the main branch", user_id: "u1", session_id: "s1" },
  { content: "u2 deploys from a fork, never from main", user_id: "u2" },
  { content: "Deploys freeze on Fridays", agent_id: "planner" },
  { content: "Deploy questions go to the #ops channel", agent_id: "support" },
  // t_event: when it happened (epoch ms), when that is not the write time
  { content: "Deployed v1.0 to production", user_id: "u1", t_event: Date.UTC(2025, 0, 12) },
  { content: "Deployed v2.0 to production", user_id: "u1", t_event: Date.UTC(2025, 5, 3) },
]);
await memd.remember("To deploy: tag the release, then run make ship", { kind: "procedure" });
await memd.remember("Always confirm before a production deploy", { kind: "pin" });
await memd.remember("The user deploys to us-east-1", { user_id: "u1", entity_keys: ["user.region"] });
const beforeMove = Date.now();
await sleep(20);
await memd.remember("The user deploys to eu-west-1", { user_id: "u1", entity_keys: ["user.region"], valid_from: Date.now() });

// --- 1. scope filters ---------------------------------------------------------
// A query names the scope it speaks for (user_id, session_id, agent_id, org_id) and
// sees records whose scope matches what both sides set: a user query sees that
// user's records plus records that name no user, never another user's.
const forU1 = await memd.search("deploy", { user_id: "u1" });
console.log("user u1:", contents(forU1));
assert.ok(!contents(forU1).some((c) => c.startsWith("u2")), "never another user's records");

// Narrower: the planner agent working for u1 does not see the support agent's notes.
const planner = await memd.search("deploy", { user_id: "u1", agent_id: "planner" });
assert.ok(contents(planner).includes("Deploys freeze on Fridays"));
assert.ok(!contents(planner).some((c) => c.includes("#ops")));

// Every hit says how it was found and where it lives.
const top = forU1.items[0];
assert.ok(top);
console.log(`top: ${top.content} | ${top.kind}, ${top.source}, lanes ${top.lanes.join("+")}, score ${top.score}, ns ${top.namespace}`);
console.log(`query class: ${forU1.query_class}, ${forU1.latency_ms} ms`);

// --- 2. namespaces --------------------------------------------------------------
// The client's namespace is the default; any call can name another. A namespace key
// reaches only its own namespace (04_http_api.ts runs one namespace per user with
// the operator key, which spans them).
try {
  await memd.search("deploy", { namespace: "another-team" });
  console.log("another namespace: allowed (this key spans namespaces)");
} catch (err) {
  if (!(err instanceof PermissionDeniedError)) throw err;
  console.log(`another namespace: ${err.status} ${err.code} (${err.message})`);
}

// --- 3. kinds -----------------------------------------------------------------
// The kind vocabulary is closed (KINDS); an unknown kind is a 422.
console.log("kinds:", KINDS.join(", "));
const wanted: Kind[] = ["procedure", "pin"];
const howTo = await memd.search("how do we deploy to production?", { kinds: wanted });
console.log("procedures and pins:", howTo.items.map((h) => `${h.kind}: ${h.content}`));
assert.ok(howTo.items.length > 0 && howTo.items.every((h) => wanted.includes(h.kind)));

// --- 4. budgets and limits ----------------------------------------------------
// There is no top-k: results are packed into budget_tokens (64 to 128,000; default
// 2,000) and `truncated` says whether anything was left out.
const tight = await memd.search("deploy", { user_id: "u1", budget_tokens: 64 });
console.log(`budget 64: ${tight.items.length} item(s), ${tight.tokens_used}/${tight.budget} tokens, truncated=${tight.truncated}`);
assert.ok(tight.truncated && tight.items.length < forU1.items.length);
// A fixed count is a slice of the ranked items.
const top3 = forU1.items.slice(0, 3);
assert.equal(top3.length, 3);
// findIds: every match, unbounded by any budget (the view a forget sweep uses).
const allIds = await memd.findIds("deploy", { user_id: "u1" });
console.log(`findIds: ${allIds.length} ids`);
assert.ok(allIds.length >= forU1.items.length);

// --- 5. time ------------------------------------------------------------------
// as_of (epoch ms): which facts were valid then. Without it a search sees current facts.
const regionThen = await memd.search("which region does the user deploy to?", { user_id: "u1", kinds: ["fact"], as_of: beforeMove });
const regionNow = await memd.search("which region does the user deploy to?", { user_id: "u1", kinds: ["fact"] });
console.log("region then:", regionThen.items[0]?.content, "| now:", regionNow.items[0]?.content);
assert.match(regionThen.items[0]?.content ?? "", /us-east-1/);
assert.match(regionNow.items[0]?.content ?? "", /eu-west-1/);
// A time window over when things happened: search has no range filter, but every
// hit carries t_event, so filter the ranked hits.
const [from, until] = [Date.UTC(2025, 3, 1), Date.UTC(2025, 6, 1)]; // Q2 2025
const deploys = await memd.search("deployed to production", { user_id: "u1", kinds: ["raw_event"] });
const inQ2 = deploys.items.filter((h) => h.t_event >= from && h.t_event < until);
console.log("deployed in Q2 2025:", inQ2.map((h) => `${new Date(h.t_event).toISOString().slice(0, 10)} ${h.content}`));
assert.deepEqual(inQ2.map((h) => h.content), ["Deployed v2.0 to production"]);

// --- 6. reranking and quarantine ----------------------------------------------
// The reranker is the server's choice (MEMD_RERANKER: Jev or a local cross-encoder;
// none by default); the SDK has no per-call switch. stats() says which one runs.
const stats = await memd.stats();
console.log("server reranker:", stats.reranker.name);
// include_quarantined: also return records held back as a burst or near-duplicate.
const everything = await memd.search("deploy", { user_id: "u1", include_quarantined: true });
assert.ok(everything.items.length >= forU1.items.length);
console.log("ok");
