// Sessions and facts with @memd/client: capture a conversation, close the session to extract facts, supersede a fact, read its history, time-travel.
// Run (in examples/ts): python ../local_server.py -- npx tsx 02_sessions_and_facts.ts   (or set MEMD_URL / MEMD_API_KEY / MEMD_NAMESPACE yourself)
import assert from "node:assert/strict";
import { setTimeout as sleep } from "node:timers/promises";
import { MemdClient, type ChatMessage } from "@memd/client";

const { MEMD_URL, MEMD_API_KEY, MEMD_NAMESPACE = "default" } = process.env;
if (!MEMD_URL || !MEMD_API_KEY) {
  console.error("set MEMD_URL and MEMD_API_KEY, or run under examples/local_server.py");
  process.exit(2);
}

const memd = new MemdClient({ baseUrl: MEMD_URL, apiKey: MEMD_API_KEY, namespace: MEMD_NAMESPACE });
const userId = "u1";

// --- 1. A conversation session ---------------------------------------------
// observe() is the glue after each LLM call: every message with text, plus the
// reply as `assistant`, stored on the raw lane in one durable batch.
const session = "chat-1";
const turn: ChatMessage[] = [{ role: "user", content: "I live in Berlin and I prefer dark mode" }];
await memd.observe(turn, "Noted: Berlin, dark mode.", { user_id: userId, session_id: session });
await memd.add("My favourite editor is Helix", { user_id: userId, session_id: session, role: "user" });

// The raw turns are searchable at once, before any extraction.
const raw = await memd.search("where do I live?", { user_id: userId, session_id: session, kinds: ["raw_event"] });
console.log("raw turns:", raw.items.map((h) => h.content));
assert.ok(raw.items.some((h) => h.content.includes("Berlin")));

// --- 2. Closing it ---------------------------------------------------------
// The session boundary: the raw turns go through the extractor (pattern-based by
// default; an LLM when the server has MEMD_EXTRACTION_API_KEY), the facts are
// consolidated against what is already known, and the log rotates.
const report = await memd.closeSession(session);
console.log("closeSession:", report);
assert.ok(report.facts_written > 0, "the session yielded facts");

const facts = await memd.search("where does the user live?", { user_id: userId, kinds: ["fact"] });
console.log("facts:", facts.items.map((f) => `${f.content} [${f.entity_keys.join(", ")}]`));
assert.ok(facts.items.some((f) => f.content.includes("Berlin")));

// The next conversation starts from what the first one taught: pack() searches
// with the last user message and injects the packed context as a system message.
// Scope the read to the user, not to the new session: the facts carry chat-1's
// scope, and a session-scoped query sees only its own session.
const next: ChatMessage[] = [
  { role: "system", content: "You are a helpful assistant." },
  { role: "user", content: "Which city do I live in?" },
];
const packed = await memd.pack(next, { user_id: userId });
assert.equal(packed.length, next.length + 1, "context injected after the leading system message");
assert.match(String(packed[1]?.content), /Berlin/);
const otherSession = await memd.search("Which city do I live in?", { user_id: userId, session_id: "chat-2" });
assert.equal(otherSession.items.length, 0, "a session query never sees another session");

// --- 3. Facts superseding older ones ---------------------------------------
// A fact on an entity key is consolidated against the current facts on that key
// (same user): a different statement supersedes the old one. The old version is
// invalidated, not deleted.
const helix = await memd.remember("The user's editor is Helix", { user_id: userId, entity_keys: ["user.editor"] });
const beforeSwitch = Date.now();
await sleep(20);
const zed = await memd.remember("The user's editor is Zed", {
  user_id: userId,
  entity_keys: ["user.editor"],
  valid_from: Date.now(), // holds from now on; without it a fact holds from the start
});

const current = await memd.search("which editor does the user use?", { user_id: userId, kinds: ["fact"] });
console.log("current:", current.items.map((h) => h.content));
assert.equal(current.items[0]?.id, zed, "searches see current facts only");
assert.ok(!current.items.some((h) => h.id === helix));

// --- 4. History -------------------------------------------------------------
// get(id, { history: true }) walks the supersedence chain, oldest first.
const withHistory = await memd.get(zed, { history: true });
const chain = withHistory?.history ?? [];
for (const version of chain) {
  const { superseded_by, invalidated_at } = version.time;
  console.log(`  ${version.content}`, superseded_by ? `(superseded at ${new Date(invalidated_at ?? 0).toISOString()})` : "(current)");
}
assert.deepEqual(chain.map((v) => v.id), [helix, zed]);

// The old record itself is still readable by id, marked superseded.
const old = await memd.get(helix);
assert.equal(old?.time.superseded_by, zed);

// as_of: what was valid at a past time (epoch milliseconds).
const then = await memd.search("which editor does the user use?", { user_id: userId, kinds: ["fact"], as_of: beforeSwitch });
console.log("as_of before the switch:", then.items[0]?.content);
assert.equal(then.items[0]?.id, helix);
console.log("ok");
