// TypeScript SDK (@memd/client): add, remember, search, pack, observe, forget and typed errors against a memd server.
// Run (in examples/ts): python ../local_server.py -- node quickstart.mjs   (plain JavaScript, no build step; or set MEMD_URL / MEMD_API_KEY / MEMD_NAMESPACE yourself)
import { MemdClient, PermissionDeniedError } from "@memd/client";

const { MEMD_URL, MEMD_API_KEY, MEMD_NAMESPACE = "default" } = process.env;
if (!MEMD_URL || !MEMD_API_KEY) {
  console.error("set MEMD_URL and MEMD_API_KEY, or run under examples/local_server.py");
  process.exit(2);
}

function check(cond, what) {
  if (!cond) throw new Error(`check failed: ${what}`);
}

const memd = new MemdClient({ baseUrl: MEMD_URL, apiKey: MEMD_API_KEY, namespace: MEMD_NAMESPACE });
console.log("server:", await memd.health());

await memd.add("We deploy with `make ship`, never from CI", { user_id: "u1", session_id: "s1" });
const fact = await memd.remember("The user prefers dark mode", { user_id: "u1", entity_keys: ["user.theme"] });

const hits = await memd.search("how do we deploy?", { user_id: "u1", budget_tokens: 500 });
console.log(hits.packed_context);
check(hits.items[0].content.includes("make ship"), "deploy recall");

// two lines in any agent loop (OpenAI-style messages)
const messages = [{ role: "user", content: "Remind me how we deploy" }];
const withMemory = await memd.pack(messages, { user_id: "u1" });
check(withMemory[0].role === "system", "packed context injected");
await memd.observe(messages, "With `make ship`.", { user_id: "u1", session_id: "s1" });
console.log("closeSession:", await memd.closeSession("s1"));

// forget: preview, then confirm exactly the previewed set
const preview = await memd.forget("dark mode", { user_id: "u1" });
console.log("forget preview:", preview.count, "record(s)");
const deleted = await memd.forget("dark mode", { user_id: "u1", confirm: preview });
check(deleted.includes(fact) && (await memd.get(fact)) === null, "forget deleted the fact");

// each HTTP status maps to a typed error
try {
  await memd.search("x", { namespace: "someone-elses-namespace" });
  check(false, "a foreign namespace must be refused");
} catch (err) {
  check(err instanceof PermissionDeniedError, `PermissionDeniedError, got ${err}`);
  console.log("other namespace refused:", err.status, err.code);
}
console.log("ok");
