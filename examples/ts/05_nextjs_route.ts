// Calls the Next.js route handler in nextjs/app/api/memory/route.ts with standard Request objects: no Next.js install, no dev server.
// Run (in examples/ts): python ../local_server.py -- npx tsx 05_nextjs_route.ts   (or set MEMD_URL / MEMD_API_KEY / MEMD_NAMESPACE yourself)
import assert from "node:assert/strict";
import { DELETE, GET, POST } from "./nextjs/app/api/memory/route.ts";

if (!process.env.MEMD_URL || !process.env.MEMD_API_KEY) {
  console.error("set MEMD_URL and MEMD_API_KEY, or run under examples/local_server.py");
  process.exit(2);
}

// Next.js hands a route handler a Request (a NextRequest extends it); so does this.
const endpoint = "http://localhost:3000/api/memory";
function request(user: string | null, query = "", init: RequestInit = {}): Request {
  const headers = new Headers(init.headers);
  if (user) headers.set("x-user-id", user); // what the stand-in currentUser() reads
  return new Request(endpoint + query, { ...init, headers });
}
const post = (user: string | null, body: string) =>
  POST(request(user, "", { method: "POST", body, headers: { "content-type": "application/json" } }));

interface Body {
  id?: string;
  context?: string;
  hits?: Array<{ id: string; content: string; kind: string }>;
  error?: { code: string; message: string; issues?: string[] };
}
const json = async (res: Response): Promise<Body> => (await res.json()) as Body;

// remember, per user
const created = await post("alice", JSON.stringify({ text: "The user's editor is Helix", key: "user.editor" }));
assert.equal(created.status, 201);
const { id } = await json(created);
assert.ok(id);
assert.equal((await post("bob", JSON.stringify({ text: "The user's editor is Emacs", key: "user.editor" }))).status, 201);

// search sees only the caller's memories
const alice = await json(await GET(request("alice", "?q=which+editor+does+the+user+use")));
const bob = await json(await GET(request("bob", "?q=which+editor+does+the+user+use")));
console.log("alice:", alice.hits?.map((h) => h.content), "| bob:", bob.hits?.map((h) => h.content));
assert.deepEqual(alice.hits?.map((h) => h.content), ["The user's editor is Helix"]);
assert.deepEqual(bob.hits?.map((h) => h.content), ["The user's editor is Emacs"]);
assert.match(alice.context ?? "", /Helix/);

// validation: 400 with the issues, 401 without a user
const bad = await post("alice", JSON.stringify({ text: "", key: "Not A Key" }));
console.log("400:", (await json(bad)).error?.issues);
assert.equal(bad.status, 400);
assert.equal((await post("alice", "{oops")).status, 400);
assert.equal((await GET(request("alice", "?q="))).status, 400);
assert.equal((await GET(request(null, "?q=editor"))).status, 401);

// a request the browser already gave up on: req.signal cancels the memd call
const gone = await GET(request("alice", "?q=editor", { signal: AbortSignal.abort() }));
assert.equal(gone.status, 499);

// delete: only your own
assert.equal((await DELETE(request("bob", `?id=${id}`, { method: "DELETE" }))).status, 404);
assert.equal((await DELETE(request("alice", `?id=${id}`, { method: "DELETE" }))).status, 204);
assert.equal((await DELETE(request("alice", `?id=${id}`, { method: "DELETE" }))).status, 404);
assert.deepEqual((await json(await GET(request("alice", "?q=editor")))).hits, []);
console.log("ok");
