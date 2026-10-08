// An HTTP API (node:http, no framework) that gives an agent memory per user: one namespace each, input validation, errors mapped to statuses.
// Run (in examples/ts): python ../local_server.py --admin -- npx tsx 04_http_api.ts   (or set MEMD_URL and MEMD_ADMIN_KEY yourself)
import assert from "node:assert/strict";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import {
  BadRequestError,
  MemdClient,
  NetworkError,
  RateLimitError,
  RequestAbortedError,
  ServerError,
  ValidationError,
  type ChatMessage,
  type Kind,
} from "memd-engine";

const { MEMD_URL, MEMD_ADMIN_KEY } = process.env;
if (!MEMD_URL || !MEMD_ADMIN_KEY) {
  console.error("set MEMD_URL and MEMD_ADMIN_KEY, or run under examples/local_server.py --admin");
  process.exit(2);
}

// One namespace per user: each has its own log, index and data key, so erasing a
// user is a crypto-shred of one namespace. A namespace key is bound to a single
// namespace, so this backend holds the operator key, which spans them. It never
// leaves the server: browsers and agents call this API, not memd.
// (Lighter alternative: one namespace for the app, a namespace key, and a
// user_id scope per user. nextjs/app/api/memory/route.ts does that.)
const memd = new MemdClient({ baseUrl: MEMD_URL, apiKey: MEMD_ADMIN_KEY, timeoutMs: 5_000 });

const ID = /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/; // user and session ids
const ENTITY_KEY = /^[a-z0-9_]+(\.[a-z0-9_]+)*$/; // user.editor, project.db.engine
const MEMORY_KINDS = ["fact", "procedure", "pin"] as const satisfies readonly Kind[];
type MemoryKind = (typeof MEMORY_KINDS)[number];
const MAX_TEXT = 10_000;
const MAX_BODY_BYTES = 64 * 1024;

/** Namespace names are [A-Za-z0-9][A-Za-z0-9_.-]{0,127}; a validated user id always fits. */
const namespaceFor = (userId: string): string => `user-${userId}`;

// --- errors -------------------------------------------------------------------

/** An error this API answers with: its status, a stable code and a safe message. */
class HttpError extends Error {
  readonly status: number;
  readonly code: string;
  readonly issues: string[] | undefined;
  readonly headers: Record<string, string>;

  constructor(status: number, code: string, message: string, extra: { issues?: string[]; headers?: Record<string, string> } = {}) {
    super(message);
    this.status = status;
    this.code = code;
    this.issues = extra.issues;
    this.headers = extra.headers ?? {};
  }
}

const invalid = (issues: string[]) => new HttpError(400, "invalid_request", "the request is invalid", { issues });

/** Map anything thrown while handling a request to what the caller may see. */
function toHttpError(err: unknown): HttpError {
  if (err instanceof HttpError) return err;
  // memd refused input our checks let through: still the caller's mistake
  if (err instanceof ValidationError || err instanceof BadRequestError) {
    return new HttpError(400, "invalid_request", err.message);
  }
  // pass memd's backpressure on, with its hint of when to come back
  if (err instanceof RateLimitError) {
    return new HttpError(429, "rate_limited", "too many requests", {
      headers: { "retry-after": String(Math.ceil(err.retryAfter ?? 1)) },
    });
  }
  // memd is down, slow or restarting; the client already retried what was safe to retry
  if (err instanceof NetworkError || err instanceof ServerError) {
    return new HttpError(503, "memory_unavailable", "memory is unavailable, retry shortly", { headers: { "retry-after": "1" } });
  }
  // anything else (an AuthenticationError means this server's own key is wrong): log, say nothing
  console.error("unexpected error:", err);
  return new HttpError(500, "internal_error", "internal error");
}

// --- input --------------------------------------------------------------------

/** Who is calling. A stand-in: use your real authentication (a session cookie, a verified JWT). */
function authenticate(req: IncomingMessage): string {
  const userId = req.headers["x-user-id"];
  if (typeof userId !== "string" || !ID.test(userId)) {
    throw new HttpError(401, "unauthenticated", "missing or invalid x-user-id");
  }
  return userId;
}

async function readJson(req: IncomingMessage): Promise<unknown> {
  if (!(req.headers["content-type"] ?? "").startsWith("application/json")) {
    throw new HttpError(415, "unsupported_media_type", "send application/json");
  }
  if (Number(req.headers["content-length"] ?? 0) > MAX_BODY_BYTES) {
    throw new HttpError(413, "payload_too_large", `the body is over ${MAX_BODY_BYTES} bytes`);
  }
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req as AsyncIterable<Buffer>) {
    size += chunk.length;
    if (size > MAX_BODY_BYTES) throw new HttpError(413, "payload_too_large", `the body is over ${MAX_BODY_BYTES} bytes`);
    chunks.push(chunk);
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw new HttpError(400, "invalid_json", "the body is not JSON");
  }
}

const isObject = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);
const isText = (v: unknown, max = MAX_TEXT): v is string => typeof v === "string" && v.trim() !== "" && v.length <= max;
const isMemoryKind = (v: unknown): v is MemoryKind => MEMORY_KINDS.includes(v as MemoryKind);

/** POST /memories: { text, kind?, key? } */
function parseNewMemory(body: unknown): { text: string; kind: MemoryKind; key?: string } {
  const b = isObject(body) ? body : {};
  const { text, kind = "fact", key } = b;
  const issues: string[] = [];
  if (!isObject(body)) issues.push("body: must be a JSON object");
  if (!isText(text)) issues.push(`text: a non-empty string of at most ${MAX_TEXT} characters`);
  if (!isMemoryKind(kind)) issues.push(`kind: one of ${MEMORY_KINDS.join(", ")}`);
  if (key !== undefined && !(typeof key === "string" && key.length <= 64 && ENTITY_KEY.test(key))) {
    issues.push("key: an entity key such as user.editor");
  }
  if (issues.length > 0 || !isText(text) || !isMemoryKind(kind)) throw invalid(issues);
  return typeof key === "string" ? { text, kind, key } : { text, kind };
}

/** POST /turns: { sessionId, messages: [{ role, content }], reply } */
function parseTurn(body: unknown): { sessionId: string; messages: ChatMessage[]; reply: string } {
  const b = isObject(body) ? body : {};
  const { sessionId, messages, reply } = b;
  const issues: string[] = [];
  if (!(typeof sessionId === "string" && ID.test(sessionId))) issues.push("sessionId: letters, digits, _ or -");
  const ok =
    Array.isArray(messages) &&
    messages.length > 0 &&
    messages.length <= 50 &&
    messages.every((m) => isObject(m) && ["user", "assistant", "system", "tool"].includes(String(m["role"])) && isText(m["content"]));
  if (!ok) issues.push("messages: 1 to 50 of { role: user|assistant|system|tool, content: non-empty string }");
  if (!isText(reply)) issues.push("reply: a non-empty string");
  if (issues.length > 0 || typeof sessionId !== "string" || !isText(reply)) throw invalid(issues);
  return { sessionId, messages: messages as ChatMessage[], reply };
}

/** GET /memories?q=...&limit=... */
function parseSearch(url: URL): { q: string; limit: number } {
  const q = url.searchParams.get("q") ?? "";
  const limit = Number(url.searchParams.get("limit") ?? "5");
  const issues: string[] = [];
  if (!isText(q, 1_000)) issues.push("q: a non-empty query of at most 1000 characters");
  if (!Number.isInteger(limit) || limit < 1 || limit > 50) issues.push("limit: an integer from 1 to 50");
  if (issues.length > 0) throw invalid(issues);
  return { q, limit };
}

// --- routes -------------------------------------------------------------------

function send(res: ServerResponse, status: number, body?: unknown, headers: Record<string, string> = {}): void {
  if (body === undefined) {
    res.writeHead(status, headers).end();
    return;
  }
  res.writeHead(status, { "content-type": "application/json", ...headers }).end(JSON.stringify(body));
}

async function route(req: IncomingMessage, res: ServerResponse, signal: AbortSignal): Promise<void> {
  const url = new URL(req.url ?? "/", "http://localhost");
  const path = url.pathname;
  if (path === "/healthz" && req.method === "GET") return send(res, 200, { ok: true });

  const userId = authenticate(req);
  // every memd call: the caller's namespace, cancelled if the caller goes away
  const scope = { namespace: namespaceFor(userId), signal };

  if (path === "/memories" && req.method === "POST") {
    const input = parseNewMemory(await readJson(req));
    const id = await memd.remember(input.text, { ...scope, kind: input.kind, entity_keys: input.key ? [input.key] : null });
    return send(res, 201, { id });
  }
  if (path === "/memories" && req.method === "GET") {
    const { q, limit } = parseSearch(url);
    const result = await memd.search(q, { ...scope, budget_tokens: 1_000 });
    const hits = result.items.slice(0, limit).map(({ id, content, kind, score }) => ({ id, content, kind, score }));
    // `context` is what the agent puts in its prompt
    return send(res, 200, { context: result.packed_context, hits });
  }
  if (path === "/turns" && req.method === "POST") {
    // the agent posts each turn after its LLM call; memd stores it on the raw lane
    const turn = parseTurn(await readJson(req));
    const ids = await memd.observe(turn.messages, turn.reply, { ...scope, session_id: turn.sessionId });
    return send(res, 202, { stored: ids.length });
  }
  const one = /^\/memories\/([A-Za-z0-9]{1,64})$/.exec(path);
  if (one?.[1] && req.method === "DELETE") {
    // another user's id is simply not found: it lives in another namespace
    if (!(await memd.delete(one[1], scope))) throw new HttpError(404, "not_found", "no such memory");
    return send(res, 204);
  }
  if (path === "/me" && req.method === "DELETE") {
    // account deletion: crypto-shred the user's namespace (its data key is destroyed)
    await memd.destroyNamespace(scope.namespace, { signal });
    return send(res, 204);
  }
  if (one || ["/memories", "/turns", "/me"].includes(path)) {
    throw new HttpError(405, "method_not_allowed", `${req.method} is not allowed here`);
  }
  throw new HttpError(404, "not_found", "no such route");
}

const server = createServer((req, res) => {
  const ctrl = new AbortController();
  res.on("close", () => {
    if (!res.writableFinished) ctrl.abort(); // the caller hung up: stop its memd calls
  });
  route(req, res, ctrl.signal).catch((err: unknown) => {
    if (err instanceof RequestAbortedError || res.headersSent) return;
    const e = toHttpError(err);
    req.resume(); // drain an unread body, so the connection stays usable
    send(res, e.status, { error: { code: e.code, message: e.message, issues: e.issues } }, e.headers);
  });
});

// --- try it -------------------------------------------------------------------

await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
console.log("memory API on", base);

interface ApiBody {
  id?: string;
  stored?: number;
  context?: string;
  hits?: Array<{ id: string; content: string; kind: Kind; score: number }>;
  error?: { code: string; message: string; issues?: string[] };
}

async function call(user: string | null, method: string, path: string, body?: unknown, contentType = "application/json") {
  const headers: Record<string, string> = {};
  if (user !== null) headers["x-user-id"] = user;
  if (body !== undefined) headers["content-type"] = contentType;
  const res = await fetch(base + path, {
    method,
    headers,
    body: body === undefined ? undefined : typeof body === "string" ? body : JSON.stringify(body),
  });
  const text = await res.text();
  return { status: res.status, body: (text ? JSON.parse(text) : {}) as ApiBody };
}

try {
  // two users, each in a namespace of their own
  const tea = await call("alice", "POST", "/memories", { text: "The user drinks oolong tea", key: "user.drink" });
  assert.equal(tea.status, 201);
  assert.equal((await call("bob", "POST", "/memories", { text: "The user drinks espresso", key: "user.drink" })).status, 201);
  const turn = await call("alice", "POST", "/turns", {
    sessionId: "chat-1",
    messages: [{ role: "user", content: "Remind me to water the plants on Sunday" }],
    reply: "I will remind you on Sunday.",
  });
  assert.deepEqual([turn.status, turn.body.stored], [202, 2]);

  const alice = await call("alice", "GET", "/memories?q=what+does+the+user+drink");
  const bob = await call("bob", "GET", "/memories?q=what+does+the+user+drink");
  console.log("alice:", alice.body.hits?.map((h) => h.content), "| bob:", bob.body.hits?.map((h) => h.content));
  assert.deepEqual(alice.body.hits?.map((h) => h.content), ["The user drinks oolong tea"]);
  assert.deepEqual(bob.body.hits?.map((h) => h.content), ["The user drinks espresso"]);
  assert.match(alice.body.context ?? "", /oolong/);

  // validation and errors: a stable shape, never a stack trace
  const bad = await call("alice", "POST", "/memories", { text: "", kind: "secret" });
  console.log("400:", bad.body.error);
  assert.equal(bad.status, 400);
  assert.equal(bad.body.error?.issues?.length, 2);
  const failures: Array<[number, () => ReturnType<typeof call>]> = [
    [400, () => call("alice", "POST", "/memories", "{not json")],
    [400, () => call("alice", "GET", "/memories?q=tea&limit=500")],
    [400, () => call("alice", "POST", "/turns", { sessionId: "chat-1", messages: [], reply: "x" })],
    [401, () => call(null, "GET", "/memories?q=tea")],
    [401, () => call("../../etc", "GET", "/memories?q=tea")],
    [404, () => call("alice", "GET", "/nowhere")],
    [405, () => call("alice", "PUT", "/memories", {})],
    [413, () => call("alice", "POST", "/memories", { text: "x".repeat(MAX_BODY_BYTES) })],
    [415, () => call("alice", "POST", "/memories", "text=tea", "application/x-www-form-urlencoded")],
  ];
  for (const [status, request] of failures) {
    const res = await request();
    assert.equal(res.status, status, JSON.stringify(res.body));
    console.log(`${status}: ${res.body.error?.code}`);
  }

  // a user can delete only their own memories
  const teaId = tea.body.id ?? "";
  assert.equal((await call("bob", "DELETE", `/memories/${teaId}`)).status, 404);
  assert.equal((await call("alice", "DELETE", `/memories/${teaId}`)).status, 204);
  assert.equal((await call("alice", "DELETE", `/memories/${teaId}`)).status, 404);

  // account deletion shreds alice's namespace; bob's is untouched
  assert.equal((await call("alice", "DELETE", "/me")).status, 204);
  assert.deepEqual((await call("alice", "GET", "/memories?q=plants+Sunday")).body.hits, []);
  assert.equal((await call("bob", "GET", "/memories?q=espresso")).body.hits?.length, 1);
  console.log("ok");
} finally {
  server.close();
}
