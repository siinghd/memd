// A Next.js App Router route handler (app/api/memory/route.ts) giving each signed-in user memory, on web-standard Request/Response only.
// Copy into a Next.js app as app/api/memory/route.ts (set MEMD_URL, MEMD_API_KEY, MEMD_NAMESPACE); ../../../../05_nextjs_route.ts calls it without Next.
import {
  BadRequestError,
  MemdClient,
  NetworkError,
  RateLimitError,
  RequestAbortedError,
  ServerError,
  ValidationError,
} from "memd-engine";

// "edge" works as well: the client uses only fetch, and this file no Node-only API
// (Next.js provides process.env on both runtimes).
export const runtime = "nodejs";
// memory differs per user and per request: never cache these responses
export const dynamic = "force-dynamic";

// One namespace for the app, a namespace key, and a user_id scope per user: a
// user's queries see their own memories and none of anyone else's.
// The client holds no connection state, so one per server instance is fine. It
// is made on first use, so a missing variable fails the request, not the build.
let client: MemdClient | undefined;
function memd(): MemdClient {
  const { MEMD_URL, MEMD_API_KEY, MEMD_NAMESPACE } = process.env;
  if (!MEMD_URL || !MEMD_API_KEY) throw new Error("MEMD_URL and MEMD_API_KEY must be set");
  client ??= new MemdClient({ baseUrl: MEMD_URL, apiKey: MEMD_API_KEY, namespace: MEMD_NAMESPACE ?? "default", timeoutMs: 5_000 });
  return client;
}

/**
 * The signed-in user. A stand-in: use your auth library here (Auth.js `auth()`,
 * Clerk `auth()`, a verified session cookie). Never trust a user id from the body.
 */
function currentUser(req: Request): string | null {
  const id = req.headers.get("x-user-id");
  return id && /^[A-Za-z0-9_-]{1,64}$/.test(id) ? id : null;
}

const error = (status: number, code: string, message: string, extra: { issues?: string[]; headers?: Record<string, string> } = {}) =>
  Response.json({ error: { code, message, issues: extra.issues } }, { status, headers: extra.headers });

/** memd's typed errors, as this route answers them. */
function fromMemd(err: unknown): Response {
  if (err instanceof RequestAbortedError) return error(499, "client_closed_request", "the request was aborted");
  if (err instanceof ValidationError || err instanceof BadRequestError) return error(400, "invalid_request", err.message);
  if (err instanceof RateLimitError) {
    return error(429, "rate_limited", "too many requests", { headers: { "retry-after": String(Math.ceil(err.retryAfter ?? 1)) } });
  }
  if (err instanceof NetworkError || err instanceof ServerError) {
    return error(503, "memory_unavailable", "memory is unavailable, retry shortly", { headers: { "retry-after": "1" } });
  }
  console.error("memory route:", err); // a misconfigured key lands here: log it, show nothing
  return error(500, "internal_error", "internal error");
}

/** GET /api/memory?q=...: the user's memories for a query, packed for a prompt. */
export async function GET(req: Request): Promise<Response> {
  const user = currentUser(req);
  if (!user) return error(401, "unauthenticated", "sign in first");
  const q = new URL(req.url).searchParams.get("q")?.trim() ?? "";
  if (!q || q.length > 1_000) return error(400, "invalid_request", "invalid query", { issues: ["q: 1 to 1000 characters"] });
  try {
    // req.signal: if the browser goes away, the memd call is cancelled too
    const res = await memd().search(q, { user_id: user, budget_tokens: 1_000, signal: req.signal });
    return Response.json({
      context: res.packed_context,
      hits: res.items.map(({ id, content, kind }) => ({ id, content, kind })),
    });
  } catch (err) {
    return fromMemd(err);
  }
}

/** POST /api/memory { text, key? }: remember something for the user. */
export async function POST(req: Request): Promise<Response> {
  const user = currentUser(req);
  if (!user) return error(401, "unauthenticated", "sign in first");
  let body: unknown;
  try {
    body = await req.json();
  } catch {
    return error(400, "invalid_json", "the body is not JSON");
  }
  const { text, key } = (typeof body === "object" && body !== null ? body : {}) as Record<string, unknown>;
  const issues: string[] = [];
  if (typeof text !== "string" || !text.trim() || text.length > 10_000) issues.push("text: 1 to 10000 characters");
  if (key !== undefined && (typeof key !== "string" || !/^[a-z0-9_.]{1,64}$/.test(key))) issues.push("key: an entity key such as user.editor");
  if (issues.length > 0 || typeof text !== "string") return error(400, "invalid_request", "invalid memory", { issues });
  try {
    const id = await memd().remember(text, {
      user_id: user,
      entity_keys: typeof key === "string" ? [key] : null,
      signal: req.signal,
    });
    return Response.json({ id }, { status: 201 });
  } catch (err) {
    return fromMemd(err);
  }
}

/** DELETE /api/memory?id=...: delete one of the user's memories. */
export async function DELETE(req: Request): Promise<Response> {
  const user = currentUser(req);
  if (!user) return error(401, "unauthenticated", "sign in first");
  const id = new URL(req.url).searchParams.get("id") ?? "";
  if (!/^[A-Za-z0-9]{1,64}$/.test(id)) return error(400, "invalid_request", "invalid id", { issues: ["id: a memory id"] });
  try {
    // check ownership first: within one namespace, the id alone is not enough
    const record = await memd().get(id, { signal: req.signal });
    if (!record || record.scope.user !== user) return error(404, "not_found", "no such memory");
    await memd().delete(id, { signal: req.signal });
    return new Response(null, { status: 204 });
  } catch (err) {
    return fromMemd(err);
  }
}
