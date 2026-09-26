import { MemdClient, type MemdClientOptions } from "../../src/index.js";

export interface Call {
  url: string;
  method: string;
  headers: Record<string, string>;
  body: unknown;
  signal: AbortSignal | undefined;
}

export type Reply = Response | Error | ((call: Call) => Response | Promise<Response>);

/** A scripted fetch: each call consumes the next reply (the last one repeats). */
export function scriptedFetch(...replies: Reply[]) {
  const calls: Call[] = [];
  const fetch = async (input: string, init: RequestInit): Promise<Response> => {
    const headers: Record<string, string> = {};
    new Headers(init.headers).forEach((v, k) => {
      headers[k] = v;
    });
    const call: Call = {
      url: input,
      method: init.method ?? "GET",
      headers,
      body: typeof init.body === "string" ? JSON.parse(init.body) : undefined,
      signal: init.signal ?? undefined,
    };
    calls.push(call);
    const reply = replies[Math.min(calls.length - 1, replies.length - 1)];
    if (reply === undefined) throw new Error("scriptedFetch: no reply configured");
    if (reply instanceof Error) throw reply;
    if (typeof reply === "function") return reply(call);
    return reply.clone();
  };
  return { fetch, calls };
}

export function json(body: unknown, status = 200, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", ...headers },
  });
}

/** A fetch that never answers until its signal aborts (then rejects like undici does). */
export function hangingFetch() {
  const calls: Call[] = [];
  const fetch = (input: string, init: RequestInit): Promise<Response> => {
    calls.push({ url: input, method: init.method ?? "GET", headers: {}, body: undefined, signal: init.signal ?? undefined });
    return new Promise((_, reject) => {
      const signal = init.signal;
      const fail = () => reject(new DOMException("This operation was aborted", "AbortError"));
      if (signal?.aborted) fail();
      signal?.addEventListener("abort", fail, { once: true });
    });
  };
  return { fetch, calls };
}

export function client(fetch: MemdClientOptions["fetch"], extra: Partial<MemdClientOptions> = {}): MemdClient {
  return new MemdClient({
    apiKey: "memd_test_key",
    baseUrl: "http://memd.test/",
    retryBaseDelayMs: 1,
    retryMaxDelayMs: 2,
    fetch,
    ...extra,
  });
}

export const SEARCH_RESULT = {
  packed_context: "Relevant memories:\n<memory>We deploy with make ship</memory>",
  items: [
    {
      id: "01A",
      content: "We deploy with make ship",
      kind: "raw_event",
      source: "user",
      actor_id: "key:abc",
      t_event: 1790379400749,
      valid: true,
      score: 0.03,
      lanes: ["bm25", "vector"],
      entity_keys: [],
      namespace: "default",
    },
  ],
  tokens_used: 40,
  budget: 2000,
  truncated: false,
  query_class: "procedural",
  latency_ms: 3.2,
};

export const EMPTY_SEARCH = { ...SEARCH_RESULT, packed_context: "", items: [], tokens_used: 0 };

export const RECORD = {
  id: "01A",
  namespace: "default",
  kind: "raw_event",
  content: "We deploy with make ship",
  scope: { user: "u1", session: "s1" },
  provenance: { source: "user", actor_id: "key:abc", session_id: "s1", lineage: [], extractor: null },
  time: { t_event: 1, t_ingested: 1, valid_from: null, invalidated_at: null, superseded_by: null },
  entity_keys: [],
  embedding_version: null,
  meta: {},
  deleted: false,
};
