import { afterEach, describe, expect, it, vi } from "vitest";
import { ForgetPreviewMismatchError, MemdClient } from "../../src/index.js";
import { EMPTY_SEARCH, RECORD, SEARCH_RESULT, client, json, scriptedFetch } from "./helpers.js";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("construction", () => {
  it("requires an apiKey", () => {
    expect(() => new MemdClient({ apiKey: "" })).toThrow(TypeError);
    expect(() => new MemdClient(undefined as never)).toThrow(TypeError);
  });

  it("defaults like the Python SDK", () => {
    const c = new MemdClient({ apiKey: "k" });
    expect(c.baseUrl).toBe("http://localhost:8700");
    expect(c.namespace).toBe("default");
    expect(c.timeoutMs).toBe(30_000);
    expect(c.retries).toBe(2);
  });

  it("strips trailing slashes from baseUrl", async () => {
    const { fetch, calls } = scriptedFetch(json({ ok: true, version: "0.2.0" }));
    await client(fetch, { baseUrl: "https://memd.example.com///" }).health();
    expect(calls[0]!.url).toBe("https://memd.example.com/health");
  });

  it("calls the global fetch unbound-safe when none is injected", async () => {
    const seen: unknown[] = [];
    vi.stubGlobal("fetch", function (this: unknown, input: string) {
      seen.push(this, input);
      return Promise.resolve(json({ ok: true, version: "x" }));
    });
    const c = new MemdClient({ apiKey: "k", baseUrl: "http://h" });
    await c.health();
    // never invoked as a method of the client (that throws on Workers)
    expect(seen[0]).not.toBe(c);
    expect(seen[1]).toBe("http://h/health");
  });
});

describe("headers", () => {
  it("sends the bearer key, JSON content type and extra headers", async () => {
    const { fetch, calls } = scriptedFetch(json(SEARCH_RESULT));
    await client(fetch, { headers: { "x-gateway": "g1" } }).search("q");
    const h = calls[0]!.headers;
    expect(h["authorization"]).toBe("Bearer memd_test_key");
    expect(h["content-type"]).toBe("application/json");
    expect(h["accept"]).toBe("application/json");
    expect(h["x-gateway"]).toBe("g1");
  });

  it("does not send a content type without a body", async () => {
    const { fetch, calls } = scriptedFetch(json({}));
    await client(fetch).stats();
    expect(calls[0]!.headers["content-type"]).toBeUndefined();
  });

  it("an extra header cannot override Authorization", async () => {
    const { fetch, calls } = scriptedFetch(json({}));
    await client(fetch, { headers: { authorization: "Bearer other", Authorization: "Bearer x" } }).stats();
    expect(calls[0]!.headers["authorization"]).toBe("Bearer memd_test_key");
  });
});

describe("writes", () => {
  it("add posts one raw-lane event and returns its ids", async () => {
    const { fetch, calls } = scriptedFetch(json({ ids: ["01A"], accepted: 1 }, 202));
    const ids = await client(fetch).add("We deploy with make ship", {
      user_id: "u1",
      session_id: "s1",
      meta: { turn: 3 },
      agent_id: null,
      timeoutMs: 5000,
    });
    expect(ids).toEqual(["01A"]);
    expect(calls[0]!.method).toBe("POST");
    expect(calls[0]!.url).toBe("http://memd.test/v1/ns/default/events");
    // nulls and request options never reach the wire
    expect(calls[0]!.body).toEqual({
      events: [{ content: "We deploy with make ship", user_id: "u1", session_id: "s1", meta: { turn: 3 } }],
    });
  });

  it("addEvents sends only EventIn fields", async () => {
    const { fetch, calls } = scriptedFetch(json({ ids: ["1", "2"], accepted: 2 }, 202));
    const events = [
      { content: "a", role: "assistant", kind: "raw_event" as const, extra: "dropped" },
      { content: "b", source: "tool" as const, t_event: 5 },
    ];
    const ids = await client(fetch).addEvents(events, { namespace: "acme" });
    expect(ids).toEqual(["1", "2"]);
    expect(calls[0]!.url).toBe("http://memd.test/v1/ns/acme/events");
    expect(calls[0]!.body).toEqual({
      events: [
        { content: "a", role: "assistant", kind: "raw_event" },
        { content: "b", source: "tool", t_event: 5 },
      ],
    });
  });

  it("remember posts to /memories and returns the id", async () => {
    const { fetch, calls } = scriptedFetch(json({ id: "01F" }, 201));
    const id = await client(fetch).remember("The user prefers dark mode", {
      entity_keys: ["user.theme"],
      user_id: "u1",
      valid_from: 10,
    });
    expect(id).toBe("01F");
    expect(calls[0]!.url).toBe("http://memd.test/v1/ns/default/memories");
    expect(calls[0]!.body).toEqual({
      content: "The user prefers dark mode",
      entity_keys: ["user.theme"],
      user_id: "u1",
      valid_from: 10,
    });
  });

  it("encodes namespace and ids into the path", async () => {
    const { fetch, calls } = scriptedFetch(json(RECORD));
    await client(fetch, { namespace: "a b/c" }).get("id/with?chars");
    expect(calls[0]!.url).toBe("http://memd.test/v1/ns/a%20b%2Fc/memories/id%2Fwith%3Fchars");
  });
});

describe("reads", () => {
  it("search sends SearchIn and returns the result as-is", async () => {
    const { fetch, calls } = scriptedFetch(json(SEARCH_RESULT));
    const res = await client(fetch).search("how do we deploy?", {
      user_id: "u1",
      budget_tokens: 1500,
      kinds: ["fact", "raw_event"],
      include_quarantined: false,
      as_of: undefined,
      namespace: "acme",
    });
    expect(res).toEqual(SEARCH_RESULT);
    expect(calls[0]!.url).toBe("http://memd.test/v1/ns/acme/search");
    expect(calls[0]!.body).toEqual({
      query: "how do we deploy?",
      user_id: "u1",
      budget_tokens: 1500,
      kinds: ["fact", "raw_event"],
      include_quarantined: false,
    });
  });

  it("get returns the record, passes history, and maps 404 to null", async () => {
    const { fetch, calls } = scriptedFetch(json(RECORD), json({ detail: "not found" }, 404));
    const c = client(fetch);
    expect(await c.get("01A", { history: true })).toEqual(RECORD);
    expect(calls[0]!.url).toBe("http://memd.test/v1/ns/default/memories/01A?history=true");
    expect(await c.get("missing")).toBeNull();
    expect(calls[1]!.url).toBe("http://memd.test/v1/ns/default/memories/missing");
  });

  it("get passes include_deleted for admin reads", async () => {
    const { fetch, calls } = scriptedFetch(json({ ...RECORD, deleted: true }));
    expect((await client(fetch).get("01A", { history: true, include_deleted: true }))?.deleted).toBe(true);
    expect(calls[0]!.url).toBe("http://memd.test/v1/ns/default/memories/01A?history=true&include_deleted=true");
  });

  it("findIds returns the id list", async () => {
    const { fetch, calls } = scriptedFetch(json({ ids: ["a", "b"] }));
    expect(await client(fetch).findIds("deploy", { kinds: ["fact"], user_id: "u1" })).toEqual(["a", "b"]);
    expect(calls[0]!.body).toEqual({ query: "deploy", kinds: ["fact"], user_id: "u1" });
  });

  it("stats, status and health hit their routes", async () => {
    const { fetch, calls } = scriptedFetch(json({ records: 3 }));
    const c = client(fetch);
    await c.stats({ namespace: "acme" });
    await c.status();
    await c.health();
    expect(calls.map((x) => `${x.method} ${x.url}`)).toEqual([
      "GET http://memd.test/v1/ns/acme/stats",
      "GET http://memd.test/v1/status",
      "GET http://memd.test/health",
    ]);
  });
});

describe("lifecycle", () => {
  it("delete is soft by default, hard on request, false on 404", async () => {
    const { fetch, calls } = scriptedFetch(
      json({ deleted: "01A", hard: false, note: "n" }),
      json({ deleted: "01B", hard: true, note: "purged" }),
      json({ detail: "not found" }, 404),
    );
    const c = client(fetch);
    expect(await c.delete("01A")).toBe(true);
    expect(await c.delete("01B", { hard: true })).toBe(true);
    expect(await c.delete("gone")).toBe(false);
    expect(calls.map((x) => `${x.method} ${x.url}`)).toEqual([
      "DELETE http://memd.test/v1/ns/default/memories/01A",
      "DELETE http://memd.test/v1/ns/default/memories/01B?hard=true",
      "DELETE http://memd.test/v1/ns/default/memories/gone",
    ]);
  });

  const preview = {
    will_delete: [{ id: "a", content: "x" }],
    count: 1,
    confirmed: false as const,
    fingerprint: "3f0c2a9d8e7b6a5f4e3d2c1b0a998877",
  };
  const done = { deleted: ["a"], count: 1, confirmed: true };

  it("forget previews without confirm", async () => {
    const { fetch, calls } = scriptedFetch(json(preview));
    expect(await client(fetch).forget("deploy", { user_id: "u1", kinds: ["fact"] })).toEqual(preview);
    expect(calls[0]!.url).toBe("http://memd.test/v1/ns/default/forget");
    expect(calls[0]!.body).toEqual({ query: "deploy", user_id: "u1", kinds: ["fact"], confirm: false });
  });

  it("confirming with the preview sends its fingerprint and the same filters", async () => {
    const { fetch, calls } = scriptedFetch(json(preview), json(done));
    const c = client(fetch);
    const filters = { user_id: "u1", kinds: ["fact" as const], as_of: 1790000000000 };
    const p = await c.forget("deploy", filters);
    expect(await c.forget("deploy", { ...filters, confirm: p })).toEqual(["a"]);
    expect(calls[1]!.body).toEqual({
      query: "deploy",
      user_id: "u1",
      kinds: ["fact"],
      as_of: 1790000000000,
      confirm: true,
      fingerprint: preview.fingerprint,
    });
  });

  it("confirm: true sends an explicit fingerprint, or none", async () => {
    const { fetch, calls } = scriptedFetch(json(done));
    const c = client(fetch);
    await c.forget("deploy", { confirm: true, fingerprint: "abc" });
    await c.forget("deploy", { confirm: true });
    await c.forget("deploy", { fingerprint: "ignored-without-confirm" }).catch(() => undefined);
    expect(calls[0]!.body).toEqual({ query: "deploy", confirm: true, fingerprint: "abc" });
    expect(calls[1]!.body).toEqual({ query: "deploy", confirm: true });
    expect(calls[2]!.body).toEqual({ query: "deploy", confirm: false });
  });

  it("a changed match set surfaces as ForgetPreviewMismatchError", async () => {
    const { fetch, calls } = scriptedFetch(
      json({ detail: "the query now matches 2 record(s), not the previewed set: preview again and confirm that", code: "preview_mismatch" }, 409),
    );
    const err = await client(fetch, { retries: 5 })
      .forget("deploy", { confirm: preview })
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ForgetPreviewMismatchError);
    expect((err as ForgetPreviewMismatchError).code).toBe("preview_mismatch");
    expect(calls).toHaveLength(1); // a confirm is never retried
  });

  it("closeSession, compact, reembed post to their routes", async () => {
    const { fetch, calls } = scriptedFetch(json({}));
    const c = client(fetch);
    await c.closeSession("s 1");
    await c.compact({ force: true });
    await c.compact();
    await c.reembed({ namespace: "acme" });
    expect(calls.map((x) => `${x.method} ${x.url}`)).toEqual([
      "POST http://memd.test/v1/ns/default/sessions/s%201/close",
      "POST http://memd.test/v1/ns/default/compact?force=true",
      "POST http://memd.test/v1/ns/default/compact",
      "POST http://memd.test/v1/ns/acme/reembed",
    ]);
    expect(calls.every((x) => x.body === undefined)).toBe(true);
  });

  it("destroyNamespace targets the given, per-call or default namespace", async () => {
    const { fetch, calls } = scriptedFetch(json({ destroyed: "x", crypto_shred: true }));
    const c = client(fetch, { namespace: "home" });
    expect(await c.destroyNamespace("tenant-9")).toBe(true);
    await c.destroyNamespace(undefined, { namespace: "other" });
    await c.destroyNamespace();
    expect(calls.map((x) => `${x.method} ${x.url}`)).toEqual([
      "DELETE http://memd.test/v1/ns/tenant-9",
      "DELETE http://memd.test/v1/ns/other",
      "DELETE http://memd.test/v1/ns/home",
    ]);
  });

  it("destroyNamespace returns false when the namespace does not exist (404)", async () => {
    const { fetch, calls } = scriptedFetch(json({ detail: "namespace 'nope' not found", code: "not_found" }, 404));
    expect(await client(fetch).destroyNamespace("nope")).toBe(false);
    expect(calls).toHaveLength(1);
  });
});

describe("export", () => {
  const lines = [RECORD, { ...RECORD, id: "01B", content: "second" }];
  const ndjson = lines.map((r) => JSON.stringify(r)).join("\n") + "\n";

  it("exportJsonl returns the raw NDJSON and asks for it", async () => {
    const { fetch, calls } = scriptedFetch(new Response(ndjson, { headers: { "content-type": "application/x-ndjson" } }));
    expect(await client(fetch).exportJsonl()).toBe(ndjson);
    expect(calls[0]!.method).toBe("POST");
    expect(calls[0]!.url).toBe("http://memd.test/v1/ns/default/export");
    expect(calls[0]!.headers["accept"]).toBe("application/x-ndjson");
  });

  it("export parses every line", async () => {
    const { fetch } = scriptedFetch(new Response(ndjson));
    expect(await client(fetch).export()).toEqual(lines);
  });

  it("export of an empty namespace is an empty list", async () => {
    const { fetch } = scriptedFetch(new Response(""));
    expect(await client(fetch).export()).toEqual([]);
  });

  it("exportStream reassembles records split across chunks", async () => {
    const bytes = new TextEncoder().encode(ndjson.replace(/\n$/, "")); // no trailing newline
    const body = new ReadableStream<Uint8Array>({
      start(ctrl) {
        for (let i = 0; i < bytes.length; i += 7) ctrl.enqueue(bytes.slice(i, i + 7));
        ctrl.close();
      },
    });
    const { fetch } = scriptedFetch(() => new Response(body));
    const got = [];
    for await (const rec of client(fetch).exportStream()) got.push(rec);
    expect(got).toEqual(lines);
  });

  it("exportStream cancels the body when the consumer stops early", async () => {
    let cancelled = false;
    const body = new ReadableStream<Uint8Array>({
      pull(ctrl) {
        ctrl.enqueue(new TextEncoder().encode(JSON.stringify(RECORD) + "\n"));
      },
      cancel() {
        cancelled = true;
      },
    });
    const { fetch } = scriptedFetch(() => new Response(body));
    let n = 0;
    for await (const _ of client(fetch).exportStream()) {
      if (++n === 3) break;
    }
    expect(n).toBe(3);
    expect(cancelled).toBe(true);
  });
});

describe("pack", () => {
  it("inserts packed context after the leading system messages", async () => {
    const { fetch, calls } = scriptedFetch(json(SEARCH_RESULT));
    const messages = [
      { role: "system", content: "You are helpful." },
      { role: "user", content: "hi" },
      { role: "assistant", content: "hello" },
      { role: "user", content: "how do we deploy?" },
    ];
    const out = await client(fetch).pack(messages, { user_id: "u1", budget_tokens: 800 });
    expect(out).toHaveLength(5);
    expect(out[0]).toBe(messages[0]);
    expect(out[1]).toEqual({ role: "system", content: SEARCH_RESULT.packed_context });
    expect(out.slice(2)).toEqual(messages.slice(1));
    expect(messages).toHaveLength(4); // input untouched
    expect(calls[0]!.body).toEqual({ query: "how do we deploy?", user_id: "u1", budget_tokens: 800 });
  });

  it("puts the context first when there is no system prompt", async () => {
    const { fetch } = scriptedFetch(json(SEARCH_RESULT));
    const out = await client(fetch).pack([{ role: "user", content: "deploy?" }]);
    expect(out[0]).toEqual({ role: "system", content: SEARCH_RESULT.packed_context });
  });

  it("returns the messages unchanged with no hits or no user message", async () => {
    const { fetch, calls } = scriptedFetch(json(EMPTY_SEARCH));
    const c = client(fetch);
    const msgs = [{ role: "user", content: "anything?" }];
    expect(await c.pack(msgs)).toEqual(msgs);
    expect(await c.pack([{ role: "system", content: "sys" }])).toEqual([{ role: "system", content: "sys" }]);
    expect(await c.pack([{ role: "user", content: "" }])).toEqual([{ role: "user", content: "" }]);
    expect(calls).toHaveLength(1); // no search without a user query
  });

  it("reads text out of content-part arrays", async () => {
    const { fetch, calls } = scriptedFetch(json(SEARCH_RESULT));
    await client(fetch).pack([
      {
        role: "user",
        content: [
          { type: "text", text: "how do we" },
          { type: "image_url", image_url: { url: "https://x/y.png" } },
          { type: "text", text: "deploy?" },
        ],
      },
    ]);
    expect((calls[0]!.body as { query: string }).query).toBe("how do we\ndeploy?");
  });

  it("trims an over-long user message to the server's 10,000-character query cap", async () => {
    const { fetch, calls } = scriptedFetch(json(EMPTY_SEARCH));
    await client(fetch).pack([{ role: "user", content: "🙂".repeat(12_000) }]);
    const q = (calls[0]!.body as { query: string }).query;
    expect(Array.from(q)).toHaveLength(10_000);
    expect(q.endsWith("🙂")).toBe(true); // no split surrogate pair
  });
});

describe("observe", () => {
  it("captures every message with text plus the response, in one batch", async () => {
    const { fetch, calls } = scriptedFetch(json({ ids: ["1", "2", "3"], accepted: 3 }, 202));
    const ids = await client(fetch).observe(
      [
        { role: "system", content: "sys" },
        { role: "user", content: "hi" },
        { role: "assistant", content: null },
        { role: "tool", content: [{ type: "text", text: "tool out" }] },
      ],
      "hello!",
      { session_id: "s1", user_id: "u1", agent_id: undefined, namespace: "acme" },
    );
    expect(ids).toEqual(["1", "2", "3"]);
    expect(calls[0]!.url).toBe("http://memd.test/v1/ns/acme/events");
    expect(calls[0]!.body).toEqual({
      events: [
        { content: "sys", role: "system", session_id: "s1", user_id: "u1" },
        { content: "hi", role: "user", session_id: "s1", user_id: "u1" },
        { content: "tool out", role: "tool", session_id: "s1", user_id: "u1" },
        { content: "hello!", role: "assistant", session_id: "s1", user_id: "u1" },
      ],
    });
  });

  it("accepts the response as a message object", async () => {
    const { fetch, calls } = scriptedFetch(json({ ids: ["1", "2"], accepted: 2 }, 202));
    await client(fetch).observe([{ role: "user", content: "q" }], { role: "assistant", content: "a" });
    expect(calls[0]!.body).toEqual({
      events: [
        { content: "q", role: "user" },
        { content: "a", role: "assistant" },
      ],
    });
  });

  it("makes no request when there is nothing to store", async () => {
    const { fetch, calls } = scriptedFetch(json({ ids: [], accepted: 0 }, 202));
    expect(await client(fetch).observe([{ role: "user", content: "" }], "")).toEqual([]);
    expect(calls).toHaveLength(0);
  });
});
