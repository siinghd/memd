import { afterEach, describe, expect, it, vi } from "vitest";
import {
  AuthenticationError,
  NetworkError,
  RateLimitError,
  RequestTimeoutError,
  ServerError,
  ValidationError,
} from "../../src/index.js";
import { EMPTY_SEARCH, RECORD, client, hangingFetch, json, scriptedFetch } from "./helpers.js";

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

const unavailable = () => json({ detail: "namespace re-opening; retry" }, 503);
const limited = () => json({ detail: "rate limit exceeded" }, 429);
const reset = () => new TypeError("fetch failed", { cause: new Error("ECONNRESET") });

describe("idempotent requests retry", () => {
  it("search retries a 503 and succeeds", async () => {
    const { fetch, calls } = scriptedFetch(unavailable(), json(EMPTY_SEARCH));
    expect(await client(fetch).search("q")).toEqual(EMPTY_SEARCH);
    expect(calls).toHaveLength(2);
  });

  it("GET retries a 429 then a network error", async () => {
    const { fetch, calls } = scriptedFetch(limited(), reset(), json(RECORD));
    expect(await client(fetch).get("01A")).toEqual(RECORD);
    expect(calls).toHaveLength(3);
  });

  it("gives up after `retries` extra attempts and throws the last error", async () => {
    const { fetch, calls } = scriptedFetch(unavailable());
    const err = await client(fetch, { retries: 3 })
      .stats()
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ServerError);
    expect(calls).toHaveLength(4);
  });

  it("network errors surface as NetworkError with the cause", async () => {
    const cause = reset();
    const { fetch, calls } = scriptedFetch(cause);
    const err = (await client(fetch)
      .status()
      .catch((e: unknown) => e)) as NetworkError;
    expect(err).toBeInstanceOf(NetworkError);
    expect(err.status).toBe(0);
    expect(err.code).toBe("network_error");
    expect(err.cause).toBe(cause);
    expect(calls).toHaveLength(3);
  });

  it("read-only POSTs retry: find_ids, export, forget preview", async () => {
    for (const run of [
      (c: ReturnType<typeof client>) => c.findIds("q"),
      (c: ReturnType<typeof client>) => c.exportJsonl(),
      (c: ReturnType<typeof client>) => c.forget("q"),
    ]) {
      const { fetch, calls } = scriptedFetch(
        unavailable(),
        json({ ids: [], will_delete: [], count: 0, confirmed: false }),
      );
      await run(client(fetch));
      expect(calls).toHaveLength(2);
    }
  });

  it("record DELETE retries (the end state is the same)", async () => {
    const { fetch, calls } = scriptedFetch(unavailable(), json({ deleted: "a", hard: false, note: "" }));
    expect(await client(fetch).delete("a")).toBe(true);
    expect(calls).toHaveLength(2);
  });

  it("per-call retries override the client default", async () => {
    const { fetch, calls } = scriptedFetch(unavailable());
    await client(fetch, { retries: 5 })
      .stats({ retries: 0 })
      .catch(() => undefined);
    expect(calls).toHaveLength(1);
  });

  it("timeouts are retried for idempotent requests", async () => {
    const { fetch, calls } = hangingFetch();
    const err = await client(fetch, { timeoutMs: 10, retries: 1 })
      .search("q")
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(RequestTimeoutError);
    expect(calls).toHaveLength(2);
  });
});

describe("writes never retry (the API has no idempotency key)", () => {
  const writes: Array<[string, (c: ReturnType<typeof client>) => Promise<unknown>]> = [
    ["add", (c) => c.add("x")],
    ["addEvents", (c) => c.addEvents([{ content: "x" }])],
    ["remember", (c) => c.remember("x")],
    ["observe", (c) => c.observe([{ role: "user", content: "x" }], "y")],
    ["forget confirm", (c) => c.forget("x", { confirm: true })],
    ["closeSession", (c) => c.closeSession("s")],
    ["compact", (c) => c.compact()],
    ["reembed", (c) => c.reembed()],
    ["destroyNamespace", (c) => c.destroyNamespace("n")],
  ];

  for (const [name, run] of writes) {
    for (const [what, reply] of [
      ["503", unavailable],
      ["429", limited],
      ["network error", reset],
    ] as const) {
      it(`${name} is not retried on ${what}`, async () => {
        const { fetch, calls } = scriptedFetch(reply(), json({ ids: ["1"], accepted: 1 }));
        await expect(run(client(fetch, { retries: 5 }))).rejects.toBeDefined();
        expect(calls).toHaveLength(1);
      });
    }
  }

  it("a timed-out write is not retried", async () => {
    const { fetch, calls } = hangingFetch();
    const err = await client(fetch, { timeoutMs: 10 })
      .add("x")
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(RequestTimeoutError);
    expect(calls).toHaveLength(1);
  });
});

describe("client errors never retry", () => {
  for (const [status, Cls] of [
    [400, Error],
    [401, AuthenticationError],
    [403, Error],
    [404, Error],
    [422, ValidationError],
  ] as const) {
    it(`${status} on search`, async () => {
      const { fetch, calls } = scriptedFetch(json({ detail: status === 422 ? [] : "x" }, status));
      await expect(client(fetch).search("q")).rejects.toBeInstanceOf(Cls);
      expect(calls).toHaveLength(1);
    });
  }
});

describe("backoff", () => {
  it("doubles per attempt, with jitter in [step/2, step]", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(1);
    const { fetch, calls } = scriptedFetch(unavailable(), unavailable(), json({}));
    const done = client(fetch, { retryBaseDelayMs: 100, retryMaxDelayMs: 10_000 }).stats();
    await vi.advanceTimersByTimeAsync(0);
    expect(calls).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(99);
    expect(calls).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1); // first wait: 100 ms
    expect(calls).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(199);
    expect(calls).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(1); // second wait: 200 ms
    expect(calls).toHaveLength(3);
    await done;
  });

  it("jitter never drops below half the step", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(0);
    const { fetch, calls } = scriptedFetch(unavailable(), json({}));
    const done = client(fetch, { retryBaseDelayMs: 100, retryMaxDelayMs: 10_000 }).stats();
    await vi.advanceTimersByTimeAsync(49);
    expect(calls).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(calls).toHaveLength(2);
    await done;
  });

  it("is capped by retryMaxDelayMs", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(1);
    const { fetch, calls } = scriptedFetch(unavailable(), unavailable(), unavailable(), json({}));
    const done = client(fetch, { retries: 3, retryBaseDelayMs: 100, retryMaxDelayMs: 150 }).stats();
    await vi.advanceTimersByTimeAsync(100 + 150 + 149);
    expect(calls).toHaveLength(3);
    await vi.advanceTimersByTimeAsync(1);
    expect(calls).toHaveLength(4);
    await done;
  });

  it("honors Retry-After instead of the backoff", async () => {
    vi.useFakeTimers();
    // memd's own 429: whole-second Retry-After, code in the body
    const { fetch, calls } = scriptedFetch(
      json({ detail: "rate limit exceeded", code: "rate_limited" }, 429, { "Retry-After": "3" }),
      json(EMPTY_SEARCH),
    );
    const done = client(fetch, { retryBaseDelayMs: 1 }).search("q");
    await vi.advanceTimersByTimeAsync(2_999);
    expect(calls).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(calls).toHaveLength(2);
    await done;
  });

  it("does not wait out a Retry-After longer than a minute", async () => {
    const { fetch, calls } = scriptedFetch(json({ detail: "slow down" }, 429, { "retry-after": "600" }));
    const err = (await client(fetch)
      .stats()
      .catch((e: unknown) => e)) as RateLimitError;
    expect(err).toBeInstanceOf(RateLimitError);
    expect(err.retryAfter).toBe(600);
    expect(calls).toHaveLength(1);
  });
});
