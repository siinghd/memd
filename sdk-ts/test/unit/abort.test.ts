import { describe, expect, it, vi } from "vitest";
import { MemdError, RequestAbortedError, RequestTimeoutError } from "../../src/index.js";
import { client, hangingFetch, json, scriptedFetch } from "./helpers.js";

describe("timeout", () => {
  it("aborts a hung request after timeoutMs", async () => {
    const { fetch, calls } = hangingFetch();
    const t0 = Date.now();
    const err = (await client(fetch, { timeoutMs: 30, retries: 0 })
      .stats()
      .catch((e: unknown) => e)) as RequestTimeoutError;
    expect(err).toBeInstanceOf(RequestTimeoutError);
    expect(err).toBeInstanceOf(MemdError);
    expect(err.code).toBe("timeout");
    expect(err.status).toBe(0);
    expect(err.message).toBe("request timed out after 30 ms");
    expect(Date.now() - t0).toBeLessThan(2_000);
    expect(calls[0]!.signal!.aborted).toBe(true);
  });

  it("a per-call timeoutMs overrides the client's", async () => {
    const { fetch } = hangingFetch();
    const err = await client(fetch, { timeoutMs: 60_000, retries: 0 })
      .stats({ timeoutMs: 20 })
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(RequestTimeoutError);
  });

  it("timeoutMs: 0 disables the timer", async () => {
    const spy = vi.spyOn(globalThis, "setTimeout");
    const { fetch } = scriptedFetch(json({}));
    await client(fetch, { timeoutMs: 0 }).stats();
    expect(spy).not.toHaveBeenCalled();
    spy.mockRestore();
  });

  it("the timeout covers reading the body, not just the headers", async () => {
    const fetch = (_: string, init: RequestInit) =>
      Promise.resolve(
        new Response(
          new ReadableStream({
            start(ctrl) {
              init.signal?.addEventListener("abort", () => ctrl.error(new DOMException("aborted", "AbortError")));
            },
          }),
        ),
      );
    const err = await client(fetch, { timeoutMs: 20, retries: 0 })
      .stats()
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(RequestTimeoutError);
  });
});

describe("caller abort", () => {
  it("an already-aborted signal never reaches fetch", async () => {
    const { fetch, calls } = scriptedFetch(json({}));
    const ctrl = new AbortController();
    ctrl.abort();
    await expect(client(fetch).stats({ signal: ctrl.signal })).rejects.toBeInstanceOf(RequestAbortedError);
    expect(calls).toHaveLength(0);
  });

  it("aborting mid-flight rejects with RequestAbortedError and is not retried", async () => {
    const { fetch, calls } = hangingFetch();
    const ctrl = new AbortController();
    const p = client(fetch, { retries: 5 }).search("q", { signal: ctrl.signal });
    await new Promise((r) => setTimeout(r, 5));
    ctrl.abort(new Error("user navigated away"));
    const err = (await p.catch((e: unknown) => e)) as RequestAbortedError;
    expect(err).toBeInstanceOf(RequestAbortedError);
    expect(err.code).toBe("aborted");
    expect(err.cause).toEqual(new Error("user navigated away"));
    expect(calls).toHaveLength(1);
    // fetch got the client's own linked signal, now aborted
    expect(calls[0]!.signal).not.toBe(ctrl.signal);
    expect(calls[0]!.signal!.aborted).toBe(true);
  });

  it("aborting during a backoff wait stops the retry loop", async () => {
    const { fetch, calls } = scriptedFetch(json({ detail: "busy" }, 503));
    const ctrl = new AbortController();
    const p = client(fetch, { retries: 3, retryBaseDelayMs: 10_000, retryMaxDelayMs: 10_000 }).stats({
      signal: ctrl.signal,
    });
    await new Promise((r) => setTimeout(r, 10));
    ctrl.abort();
    await expect(p).rejects.toBeInstanceOf(RequestAbortedError);
    expect(calls).toHaveLength(1);
  });

  it("removes its listeners from the caller's signal after each request", async () => {
    const { fetch } = scriptedFetch(json({}), json({ detail: "x" }, 404), new TypeError("down"));
    const ctrl = new AbortController();
    const add = vi.spyOn(ctrl.signal, "addEventListener");
    const remove = vi.spyOn(ctrl.signal, "removeEventListener");
    const c = client(fetch, { retries: 0 });
    await c.stats({ signal: ctrl.signal });
    await c.get("x", { signal: ctrl.signal });
    await c.stats({ signal: ctrl.signal }).catch(() => undefined);
    expect(add).toHaveBeenCalledTimes(3);
    expect(remove).toHaveBeenCalledTimes(3);
  });

  it("aborting an export stream mid-body rejects the iterator", async () => {
    const ctrl = new AbortController();
    const fetch = (_: string, init: RequestInit) =>
      Promise.resolve(
        new Response(
          new ReadableStream({
            start(c) {
              c.enqueue(new TextEncoder().encode('{"id":"1"}\n'));
              init.signal?.addEventListener("abort", () => c.error(new DOMException("aborted", "AbortError")));
            },
          }),
        ),
      );
    const it = client(fetch).exportStream({ signal: ctrl.signal });
    const first = await it.next();
    expect(first.value).toEqual({ id: "1" });
    ctrl.abort();
    await expect(it.next()).rejects.toBeInstanceOf(RequestAbortedError);
  });
});
