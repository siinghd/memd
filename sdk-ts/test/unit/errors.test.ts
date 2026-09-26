import { describe, expect, it } from "vitest";
import {
  AuthenticationError,
  BadRequestError,
  ConflictError,
  ForgetPreviewMismatchError,
  GoneError,
  MemdError,
  NotFoundError,
  PayloadTooLargeError,
  PermissionDeniedError,
  RateLimitError,
  ServerError,
  ValidationError,
} from "../../src/index.js";
import { errorFromResponse, parseRetryAfter } from "../../src/errors.js";
import { client, json, scriptedFetch } from "./helpers.js";

// [status, class, the server's `code`, detail] as memd sends them
const cases = [
  [400, BadRequestError, "validation_error", "serialized meta exceeds 65536 byte cap"],
  [401, AuthenticationError, "unauthorized", "invalid key"],
  [403, PermissionDeniedError, "forbidden", "key not valid for namespace 'b'"],
  [404, NotFoundError, "not_found", "not found"],
  [409, ConflictError, "conflict", "conflict"],
  [409, ForgetPreviewMismatchError, "preview_mismatch", "the query now matches 2 record(s), not the previewed set: preview again and confirm that"],
  [410, GoneError, "namespace_destroyed", "namespace destroyed"],
  [413, PayloadTooLargeError, "payload_too_large", "request body too large"],
  [429, RateLimitError, "rate_limited", "rate limit exceeded"],
  [500, ServerError, "internal_error", "internal error"],
  [503, ServerError, "unavailable", "namespace unavailable (destroyed or rebuilding)"],
] as const;

describe("status mapping", () => {
  for (const [status, Cls, code, detail] of cases) {
    it(`${status} ${code} -> ${Cls.name}`, async () => {
      const { fetch } = scriptedFetch(json({ detail, code }, status));
      const err = await client(fetch, { retries: 0 })
        .stats()
        .catch((e: unknown) => e);
      expect(err).toBeInstanceOf(Cls);
      expect(err).toBeInstanceOf(MemdError);
      expect(err).toBeInstanceOf(Error);
      const e = err as MemdError;
      expect(e.status).toBe(status);
      expect(e.code).toBe(code);
      expect(e.message).toBe(detail);
      expect(e.detail).toBe(detail);
      expect(e.name).toBe(Cls.name);
    });
  }

  it("a preview mismatch is still a ConflictError", () => {
    const body = JSON.stringify({ detail: "changed", code: "preview_mismatch" });
    const e = errorFromResponse(409, body, new Headers());
    expect(e).toBeInstanceOf(ForgetPreviewMismatchError);
    expect(e).toBeInstanceOf(ConflictError);
  });

  it("a body without `code` gets the server's default code for the status", () => {
    const fallback = (status: number) => errorFromResponse(status, JSON.stringify({ detail: "x" }), new Headers()).code;
    expect(fallback(400)).toBe("validation_error");
    expect(fallback(404)).toBe("not_found");
    expect(fallback(409)).toBe("conflict");
    expect(fallback(429)).toBe("rate_limited");
    expect(fallback(500)).toBe("internal_error");
    expect(fallback(503)).toBe("unavailable");
    expect(fallback(502)).toBe("error");
  });

  it("an unknown server code passes through", () => {
    const e = errorFromResponse(403, JSON.stringify({ detail: "x", code: "quota_exhausted" }), new Headers());
    expect(e).toBeInstanceOf(PermissionDeniedError);
    expect(e.code).toBe("quota_exhausted");
  });

  it("an unmapped 4xx is a plain MemdError", () => {
    const e = errorFromResponse(418, JSON.stringify({ detail: "teapot" }), new Headers());
    expect(e.constructor).toBe(MemdError);
    expect(e.code).toBe("error");
    expect(e.status).toBe(418);
  });

  it("502 from a proxy with an HTML body is a ServerError carrying the text", () => {
    const e = errorFromResponse(502, "<html>Bad Gateway</html>", new Headers());
    expect(e).toBeInstanceOf(ServerError);
    expect(e.message).toBe("<html>Bad Gateway</html>");
    expect(e.code).toBe("error");
  });

  it("a JSON body without detail falls back to the status", () => {
    const e = errorFromResponse(500, JSON.stringify({ error: "x" }), new Headers());
    expect(e.message).toBe("HTTP 500");
    expect(e.detail).toEqual({ error: "x" });
  });

  it("an empty body falls back to the status", () => {
    expect(errorFromResponse(503, "", new Headers()).message).toBe("HTTP 503");
  });
});

describe("422 validation", () => {
  // the exact body the real server sends for {"query":"","kinds":["bogus"]}
  const detail = [
    {
      type: "string_too_short",
      loc: ["body", "query"],
      msg: "String should have at least 1 character",
      input: "",
      ctx: { min_length: 1 },
    },
    {
      type: "value_error",
      loc: ["body", "kinds"],
      msg: "Value error, unknown kind(s) ['bogus']",
      input: ["bogus"],
      ctx: { error: {} },
    },
  ];

  it("exposes every issue and summarizes them in the message", async () => {
    const { fetch } = scriptedFetch(json({ detail, code: "validation_error" }, 422));
    const err = await client(fetch)
      .search("")
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ValidationError);
    const e = err as ValidationError;
    expect(e.code).toBe("validation_error");
    expect(e.issues).toEqual(detail);
    expect(e.message).toBe("query: String should have at least 1 character; kinds: Value error, unknown kind(s) ['bogus']");
  });

  it("a 422 with a string detail has no issues", () => {
    const e = errorFromResponse(422, JSON.stringify({ detail: "nope" }), new Headers()) as ValidationError;
    expect(e.issues).toEqual([]);
    expect(e.message).toBe("nope");
  });
});

describe("Retry-After", () => {
  it("parses delta-seconds", () => {
    expect(parseRetryAfter("7")).toBe(7);
    expect(parseRetryAfter("0")).toBe(0);
    expect(parseRetryAfter("1.5")).toBe(1.5);
  });

  it("parses an HTTP date relative to now", () => {
    const now = Date.parse("2026-09-25T12:00:00Z");
    expect(parseRetryAfter("Fri, 25 Sep 2026 12:00:30 GMT", now)).toBe(30);
    expect(parseRetryAfter("Fri, 25 Sep 2026 11:00:00 GMT", now)).toBe(0);
  });

  it("ignores absent or junk values", () => {
    expect(parseRetryAfter(null)).toBeUndefined();
    expect(parseRetryAfter("")).toBeUndefined();
    expect(parseRetryAfter("soon")).toBeUndefined();
  });

  it("RateLimitError carries retryAfter from the header", async () => {
    const { fetch } = scriptedFetch(json({ detail: "rate limit exceeded" }, 429, { "retry-after": "120" }));
    const err = (await client(fetch)
      .add("x")
      .catch((e: unknown) => e)) as RateLimitError;
    expect(err).toBeInstanceOf(RateLimitError);
    expect(err.retryAfter).toBe(120);
  });

  it("RateLimitError from memd's real 429 (whole-second Retry-After, code)", async () => {
    const { fetch } = scriptedFetch(
      json({ detail: "find_ids rate limit exceeded; retry later", code: "rate_limited" }, 429, { "Retry-After": "6" }),
    );
    const err = (await client(fetch, { retries: 0 })
      .findIds("q")
      .catch((e: unknown) => e)) as RateLimitError;
    expect(err).toBeInstanceOf(RateLimitError);
    expect(err.code).toBe("rate_limited");
    expect(err.retryAfter).toBe(6);
    expect(err.message).toBe("find_ids rate limit exceeded; retry later");
  });

  it("retryAfter is undefined when the response has no Retry-After", async () => {
    const { fetch } = scriptedFetch(json({ detail: "rate limit exceeded" }, 429));
    const err = (await client(fetch, { retries: 0 })
      .stats()
      .catch((e: unknown) => e)) as RateLimitError;
    expect(err.retryAfter).toBeUndefined();
  });
});

describe("malformed success bodies", () => {
  it("a 200 that is not JSON is a MemdError, not a SyntaxError", async () => {
    const { fetch, calls } = scriptedFetch(new Response("<html>captive portal</html>", { status: 200 }));
    const err = (await client(fetch)
      .stats()
      .catch((e: unknown) => e)) as MemdError;
    expect(err).toBeInstanceOf(MemdError);
    expect(err.code).toBe("invalid_response");
    expect(err.cause).toBeInstanceOf(SyntaxError);
    expect(calls).toHaveLength(1); // not retried
  });
});
