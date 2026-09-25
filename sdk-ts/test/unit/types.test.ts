/**
 * Compile-time contract. `npm run typecheck` fails on any regression here
 * (including an `@ts-expect-error` that stops erroring); at runtime vitest
 * only checks that the file loads.
 */
import { describe, expectTypeOf, it } from "vitest";
import {
  KINDS,
  MemdClient,
  MemdError,
  RateLimitError,
  ValidationError,
  type EventIn,
  type ForgetPreview,
  type Kind,
  type MemoryRecord,
  type NamespaceStats,
  type PackedContextMessage,
  type SearchHit,
  type SearchResult,
  type ValidationIssue,
} from "../../src/index.js";

const c = new MemdClient({ apiKey: "k", fetch: async () => new Response("{}") });

/** Type-level only: the body is compiled, never run. */
function typeOnly(_check: () => void): void {}

describe("types", () => {
  it("method return types", () => {
    expectTypeOf(c.add).returns.resolves.toEqualTypeOf<string[]>();
    expectTypeOf(c.addEvents).returns.resolves.toEqualTypeOf<string[]>();
    expectTypeOf(c.remember).returns.resolves.toEqualTypeOf<string>();
    expectTypeOf(c.search).returns.resolves.toEqualTypeOf<SearchResult>();
    expectTypeOf(c.get).returns.resolves.toEqualTypeOf<MemoryRecord | null>();
    expectTypeOf(c.delete).returns.resolves.toEqualTypeOf<boolean>();
    expectTypeOf(c.export).returns.resolves.toEqualTypeOf<MemoryRecord[]>();
    expectTypeOf(c.exportJsonl).returns.resolves.toEqualTypeOf<string>();
    expectTypeOf(c.stats).returns.resolves.toEqualTypeOf<NamespaceStats>();
    expectTypeOf(c.destroyNamespace).returns.resolves.toEqualTypeOf<boolean>();
    expectTypeOf<SearchResult["items"]>().toEqualTypeOf<SearchHit[]>();
    expectTypeOf<Kind>().toEqualTypeOf<(typeof KINDS)[number]>();
  });

  it("forget's result follows confirm", () => typeOnly(() => {
    expectTypeOf(c.forget("q", { confirm: true })).resolves.toEqualTypeOf<string[]>();
    expectTypeOf(c.forget("q")).resolves.toEqualTypeOf<ForgetPreview>();
    expectTypeOf(c.forget("q", { confirm: false })).resolves.toEqualTypeOf<ForgetPreview>();
    const flag = Math.random() > 0.5;
    expectTypeOf(c.forget("q", { confirm: flag })).resolves.toEqualTypeOf<string[] | ForgetPreview>();
  }));

  it("pack keeps the caller's message type", () => typeOnly(() => {
    type OpenAIish =
      | { role: "system"; content: string }
      | { role: "user"; content: string | Array<{ type: "text"; text: string }> }
      | { role: "assistant"; content: string | null; tool_calls?: unknown[] };
    const msgs: OpenAIish[] = [{ role: "user", content: "hi" }];
    expectTypeOf(c.pack(msgs)).resolves.toEqualTypeOf<Array<OpenAIish | PackedContextMessage>>();
    // the packed message is itself a valid OpenAI-style system message
    expectTypeOf<PackedContextMessage>().toMatchTypeOf<OpenAIish>();
  }));

  it("errors narrow by class", () => {
    expectTypeOf<RateLimitError["retryAfter"]>().toEqualTypeOf<number | undefined>();
    expectTypeOf<ValidationError["issues"]>().toEqualTypeOf<ValidationIssue[]>();
    expectTypeOf<MemdError["status"]>().toEqualTypeOf<number>();
  });

  it("rejects what the server would 422", () => typeOnly(() => {
    // @ts-expect-error kinds is the closed vocabulary
    void c.search("q", { kinds: ["bogus"] });
    // @ts-expect-error source is a known trust tier
    void c.remember("x", { source: "admin" });
    // @ts-expect-error events need content
    const bad: EventIn = { role: "user" };
    void bad;
    // @ts-expect-error apiKey is required
    void new MemdClient({ baseUrl: "http://x" });
    // any role string is allowed (the server maps unknown roles to the tool tier)
    void c.add("x", { role: "developer" });
  }));
});
