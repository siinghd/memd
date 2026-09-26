/**
 * `MemdClient`: the memd REST API over the platform's global `fetch`.
 *
 * Method names and semantics mirror the Python SDK's `HostedMemory`
 * (src/memd/sdk/client.py). Only standard web APIs are used (fetch,
 * AbortController, TextDecoder, setTimeout), so the same build runs on
 * Node >= 18, Bun, Deno, Cloudflare Workers and other edge runtimes.
 */
import {
  MemdError,
  NetworkError,
  NotFoundError,
  RequestAbortedError,
  RequestTimeoutError,
  errorFromResponse,
} from "./errors.js";
import { injectContext, lastUserText, turnEvents } from "./messages.js";
import type {
  ChatMessage,
  CloseSessionResult,
  CompactionReport,
  EventIn,
  EventsAccepted,
  FindIdsResult,
  FindIn,
  ForgetPreview,
  ForgetResult,
  HealthResult,
  MemoryCreated,
  MemoryIn,
  MemoryRecord,
  NamespaceStats,
  PackedContextMessage,
  ReembedResult,
  ScopeFields,
  SearchIn,
  SearchResult,
  ServerStatus,
} from "./types.js";

/** The subset of `fetch` the client calls. Inject one for tests or custom agents. */
export type FetchLike = (input: string, init: RequestInit) => Promise<Response>;

export interface MemdClientOptions {
  /** Bearer key: a per-namespace key from `memd key create`, or the admin key. */
  apiKey: string;
  /** Default `http://localhost:8700`. */
  baseUrl?: string;
  /** Namespace for calls that don't pass one. Default `"default"`. */
  namespace?: string;
  /** Per-attempt timeout in milliseconds. Default 30000; `0` disables it. */
  timeoutMs?: number;
  /**
   * Retries for idempotent requests on 429, 5xx, timeouts and network
   * errors. Default 2. Writes are never retried: the API has no
   * idempotency key, so a retried write could store twice.
   */
  retries?: number;
  /** First backoff step in milliseconds (doubles per attempt, with jitter). Default 250. */
  retryBaseDelayMs?: number;
  /** Backoff ceiling in milliseconds. Default 8000. */
  retryMaxDelayMs?: number;
  /** Replaces the global `fetch`. */
  fetch?: FetchLike;
  /** Extra headers sent with every request (for a gateway in front of memd, say). */
  headers?: Record<string, string>;
}

/** Per-call options every method accepts. */
export interface RequestOptions {
  /** Overrides the client's namespace for this call. */
  namespace?: string;
  signal?: AbortSignal;
  timeoutMs?: number;
  /** Overrides the client's retry count (idempotent requests only). */
  retries?: number;
}

export type AddOptions = Omit<EventIn, "content"> & RequestOptions;
export type RememberOptions = Omit<MemoryIn, "content"> & RequestOptions;
export type SearchOptions = Omit<SearchIn, "query"> & RequestOptions;
export type FindOptions = Omit<FindIn, "query" | "confirm" | "fingerprint"> & RequestOptions;
export type ForgetOptions = FindOptions & {
  /**
   * `true` deletes; the preview itself (what an unconfirmed `forget`
   * returned) deletes and sends its `fingerprint`, so the server deletes
   * nothing if the matches changed since.
   */
  confirm?: boolean | ForgetPreview;
  /** A preview's fingerprint, when confirming with `confirm: true`. */
  fingerprint?: string;
};
export type ObserveOptions = ScopeFields & RequestOptions;
export interface GetOptions extends RequestOptions {
  /** Include the supersedence chain (superseded versions; deleted ones only with `include_deleted`). */
  history?: boolean;
  /** Serve a deleted record, or deleted versions in its history. Override-capable (admin) keys only: 403 otherwise. */
  include_deleted?: boolean;
}
export interface DeleteOptions extends RequestOptions {
  /** Hard delete: purged from storage within the deadline, not just tombstoned. */
  hard?: boolean;
}
export interface CompactOptions extends RequestOptions {
  force?: boolean;
}

type Method = "GET" | "POST" | "DELETE";

interface CallSpec {
  method: Method;
  path: string;
  query?: Record<string, string | number | boolean | undefined>;
  body?: unknown;
  /** Safe to retry: repeating it cannot change server state beyond the first success. */
  idempotent: boolean;
  accept?: string;
}

type Attempt<T> = { ok: true; value: T } | { ok: false; error: MemdError; retryable: boolean };

const DEFAULT_BASE_URL = "http://localhost:8700";
const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_RETRIES = 2;
/** A server asking for a longer pause than this gets the error back instead. */
const MAX_RETRY_AFTER_MS = 60_000;
const RETRYABLE_STATUS = new Set([429, 500, 502, 503, 504]);
/** `SearchIn.query` max_length: `pack()` trims a longer user message to it. */
const MAX_QUERY_CHARS = 10_000;

const EVENT_KEYS = [
  "content", "role", "session_id", "user_id", "agent_id", "org_id",
  "source", "actor_id", "t_event", "kind", "meta",
] as const;
const MEMORY_KEYS = [
  "kind", "entity_keys", "session_id", "user_id", "agent_id", "org_id",
  "source", "actor_id", "t_event", "valid_from",
] as const;
const SEARCH_KEYS = [
  "user_id", "session_id", "agent_id", "org_id", "budget_tokens", "as_of", "kinds",
  "include_quarantined",
] as const;
const FIND_KEYS = ["user_id", "session_id", "agent_id", "org_id", "as_of", "kinds"] as const;

/** Copy the listed wire fields that are set (`null`/`undefined` mean "server default"). */
function pick(src: object, keys: readonly string[]): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  const rec = src as Record<string, unknown>;
  for (const k of keys) {
    const v = rec[k];
    if (v !== undefined && v !== null) out[k] = v;
  }
  return out;
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(new RequestAbortedError(signal.reason));
      return;
    }
    const onAbort = () => {
      clearTimeout(timer);
      reject(new RequestAbortedError(signal?.reason));
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

/** Cut to at most `max` code points without splitting a surrogate pair. */
function clampChars(s: string, max: number): string {
  if (s.length <= max) return s;
  return Array.from(s).slice(0, max).join("");
}

export class MemdClient {
  readonly baseUrl: string;
  readonly namespace: string;
  readonly timeoutMs: number;
  readonly retries: number;
  private readonly apiKey: string;
  private readonly retryBaseDelayMs: number;
  private readonly retryMaxDelayMs: number;
  private readonly fetchImpl: FetchLike;
  private readonly extraHeaders: Record<string, string>;

  constructor(options: MemdClientOptions) {
    if (!options || typeof options.apiKey !== "string" || options.apiKey === "") {
      throw new TypeError("MemdClient: apiKey is required");
    }
    this.apiKey = options.apiKey;
    this.baseUrl = (options.baseUrl ?? DEFAULT_BASE_URL).replace(/\/+$/, "");
    this.namespace = options.namespace ?? "default";
    this.timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    this.retries = Math.max(0, Math.floor(options.retries ?? DEFAULT_RETRIES));
    this.retryBaseDelayMs = options.retryBaseDelayMs ?? 250;
    this.retryMaxDelayMs = options.retryMaxDelayMs ?? 8_000;
    this.extraHeaders = { ...(options.headers ?? {}) };
    const injected = options.fetch;
    // Never store the global fetch unbound: calling it as a method of another
    // object throws "Illegal invocation" on Workers and in browsers.
    this.fetchImpl = injected ?? ((input, init) => globalThis.fetch(input, init));
  }

  // -- writes ------------------------------------------------------------

  /** Capture one raw-lane event. Returns the new record ids. Not retried. */
  async add(content: string, options: AddOptions = {}): Promise<string[]> {
    return this.addEvents([{ ...pick(options, EVENT_KEYS), content } as EventIn], options);
  }

  /** Capture a batch of 1 to 1000 raw-lane events in one durable append. Not retried. */
  async addEvents(events: readonly EventIn[], options: RequestOptions = {}): Promise<string[]> {
    const body = { events: events.map((e) => pick(e, EVENT_KEYS)) };
    const out = await this.json<EventsAccepted>(
      { method: "POST", path: this.nsPath(options, "/events"), body, idempotent: false },
      options,
    );
    return out.ids;
  }

  /** Write to the explicit lane ("remember this"). Returns the record id. Not retried. */
  async remember(content: string, options: RememberOptions = {}): Promise<string> {
    const body = { ...pick(options, MEMORY_KEYS), content };
    const out = await this.json<MemoryCreated>(
      { method: "POST", path: this.nsPath(options, "/memories"), body, idempotent: false },
      options,
    );
    return out.id;
  }

  /**
   * Capture a whole turn after the LLM call: every message with text content
   * plus the response as `assistant`, in one batch. Returns the record ids
   * (`[]`, with no request, when there is no text to store).
   */
  async observe(
    messages: readonly ChatMessage[],
    response: string | ChatMessage,
    options: ObserveOptions = {},
  ): Promise<string[]> {
    const events = turnEvents(messages, response, options);
    if (events.length === 0) return [];
    return this.addEvents(events, options);
  }

  // -- reads -------------------------------------------------------------

  /** Hybrid retrieval, packed to `budget_tokens`. */
  async search(query: string, options: SearchOptions = {}): Promise<SearchResult> {
    const body = { ...pick(options, SEARCH_KEYS), query };
    return this.json<SearchResult>(
      { method: "POST", path: this.nsPath(options, "/search"), body, idempotent: true },
      options,
    );
  }

  /**
   * Inject packed memory context before the LLM call. Searches with the last
   * user message and, when anything matches, inserts one system message
   * after the leading system messages. Otherwise returns a copy of `messages`.
   */
  async pack<M extends ChatMessage>(
    messages: readonly M[],
    options: SearchOptions = {},
  ): Promise<Array<M | PackedContextMessage>> {
    const query = lastUserText(messages);
    if (!query) return [...messages];
    const res = await this.search(clampChars(query, MAX_QUERY_CHARS), options);
    if (res.items.length === 0) return [...messages];
    return injectContext(messages, res.packed_context);
  }

  /** One record by id, or `null` when it does not exist (or is deleted, unless `include_deleted`). */
  async get(recordId: string, options: GetOptions = {}): Promise<MemoryRecord | null> {
    try {
      return await this.json<MemoryRecord>(
        {
          method: "GET",
          path: this.nsPath(options, `/memories/${encodeURIComponent(recordId)}`),
          query: { history: options.history, include_deleted: options.include_deleted },
          idempotent: true,
        },
        options,
      );
    } catch (err) {
      if (err instanceof NotFoundError) return null;
      throw err;
    }
  }

  /** Ids matching a query, unbounded by any packing budget (the forget sweep's view). */
  async findIds(query: string, options: FindOptions = {}): Promise<string[]> {
    const body = { ...pick(options, FIND_KEYS), query };
    const out = await this.json<FindIdsResult>(
      { method: "POST", path: this.nsPath(options, "/find_ids"), body, idempotent: true },
      options,
    );
    return out.ids;
  }

  /** Namespace statistics. */
  async stats(options: RequestOptions = {}): Promise<NamespaceStats> {
    return this.json<NamespaceStats>(
      { method: "GET", path: this.nsPath(options, "/stats"), idempotent: true },
      options,
    );
  }

  /** Engine status; a namespace key sees only its own namespace. */
  async status(options: RequestOptions = {}): Promise<ServerStatus> {
    return this.json<ServerStatus>({ method: "GET", path: "/v1/status", idempotent: true }, options);
  }

  /** Liveness probe (no auth needed). */
  async health(options: RequestOptions = {}): Promise<HealthResult> {
    return this.json<HealthResult>({ method: "GET", path: "/health", idempotent: true }, options);
  }

  /** Every live record in the namespace, parsed. Use `exportStream()` for large namespaces. */
  async export(options: RequestOptions = {}): Promise<MemoryRecord[]> {
    const text = await this.exportJsonl(options);
    const out: MemoryRecord[] = [];
    for (const line of text.split("\n")) {
      if (line.trim()) out.push(JSON.parse(line) as MemoryRecord);
    }
    return out;
  }

  /** The namespace export as raw NDJSON (one record per line): the anti-lock-in format. */
  async exportJsonl(options: RequestOptions = {}): Promise<string> {
    return (await this.execute(this.exportSpec(options), options, "text")) as string;
  }

  /** Stream the export record by record without buffering the namespace in memory. */
  async *exportStream(options: RequestOptions = {}): AsyncGenerator<MemoryRecord, void, undefined> {
    const { res, release } = (await this.execute(this.exportSpec(options), options, "stream")) as {
      res: Response;
      release: () => void;
    };
    try {
      if (!res.body) {
        for (const line of (await res.text()).split("\n")) {
          if (line.trim()) yield JSON.parse(line) as MemoryRecord;
        }
        return;
      }
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buf = "";
      try {
        for (;;) {
          let chunk: ReadableStreamReadResult<Uint8Array>;
          try {
            chunk = await reader.read();
          } catch (err) {
            if (options.signal?.aborted) throw new RequestAbortedError(options.signal.reason);
            throw new NetworkError("export stream interrupted", err);
          }
          if (chunk.done) break;
          buf += decoder.decode(chunk.value, { stream: true });
          let nl: number;
          while ((nl = buf.indexOf("\n")) >= 0) {
            const line = buf.slice(0, nl);
            buf = buf.slice(nl + 1);
            if (line.trim()) yield JSON.parse(line) as MemoryRecord;
          }
        }
        buf += decoder.decode();
        if (buf.trim()) yield JSON.parse(buf) as MemoryRecord;
      } finally {
        // a consumer that stops early must not leave the connection open
        await reader.cancel().catch(() => undefined);
      }
    } finally {
      release();
    }
  }

  // -- lifecycle ---------------------------------------------------------

  /**
   * Delete a record. Soft by default (tombstoned now, physically purged at the
   * next compaction); `hard: true` purges it. Returns `false` when the record
   * does not exist. Retried like other idempotent calls, so after a lost
   * response `false` can also mean an earlier attempt already deleted it.
   */
  async delete(recordId: string, options: DeleteOptions = {}): Promise<boolean> {
    try {
      await this.json<unknown>(
        {
          method: "DELETE",
          path: this.nsPath(options, `/memories/${encodeURIComponent(recordId)}`),
          query: { hard: options.hard },
          idempotent: true,
        },
        options,
      );
      return true;
    } catch (err) {
      if (err instanceof NotFoundError) return false;
      throw err;
    }
  }

  /**
   * Query-driven deletion, two-phase like the MCP tool. Without `confirm`
   * returns a preview of what WOULD be deleted (retried like any read). To
   * delete, pass that preview back as `confirm` with the same query and
   * filters: its `fingerprint` goes along, and if the matches changed since,
   * the server deletes nothing and this throws `ForgetPreviewMismatchError`.
   * `confirm: true` deletes without that check. Returns the deleted ids;
   * never retried.
   *
   * ```ts
   * const preview = await memd.forget("wifi password", { user_id: "u1" });
   * const deleted = await memd.forget("wifi password", { user_id: "u1", confirm: preview });
   * ```
   */
  async forget(
    query: string,
    options: FindOptions & { confirm: true | ForgetPreview; fingerprint?: string },
  ): Promise<string[]>;
  async forget(query: string, options?: FindOptions & { confirm?: false }): Promise<ForgetPreview>;
  async forget(query: string, options?: ForgetOptions): Promise<string[] | ForgetPreview>;
  async forget(query: string, options: ForgetOptions = {}): Promise<string[] | ForgetPreview> {
    const preview = typeof options.confirm === "object" && options.confirm !== null ? options.confirm : undefined;
    const confirm = options.confirm === true || preview !== undefined;
    const fingerprint = options.fingerprint ?? preview?.fingerprint;
    const body: Record<string, unknown> = { ...pick(options, FIND_KEYS), query, confirm };
    if (confirm && fingerprint) body["fingerprint"] = fingerprint;
    const out = await this.json<ForgetPreview | ForgetResult>(
      // the preview only reads; the confirmed sweep deletes
      { method: "POST", path: this.nsPath(options, "/forget"), body, idempotent: !confirm },
      options,
    );
    return out.confirmed ? out.deleted : out;
  }

  /** Session boundary: extract facts, consolidate, rotate the segment. Not retried. */
  async closeSession(sessionId: string, options: RequestOptions = {}): Promise<CloseSessionResult> {
    return this.json<CloseSessionResult>(
      {
        method: "POST",
        path: this.nsPath(options, `/sessions/${encodeURIComponent(sessionId)}/close`),
        idempotent: false,
      },
      options,
    );
  }

  /** Fold segments and enforce purge deadlines. Not retried. */
  async compact(options: CompactOptions = {}): Promise<CompactionReport> {
    return this.json<CompactionReport>(
      { method: "POST", path: this.nsPath(options, "/compact"), query: { force: options.force }, idempotent: false },
      options,
    );
  }

  /** Rebuild the vector lane for records missing a current embedding. Not retried. */
  async reembed(options: RequestOptions = {}): Promise<ReembedResult> {
    return this.json<ReembedResult>(
      { method: "POST", path: this.nsPath(options, "/reembed"), idempotent: false },
      options,
    );
  }

  /**
   * Crypto-shred a whole namespace (default: the client's). Needs an
   * override-capable (admin) key. Not retried.
   */
  async destroyNamespace(namespace?: string, options: RequestOptions = {}): Promise<boolean> {
    const ns = namespace ?? options.namespace ?? this.namespace;
    try {
      await this.json<unknown>(
        { method: "DELETE", path: `/v1/ns/${encodeURIComponent(ns)}`, idempotent: false },
        options,
      );
      return true;
    } catch (err) {
      if (err instanceof NotFoundError) return false;
      throw err;
    }
  }

  // -- transport ---------------------------------------------------------

  private nsPath(options: RequestOptions, suffix: string): string {
    const ns = options.namespace ?? this.namespace;
    return `/v1/ns/${encodeURIComponent(ns)}${suffix}`;
  }

  private exportSpec(options: RequestOptions): CallSpec {
    // read-only, so safe to retry (it only spends the export budget)
    return {
      method: "POST",
      path: this.nsPath(options, "/export"),
      idempotent: true,
      accept: "application/x-ndjson",
    };
  }

  private async json<T>(spec: CallSpec, options: RequestOptions): Promise<T> {
    return (await this.execute(spec, options, "json")) as T;
  }

  private url(spec: CallSpec): string {
    let url = this.baseUrl + spec.path;
    const params: string[] = [];
    for (const [k, v] of Object.entries(spec.query ?? {})) {
      if (v !== undefined) params.push(`${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`);
    }
    if (params.length) url += `?${params.join("&")}`;
    return url;
  }

  private async execute(spec: CallSpec, options: RequestOptions, read: "json" | "text" | "stream"): Promise<unknown> {
    const url = this.url(spec);
    // lowercase keys: `Authorization` and `authorization` must not both reach
    // the wire (Headers would join them into one comma-separated value)
    const headers: Record<string, string> = {};
    for (const [k, v] of Object.entries(this.extraHeaders)) headers[k.toLowerCase()] = v;
    headers["authorization"] = `Bearer ${this.apiKey}`;
    headers["accept"] = spec.accept ?? "application/json";
    let body: string | undefined;
    if (spec.body !== undefined) {
      body = JSON.stringify(spec.body);
      headers["content-type"] = "application/json";
    }
    const init: RequestInit = { method: spec.method, headers };
    if (body !== undefined) init.body = body;
    const retries = spec.idempotent ? Math.max(0, Math.floor(options.retries ?? this.retries)) : 0;
    const timeoutMs = options.timeoutMs ?? this.timeoutMs;

    for (let attempt = 0; ; attempt++) {
      if (options.signal?.aborted) throw new RequestAbortedError(options.signal.reason);
      const outcome = await this.attempt(url, init, timeoutMs, options.signal, read);
      if (outcome.ok) return outcome.value;
      if (outcome.retryable && attempt < retries) {
        const delay = this.backoff(attempt, outcome.error.retryAfter);
        if (delay !== undefined) {
          await sleep(delay, options.signal);
          continue;
        }
      }
      throw outcome.error;
    }
  }

  /** One HTTP exchange under its own timeout, linked to the caller's signal. */
  private async attempt(
    url: string,
    init: RequestInit,
    timeoutMs: number,
    signal: AbortSignal | undefined,
    read: "json" | "text" | "stream",
  ): Promise<Attempt<unknown>> {
    const ctrl = new AbortController();
    let timedOut = false;
    const timer =
      timeoutMs > 0
        ? setTimeout(() => {
            timedOut = true;
            ctrl.abort();
          }, timeoutMs)
        : undefined;
    const onAbort = () => ctrl.abort();
    signal?.addEventListener("abort", onAbort, { once: true });
    let streaming = false;
    const release = () => signal?.removeEventListener("abort", onAbort);
    try {
      const res = await this.fetchImpl(url, { ...init, signal: ctrl.signal });
      if (!res.ok) {
        const text = await res.text().catch(() => "");
        return {
          ok: false,
          error: errorFromResponse(res.status, text, res.headers),
          retryable: RETRYABLE_STATUS.has(res.status),
        };
      }
      if (read === "stream") {
        // headers are in: the timeout bounded time-to-first-byte; the
        // caller's signal keeps governing the body until release()
        clearTimeout(timer);
        streaming = true;
        return { ok: true, value: { res, release } };
      }
      const text = await res.text();
      if (read === "text") return { ok: true, value: text };
      if (text === "") return { ok: true, value: undefined };
      try {
        return { ok: true, value: JSON.parse(text) };
      } catch (err) {
        return {
          ok: false,
          error: new MemdError({
            status: res.status,
            code: "invalid_response",
            message: "response body is not JSON",
            detail: text.slice(0, 500),
            cause: err,
          }),
          retryable: false,
        };
      }
    } catch (err) {
      if (signal?.aborted) return { ok: false, error: new RequestAbortedError(signal.reason), retryable: false };
      if (timedOut) return { ok: false, error: new RequestTimeoutError(timeoutMs, err), retryable: true };
      const msg = err instanceof Error ? err.message : String(err);
      return { ok: false, error: new NetworkError(`network error: ${msg}`, err), retryable: true };
    } finally {
      clearTimeout(timer);
      if (!streaming) release();
    }
  }

  /** Delay before the next attempt, or `undefined` to give up now. */
  private backoff(attempt: number, retryAfter: number | undefined): number | undefined {
    if (retryAfter !== undefined) {
      const ms = retryAfter * 1000;
      return ms <= MAX_RETRY_AFTER_MS ? ms : undefined;
    }
    // exponential with "equal jitter": at least half the step, never in lockstep
    const step = Math.min(this.retryMaxDelayMs, this.retryBaseDelayMs * 2 ** attempt);
    return step / 2 + Math.random() * (step / 2);
  }
}
