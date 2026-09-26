/**
 * Wire models for the memd REST API (src/memd/server/http.py).
 *
 * Field names are the server's, snake_case, exactly as they travel over the
 * wire: request types mirror the pydantic models (`EventIn`, `MemoryIn`,
 * `SearchIn`, `FindIn`), response types mirror what each handler returns.
 */

/** The closed kind vocabulary (`memd.core.schema.Kind.ALL`). */
export const KINDS = ["raw_event", "fact", "link", "procedure", "summary", "pin"] as const;
export type Kind = (typeof KINDS)[number];

/**
 * Trust tiers, highest first (`memd.core.schema.Source`). Only an
 * override-capable key may assert `user` (or `agent` on the raw lane); the
 * server silently downgrades the claim for every other key.
 */
export type Source = "user" | "agent" | "tool" | "web" | "import";

/** Chat role. `user` maps to the user tier, `assistant`/`agent` to agent, anything else to tool. */
export type Role = "user" | "assistant" | "agent" | "system" | "tool" | (string & {});

/** Scope filters shared by writes and reads (a hierarchy: org > agent > user > session). */
export interface ScopeFields {
  user_id?: string | null;
  session_id?: string | null;
  agent_id?: string | null;
  org_id?: string | null;
}

// ---------------------------------------------------------------- requests

/** One raw-lane event: an item of `POST /v1/ns/{ns}/events` (pydantic `EventIn`). */
export interface EventIn extends ScopeFields {
  /** 1 to 1,000,000 characters. */
  content: string;
  /** Default `"user"`. */
  role?: Role;
  source?: Source | null;
  actor_id?: string | null;
  /** Epoch milliseconds; defaults to ingest time. */
  t_event?: number | null;
  /** Default `"raw_event"`. */
  kind?: Kind;
  /** At most 64 KiB once serialized. */
  meta?: Record<string, unknown> | null;
}

/** Body of `POST /v1/ns/{ns}/events` (pydantic `EventsIn`): 1 to 1000 events. */
export interface EventsIn {
  events: EventIn[];
}

/** Body of `POST /v1/ns/{ns}/memories` (pydantic `MemoryIn`): the explicit lane. */
export interface MemoryIn extends ScopeFields {
  /** 1 to 100,000 characters. */
  content: string;
  /** Default `"fact"`. */
  kind?: Kind;
  /** Entity keys drive supersedence: a new fact on a key supersedes the old cluster. At most 8 are kept. */
  entity_keys?: string[] | null;
  /** Default `"agent"`. */
  source?: Source;
  actor_id?: string | null;
  t_event?: number | null;
  valid_from?: number | null;
}

/** Body of `POST /v1/ns/{ns}/search` (pydantic `SearchIn`). */
export interface SearchIn extends ScopeFields {
  /** 1 to 10,000 characters. */
  query: string;
  /** Packing budget, 64 to 128,000 tokens. Default 2000. */
  budget_tokens?: number;
  /** Time travel: epoch milliseconds. */
  as_of?: number | null;
  kinds?: Kind[] | null;
  include_quarantined?: boolean;
}

/** Body of `POST /v1/ns/{ns}/find_ids` and `POST /v1/ns/{ns}/forget` (pydantic `FindIn`). */
export interface FindIn extends ScopeFields {
  query: string;
  as_of?: number | null;
  kinds?: Kind[] | null;
  /** `forget` only: without it the server returns a preview. */
  confirm?: boolean;
  /**
   * `forget` confirm only: the preview's `fingerprint`. The server refuses
   * (409 `preview_mismatch`) and deletes nothing if the confirm would delete
   * a different set.
   */
  fingerprint?: string | null;
}

// --------------------------------------------------------------- responses

/** `202` from `POST /v1/ns/{ns}/events`. */
export interface EventsAccepted {
  ids: string[];
  accepted: number;
}

/** `201` from `POST /v1/ns/{ns}/memories`. */
export interface MemoryCreated {
  id: string;
}

/** One ranked hit (`memd.engine.memory.SearchHit`). */
export interface SearchHit {
  id: string;
  content: string;
  kind: Kind;
  source: Source;
  actor_id: string | null;
  t_event: number;
  valid: boolean;
  score: number;
  /** Retrieval lanes that surfaced the hit, e.g. `["bm25", "vector"]`. */
  lanes: string[];
  entity_keys: string[];
  namespace: string;
}

/** `POST /v1/ns/{ns}/search` (`memd.engine.memory.SearchResult`). */
export interface SearchResult {
  /** Provenance-tagged context, ready to inject into a prompt. */
  packed_context: string;
  items: SearchHit[];
  tokens_used: number;
  budget: number;
  truncated: boolean;
  query_class: string;
  latency_ms: number;
}

export interface RecordScope {
  org?: string;
  agent?: string;
  user?: string;
  session?: string;
}

export interface Provenance {
  source: Source;
  actor_id: string | null;
  session_id: string | null;
  /** Ids of the records this one was derived from. */
  lineage: string[];
  extractor: { model: string; prompt_version: string } | null;
}

/** Bitemporal axis, all epoch milliseconds. */
export interface TimeAxis {
  t_event: number;
  t_ingested: number;
  valid_from: number | null;
  invalidated_at: number | null;
  superseded_by: string | null;
}

/** A stored record (`MemoryRecord.to_dict()`), as `GET /memories/{id}` and export return it. */
export interface MemoryRecord {
  id: string;
  namespace: string;
  kind: Kind;
  content: string;
  scope: RecordScope;
  provenance: Provenance;
  time: TimeAxis;
  entity_keys: string[];
  embedding_version: string | null;
  meta: Record<string, unknown>;
  deleted: boolean;
  /** Present when fetched with `history: true`: the supersedence chain. */
  history?: MemoryRecord[];
}

/** `DELETE /v1/ns/{ns}/memories/{id}`. */
export interface DeleteResult {
  deleted: string;
  hard: boolean;
  note: string;
}

/** `DELETE /v1/ns/{ns}` (crypto-shred). */
export interface DestroyResult {
  destroyed: string;
  crypto_shred: boolean;
}

/** `POST /v1/ns/{ns}/find_ids`. */
export interface FindIdsResult {
  ids: string[];
}

/** `POST /v1/ns/{ns}/forget` without `confirm`: what WOULD be deleted. */
export interface ForgetPreview {
  /** The first 20 matches, content cut to 200 characters. */
  will_delete: Array<{ id: string; content: string }>;
  count: number;
  confirmed: false;
  /** Identifies the full matched set; pass the preview back as `confirm` to send it. */
  fingerprint: string;
}

/** `POST /v1/ns/{ns}/forget` with `confirm: true`. */
export interface ForgetResult {
  deleted: string[];
  count: number;
  confirmed: true;
}

/** `POST /v1/ns/{ns}/sessions/{sid}/close`. */
export interface CloseSessionResult {
  segment: string;
  raw_considered: number;
  facts_extracted: number;
  facts_written: number;
  superseded: number;
  dupes_dropped: number;
}

/** `POST /v1/ns/{ns}/compact` (`memd.storage.engine.CompactionReport`). */
export interface CompactionReport {
  segments_in: number;
  segments_out: number;
  records_folded: number;
  records_purged: number;
  hard_deleted_purged: number;
  bytes_before: number;
  bytes_after: number;
  duration_ms: number;
}

/** `POST /v1/ns/{ns}/reembed`. */
export interface ReembedResult {
  namespace: string;
  missing: number;
  embedded: number;
  model: string;
}

/** `GET /v1/ns/{ns}/stats`. Diagnostic fields beyond these may appear. */
export interface NamespaceStats {
  namespace: string;
  records: number;
  tombstones: number;
  facts: number;
  superseded: number;
  quarantined: number;
  vectors: number;
  vec_dim: number | null;
  embedding_model: string | null;
  links: number;
  segments: number;
  wal_bytes: number;
  segment_bytes: number;
  segments_collected: number;
  snapshots_collected: number;
  embedder: string;
  embedder_kind: string;
  embedder_ready: boolean;
  embed_pending: number;
  fuse_vector: boolean;
  reranker: { name: string; [key: string]: unknown };
  pack_mode: string;
  lexical: { backend: string; [key: string]: unknown };
  extractor: string;
  [key: string]: unknown;
}

/** `GET /v1/status`: scoped to the key's namespace unless it is an admin key. */
export interface ServerStatus {
  mode: string;
  namespaces: string[];
  default_namespace: string;
  embedder: string;
  extractor: string;
  version: string;
}

/** `GET /health` (unauthenticated). */
export interface HealthResult {
  ok: boolean;
  version: string;
}

// ------------------------------------------------------------ chat glue

/**
 * An OpenAI-style chat message. `content` may be a string or an array of
 * parts; text parts (`{ type: "text", text }` or bare strings) are what memd
 * stores and searches.
 */
export interface ChatMessage {
  role: string;
  content?: unknown;
}

/** The system message `pack()` inserts. */
export interface PackedContextMessage {
  role: "system";
  content: string;
}
