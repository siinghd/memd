/**
 * Chat glue shared by `pack()` and `observe()`: the same OpenAI-style message
 * shape the Python SDK takes (`{ role, content }`).
 */
import type { ChatMessage, EventIn, PackedContextMessage, ScopeFields } from "./types.js";

/**
 * The text of a message's `content`. Strings pass through; an array of parts
 * (OpenAI / Anthropic content blocks) contributes its text parts, joined by
 * newlines. Images, tool calls and other non-text parts are skipped.
 */
export function messageText(content: unknown): string {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    const parts: string[] = [];
    for (const part of content) {
      if (typeof part === "string") {
        parts.push(part);
      } else if (part && typeof part === "object" && typeof (part as { text?: unknown }).text === "string") {
        const type = (part as { type?: unknown }).type;
        if (type === undefined || type === "text" || type === "input_text" || type === "output_text") {
          parts.push((part as { text: string }).text);
        }
      }
    }
    return parts.filter((p) => p !== "").join("\n");
  }
  return "";
}

/** The text of the last `user` message, or `""` when there is none. */
export function lastUserText(messages: readonly ChatMessage[]): string {
  for (let i = messages.length - 1; i >= 0; i--) {
    const m = messages[i];
    if (m && m.role === "user") return messageText(m.content);
  }
  return "";
}

/**
 * `messages` with one system message holding `packedContext`, inserted after
 * the leading run of system messages (the Python SDK's placement: the
 * operator's own system prompt stays first).
 */
export function injectContext<M extends ChatMessage>(
  messages: readonly M[],
  packedContext: string,
): Array<M | PackedContextMessage> {
  const out: Array<M | PackedContextMessage> = [...messages];
  let insertAt = 0;
  for (const m of messages) {
    if (m.role !== "system") break;
    insertAt++;
  }
  out.splice(insertAt, 0, { role: "system", content: packedContext });
  return out;
}

const SCOPE_KEYS = ["session_id", "user_id", "agent_id", "org_id"] as const;

/**
 * The raw-lane events for one turn: every message with text content (role
 * defaults to `user`), then the model's response as `assistant`.
 */
export function turnEvents(
  messages: readonly ChatMessage[],
  response: string | ChatMessage,
  scope: ScopeFields,
): EventIn[] {
  const shared: ScopeFields = {};
  for (const k of SCOPE_KEYS) {
    const v = scope[k];
    if (v != null) shared[k] = v;
  }
  const events: EventIn[] = [];
  for (const m of messages) {
    if (!m || typeof m !== "object") continue;
    const text = messageText(m.content);
    if (!text) continue;
    events.push({ content: text, role: m.role || "user", ...shared });
  }
  const reply = typeof response === "string" ? response : messageText(response.content);
  if (reply) events.push({ content: reply, role: "assistant", ...shared });
  return events;
}
