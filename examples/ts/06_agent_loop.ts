// A minimal tool-calling agent loop with memd as its memory: the model calls remember/search tools, every turn is captured. A stub model, so it runs offline.
// Run (in examples/ts): python ../local_server.py -- npx tsx 06_agent_loop.ts   (or set MEMD_URL / MEMD_API_KEY / MEMD_NAMESPACE yourself)
import assert from "node:assert/strict";
import { MemdClient, MemdError } from "memd-engine";

const { MEMD_URL, MEMD_API_KEY, MEMD_NAMESPACE = "default" } = process.env;
if (!MEMD_URL || !MEMD_API_KEY) {
  console.error("set MEMD_URL and MEMD_API_KEY, or run under examples/local_server.py");
  process.exit(2);
}

const memd = new MemdClient({ baseUrl: MEMD_URL, apiKey: MEMD_API_KEY, namespace: MEMD_NAMESPACE });
const userId = "u1";

// --- the chat shapes (OpenAI-style function calling) ---------------------------

interface ToolCall {
  id: string;
  type: "function";
  function: { name: string; arguments: string }; // arguments: JSON text, as models send it
}
type Message =
  | { role: "system" | "user"; content: string }
  | { role: "assistant"; content: string; tool_calls?: ToolCall[] }
  | { role: "tool"; tool_call_id: string; content: string };
type AssistantMessage = Extract<Message, { role: "assistant" }>;

// What the model is told it can call: JSON schema, as a real LLM API takes it.
const TOOLS = [
  {
    type: "function",
    function: {
      name: "remember_fact",
      description: "Save a lasting fact about the user. A new fact on the same key replaces the old one.",
      parameters: {
        type: "object",
        properties: {
          fact: { type: "string", description: "One self-contained sentence" },
          key: { type: "string", description: "What the fact is about, e.g. user.editor" },
        },
        required: ["fact"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "search_memory",
      description: "Look up what is known about the user.",
      parameters: { type: "object", properties: { query: { type: "string" } }, required: ["query"] },
    },
  },
] as const;
type ToolName = (typeof TOOLS)[number]["function"]["name"];

// --- STUB MODEL: replace with your LLM call -----------------------------------
// It stands in for e.g. `openai.chat.completions.create({ model, messages, tools })`
// so the example runs offline and deterministically. Its "reasoning" is a few
// string rules; the loop around it is what a real agent does.
let callCount = 0;
async function stubModel(messages: readonly Message[], _tools: typeof TOOLS): Promise<AssistantMessage> {
  const last = messages[messages.length - 1];
  const call = (name: ToolName, args: object): AssistantMessage => ({
    role: "assistant",
    content: "",
    tool_calls: [{ id: `call_${++callCount}`, type: "function", function: { name, arguments: JSON.stringify(args) } }],
  });
  if (last?.role === "tool") {
    // the tool ran: answer from its output
    const output = JSON.parse(last.content) as { saved?: string; hits?: string[]; error?: string };
    if (output.error) return { role: "assistant", content: `Sorry, my memory failed (${output.error}).` };
    if (output.saved) return { role: "assistant", content: `Noted: ${output.saved}` };
    const [first] = output.hits ?? [];
    return { role: "assistant", content: first ? `From memory: ${first}` : "I don't know that yet." };
  }
  const text = last?.role === "user" ? last.content : "";
  const fact = /^remember that my (\w+) is (.+?)\.?$/i.exec(text);
  if (fact?.[1] && fact[2]) {
    const [, thing, value] = fact;
    return call("remember_fact", { fact: `The user's ${thing} is ${value}`, key: `user.${thing.toLowerCase()}` });
  }
  if (text.trim().endsWith("?")) return call("search_memory", { query: text });
  return { role: "assistant", content: "You're welcome." };
}
// --- end of the stub ------------------------------------------------------------

// --- tools: the model's arguments are untrusted input --------------------------
async function runTool(call: ToolCall, sessionId: string): Promise<string> {
  let args: Record<string, unknown>;
  try {
    args = JSON.parse(call.function.arguments) as Record<string, unknown>;
  } catch {
    return JSON.stringify({ error: "arguments are not JSON" }); // tell the model, let it retry
  }
  try {
    switch (call.function.name) {
      case "remember_fact": {
        const { fact, key } = args;
        if (typeof fact !== "string" || !fact.trim()) return JSON.stringify({ error: "fact must be a non-empty string" });
        await memd.remember(fact, {
          user_id: userId,
          session_id: sessionId,
          entity_keys: typeof key === "string" ? [key] : null,
        });
        return JSON.stringify({ saved: fact });
      }
      case "search_memory": {
        const { query } = args;
        if (typeof query !== "string" || !query.trim()) return JSON.stringify({ error: "query must be a non-empty string" });
        // Facts only: the curated, current memory (superseded facts are left out).
        // The raw lane holds the transcript, "remember that ..." lines included.
        // Scoped to the user, not the session: recall spans conversations.
        const res = await memd.search(query, { user_id: userId, kinds: ["fact"], budget_tokens: 500 });
        return JSON.stringify({ hits: res.items.slice(0, 3).map((h) => h.content) });
      }
      default:
        return JSON.stringify({ error: `no tool named ${call.function.name}` });
    }
  } catch (err) {
    // a memd failure becomes a tool error the model can explain, not a crash
    if (err instanceof MemdError) return JSON.stringify({ error: err.code });
    throw err;
  }
}

// --- the loop ---------------------------------------------------------------------
const SYSTEM: Message = { role: "system", content: "You are a helpful assistant with a long-term memory." };
const MAX_STEPS = 5;

/** One user turn: call the model, run the tools it asks for, until it answers. */
async function agentTurn(history: Message[], userText: string, sessionId: string): Promise<string> {
  const user: Message = { role: "user", content: userText };
  const steps: Message[] = [user];
  for (let step = 0; step < MAX_STEPS; step++) {
    const reply = await stubModel([SYSTEM, ...history, ...steps], TOOLS);
    steps.push(reply);
    if (!reply.tool_calls?.length) {
      // capture the turn on the raw lane: the transcript closeSession() extracts from
      await memd.observe([user], reply.content, { user_id: userId, session_id: sessionId });
      history.push(user, reply);
      return reply.content;
    }
    for (const call of reply.tool_calls) {
      const output = await runTool(call, sessionId);
      console.log(`    tool ${call.function.name}(${call.function.arguments}) -> ${output}`);
      steps.push({ role: "tool", tool_call_id: call.id, content: output });
    }
  }
  throw new Error(`no answer after ${MAX_STEPS} steps`);
}

async function chat(sessionId: string, lines: string[]): Promise<string[]> {
  console.log(`session ${sessionId}`);
  const history: Message[] = []; // a new conversation starts empty: memd carries what lasts
  const answers: string[] = [];
  for (const line of lines) {
    console.log(`  user: ${line}`);
    const answer = await agentTurn(history, line, sessionId);
    console.log(`  assistant: ${answer}`);
    answers.push(answer);
  }
  await memd.closeSession(sessionId);
  return answers;
}

const first = await chat("chat-1", ["Remember that my editor is Helix.", "What is my editor?", "Thanks!"]);
assert.match(first[1] ?? "", /^From memory: .* editor is Helix$/);

// A later conversation: no shared history, the memory carries over. (Closing
// chat-1 also extracted the fact from its transcript, so the wording may be the
// extractor's.) A new fact on the same key supersedes the old one.
const second = await chat("chat-2", ["What is my editor?", "Remember that my editor is Zed.", "What is my editor?"]);
assert.match(second[0] ?? "", /^From memory: .* editor is Helix$/);
assert.match(second[2] ?? "", /^From memory: .* editor is Zed$/);

// Every turn was captured on the raw lane as well.
const transcript = await memd.search("editor", { user_id: userId, kinds: ["raw_event"] });
console.log(`raw lane: ${transcript.items.length} turn messages about the editor`);
assert.ok(transcript.items.length >= 6);
console.log("ok");
