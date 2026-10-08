// Eventual reads with memd-engine: consistency "eventual" with a staleness bound, who served each read (lastRead and the X-Memd-* headers), read-your-writes.
// Run (in examples/ts): python ../local_server.py -- npx tsx 07_eventual_reads.ts   (or point MEMD_URL / MEMD_API_KEY / MEMD_NAMESPACE at a cluster node)
import assert from "node:assert/strict";
import { BadRequestError, MemdClient, type FetchLike, type ReadInfo } from "memd-engine";

const { MEMD_URL, MEMD_API_KEY, MEMD_NAMESPACE = "default" } = process.env;
if (!MEMD_URL || !MEMD_API_KEY) {
  console.error("set MEMD_URL and MEMD_API_KEY, or run under examples/local_server.py");
  process.exit(2);
}

// Reads are strong by default: the node holding the namespace's writer lease
// serves them. In a cluster, a search or get may accept "eventual" consistency
// instead: then the node that received it can serve it from its read replica, no
// hop, as long as that replica is no older than the bound. A load balancer in
// front spreads a hot namespace's reads over every node that way.
const MAX_STALENESS_MS = 5_000;

// The client records who served each read in `lastRead`. To see the raw response
// headers too (for logs or metrics), wrap fetch: the `fetch` option takes any
// (url, init) => Promise<Response>.
let lastMemdHeaders: Record<string, string> = {};
const recordingFetch: FetchLike = async (url, init) => {
  const res = await fetch(url, init);
  lastMemdHeaders = {};
  res.headers.forEach((value, name) => {
    if (name.startsWith("x-memd-")) lastMemdHeaders[name] = value;
  });
  return res;
};

// Eventual for every read this client makes, unless a call says otherwise.
const memd = new MemdClient({
  baseUrl: MEMD_URL,
  apiKey: MEMD_API_KEY,
  namespace: MEMD_NAMESPACE,
  consistency: "eventual",
  maxStalenessMs: MAX_STALENESS_MS,
  fetch: recordingFetch,
});

/** One line for a log: who served the read, and how stale it could be. */
function describe(info: ReadInfo | null): string {
  if (!info) return "no read yet";
  if (info.servedBy === "leader") return "served by the writer (always current)";
  return `served by a replica: applied through seq ${info.appliedSeq}, ${info.ageMs} ms old`;
}

/** The guarantee an eventual read carries: a replica never older than the bound. */
function checkBound(info: ReadInfo | null, boundMs: number): void {
  assert.ok(info, "a read happened");
  if (info.servedBy === "replica") {
    assert.ok(info.ageMs !== null && info.ageMs <= boundMs, `replica age ${info.ageMs} ms within ${boundMs} ms`);
  }
}

// --- writes always go to the writer ---------------------------------------------
const id = await memd.remember("The on-call rotation changes every Monday", { user_id: "u1", entity_keys: ["team.oncall"] });

// --- read-your-writes: ask for strong on the read that must see the write ---------
// An eventual read may miss a write acknowledged less than the bound ago (and may
// still serve a record deleted that recently). Strong for this one call:
const mine = await memd.get(id, { consistency: "strong" });
assert.equal(mine?.content, "The on-call rotation changes every Monday");
console.log("strong get:", describe(memd.lastRead), lastMemdHeaders);
assert.equal(memd.lastRead?.servedBy, "leader", "a strong read is always the writer's");

// --- the hot path: eventual is fine for "what is relevant to this message?" -------
// search, pack and get use the client's default (eventual, 5 s bound here).
const hits = await memd.search("when does on-call change?", { user_id: "u1" });
console.log("eventual search:", describe(memd.lastRead), lastMemdHeaders);
checkBound(memd.lastRead, MAX_STALENESS_MS);
// Read straight off the response: X-Memd-Served-By, and for a replica
// X-Memd-Replica-Seq and X-Memd-Replica-Age-Ms.
assert.equal(lastMemdHeaders["x-memd-served-by"], memd.lastRead?.servedBy);
if (hits.items.length === 0) {
  console.log("  (a replica that had not applied the write yet: allowed within the bound)");
}

const packed = await memd.pack([{ role: "user", content: "Who is on call this week?" }], { user_id: "u1" });
console.log("eventual pack:", describe(memd.lastRead), `(${packed.length} messages)`);
checkBound(memd.lastRead, MAX_STALENESS_MS);

// A tighter bound for one call: a replica older than 500 ms makes this read wait
// for its next refresh (briefly), or go to the writer.
await memd.search("on-call", { user_id: "u1", maxStalenessMs: 500 });
console.log("eventual search, 500 ms bound:", describe(memd.lastRead));
checkBound(memd.lastRead, 500);

// The bound is validated by the server: 0 to 86,400,000 ms.
try {
  await memd.search("on-call", { maxStalenessMs: 90_000_000 });
  assert.fail("an out-of-range bound is refused");
} catch (err) {
  if (!(err instanceof BadRequestError)) throw err;
  console.log(`bound out of range: ${err.status} ${err.message}`);
}

// Only search, pack and get take a consistency. Exports, findIds, forget previews
// and stats always go to the writer: a forget preview must match what its confirm
// deletes. Against a single server, as under local_server.py, the writer serves
// every read ("leader"); against a cluster node that does not own the namespace,
// the eventual ones above come back "replica".
console.log("ok");
