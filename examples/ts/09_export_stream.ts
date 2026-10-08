// Export streaming with memd-engine: stream a namespace to a JSONL file record by record, check the skipped-frames signal, read the file back.
// Run (in examples/ts): python ../local_server.py -- npx tsx 09_export_stream.ts [OUT.jsonl]   (default: a temp file, removed after)
import assert from "node:assert/strict";
import { createReadStream, createWriteStream } from "node:fs";
import { mkdtemp, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { MemdClient, type EventIn, type MemoryRecord } from "memd-engine";

const { MEMD_URL, MEMD_API_KEY, MEMD_NAMESPACE = "default" } = process.env;
if (!MEMD_URL || !MEMD_API_KEY) {
  console.error("set MEMD_URL and MEMD_API_KEY, or run under examples/local_server.py");
  process.exit(2);
}

const memd = new MemdClient({ baseUrl: MEMD_URL, apiKey: MEMD_API_KEY, namespace: MEMD_NAMESPACE });

// --- something to export -----------------------------------------------------
const events: EventIn[] = Array.from({ length: 1000 }, (_, i) => ({
  content: `ticket #${i}: ${i % 2 ? "login fails on Safari" : "export button is slow"}`,
  user_id: `u${i % 7}`,
}));
for (let i = 0; i < events.length; i += 500) await memd.addEvents(events.slice(i, i + 500)); // up to 1000 per call
await memd.remember("The support rotation is weekly", { entity_keys: ["team.support"] });
const deletedId = await memd.remember("a record deleted before the export");
await memd.delete(deletedId);

// --- stream it to a file ---------------------------------------------------------
// exportStream() yields records as the response arrives: the namespace is never
// held in memory. pipeline() pulls the next record only when the file has taken
// the last one (backpressure), and closes everything on success or failure.
const dir = process.argv[2] ? undefined : await mkdtemp(join(tmpdir(), "memd-export-"));
const outFile = process.argv[2] ?? join(dir ?? ".", "export.jsonl");

async function* jsonLines(records: AsyncIterable<MemoryRecord>): AsyncGenerator<string> {
  for await (const record of records) yield `${JSON.stringify(record)}\n`; // NDJSON: one record per line
}

const started = Date.now();
await pipeline(
  Readable.from(jsonLines(memd.exportStream({ signal: AbortSignal.timeout(120_000) }))),
  createWriteStream(outFile),
);
// Read the skipped-frames signal right after the export it belongs to (another
// export on this client overwrites it). The server exports everything it can
// read; a damaged log frame it could not read is left out and counted in the
// X-Memd-Export-Skipped-Frames header (its audit entry and log say where).
const skipped = memd.lastExportSkippedFrames;
console.log(`exported to ${outFile}: ${(await stat(outFile)).size} bytes in ${Date.now() - started} ms`);
if (skipped > 0) {
  // keep the file, but never treat it as a complete backup
  console.error(`INCOMPLETE: the server skipped ${skipped} unreadable log frame(s); see SECURITY.md`);
  process.exitCode = 3;
} else {
  console.log("complete: no frames skipped");
}

// --- read it back --------------------------------------------------------------------
// Each line is a whole record: id, content, kind, scope, provenance, time axes,
// entity keys, meta. The anti-lock-in format: everything memd knows, as plain JSON.
const ids = new Set<string>();
const kinds = new Map<string, number>();
for await (const line of createInterface({ input: createReadStream(outFile), crlfDelay: Infinity })) {
  if (!line.trim()) continue;
  const record = JSON.parse(line) as MemoryRecord;
  ids.add(record.id);
  kinds.set(record.kind, (kinds.get(record.kind) ?? 0) + 1);
}
console.log(`read back ${ids.size} records:`, Object.fromEntries(kinds));
assert.equal(ids.size, events.length + 1, "every live record, once");
assert.ok(!ids.has(deletedId), "a deleted record is not exported");
assert.equal(skipped, 0);

// --- stopping early --------------------------------------------------------------------
// Breaking out of the loop cancels the response body: the connection is not left
// open. (A signal does the same from outside: the loop then throws RequestAbortedError.)
const sample: MemoryRecord[] = [];
for await (const record of memd.exportStream()) {
  sample.push(record);
  if (sample.length === 5) break;
}
console.log("first 5:", sample.map((r) => r.content));
assert.equal(sample.length, 5);

if (dir) await rm(dir, { recursive: true, force: true });
console.log("ok");
