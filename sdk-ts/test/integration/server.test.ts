/**
 * End to end against a real memd server started from this repository's
 * source (../src). Skipped when python cannot import memd; set
 * MEMD_REQUIRE_INTEGRATION=1 (CI does) to make that a failure instead.
 *
 *   MEMD_PYTHON  interpreter with memd's dependencies (default: python3)
 */
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { randomBytes } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { delimiter, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  AuthenticationError,
  MemdClient,
  PermissionDeniedError,
  ValidationError,
  type MemoryRecord,
} from "../../src/index.js";

const REPO_SRC = resolve(fileURLToPath(new URL("../../../src", import.meta.url)));
const PYTHON = process.env.MEMD_PYTHON || "python3";
const PY_ENV: NodeJS.ProcessEnv = {
  ...process.env,
  PYTHONPATH: [REPO_SRC, process.env.PYTHONPATH].filter(Boolean).join(delimiter),
  // deterministic and light: no model download, no ONNX runtime in memory
  MEMD_EMBEDDER: process.env.MEMD_EMBEDDER || "hash",
};

function memdImportable(): boolean {
  const r = spawnSync(PYTHON, ["-c", "import memd.cli, fastapi, uvicorn"], { env: PY_ENV, stdio: "ignore" });
  return r.status === 0;
}

const AVAILABLE = memdImportable();
if (!AVAILABLE && process.env.MEMD_REQUIRE_INTEGRATION === "1") {
  throw new Error(`MEMD_REQUIRE_INTEGRATION=1 but ${PYTHON} cannot import memd from ${REPO_SRC}`);
}

function freePort(): Promise<number> {
  return new Promise((ok, fail) => {
    const srv = createServer();
    srv.once("error", fail);
    srv.listen(0, "127.0.0.1", () => {
      const addr = srv.address();
      srv.close(() => (typeof addr === "object" && addr ? ok(addr.port) : fail(new Error("no port"))));
    });
  });
}

async function waitHealthy(url: string, proc: ChildProcess, log: () => string, timeoutMs = 90_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (proc.exitCode !== null) throw new Error(`memd exited with ${proc.exitCode}:\n${log()}`);
    try {
      const res = await fetch(`${url}/health`);
      if (res.ok) return;
    } catch {
      // not listening yet
    }
    await new Promise((r) => setTimeout(r, 250));
  }
  throw new Error(`memd did not become healthy within ${timeoutMs} ms:\n${log()}`);
}

describe.skipIf(!AVAILABLE)("against a real memd server", () => {
  let proc: ChildProcess;
  let dataDir: string;
  let baseUrl: string;
  let logs = "";
  const adminKey = `it-admin-${randomBytes(16).toString("hex")}`;
  const nsA = `sdk-a-${randomBytes(3).toString("hex")}`;
  const nsB = `sdk-b-${randomBytes(3).toString("hex")}`;
  let keyA: string;
  let keyB: string;
  let admin: MemdClient;
  let a: MemdClient;
  let b: MemdClient;

  function mintKey(namespace: string): string {
    const r = spawnSync(PYTHON, ["-m", "memd.cli", "key", "create", "--ns", namespace, "--data", dataDir], {
      env: PY_ENV,
      encoding: "utf8",
    });
    if (r.status !== 0) throw new Error(`key create failed: ${r.stderr}`);
    return (JSON.parse(r.stdout) as { key: string }).key;
  }

  beforeAll(async () => {
    dataDir = mkdtempSync(join(tmpdir(), "memd-sdk-it-"));
    const port = await freePort();
    baseUrl = `http://127.0.0.1:${port}`;
    proc = spawn(PYTHON, ["-m", "memd.cli", "serve", "--http", "--host", "127.0.0.1", "--port", String(port)], {
      env: { ...PY_ENV, MEMD_DATA: dataDir, MEMD_ADMIN_KEY: adminKey },
      stdio: ["ignore", "pipe", "pipe"],
    });
    const keep = (chunk: Buffer) => {
      logs = (logs + chunk.toString()).slice(-20_000);
    };
    proc.stdout?.on("data", keep);
    proc.stderr?.on("data", keep);
    await waitHealthy(baseUrl, proc, () => logs);
    // minted after start: the running server picks up new keys from the store
    keyA = mintKey(nsA);
    keyB = mintKey(nsB);
    admin = new MemdClient({ apiKey: adminKey, baseUrl, namespace: nsA });
    a = new MemdClient({ apiKey: keyA, baseUrl, namespace: nsA });
    b = new MemdClient({ apiKey: keyB, baseUrl, namespace: nsB });
  });

  afterAll(async () => {
    if (proc && proc.exitCode === null) {
      const exited = new Promise((r) => proc.once("exit", r));
      proc.kill("SIGTERM");
      const timer = setTimeout(() => proc.kill("SIGKILL"), 10_000);
      await exited;
      clearTimeout(timer);
    }
    if (dataDir) rmSync(dataDir, { recursive: true, force: true });
  });

  it("health and scoped status", async () => {
    const h = await a.health();
    expect(h.ok).toBe(true);
    expect(typeof h.version).toBe("string");
    // a namespace key sees only its own namespace in the inventory
    await a.add("warm-up so the namespace exists");
    const st = await a.status();
    expect(st.namespaces).toEqual([nsA]);
  });

  it("add -> search -> get -> delete -> export", async () => {
    const [rawId] = await a.add("We deploy with make ship, never CI", { user_id: "u1", session_id: "s1" });
    expect(rawId).toMatch(/^[0-9A-Z]{26}$/);
    const factId = await a.remember("The user prefers dark mode", {
      user_id: "u1",
      entity_keys: ["user.theme"],
    });

    const res = await a.search("how do we deploy?", { user_id: "u1", budget_tokens: 1500 });
    expect(res.items.map((i) => i.id)).toContain(rawId);
    expect(res.packed_context).toContain("make ship");
    expect(res.budget).toBe(1500);
    const hit = res.items.find((i) => i.id === rawId)!;
    expect(hit.namespace).toBe(nsA);
    expect(hit.kind).toBe("raw_event");
    expect(hit.lanes.length).toBeGreaterThan(0);

    const rec = (await a.get(rawId!)) as MemoryRecord;
    expect(rec.content).toBe("We deploy with make ship, never CI");
    expect(rec.scope).toEqual({ user: "u1", session: "s1" });
    expect(rec.provenance.source).toBe("user");
    expect(rec.deleted).toBe(false);
    const fact = (await a.get(factId, { history: true }))!;
    expect(fact.kind).toBe("fact");
    expect(fact.entity_keys).toEqual(["user.theme"]);
    expect(Array.isArray(fact.history)).toBe(true);

    let exported = await a.export();
    expect(exported.map((r) => r.id)).toEqual(expect.arrayContaining([rawId, factId]));
    const streamed: string[] = [];
    for await (const r of a.exportStream()) streamed.push(r.id);
    expect(streamed).toEqual(exported.map((r) => r.id));

    // soft delete: gone from reads, still visible with history, not re-deletable
    expect(await a.delete(rawId!)).toBe(true);
    expect(await a.get(rawId!)).toBeNull();
    expect((await a.get(rawId!, { history: true }))?.deleted).toBe(true);
    expect(await a.delete(rawId!)).toBe(false);

    // hard delete: gone even from history
    expect(await a.delete(factId, { hard: true })).toBe(true);
    expect(await a.get(factId, { history: true })).toBeNull();

    exported = await a.export();
    expect(exported.map((r) => r.id)).not.toContain(rawId);
    expect(exported.map((r) => r.id)).not.toContain(factId);
    const search2 = await a.search("how do we deploy?", { user_id: "u1" });
    expect(search2.items.map((i) => i.id)).not.toContain(rawId);
  });

  it("scope isolation: a key for namespace A cannot touch namespace B", async () => {
    const [secret] = await b.add("B's secret launch code is 4242", { user_id: "ub" });

    const attempts: Array<[string, () => Promise<unknown>]> = [
      ["search", () => a.search("launch code", { namespace: nsB })],
      ["get", () => a.get(secret!, { namespace: nsB })],
      ["add", () => a.add("planted", { namespace: nsB })],
      ["delete", () => a.delete(secret!, { namespace: nsB })],
      ["export", () => a.export({ namespace: nsB })],
      ["stats", () => a.stats({ namespace: nsB })],
      ["forget", () => a.forget("launch", { namespace: nsB })],
    ];
    for (const [name, attempt] of attempts) {
      const err = await attempt().catch((e: unknown) => e);
      expect(err, name).toBeInstanceOf(PermissionDeniedError);
      expect((err as PermissionDeniedError).status, name).toBe(403);
      expect((err as PermissionDeniedError).message, name).toBe(`key not valid for namespace '${nsB}'`);
    }

    // and A's own namespace never sees B's data
    const mine = await a.search("launch code 4242");
    expect(mine.items.map((i) => i.id)).not.toContain(secret);
    // B still has it, untouched
    expect((await b.get(secret!))?.content).toContain("4242");
  });

  it("pack and observe round-trip", async () => {
    const sid = "chat-1";
    const ids = await a.observe(
      [
        { role: "system", content: "You are a release assistant." },
        { role: "user", content: "Our staging database is called heron-7." },
      ],
      "Noted: staging runs on heron-7.",
      { session_id: sid, user_id: "u2" },
    );
    expect(ids).toHaveLength(3);

    const packed = await a.pack(
      [
        { role: "system", content: "You are a release assistant." },
        { role: "user", content: "What is the staging database called?" },
      ],
      { user_id: "u2" },
    );
    expect(packed).toHaveLength(3);
    expect(packed[0]).toEqual({ role: "system", content: "You are a release assistant." });
    expect(packed[1]!.role).toBe("system");
    expect(String(packed[1]!.content)).toContain("heron-7");

    const closed = await a.closeSession(sid);
    expect(closed.raw_considered).toBe(3);
    expect(typeof closed.segment).toBe("string");
  });

  it("forget previews, then deletes on confirm", async () => {
    await a.add("The office wifi password is tangerine", { user_id: "u3" });
    const preview = await a.forget("wifi password tangerine", { user_id: "u3" });
    expect(preview.confirmed).toBe(false);
    expect(preview.count).toBeGreaterThanOrEqual(1);
    expect(preview.will_delete.some((w) => w.content.includes("tangerine"))).toBe(true);
    expect(await a.findIds("wifi password tangerine", { user_id: "u3" })).toHaveLength(preview.count);

    const deleted = await a.forget("wifi password tangerine", { user_id: "u3", confirm: true });
    expect(deleted.length).toBe(preview.count);
    const after = await a.search("wifi password", { user_id: "u3" });
    expect(after.items.some((i) => i.content.includes("tangerine"))).toBe(false);
  });

  it("stats, compact and reembed return their documented shapes", async () => {
    const st = await a.stats();
    expect(st.namespace).toBe(nsA);
    expect(st.records).toBeGreaterThan(0);
    expect(typeof st.embedder).toBe("string");
    expect(typeof st.lexical.backend).toBe("string");

    const rep = await a.compact({ force: true });
    expect(Object.keys(rep).sort()).toEqual(
      [
        "bytes_after", "bytes_before", "duration_ms", "hard_deleted_purged",
        "records_folded", "records_purged", "segments_in", "segments_out",
      ].sort(),
    );
    const re = await a.reembed();
    expect(re.namespace).toBe(nsA);
    expect(typeof re.embedded).toBe("number");
  });

  it("maps real server errors to typed errors", async () => {
    const bad = new MemdClient({ apiKey: "memd_nope_00000000_ffff", baseUrl, namespace: nsA });
    await expect(bad.stats()).rejects.toBeInstanceOf(AuthenticationError);

    const err = (await a.search("q", { budget_tokens: 10 }).catch((e: unknown) => e)) as ValidationError;
    expect(err).toBeInstanceOf(ValidationError);
    expect(err.status).toBe(422);
    expect(err.issues[0]?.loc).toEqual(["body", "budget_tokens"]);

    // crypto-shred needs an override-capable key
    await expect(a.destroyNamespace()).rejects.toBeInstanceOf(PermissionDeniedError);
  });

  it("an admin key reaches any namespace and can destroy one", async () => {
    const scratch = `sdk-scratch-${randomBytes(3).toString("hex")}`;
    await admin.add("to be shredded", { namespace: scratch });
    expect((await admin.export({ namespace: scratch })).length).toBe(1);
    expect(await admin.destroyNamespace(scratch)).toBe(true);
    expect(await admin.export({ namespace: scratch })).toEqual([]);
  });
});
