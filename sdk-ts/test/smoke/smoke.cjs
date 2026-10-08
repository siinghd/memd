// The CommonJS half of smoke.mjs: require() must resolve dist/index.cjs.
const assert = require("node:assert/strict");
const { MemdClient, NotFoundError } = require("memd-engine");

(async () => {
  const client = new MemdClient({
    apiKey: "k",
    fetch: async () => new Response(JSON.stringify({ detail: "not found" }), { status: 404 }),
  });
  assert.equal(await client.get("missing"), null);
  const err = await client.stats().catch((e) => e);
  assert.ok(err instanceof NotFoundError);
  console.log(`cjs ok (node ${process.version})`);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
