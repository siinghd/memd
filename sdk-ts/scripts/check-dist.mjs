// Release gate for the built package (run after `npm run build`):
//  1. the tarball holds only dist/, README.md, LICENSE and package.json;
//  2. the bundles reference no Node-only API, so edge runtimes can load them.
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";

const failures = [];

const npm = process.platform === "win32" ? "npm.cmd" : "npm";
const [pack] = JSON.parse(
  execFileSync(npm, ["pack", "--dry-run", "--json", "--ignore-scripts"], { encoding: "utf8" }),
);
const files = pack.files.map((f) => f.path).sort();
const allowed = (p) => p.startsWith("dist/") || ["README.md", "LICENSE", "package.json"].includes(p);
for (const p of files.filter((f) => !allowed(f))) failures.push(`unexpected file in package: ${p}`);
for (const need of ["dist/index.js", "dist/index.cjs", "dist/index.d.ts", "dist/index.d.cts", "README.md", "LICENSE"]) {
  if (!files.includes(need)) failures.push(`missing from package: ${need}`);
}

const nodeOnly = [/\brequire\(/, /from\s*["']node:/, /\bprocess\./, /\bBuffer\b/, /__dirname|__filename/];
for (const bundle of ["dist/index.js", "dist/index.cjs"]) {
  const src = readFileSync(bundle, "utf8");
  for (const re of nodeOnly) if (re.test(src)) failures.push(`${bundle} uses a Node-only API (${re})`);
}

if (failures.length) {
  console.error(failures.join("\n"));
  process.exit(1);
}
console.log(`package ok: ${files.length} files, ${pack.size} bytes packed, ${pack.unpackedSize} unpacked`);
console.log(files.map((f) => `  ${f}`).join("\n"));
