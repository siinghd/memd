import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts"],
  format: ["esm", "cjs"],
  dts: true,
  clean: true,
  target: "es2020",
  platform: "neutral",
  sourcemap: false,
  minify: false,
  treeshake: true,
});
