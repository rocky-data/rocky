#!/usr/bin/env node
// esbuild configuration for the Rocky VS Code extension.
//
// Bundles `src/extension.ts` + its runtime deps (vscode-languageclient + transitives)
// into a single `dist/extension.js` file. The `vscode` module is marked external —
// it's provided by the extension host at runtime, not a real npm package.
//
// Webview assets (viz.js) are handled separately by `scripts/bundle-webview.mjs`
// because they load inside the webview sandbox, not the extension host.

import { build, context } from "esbuild";

const watch = process.argv.includes("--watch");
const production = !watch && !process.argv.includes("--dev");

/** @type {import('esbuild').BuildOptions} */
const options = {
  entryPoints: ["src/extension.ts"],
  bundle: true,
  outfile: "dist/extension.js",
  platform: "node",
  format: "cjs",
  target: "node20",
  external: ["vscode"],
  minify: production,
  sourcemap: !production,
  logLevel: "info",
};

if (watch) {
  const ctx = await context(options);
  await ctx.watch();
  console.log("esbuild: watching for changes…");
} else {
  await build(options);
}
