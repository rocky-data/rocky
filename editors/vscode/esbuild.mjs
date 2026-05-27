#!/usr/bin/env node
// esbuild configuration for the Rocky VS Code extension.
//
// Two builds run from a single invocation:
//   1. The extension host — `src/extension.ts` + its runtime deps
//      (vscode-languageclient + transitives) → `dist/extension.js` (CJS).
//      The `vscode` module is external; the host provides it at runtime.
//   2. The webview apps — one React entry per `webview-ui/panels/*/main.tsx`,
//      code-split into `dist/webviews/` as ESM. These run inside the sandboxed
//      webview, so they target the browser and are fully self-contained.
//
// Tailwind CSS is generated separately (`npm run tailwind`, wired as the
// `prebundle` hook) into `webview-ui/styles/tailwind.generated.css`, which each
// panel entry imports; esbuild's css loader then emits `dist/webviews/<panel>.css`.

import { build, context } from "esbuild";
import { existsSync, readdirSync, rmSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const watch = process.argv.includes("--watch");
const production = !watch && !process.argv.includes("--dev");

const here = dirname(fileURLToPath(import.meta.url));

/** @type {import('esbuild').BuildOptions} */
const hostOptions = {
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

// One React entry per `webview-ui/panels/<name>/main.tsx`. Adding a panel needs
// no edit here — just a new directory with a `main.tsx`.
function webviewEntryPoints() {
  const panelsDir = join(here, "webview-ui", "panels");
  if (!existsSync(panelsDir)) return {};
  /** @type {Record<string, string>} */
  const entries = {};
  for (const name of readdirSync(panelsDir)) {
    const entry = join(panelsDir, name, "main.tsx");
    if (existsSync(entry)) entries[name] = entry;
  }
  return entries;
}

const webviewEntries = webviewEntryPoints();
const hasWebviews = Object.keys(webviewEntries).length > 0;

/** @type {import('esbuild').BuildOptions} */
const webviewOptions = {
  entryPoints: webviewEntries,
  bundle: true,
  splitting: true,
  format: "esm",
  platform: "browser",
  target: "es2022",
  outdir: "dist/webviews",
  entryNames: "[name]",
  chunkNames: "chunks/[name]-[hash]",
  assetNames: "assets/[name]-[hash]",
  loader: { ".css": "css" },
  jsx: "automatic",
  minify: production,
  sourcemap: !production,
  define: {
    "process.env.NODE_ENV": JSON.stringify(
      production ? "production" : "development",
    ),
  },
  logLevel: "info",
};

// esbuild appends content-hashes to chunk names but never prunes the outdir,
// so clear it once up front to keep stale chunks out of the package.
if (hasWebviews) {
  rmSync(join(here, "dist", "webviews"), { recursive: true, force: true });
}

if (watch) {
  const ctxs = await Promise.all([
    context(hostOptions),
    ...(hasWebviews ? [context(webviewOptions)] : []),
  ]);
  await Promise.all(ctxs.map((c) => c.watch()));
  console.log("esbuild: watching for changes…");
} else {
  await Promise.all([
    build(hostOptions),
    ...(hasWebviews ? [build(webviewOptions)] : []),
  ]);
}
