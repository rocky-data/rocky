#!/usr/bin/env node
// Bundles third-party webview assets into media/ using esbuild.
// Run via `npm run bundle:webview` or as part of `vscode:prepublish`.
//
// Webview scripts load inside the extension's sandboxed webview, not
// the extension host.  They must be self-contained (no dynamic require)
// and must not rely on Node.js globals.
//
// Assets bundled:
//   • media/lineage-graph.js — @dagrejs/dagre + d3 in a single IIFE that
//     exposes `window.dagreD3` (an object with { dagre, d3 }).
//     Replaces the old viz.js (Graphviz/DOT renderer) which is no longer used.

import { build } from "esbuild";
import { mkdirSync, statSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, "..");

mkdirSync(resolve(root, "media"), { recursive: true });

// Write a tiny entry-point that re-exports both libs under a single namespace.
// esbuild bundles it into an IIFE so the webview can reference window.dagreD3.
const entryPoint = resolve(root, "media", "_lineage-entry.js");
writeFileSync(
  entryPoint,
  `import * as dagre from "@dagrejs/dagre";\nimport * as d3 from "d3";\nwindow.dagreD3 = { dagre, d3 };\n`,
);

await build({
  entryPoints: [entryPoint],
  bundle: true,
  outfile: resolve(root, "media", "lineage-graph.js"),
  platform: "browser",
  format: "iife",
  // globalName not needed — the entry-point assigns to window directly.
  minify: true,
  logLevel: "info",
});

const sizeKb = Math.round(
  statSync(resolve(root, "media", "lineage-graph.js")).size / 1024,
);
console.log(
  `bundled media/lineage-graph.js (${sizeKb} KB) — @dagrejs/dagre + d3 v7 webview renderer`,
);

// Clean up temp entry-point.
import { unlinkSync } from "node:fs";
try {
  unlinkSync(entryPoint);
} catch {
  // best-effort
}
