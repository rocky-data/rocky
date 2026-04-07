#!/usr/bin/env node
// Copies third-party webview assets from node_modules into media/.
// Run via `npm run bundle:webview` or as part of `vscode:prepublish`.
//
// The lineage webview loads viz.js (Graphviz → SVG renderer) locally rather
// than from a CDN so the extension works offline and complies with the
// recommended webview Content-Security-Policy.

import { copyFileSync, mkdirSync, statSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, "..");

const assets = [
  {
    source: "node_modules/@viz-js/viz/dist/viz-global.js",
    target: "media/viz.js",
    description: "Graphviz renderer (window.Viz)",
  },
];

mkdirSync(resolve(root, "media"), { recursive: true });

for (const asset of assets) {
  const src = resolve(root, asset.source);
  const dst = resolve(root, asset.target);
  copyFileSync(src, dst);
  const sizeKb = Math.round(statSync(dst).size / 1024);
  console.log(`bundled ${asset.target} (${sizeKb} KB) — ${asset.description}`);
}
