#!/usr/bin/env node
// Refuse a build that references another host.
//
// The UI is served from files embedded in the Rocky binary, and the server's
// CSP allows scripts, styles, images and connections from the SPA's own
// origin only. A bundle that points at a CDN would fail silently at runtime
// and, worse, would be a third-party script in the trust surface. This scan
// runs after every `vite build` and fails the build on the first hit.
//
// What counts: an absolute `http://` or `https://` URL in a position the
// browser would load, in any `.html`, `.js` or `.css` file under `dist/`. A
// URL inside a string that is never loaded (a docs link, the OpenAPI
// externalDocs URL) is not a load, so the scan looks at load positions:
// `src=`, `href=`, `url(`, `import(`, `fetch(`, `new URL(`, `@import`.

import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, extname } from "node:path";

const LOAD_POSITIONS = [
  /\bsrc\s*=\s*["']https?:\/\//i,
  /\bhref\s*=\s*["']https?:\/\//i,
  /url\(\s*["']?https?:\/\//i,
  /\bimport\(\s*["']https?:\/\//i,
  /\bfetch\(\s*["']https?:\/\//i,
  /new\s+URL\(\s*["']https?:\/\//i,
  /@import\s+(url\()?\s*["']https?:\/\//i,
];

export function externalLoads(text) {
  const hits = [];
  for (const pattern of LOAD_POSITIONS) {
    const match = pattern.exec(text);
    if (match) hits.push(match[0]);
  }
  return hits;
}

export function scan(dir) {
  const problems = [];
  const walk = (d) => {
    for (const name of readdirSync(d)) {
      const path = join(d, name);
      if (statSync(path).isDirectory()) {
        walk(path);
        continue;
      }
      if (![".html", ".js", ".css", ".mjs"].includes(extname(path))) continue;
      const hits = externalLoads(readFileSync(path, "utf8"));
      for (const hit of hits) problems.push(`${path}: ${hit}`);
    }
  };
  walk(dir);
  return problems;
}

const invokedDirectly = process.argv[1] && process.argv[1].endsWith("check-no-external.mjs");
if (invokedDirectly) {
  const dir = process.argv[2] ?? "dist";
  const problems = scan(dir);
  if (problems.length > 0) {
    console.error("the build references another host; the UI must load nothing external:");
    for (const p of problems) console.error(`  ${p}`);
    process.exit(1);
  }
  console.log(`no external loads under ${dir}`);
}
