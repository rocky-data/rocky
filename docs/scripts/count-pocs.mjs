#!/usr/bin/env node
// Counts POCs under examples/playground/pocs/<category>/<poc>/ and emits
// a small JSON file that the docs site imports at build time.
//
// Run automatically via the `prebuild` npm script in docs/package.json.
// Re-run after adding/removing a POC directory to refresh rendered counts.

import { readdirSync, mkdirSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, "..", "..");
const pocsRoot = join(repoRoot, "examples", "playground", "pocs");
const outFile = resolve(here, "..", "src", "data", "poc-counts.json");

function listDirs(parent) {
  return readdirSync(parent, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)
    .sort();
}

const categories = listDirs(pocsRoot);
const perCategory = {};
let total = 0;
for (const category of categories) {
  const pocs = listDirs(join(pocsRoot, category));
  perCategory[category] = pocs.length;
  total += pocs.length;
}

const data = {
  total,
  categoryCount: categories.length,
  perCategory,
};

mkdirSync(dirname(outFile), { recursive: true });
writeFileSync(outFile, JSON.stringify(data, null, 2) + "\n");

console.log(
  `count-pocs: ${total} POCs across ${categories.length} categories -> ${outFile}`,
);
