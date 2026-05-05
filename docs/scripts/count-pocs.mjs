#!/usr/bin/env node
// Counts POCs under examples/playground/pocs/<category>/<poc>/ and emits:
//   1. src/data/poc-counts.json    — imported by .mdx pages at build time.
//   2. public/llms.txt             — rendered from scripts/llms.txt.tmpl with
//                                    {{TOTAL_POCS}} / {{CATEGORY_COUNT}} substitution.
//                                    Astro serves files in public/ verbatim, so
//                                    the file is generated here, not at request time.
//
// Run automatically via the `prebuild` npm script in docs/package.json.
// Re-run after adding/removing a POC directory to refresh rendered counts.

import { readdirSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, "..", "..");
const pocsRoot = join(repoRoot, "examples", "playground", "pocs");
const jsonOutFile = resolve(here, "..", "src", "data", "poc-counts.json");
const llmsTemplate = resolve(here, "llms.txt.tmpl");
const llmsOutFile = resolve(here, "..", "public", "llms.txt");

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

mkdirSync(dirname(jsonOutFile), { recursive: true });
writeFileSync(jsonOutFile, JSON.stringify(data, null, 2) + "\n");

const llmsRendered = readFileSync(llmsTemplate, "utf8")
  .replaceAll("{{TOTAL_POCS}}", String(total))
  .replaceAll("{{CATEGORY_COUNT}}", String(categories.length));
mkdirSync(dirname(llmsOutFile), { recursive: true });
writeFileSync(llmsOutFile, llmsRendered);

console.log(
  `count-pocs: ${total} POCs across ${categories.length} categories -> ${jsonOutFile}, ${llmsOutFile}`,
);
