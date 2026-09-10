/**
 * Guard that the generated TypeScript barrel re-exports every command output
 * type (#1853).
 *
 * `just codegen-vscode` regenerates every `types/generated/<command>.ts`, but
 * `types/generated/index.ts` is hand-maintained — the pipeline does not
 * re-derive it. So a new CLI command's generated interface can be produced,
 * committed, and still be unreachable by anyone importing from the barrel: the
 * type exists, `codegen-drift` is green because both sides agree, and nothing
 * says the type is inert.
 *
 * The Python side has had this check since #1730
 * (`sdk/python/tests/test_barrel_completeness.py`). This is its counterpart;
 * the two barrels are generated from the same schemas and fail the same way.
 *
 * Text-based, not a type-level check. The barrel uses `export type { … } from`,
 * which erases at runtime, so there is nothing to introspect — and importing
 * every module to compare shapes would prove the modules load, not that the
 * barrel names them.
 */
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

const GENERATED_DIR = join(dirname(fileURLToPath(import.meta.url)), "..", "types", "generated");
const BARREL = join(GENERATED_DIR, "index.ts");

/** Every generated module except the barrel itself. */
function moduleFiles(): string[] {
  return readdirSync(GENERATED_DIR)
    .filter((name) => name.endsWith(".ts") && name !== "index.ts")
    .sort();
}

/**
 * Exported interface/type names in one generated module whose name marks it a
 * command output.
 *
 * `Output` and `Result` are the two suffixes the Rust structs use, matching the
 * Python guard's rule so the two barrels are held to one standard.
 */
function commandOutputTypes(file: string): string[] {
  const source = readFileSync(join(GENERATED_DIR, file), "utf8");
  const names = new Set<string>();
  const declaration = /^export\s+(?:interface|type)\s+([A-Za-z0-9_]+)/gm;
  for (const match of source.matchAll(declaration)) {
    const name = match[1];
    if (name.endsWith("Output") || name.endsWith("Result")) {
      names.add(name);
    }
  }
  return [...names].sort();
}

/** Names the barrel re-exports, from its `export type { … }` clauses. */
function barrelExports(): Set<string> {
  const source = readFileSync(BARREL, "utf8");
  const names = new Set<string>();
  for (const clause of source.matchAll(/export\s+type\s*\{([^}]*)\}\s*from/g)) {
    for (const entry of clause[1].split(",")) {
      // `Foo as Bar` re-exports under Bar; the outward-facing name is what
      // an importer can reach, so that is what counts.
      const name = entry.split(/\s+as\s+/).pop()?.trim();
      if (name) {
        names.add(name);
      }
    }
  }
  return names;
}

describe("generated TypeScript barrel", () => {
  const exported = barrelExports();

  it("finds the types it claims to", () => {
    // The scan's own guard. A regex that silently matched nothing would make
    // every assertion below pass on an empty set — the same "absence reads as
    // agreement" failure this gate exists to close.
    expect(moduleFiles().length).toBeGreaterThan(50);
    expect(exported.size).toBeGreaterThan(50);
    expect(exported.has("RunOutput")).toBe(true);
    expect(exported.has("DiscoverOutput")).toBe(true);
  });

  it.each(moduleFiles())("%s has every command output re-exported", (file) => {
    const missing = commandOutputTypes(file).filter((name) => !exported.has(name));
    expect(
      missing,
      `${file} declares command-output type(s) missing from types/generated/index.ts: ` +
        `${missing.join(", ")}. Add an \`export type { … } from "./${file.replace(/\.ts$/, "")}"\` ` +
        `entry — the barrel is hand-maintained and codegen does not re-derive it.`,
    ).toEqual([]);
  });
});
