---
name: rocky-new-cli-command
description: End-to-end checklist for adding a new Rocky CLI subcommand across the engine, JSON schema export, Dagster Pydantic types, Dagster resource wiring, and VS Code extension command. Use whenever a new top-level CLI verb needs to be exposed.
---

# Adding a new Rocky CLI command

A new CLI verb (e.g. `rocky diff`) touches 4 layers. The engine is the source of truth; Dagster and VS Code consume its JSON output. Do all of this in one PR.

## Prereq

Read the related skills first:
- `rocky-codegen` — for the Rust → Pydantic/TS cascade (you'll run `just codegen`).
- `engine/AGENTS.md` → "JSON Output Schema" — has the full table of existing commands and their output structs.
- `integrations/dagster/AGENTS.md` → "Adding support for a new Rocky CLI command" — the 9-step dagster-side checklist.

## Step 1 — Engine: the command itself

```
engine/crates/rocky-cli/src/commands/<name>.rs      ← new file (impl)
engine/crates/rocky-cli/src/commands/mod.rs         ← register in the clap enum
engine/crates/rocky-cli/src/registry.rs             ← (if applicable) wire into dispatch
```

Conventions:
- Use `clap` derive API — `#[derive(clap::Args)]` struct for the subcommand flags.
- Support `--output json|table` (the shared `OutputFormat` enum).
- Use `tracing` for logging, never `println!`/`eprintln!`.
- Library errors via `thiserror`, binary/CLI errors via `anyhow`.
- If the command needs a config, load `rocky.toml` via the shared helper (see how `run.rs` or `plan.rs` do it).
- SQL identifiers MUST go through `rocky-sql/validation.rs` — never `format!` untrusted strings into SQL.

## Step 2 — Engine: the typed JSON output struct

In `engine/crates/rocky-cli/src/output.rs` (or co-located in `commands/<name>.rs`):

```rust
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DiffOutput {
    pub version: String,
    pub command: String,
    pub diffs: Vec<DiffEntry>,
}
```

Then register it so `export-schemas` picks it up:

```rust
// engine/crates/rocky-cli/src/commands/export_schemas.rs::schemas()
("diff", schema_for!(DiffOutput)),
```

## Step 3 — Engine: tests

- Unit tests live in the same file (`#[cfg(test)] mod tests`).
- E2E integration tests go in `engine/crates/rocky-core/tests/e2e.rs` or a command-specific test file — they run DuckDB-backed with no credentials.
- Run: `cargo test -p rocky-cli` + `cargo test -p rocky-core --test e2e`.

## Step 4 — Codegen: regenerate bindings

From the monorepo root:

```bash
just codegen
```

This rebuilds the engine in release mode, writes `schemas/diff.schema.json`, regenerates `sdk/python/src/rocky_sdk/types_generated/diff_schema.py`, and regenerates `editors/vscode/src/types/generated/diff.ts`. See the `rocky-codegen` skill for the details of what each sub-recipe does.

## Step 5 — Dagster: consume the output

9-step checklist (from `integrations/dagster/AGENTS.md`):

1. `*Output` struct exists (step 2 above). ✓
2. Registered in `export_schemas.rs::schemas()`. ✓
3. `just codegen-sdk` ran. ✓
4. Re-export the new type from `integrations/dagster/src/dagster_rocky/types.py` in the re-export section near the bottom — both the generated name (`DiffOutput`) and a legacy Python-flavored alias (`DiffResult`) for forward-compat.
5. Add a route in `parse_rocky_output()` to dispatch `"diff"` → `DiffOutput`.
6. `just regen-fixtures` from the monorepo root to capture a fresh fixture — or add a hand-crafted scenario dict to `integrations/dagster/tests/scenarios.py` if the playground POC doesn't produce that command naturally.
7. Expose it as a `*_json` pytest fixture via `integrations/dagster/tests/conftest.py`.
8. Add parsing tests in `integrations/dagster/tests/test_types.py`.
9. Add a method to `RockyResource` in `integrations/dagster/src/dagster_rocky/resource.py` that calls the CLI and returns the parsed `DiffOutput`. Follow the existing pattern (see `run`, `plan`, or `discover`).

## Step 6 — VS Code: expose as a command

```
editors/vscode/src/commands/<group>.ts     ← add handler (group by concern: ops, run, inspect, ...)
editors/vscode/src/commands/index.ts       ← register in the single registration point
editors/vscode/package.json                ← declare the command in `contributes.commands`
```

Conventions:
- Use `cp.execFile()` for subprocess calls (no shell injection — never `cp.exec()`).
- Wrap long commands with `vscode.window.withProgress()`.
- The subprocess helper `src/rockyCli.ts` already exists — reuse it.
- Type the result as the generated interface (`import { DiffOutput } from '../types/generated'`).
- If the command takes editor context (current file), get it via `vscode.window.activeTextEditor`.
- Follow the existing command pattern — see `commands/run.ts` or `commands/ops.ts` for templates.

## Step 7 — Docs + engine README

- `docs/src/content/docs/reference/commands/<name>.md` (Astro/Starlight) — user-facing reference.
- `engine/README.md` and monorepo `README.md` if the command is user-prominent enough to warrant a top-level mention.

## Final check — run everything

```bash
just test     # cargo test + pytest + vitest
just lint     # cargo clippy/fmt + ruff + eslint
just codegen  # idempotency check — should produce no diff
```

The `codegen-drift` CI workflow will fail the PR if step 4 wasn't run (or produced stale output).

## Commit style

One PR, but feel free to split into a few focused commits:

```
feat(engine/rocky-cli): add `rocky diff` subcommand
feat(engine/rocky-cli): add DiffOutput schema
chore(codegen): regenerate bindings for diff command
feat(dagster): add RockyResource.diff() + DiffOutput re-export
feat(vscode): add rocky.diff command
docs(engine): document `rocky diff`
```
