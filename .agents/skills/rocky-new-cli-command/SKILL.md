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
- `integrations/dagster/AGENTS.md` and `sdk/python/AGENTS.md` → the layer architecture each step below plugs into.

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

This builds the engine (dev profile; the output is identical to a release build), writes `schemas/diff.schema.json`, regenerates `sdk/python/src/rocky_sdk/types_generated/diff_schema.py`, and regenerates `editors/vscode/src/types/generated/diff.ts`. See the `rocky-codegen` skill for the details of what each sub-recipe does.

## Step 5 — SDK (`rocky-sdk`): parse and expose the output

`just codegen` (step 4) already wrote `sdk/python/src/rocky_sdk/types_generated/diff_schema.py` and refreshed the curated `__init__.py` barrel. Then, in `sdk/python/`:

1. Re-export the new type from `src/rocky_sdk/types.py` in the bridge section near the bottom — the generated name (`DiffOutput`) and the Python-flavored alias (`DiffResult = DiffOutput`), like the existing pairs.
2. Add a route in `parse_rocky_output()` (also in `types.py`) to dispatch `"diff"` → `DiffOutput`.
3. If you add a hand-written runtime model in `types.py`, carry every top-level field the schema has; `tests/test_schema_parity.py` fails otherwise.
4. Add a typed method to `RockyClient` in `src/rocky_sdk/client.py` that runs the CLI and returns `DiffOutput`. Follow `discover` or `plan`.
5. Add parsing tests in `tests/test_types.py` and a client test in `tests/test_client.py`.

## Step 6 — Dagster: consume the output

`dagster_rocky.types` re-exports the whole SDK surface (`from rocky_sdk.types import *`), so there is no per-type re-export. In `integrations/dagster/`:

1. Add a delegating method to `RockyResource` in `src/dagster_rocky/resource.py` that calls the new `RockyClient` method and translates SDK errors into `dagster.Failure`. Follow `discover` or `plan`.
2. Add a hand-crafted scenario dict to `tests/scenarios.py` and expose it as a `*_json` pytest fixture in `tests/conftest.py`.
3. Add tests that use it.
4. Optionally run `just regen-fixtures` (or extend `scripts/regen_fixtures.sh` with a `capture` call) so a playground POC contributes a live-binary sample under `tests/fixtures_generated/`.
5. If `resource.py` now imports a name the released `rocky-sdk` lacks, raise the `rocky-sdk>=` floor in `integrations/dagster/pyproject.toml`, and release the SDK first.

## Step 7 — VS Code: expose as a command

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

## Step 8 — Docs + engine README

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
