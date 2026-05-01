---
name: rocky
description: Rocky CLI quick reference. Use when you need to recall a `rocky <command>` invocation, JSON output shape, or exit-code behavior. For config authoring, use the `rocky-config` skill at the monorepo root. For the Rust output-struct cascade, use `rocky-codegen`.
---

# Rocky CLI reference

Concise cheat sheet for the `rocky` binary. For full detail:
- Config authoring → `rocky-config` skill (monorepo root `.claude/skills/`)
- JSON output schema cascade → `rocky-codegen` skill
- Engine architecture → `engine/CLAUDE.md`

## Commands

| Command | Purpose |
|---|---|
| `rocky init [path]` | Scaffold a new project |
| `rocky validate -c rocky.toml` | Validate config + models (no API calls) |
| `rocky compile --models models/ [--contracts contracts/]` | Type-check models, validate contracts |
| `rocky test --models models/ [--contracts contracts/]` | Run model tests against in-memory DuckDB (auto-loads `data/seed.sql`) |
| `rocky ci --models models/ [--contracts contracts/]` | Combined compile + test |
| `rocky discover -c rocky.toml` | List sources/connectors and tables |
| `rocky plan -c rocky.toml [--filter <k=v>]` | Preview SQL (dry-run) |
| `rocky run -c rocky.toml [--filter <k=v>]` | Execute the pipeline |
| `rocky run -c rocky.toml --resume-latest` | Resume a failed run from the last checkpoint |
| `rocky run -c rocky.toml --partition KEY` | Time-interval materialization (per-partition) |
| `rocky run -c rocky.toml --from KEY --to KEY` | Partition range |
| `rocky run -c rocky.toml --latest` / `--missing` | Partition discovery modes |
| `rocky state -c rocky.toml` | Show watermarks |
| `rocky history [--model <name>]` | Recent run history + trends |
| `rocky metrics <model>` | Quality snapshots, alerts, column trends |
| `rocky lineage <model> [--column <col>]` | Model/column lineage |
| `rocky lineage-diff [<base_ref>]` | Per-changed-column downstream impact between two git refs (PR-comment Markdown) |
| `rocky doctor -c rocky.toml` | Aggregate health checks (config, state, adapters, pipelines, state_sync) |
| `rocky drift -c rocky.toml` | Schema drift report |
| `rocky optimize -c rocky.toml` | Cost model + materialization-strategy recommendations |
| `rocky compare` | Shadow vs production comparison |
| `rocky compact <fqn>` / `rocky compact --catalog <name>` | OPTIMIZE/VACUUM SQL plan (single table or every Rocky-managed table in a catalog) |
| `rocky archive --older-than <span>` (`<fqn>` / `--catalog <name>`) | DELETE + VACUUM SQL plan (single table or catalog-wide) |
| `rocky profile-storage` | Column encoding recommendations |
| `rocky import-dbt <path>` | dbt → Rocky migration |
| `rocky validate-migration` | Validate a dbt → Rocky migration |
| `rocky hooks list` / `rocky hooks test <event>` | Hook management |
| `rocky ai "<intent>"` | AI-assisted model generation (needs `ANTHROPIC_API_KEY`) |
| `rocky ai-sync` / `ai-explain` / `ai-test` | AI intent layer sub-commands |
| `rocky test-adapter <name>` | Adapter conformance suite |
| `rocky lsp` | Language Server Protocol server over stdio (for the VS Code extension) |
| `rocky serve` | HTTP API mode |
| `rocky export-schemas <dir>` | Export JSON schemas for all `*Output` types (used by `just codegen`) |

## Flags worth remembering

| Flag | Effect |
|---|---|
| `--output json` | Emit typed JSON (every command has a JsonSchema-backed struct — see `rocky-codegen`) |
| `--output table` | Human-readable table (default) |
| `-c <path>`, `--config <path>` | Config file (default: `rocky.toml` in cwd) |
| `--state-path <path>` | Override embedded state store location |
| `--filter <k=v>` | Scope to matching pipelines/sources/groups (e.g. `--filter source=shopify`) |
| `--resume` / `--resume-latest` | Resume a checkpointed run |
| `--output-file <path>` | Write JSON output to file instead of stdout |

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Hard failure (config error, unreachable adapter, panic) |
| 2 | Partial success — pipeline ran but some tables failed. **Valid JSON still emitted on stdout.** The dagster integration explicitly handles this (`allow_partial=True`). |

## JSON output shape

Every `--output json` command emits a struct with `version` + `command` fields plus a command-specific payload. There are **37 schemas** total; full list in `engine/CLAUDE.md` → "JSON Output Schema". Highlights:

```
discover  → { connectors: [{ id, client, components, tables, excluded_tables }] }
plan      → { statements: [{ purpose, target, sql }] }
run       → { tables_copied, materializations, check_results, drift, permissions, anomalies }
state     → { watermarks: [{ table, last_value, updated_at }] }
doctor    → { config, state, adapters, pipelines, state_sync }
compile   → { models: [{ name, columns, contracts }], diagnostics, timings }
lineage   → { nodes, edges }  (— or ColumnLineageOutput when `--column` set)
history   → { runs: [...] }   (— or ModelHistoryOutput when `--model` set)
metrics   → { snapshots, alerts, column_trend }
```

To change any of these, see the **`rocky-codegen`** skill — the Rust struct is the source of truth and regenerated bindings ship with every change.

## Common issues

| Issue | Fix |
|---|---|
| `source.catalog not set` | Set the relevant adapter env var (e.g. `FIVETRAN_SOURCE_CATALOG`) |
| `schema does not start with prefix` | Check `[pipeline.<name>.source.schema_pattern].prefix` matches your schema naming |
| `no connectors found` | Check `destination_id`, API credentials, and that connectors are in "connected" state |
| `circular dependency` | Check `depends_on` in model sidecar `.toml` files |
| `unsafe SQL fragment` | Metadata column values must be NULL, numbers, or single-quoted strings |
| `drift detected, dropping target` | Source column type changed unsafely — target recreated automatically (see `drift.rs:is_safe_type_widening()` allowlist) |
| `tag engine-v… already exists` | You're trying to release — see the `rocky-release` skill |
| `codegen drift` in CI | You edited `output.rs` without running `just codegen` — see the `rocky-codegen` skill |

## Build + run from source

```bash
cd engine
cargo build                      # Debug
cargo build --release            # Release (used by `just codegen` + `regen-fixtures`)
cargo run -- discover -c rocky.toml --output json
cargo run -- plan     -c rocky.toml --filter source=shopify
cargo run -- run      -c rocky.toml --filter source=shopify
```

## Install from a release

```bash
# macOS / Linux
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash

# Windows
iwr -useb https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.ps1 | iex
```

Both scripts filter GitHub Releases by the `engine-v*` tag prefix.

## Dagster integration

```python
from dagster_rocky import RockyResource, load_rocky_assets
from dagster_rocky import RockyComponent

# ConfigurableResource (subprocess-based)
rocky  = RockyResource(config_path="rocky.toml")
assets = load_rocky_assets(rocky)

# State-backed component (cached discover/compile)
# In defs/rocky/defs.yaml:
#   type: dagster_rocky.RockyComponent
#   attributes:
#     config_path: rocky.toml
```

Three execution modes for `rocky run` in `RockyResource`:
- `run(...)` — buffered `subprocess.run`, no Dagster context
- `run_streaming(context, ...)` — `Popen` with stderr streaming to `context.log`
- `run_pipes(context, ...)` — full Dagster Pipes via `PipesSubprocessClient`

See `integrations/dagster/CLAUDE.md` for the layer architecture.

## Related skills

- **`rocky-config`** (monorepo root) — Full `rocky.toml` authoring reference
- **`rocky-codegen`** — JSON output struct cascade (Rust → Pydantic + TS)
- **`rocky-new-cli-command`** — Adding a new `rocky <verb>`
- **`rocky-dsl-change`** — Changing `.rocky` DSL syntax
- **`rocky-release`** — Tag-namespaced release workflow
- **`databricks-api`** / **`fivetran-api`** (this same engine-local skills dir) — REST API details for the two main adapters
