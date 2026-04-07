# Rocky CLI JSON Schemas

This directory holds the canonical JSON Schemas for every `rocky <command> --output json` payload type. They are the source of truth for the Pydantic models in `integrations/dagster/src/dagster_rocky/types.py` and the TypeScript interfaces in `editors/vscode/src/types/rockyJson.ts`.

## How they're generated

The schemas are derived from the Rust output structs in `engine/crates/rocky-cli/src/output.rs` and `engine/crates/rocky-cli/src/commands/doctor.rs` via [`schemars`](https://graham.cool/schemars/). To regenerate:

```bash
cd engine
cargo run --bin rocky -- export-schemas ../schemas
```

Or from the monorepo root, once the upcoming `just codegen` recipe is wired up:

```bash
just codegen
```

## What's covered today

| Schema | Source struct | Command |
|---|---|---|
| `discover.schema.json` | `rocky_cli::output::DiscoverOutput` | `rocky discover --output json` |
| `run.schema.json` | `rocky_cli::output::RunOutput` | `rocky run --output json` |
| `plan.schema.json` | `rocky_cli::output::PlanOutput` | `rocky plan --output json` |
| `drift.schema.json` | `rocky_cli::output::DriftOutput` | `rocky drift --output json` |
| `state.schema.json` | `rocky_cli::output::StateOutput` | `rocky state --output json` |
| `doctor.schema.json` | `rocky_cli::commands::doctor::DoctorOutput` | `rocky doctor --output json` |

## What's NOT yet covered

Seven CLI commands currently build their JSON output via `serde_json::json!()` blocks rather than typed structs. They will be refactored to use typed `JsonSchema`-deriving structs in subsequent Phase 2 rounds. Until then, the consumer types for these commands remain hand-maintained in dagster's `types.py` and vscode's `rockyJson.ts`.

- `compile`, `lineage`, `test`, `ci`
- `history`, `metrics`, `optimize`
- `ai`, `ai-sync`, `ai-explain`, `ai-test`

## Adding a new command schema

1. Define the output struct in `engine/crates/rocky-cli/src/output.rs` (or a sibling module).
2. Derive `JsonSchema` alongside the existing `Serialize`: `#[derive(Debug, Serialize, JsonSchema)]`.
3. Add an entry to `commands::export_schemas::schemas()` in `engine/crates/rocky-cli/src/commands/export_schemas.rs`.
4. Re-run `cargo run --bin rocky -- export-schemas ../schemas`.
5. Re-run the dagster + vscode codegen recipes (once wired).
6. Commit the new schema file.

CI will fail if the schemas drift from what the Rust source produces.
