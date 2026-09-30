# Rocky CLI JSON Schemas

This directory holds the canonical JSON Schemas for every `rocky <command> --output json` payload type. They are the source of truth for:

- the generated Pydantic v2 models in `sdk/python/src/rocky_sdk/types_generated/`
- the generated TypeScript interfaces in `editors/vscode/src/types/generated/`

`editors/vscode/src/types/rockyJson.ts` is a re-export shim over the generated TypeScript. `integrations/dagster/src/dagster_rocky/types.py` re-exports `rocky_sdk.types`.

The SDK also keeps hand-written runtime models in `sdk/python/src/rocky_sdk/types.py`. `parse_rocky_output()` returns these models. Codegen does not update them. When you add a field to an output struct, add it to the matching model in that file by hand. `sdk/python/tests/test_schema_parity.py` fails when a model misses a top-level schema field. It checks field names only, and it looks inside only a few known nested objects.

## How they're generated

Schemas are derived from the Rust output structs in `engine/crates/rocky-cli/src/output.rs` and `engine/crates/rocky-cli/src/commands/doctor.rs` via [`schemars`](https://graham.cool/schemars/). Regenerate from the monorepo root:

```bash
just codegen
```

That runs five steps:

1. `just codegen-rust` — runs `cargo run --quiet --bin rocky -- export-schemas ../schemas` from `engine/`. It builds the engine with the dev profile, not release, and writes the schema files here.
2. `just codegen-sdk` — runs `datamodel-codegen` over `schemas/*.schema.json` into `sdk/python/src/rocky_sdk/types_generated/`.
3. `just codegen-vscode` — runs `json2ts` per schema into `editors/vscode/src/types/generated/`.
4. `just codegen-vscode-project-schema` — copies `rocky_project.schema.json` to `editors/vscode/schemas/rocky-project.schema.json`.
5. `just codegen-openapi` — writes the `rocky serve` OpenAPI document to `docs/public/openapi.json`.

Drift is enforced by the `codegen-drift.yml` CI workflow: any PR where the committed bindings diverge from what `just codegen` produces locally will fail CI. The same check runs locally via the `.git-hooks/pre-commit` hook; enable it once with `just install-hooks`.

To regenerate only the schemas (skip the downstream bindings):

```bash
cd engine
cargo run --bin rocky -- export-schemas ../schemas
```

## What's covered

Every schema file in this directory maps to a typed Rust output struct. The full list, including the command each one backs, lives in `engine/AGENTS.md` under "JSON Output Schema". Highlights:

| Schema | Command |
|---|---|
| `discover.schema.json` | `rocky discover --output json` |
| `run.schema.json` | `rocky run --output json` |
| `plan.schema.json` | `rocky plan --output json` |
| `state.schema.json` | `rocky state --output json` |
| `doctor.schema.json` | `rocky doctor --output json` |
| `compile.schema.json` | `rocky compile --output json` |
| `test.schema.json` | `rocky test --output json` |
| `ci.schema.json` / `ci_diff.schema.json` | `rocky ci --output json` / `rocky ci-diff` |
| `lineage.schema.json` / `column_lineage.schema.json` | `rocky lineage` / `rocky lineage --column` |
| `history.schema.json` / `model_history.schema.json` | `rocky history` / `rocky history --model` |
| `dag.schema.json` / `dag_run.schema.json` | `rocky dag` / `rocky run --dag` |
| `metrics.schema.json` | `rocky metrics <model> --output json` |
| `optimize.schema.json` | `rocky optimize --output json` |
| `compare.schema.json` | `rocky compare --output json` |
| `compact.schema.json` / `compact_dedup.schema.json` | `rocky compact --output json` / `rocky compact --measure-dedup` |
| `archive.schema.json` | `rocky archive --output json` |
| `profile_storage.schema.json` | `rocky profile-storage --output json` |
| `import_dbt.schema.json` | `rocky import-dbt --output json` |
| `validate.schema.json` / `validate_migration.schema.json` | `rocky validate` / `rocky validate-migration` |
| `test_adapter.schema.json` | `rocky test-adapter --output json` |
| `hooks_list.schema.json` / `hooks_test.schema.json` | `rocky hooks list` / `rocky hooks test <event>` |
| `ai.schema.json` / `ai_sync.schema.json` / `ai_explain.schema.json` / `ai_test.schema.json` | `rocky ai*` commands |
| `estimate.schema.json` | `rocky estimate --output json` |
| `load.schema.json` | `rocky load --output json` |
| `seed.schema.json` | `rocky seed --output json` |
| `adapter_config.schema.json` | Shared adapter-config schema (used across commands) |

## Adding a new command schema

1. Define the output struct in `engine/crates/rocky-cli/src/output.rs` (or a sibling module) deriving `JsonSchema`:
   ```rust
   #[derive(Debug, Serialize, Deserialize, JsonSchema)]
   pub struct MyCommandOutput { ... }
   ```
2. Register it in `engine/crates/rocky-cli/src/commands/export_schemas.rs::schemas()`.
3. From the monorepo root, run `just codegen`.
4. Review the diff: you should see changes in `schemas/`, `sdk/python/src/rocky_sdk/types_generated/`, and `editors/vscode/src/types/generated/`.
5. Wire the new command into the SDK and the dagster consumer per the checklist in `.claude/skills/rocky-new-cli-command/SKILL.md`.
6. Commit the Rust source, the schema, and the regenerated bindings in one commit.

CI will fail if the committed bindings drift from what `just codegen` produces locally.
