# CLI Command Groups (Plan 22)

Rocky has 30+ top-level commands, making `rocky --help` a wall of text.
This document defines the logical grouping that will be migrated to nested
subcommands in a follow-up phase. The foundation step adds classification
only; no existing command invocations change.

## Proposed groups

### `rocky pipeline` -- core pipeline operations

| Command            | Description                                     |
|--------------------|-------------------------------------------------|
| `run`              | Execute the full pipeline (discover -> copy)     |
| `plan`             | Generate SQL without executing (dry-run)         |
| `discover`         | Discover connectors and tables from the source   |
| `compare`          | Compare shadow tables against production         |
| `state`            | Show stored watermarks                           |
| `history`          | Show run history and model execution history     |

### `rocky model` -- model development and analysis

| Command   | Description                                        |
|-----------|----------------------------------------------------|
| `compile` | Resolve dependencies, type check, validate         |
| `test`    | Run local model tests via DuckDB                   |
| `lineage` | Show column-level lineage for a model              |
| `metrics` | Show quality metrics for a model                   |
| `optimize`| Analyze costs and recommend strategy changes       |
| `ci`      | Run CI pipeline: compile + test                    |

### `rocky infra` -- infrastructure and maintenance

| Command           | Description                                    |
|-------------------|------------------------------------------------|
| `doctor`          | Run health checks and report system status     |
| `hooks`           | Manage and test lifecycle hooks                |
| `archive`         | Archive old data partitions                    |
| `compact`         | Generate OPTIMIZE/VACUUM SQL                   |
| `profile-storage` | Profile storage and recommend encodings        |
| `watch`           | Watch models directory and auto-recompile      |

### `rocky dev` -- development and tooling

| Command          | Description                                      |
|------------------|--------------------------------------------------|
| `init`           | Initialize a new Rocky project                   |
| `playground`     | Create a sample project with DuckDB              |
| `serve`          | Start HTTP API server                            |
| `lsp`            | Start Language Server Protocol server             |
| `list`           | List project contents (pipelines, adapters, etc) |
| `shell`          | Interactive SQL shell                            |
| `validate`       | Validate config without connecting               |
| `bench`          | Run performance benchmarks                       |
| `export-schemas` | Export JSON Schema files for codegen             |
| `fmt`            | Format Rocky files (planned, not yet implemented)|

### `rocky migrate` -- migration tooling

| Command              | Description                                   |
|----------------------|-----------------------------------------------|
| `import-dbt`         | Import a dbt project as Rocky models          |
| `validate-migration` | Validate a dbt-to-Rocky migration             |
| `init-adapter`       | Scaffold a new warehouse adapter crate        |
| `test-adapter`       | Run conformance tests against an adapter      |

### `rocky data` -- data operations

| Command | Description                              |
|---------|------------------------------------------|
| `seed`  | Load CSV seed files into the warehouse   |

### `rocky ai` -- AI-powered features

| Command      | Description                                        |
|--------------|----------------------------------------------------|
| `ai`         | Generate a model from natural language intent       |
| `ai-sync`    | Detect schema changes and propose model updates     |
| `ai-explain` | Generate intent descriptions from model code        |
| `ai-test`    | Generate test assertions from model intent          |

## Backward compatibility

When the actual migration lands (follow-up to this foundation step):

1. **Nested commands** become the canonical form: `rocky pipeline run`, `rocky model compile`, etc.
2. **Top-level aliases** are preserved for backward compat: `rocky run` still works, dispatching to `rocky pipeline run`.
3. The `--help` output shows grouped commands by default. `rocky --help --flat` shows the legacy flat list.
4. Aliases emit a deprecation warning after a configurable sunset period.

## Classification function

The `CommandGroup` enum and `classify_command()` function in
`rocky-cli/src/commands/groups.rs` implement the mapping defined above.
Every command name (as it appears in the CLI) maps to exactly one group.
