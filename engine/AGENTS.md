# Rocky

Rust SQL transformation engine. Replaces dbt's core responsibilities (DAG resolution, incremental logic, SQL generation, schema management) with a compiled, type-safe approach. No Jinja. No manifest. No parse step.

Rocky is a typed-program layer above the warehouse. It owns the DAG: compile-time types, branches + replay, column-level lineage, drift handling, incremental logic, and per-model cost attribution. Storage and compute stay with the warehouse (Databricks, Snowflake, BigQuery, DuckDB).

**License:** Apache 2.0

**Engine skills** live in `engine/.agents/skills/`: `rocky` (CLI cheat sheet), `databricks`, `fivetran`, and Rust conventions (`rust-style`, `rust-error-handling`, `rust-doc`, `rust-unsafe`, `rust-clippy-triage`, `rust-bench-criterion`, `rust-dep-hygiene`, `rust-async-tokio`, `rust-analyzer-ssr`). If your client does not load them on its own, read the matching `SKILL.md` before that kind of work.

## Key Concepts

- **Adapter-based ELT** — source adapters discover what data is available (metadata only, no extraction; the Fivetran and Airbyte adapters may start a vendor sync, but only when the key owner's `allow_sync_trigger` and the pipeline's `trigger = true` are both set, and it never moves data through Rocky); warehouse adapters execute SQL and manage governance. Rocky operates on data already in the warehouse — it does not extract from external systems
- **No model files for raw layer** — Rocky discovers tables at runtime from source adapters (Fivetran, manual config), no SQL files needed
- **Silver layer models** — either a `.sql` + `.toml` sidecar pair (pure SQL, no templating) or a `.rocky` DSL file; SQL stays first-class
- **Config-driven** — `rocky.toml` defines source, warehouse, target patterns, checks, and contracts
- **Schema patterns** — configurable prefix/separator/components parse schema names into structured data for routing

## Engine Repository Structure

Cargo workspace: the library crates under `crates/` plus the `rocky` and `rocky-lsp` binary crates. Rust edition 2024, MSRV 1.88:

The legacy `Plan` enum is gone; `ModelIr` is the sole transformation intermediate, dispatched via `ModelIrVariant`. The IR data types (`ModelIr`, `ModelIrVariant`, `ProjectIr`, lakehouse format/options, column lineage, masks, time grains, `RockyType`) live in their own `rocky-ir` crate; `rocky-core` keeps the runtime surface (adapter traits, DAG executor, state store, drift, SQL generation, breaking-change classifier, ci-diff).

```
engine/                         # this directory, inside the rocky monorepo
├── Cargo.toml                  # Workspace manifest
├── crates/                     # `ls crates/<name>/src` for files; grep for symbols
│   ├── rocky-ir/               # Typed IR, data only: ir.rs (ModelIr, ModelIrVariant, ProjectIr, MaterializationStrategy, TargetRef, SourceRef, governance, grants, masks), types.rs (RockyType, common_supertype, is_assignable), dag.rs, lineage.rs, lakehouse.rs, mask.rs, time_grain.rs
│   ├── rocky-core/             # Warehouse-agnostic engine: sql_gen.rs, drift.rs, state.rs (redb) + state_sync.rs (S3/Valkey/tiered), unified_dag.rs + dag_executor.rs, models.rs (.sql + .toml), checks.rs, contracts.rs, breaking_change.rs, ci_diff.rs, schema.rs, catalog.rs, config.rs, hooks/, optimize.rs, source.rs, traits.rs (SqlDialect), lakehouse.rs (DDL); tests/e2e.rs (DuckDB, no credentials)
│   ├── rocky-cli/              # CLI framework: commands/ (one file per subcommand or helper; doctor.rs), output.rs (JSON/table formatters), pipes.rs (Dagster Pipes emitter, active when DAGSTER_PIPES_CONTEXT/MESSAGES are set; docs: `../docs/src/content/docs/dagster/pipes.md`)
│   ├── rocky-sql/              # SQL parsing + typed AST (sqlparser-rs); validation.rs
│   ├── rocky-lang/             # Rocky DSL lexer + parser; lower.rs compiles .rocky to a SQL string (not IR)
│   ├── rocky-compiler/         # Type checking + semantic analysis
│   ├── rocky-secret-registry/  # Resolved ${VAR} values -> ${NAME}; no deps, so crates without rocky-core use it too
│   ├── rocky-server/           # HTTP API + LSP server
│   ├── rocky-mcp/              # MCP server (rmcp, stdio) behind `rocky mcp`
│   ├── rocky-fulfill/          # Fulfillment reconciler behind `rocky fulfill`; uses `rocky-cli` only through `commands/fulfill_api.rs`
│   ├── rocky-verify/           # Offline verifier for rocky-manifest v0.1 attestations
│   ├── rocky-cache/            # Memory → Valkey → API cache; Valkey tier behind cargo feature `valkey`
│   ├── rocky-adapter-sdk/, rocky-catalog-core/   # Adapter SDK; shared catalog primitives
│   ├── adapters:               # rocky-{databricks (throttle.rs: AIMD concurrency), snowflake, bigquery, duckdb, trino (`trino-conformance` feature), postgres (+ Redshift), clickhouse (beta), sqlserver, spark (beta), iceberg}; sources: rocky-{fivetran, airbyte}
│   ├── other:                  # rocky-{ai, engine, observe, wasm}; rocky-core-compiletest, rocky-cli-compiletest (compile-fail guards)
├── rocky/                      # Binary crate (the `rocky` CLI); src/main.rs holds the clap `Command` enum
├── rocky-lsp/                  # Binary crate (the standalone LSP server)
├── ui/                         # The browser UI `rocky serve --ui` embeds (vite + React + Tailwind Plus); `npm run build` → ui/dist, embedded behind cargo feature `ui`
└── examples/                   # 15 self-contained example projects (DuckDB, no credentials)
```

## User surface

The CLI verbs, global flags, `rocky.toml` sections and defaults, model file format, schema pattern and hooks are in the engine `rocky` skill: `engine/.agents/skills/rocky/references/user-surface.md`. Read it before you change behavior a user sees. Key facts:

- There is no `rocky drift` verb. Drift runs inside `rocky run` and `rocky plan` (`RunOutput.drift`, `drift.rs`).
- `rocky run` ignores every hook outcome. Only `rocky hooks test` acts on `on_failure = "abort"`.
- `--state-path` defaults to `models/.rocky-state.redb` beside the config file.

## Coding Standards

### Rust
- Edition 2024
- Use `thiserror` for library errors, `anyhow` for binary/CLI errors
- Use `#[async_trait]` for async trait definitions
- All public types must derive `Debug`, `Clone`, `Serialize`, `Deserialize` where applicable
- Use `tracing` for logging, not `println!` or `eprintln!`
- SQL identifiers must be validated via `rocky-sql/validation.rs` before interpolation — never use `format!()` to build SQL with untrusted input
- Run `cargo clippy` and `cargo fmt` before committing
- Tests go in the same file (`#[cfg(test)] mod tests`) unless they need fixtures

## Common Commands

```bash
# Build
cargo build
cargo build --release

# Test
cargo test                            # All unit + integration tests (fine locally)
cargo nextest run --all-features      # What CI runs (engine-ci.yml)
cargo test -p rocky-core              # Test specific crate
cargo test -p rocky-core --test e2e   # E2E integration tests only (DuckDB, no credentials)
cargo test -- --nocapture             # Show println output

# Lint (match CI — --all-targets lints test code too, --all-features covers gated code)
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt -- --check

# Run
cargo run -- discover --config rocky.toml --output json
cargo run -- plan --config rocky.toml --filter client=acme
cargo run -- run --config rocky.toml --filter client=acme
cargo run -- run --resume-latest --config rocky.toml --filter client=acme  # Resume failed run
cargo run -- doctor --config rocky.toml --output json   # Health checks
```

## Git Conventions

Follow "Git conventions" in the root [`AGENTS.md`](../AGENTS.md): no attribution trailers, conventional commits scoped by subproject and crate (`feat(engine/rocky-databricks): …`), and the changelog gate.

## Rocky DSL (.rocky files)

Rocky has its own transformation language as an alternative to SQL. Files use `.rocky` extension with a `.toml` sidecar for metadata.

**Pipeline steps** (top-to-bottom data flow):
- `from <model>` — Start pipeline from a model/source
- `where <predicate>` — Filter rows (becomes HAVING after group)
- `group <keys> { agg: func(...) }` — Aggregate
- `derive { name: expr }` — Add computed columns (preserves existing)
- `select { col1, col2 }` — Choose columns (replaces column set)
- `join <model> as <alias> on <key> { keep ... }` — Join
- `sort <col> asc|desc` — Order results
- `take <n>` — Limit rows
- `distinct` — Deduplicate
- `replicate <source>` — Replicate a source table

**Key difference from SQL:** `!=` compiles to `IS DISTINCT FROM` (NULL-safe).

The language spec is at `../docs/rocky-lang-spec.md`; the published DSL page is `../docs/src/content/docs/concepts/rocky-dsl.md`.

## CI/CD

GitHub Actions workflows live at the monorepo root in `../.github/workflows/`, path-filtered to `engine/**`:
- `engine-ci.yml` — Tests, clippy, fmt (on push/PR to main). Note: `CARGO_BUILD_JOBS=4` due to DuckDB C++ memory constraints.
- `engine-weekly.yml` — Coverage (tarpaulin) + security audit, runs Monday 08:00 UTC + manual dispatch.
- `engine-release.yml` — Full 5-target matrix build on tag `engine-v*` (macOS ARM64/Intel, Linux x86_64/ARM64, Windows). `scripts/release.sh` is a local-build fallback.
- `engine-bench.yml` — Benchmark on PRs labeled `perf` touching `engine/crates/**` or `engine/Cargo.*`; uploads raw bencher output as an artifact (no baseline comparison — there is deliberately no main-branch bench history).
- `engine-docs.yml` — Build + deploy Astro docs from `../docs/` to GitHub Pages.

## SQL Patterns Rocky Generates

**Incremental copy (the core operation):** watermark-filtered INSERT via `WHERE _fivetran_synced > TIMESTAMP '<prior>'` — the runner reads the prior watermark from the redb state store, threads it into SQL gen as a literal, and after the write records `MAX(ts)` over the **target** as the next watermark (incremental and microbatch; other strategies record the run time). It reads the target, not the source, so a row appended to the source mid-run is never skipped. For `strategy = "incremental"` the runner also reads `MAX(ts)` over the **source** once before the copy and adds `AND ts < <source max>` (the first-run and drift-rebuild CTAS get `WHERE (ts < <source max> OR ts IS NULL)`), so the newest batch waits one run and a late row tied with it is not skipped (#2376); `row_count` then counts the source below that cutoff. `rocky plan` shows the unbounded SQL. See `sql_gen.rs` (`generate_held_back_*`) and `commands/run.rs::resolve_new_watermark` / `query_target_max_timestamp` / `query_source_max_timestamp`.

**Schema drift:** `DESCRIBE TABLE` source vs target → safe type widening (`ALTER COLUMN TYPE`) or full refresh. See `drift.rs` and the per-dialect `SqlDialect::is_safe_type_widening()` (trait in `rocky-core/src/traits.rs`).

**Materialized views / dynamic tables:** `CREATE OR REPLACE MATERIALIZED VIEW` (Databricks) or `DYNAMIC TABLE ... TARGET_LAG` (Snowflake).

**Time-interval materialization:** partition-keyed via `@start_date`/`@end_date` placeholders. CLI: `rocky run --partition KEY` / `--from KEY --to KEY` / `--latest` / `--missing`. Per-partition state in the `PARTITIONS` redb table. Compiler diagnostics E020-E026. See `../docs/src/content/docs/concepts/time-interval.md`.

**Databricks-specific SQL, REST APIs, and auth:** use the engine-local `databricks` skill when the active agent client exposes it; otherwise start with `crates/rocky-databricks/src/` and the adapter's referenced API documentation.

**Snowflake auth:** OAuth (highest priority) → RS256 key-pair JWT → password. See `crates/rocky-snowflake/src/auth.rs`.

## Validation Rules

- **SQL identifiers** — `^[a-zA-Z0-9_]+$`. Never interpolate unvalidated strings into SQL.
- **Principal names** — `^[a-zA-Z0-9_ \-\.@]+$`. Always wrap in backticks in SQL.

## JSON Output Schema

Rocky's CLI output is the interface contract with orchestrators (e.g., Dagster). Every command's `--output json` payload is backed by a typed Rust struct in `crates/rocky-cli/src/output.rs` (or `crates/rocky-cli/src/commands/doctor.rs` for doctor) that derives `JsonSchema` via the `schemars` crate. JSON Schemas are exported to `../schemas/` and used to autogenerate Pydantic models for the dagster integration and TypeScript interfaces for the VS Code extension.

To change a CLI output, follow the root `../AGENTS.md` ("When modifying the CLI's JSON output schema") and the `rocky-codegen` skill. To find a payload's struct, open `../schemas/<command>.schema.json` (its `title` is the struct name) or grep `output.rs`. A new command registers its type in `crates/rocky-cli/src/commands/export_schemas.rs::schemas()`; the full checklist is the `rocky-new-cli-command` skill.

Plus one shared schema: `adapter_config.schema.json` — generated from `AdapterConfig` (derives `JsonSchema`). It's embedded in other command outputs rather than emitted standalone.

Test fixtures for the dagster integration are captured from the live binary by `../scripts/regen_fixtures.sh` (runs against `examples/playground/pocs/00-foundations/01-replication-basics/` — the replication-shape POC; `00-playground-default` is now a transformation pipeline).

## LSP Server

`rocky lsp` runs as a Language Server Protocol server over stdio. Used by the VS Code extension at `../editors/vscode/`.

Provides: diagnostics, hover, go-to-definition, find-references, rename, completion, signature help, document symbols, code actions, inlay hints, semantic tokens.

In `initialized` it registers `workspace/didChangeWatchedFiles` for `**/*.rocky`, `**/*.toml` and `**/models/**/*.sql` with any client that advertises dynamic registration, in a spawned task (awaiting the request there would stall the handshake).

Implementation in `crates/rocky-server/`.
