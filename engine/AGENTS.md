# Rocky

Rust SQL transformation engine. Replaces dbt's core responsibilities (DAG resolution, incremental logic, SQL generation, schema management) with a compiled, type-safe approach. No Jinja. No manifest. No parse step.

Rocky is a typed-program layer above the warehouse. It owns the DAG: compile-time types, branches + replay, column-level lineage, drift handling, incremental logic, and per-model cost attribution. Storage and compute stay with the warehouse (Databricks, Snowflake, BigQuery, DuckDB).

**License:** Apache 2.0

**Engine skills** live in `engine/.agents/skills/` (mirrored in `engine/.claude/skills/`): `rocky` (CLI cheat sheet), `databricks`, `fivetran`, and Rust conventions (`rust-style`, `rust-error-handling`, `rust-doc`, `rust-unsafe`, `rust-clippy-triage`, `rust-bench-criterion`, `rust-dep-hygiene`, `rust-async-tokio`, `rust-analyzer-ssr`). If your client does not load them on its own, read the matching `SKILL.md` before that kind of work.

## Key Concepts

- **Adapter-based ELT** — source adapters discover what data is available (metadata only, no extraction; the Fivetran adapter may start a vendor sync, but only when the key owner's `allow_sync_trigger` and the pipeline's `trigger = true` are both set, and it never moves data through Rocky); warehouse adapters execute SQL and manage governance. Rocky operates on data already in the warehouse — it does not extract from external systems
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
├── crates/                   # Key files shown for core crates only; `ls crates/<name>/src` for the rest
│   ├── rocky-ir/               # Typed IR (data only — no runtime traits)
│   │   └── src/
│   │       ├── ir.rs           # ModelIr, ModelIrVariant, ProjectIr, MaterializationStrategy (FullRefresh, Incremental, Merge, MaterializedView, DynamicTable, TimeInterval), TargetRef, SourceRef, ColumnSelection, governance, grants, masks
│   │       ├── lakehouse.rs    # LakehouseFormat, LakehouseOptions, LakehouseError (data types — DDL gen lives in rocky-core)
│   │       ├── lineage.rs      # LineageEdge, QualifiedColumn
│   │       ├── mask.rs         # MaskStrategy
│   │       ├── time_grain.rs   # TimeGrain
│   │       ├── types.rs        # RockyType, StructField, TypedColumn, common_supertype, is_assignable
│   │       └── dag.rs          # DagNode, topological_sort, execution_layers
│   ├── rocky-core/             # Generic, warehouse-agnostic SQL transformation engine
│   │   ├── src/
│   │   │   ├── breaking_change.rs # Typed-IR semantic breaking-change classifier (BreakingFinding / BreakingChange / BreakingSeverity)
│   │   │   ├── ci_diff.rs      # Structural diff used by `rocky ci-diff`
│   │   │   ├── schema.rs       # Configurable schema pattern parsing
│   │   │   ├── drift.rs        # Schema drift detection + graduated evolution (ALTER TABLE for safe widenings)
│   │   │   ├── sql_gen.rs      # IR → dialect-specific SQL generation (incl. MV, dynamic table)
│   │   │   ├── lakehouse.rs    # Dialect-aware lakehouse DDL generator (depends on the SqlDialect trait)
│   │   │   ├── state.rs        # Embedded state store (redb): watermarks, run history, checkpoint/resume progress
│   │   │   ├── state_sync.rs   # Remote state persistence (S3, Valkey, tiered)
│   │   │   ├── catalog.rs      # Catalog/schema lifecycle management
│   │   │   ├── checks.rs       # Inline data quality checks
│   │   │   ├── contracts.rs    # Data contracts (required/protected columns)
│   │   │   ├── unified_dag.rs  # Unified ELT DAG — one graph across all pipeline stages
│   │   │   ├── dag_executor.rs # DAG-driven execution in dependency order (+ dag_status.rs live status)
│   │   │   ├── models.rs       # SQL model loading (sidecar .sql + .toml)
│   │   │   ├── hooks/          # Pipeline lifecycle hooks (commands, webhooks, templates, presets)
│   │   │   ├── optimize.rs     # Cost model + materialization strategy recommendations
│   │   │   ├── source.rs       # Source adapter traits + manual source config
│   │   │   ├── config.rs       # TOML config parsing (incl. [cost], [execution])
│   │   │   ├── intern.rs       # String interning for reduced allocations
│   │   │   ├── mmap.rs         # Memory-mapped file access
│   │   │   ├── poison.rs       # Poison pill helpers
│   │   │   ├── circuit_breaker.rs # Circuit breaker for flaky adapters
│   │   │   ├── traits.rs       # Shared trait definitions
│   │   │   └── unit_test.rs    # In-process test utilities
│   │   └── tests/
│   │       └── e2e.rs, …       # Integration tests (DuckDB-backed, no credentials needed); `ls` for the rest
│   ├── rocky-secret-registry/  # Resolved ${VAR} values -> ${NAME}; no deps, so crates without rocky-core use it too
│   ├── rocky-sql/              # SQL parsing + typed AST (sqlparser-rs)
│   ├── rocky-lang/             # Rocky DSL lexer + parser; lower.rs compiles .rocky to a SQL string (not IR)
│   ├── rocky-compiler/         # Type checking + semantic analysis
│   ├── rocky-ai/               # AI intent layer
│   ├── rocky-server/           # HTTP API + LSP server
│   ├── rocky-engine/           # Local execution engine
│   ├── rocky-adapter-sdk/      # Adapter SDK for building custom warehouse adapters
│   ├── rocky-databricks/       # Databricks warehouse adapter (incl. throttle.rs: AIMD adaptive concurrency)
│   ├── rocky-fivetran/         # Fivetran source adapter
│   ├── rocky-cache/            # Three-tier caching (memory → Valkey → API); Valkey tier behind cargo feature `valkey`
│   ├── rocky-snowflake/        # Snowflake warehouse adapter
│   ├── rocky-duckdb/           # DuckDB local execution adapter
│   ├── rocky-bigquery/         # BigQuery warehouse adapter
│   ├── rocky-trino/            # Trino warehouse adapter (REST /v1/statement polling, Basic + JWT auth); Docker conformance harness behind `trino-conformance`
│   ├── rocky-postgres/         # PostgreSQL + Amazon Redshift warehouse adapters
│   ├── rocky-clickhouse/       # ClickHouse warehouse adapter (beta)
│   ├── rocky-sqlserver/        # SQL Server, Azure SQL and Fabric Warehouse adapter
│   ├── rocky-spark/            # Apache Spark adapter over Spark Connect (beta)
│   ├── rocky-airbyte/          # Airbyte source adapter
│   ├── rocky-iceberg/          # Apache Iceberg table format adapter
│   ├── rocky-catalog-core/     # Shared catalog primitives across warehouse adapters
│   ├── rocky-mcp/              # MCP server (rmcp, stdio transport) — backs `rocky mcp` for AI agents
│   ├── rocky-wasm/             # WebAssembly exports
│   ├── rocky-observe/          # Observability
│   ├── rocky-fulfill/          # Fulfillment reconciler behind `rocky fulfill`
│   ├── rocky-verify/           # Standalone offline verifier for rocky-manifest v0.1 attestations
│   ├── rocky-core-compiletest/ # trybuild compile-fail guards for rocky-core's #[must_use] boundaries
│   ├── rocky-cli-compiletest/  # Compile-fail proof that rocky-cli's plan writers stay pub(crate)
│   └── rocky-cli/              # CLI framework
│       └── src/
│           ├── commands/       # init, validate, discover, plan, run, state, doctor, + the rest (about 100 files, one per subcommand or helper; the clap `Command` enum is in `rocky/src/main.rs`)
│           │   └── doctor.rs   # Aggregate health checks (config, state, adapters, pipelines)
│           ├── pipes.rs        # Dagster Pipes protocol emitter — activates when DAGSTER_PIPES_CONTEXT/MESSAGES env vars set
│           └── output.rs       # JSON / table formatters (incl. MaterializationMetadata + sql_fingerprint helper)
├── rocky/                      # Binary crate (the `rocky` CLI); src/main.rs holds the clap `Command` enum
├── rocky-lsp/                  # Binary crate (the standalone LSP server)
├── ui/                         # The browser UI `rocky serve --ui` embeds (vite + React + Tailwind Plus); `npm run build` → ui/dist, embedded behind cargo feature `ui`
└── examples/                   # 15 self-contained example projects (DuckDB, no credentials)
```

## User Project Structure

```
my-project/
├── rocky.toml          # Pipeline configuration
└── models/                # Silver layer transformations (optional)
    ├── dim_customers.sql   # Pure SQL
    ├── dim_customers.toml  # Config (name, depends_on, strategy, target)
    ├── fct_orders.sql
    └── fct_orders.toml
```

## Model File Format (Silver Layer)

Each model is a pair: `name.sql` (pure SQL) + `name.toml` (config).

**TOML config:**
```toml
name = "fct_orders"
depends_on = ["stg_orders", "dim_customers"]

[strategy]
type = "merge"           # or "incremental" or "full_refresh"
unique_key = ["order_id"]

[target]
catalog = "analytics"
schema = "marts"
table = "fct_orders"
```

**SQL file:** Plain SQL, no templating. Use `{target}` in custom check SQL only.

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

# Release binaries are built by CI (engine-release.yml, 5-target matrix) on engine-v* tags.
# scripts/release.sh is the local fallback — see the rocky-release skill.
```

## CLI Commands

### Core Pipeline
```bash
rocky init [path]                    # Scaffold a new project
rocky validate                       # Check config, schema patterns, models without APIs
rocky discover                       # Discover available schemas/tables (metadata only, no extraction)
rocky plan --filter key=value        # Preview SQL without executing (e.g., --filter client=acme)
rocky run --filter key=value         # Execute full pipeline: discover → drift → create → copy → check
rocky state                          # Show stored watermarks
```

### Everything else

Rocky has about 75 top-level verbs: modeling (`compile`, `lineage`, `test`, `ci`), AI (`ai`, `ai-explain`, `ai-sync`, `ai-test`), development (`playground`, `import-dbt`, `serve`, `lsp`, `mcp`, `init-adapter`), administration (`history`, `metrics`, `optimize`, `doctor`, …) and more. This file does not list them. Use `rocky --help` and `rocky <verb> --help`, or the grouped reference pages in `../docs/src/content/docs/reference/commands/`.

### Products (spec-driven fulfillment, gate side)
```bash
rocky product verify <name>      # Frozen propose_only posture + tag resolution + collisions (exit 0/1/2)
rocky product compile <name>     # Verify, then lower: Phase A contract / Phase B sidecar merge (staged commit)
rocky product approve <name>     # Human authority transition: immutable snapshot + one state-store txn
rocky product status <name>      # Read-only: spec, lowering, artifact verification, approval, loop state
rocky product list               # Read-only: one status row per product (spec dir ∪ state-store records), sorted
rocky product journal <name>     # Read-only: the fulfillment journal, every persisted transition in append order
```

Product specs live at `products/<name>.toml`; `rocky validate` checks them offline (V050–V053). Reference: `../docs/src/content/docs/reference/commands/products.md`.

### Fulfillment (spec-driven fulfillment, loop side — EXPERIMENTAL)
```bash
rocky fulfill <name> [--retry]     # Drive the reconciler: elicit → approve → lower → draft (agent) → verify → propose → review → apply → observe
rocky fulfill approve-spec <name>  # Same authority transition as `rocky product approve` (one implementation, two spellings)
```

The loop lives in `crates/rocky-fulfill` (state machine + subprocess agent driver) and consumes `rocky-cli` ONLY through `commands/fulfill_api.rs`; configure it via `[fulfill.driver]` in `rocky.toml`. Reference: `../docs/src/content/docs/reference/commands/fulfill.md`.

Schema drift is not a standalone verb — there is no `rocky drift` command. Drift
detection runs inside `rocky run` / `rocky plan` (surfaced on `RunOutput.drift`);
see `drift.rs`.

Global flags: `--config` (default: `rocky.toml`), `--output` (`json`|`table`), `--state-path` (default: `models/.rocky-state.redb`; an existing `.rocky-state.redb` in the current directory still works and emits a one-time deprecation warning)

## Git Conventions

- **Never** include `Co-Authored-By` trailers in commit messages
- Use conventional commits: `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`
- Scope by subproject and crate, as the root `AGENTS.md` says: `feat(engine/rocky-databricks): add OAuth M2M auth`

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

## Schema Pattern

Rocky uses a configurable schema pattern to map source schemas to target catalogs/schemas:

- **Source:** `<source_catalog>.src__{tenant}__{regions...}__{source}.table`
- **Target:** `{catalog_template}.raw__{regions}__{source}.table`
- Example: `src__acme__us_west__shopify` -> `acme_warehouse.raw__us_west__shopify`

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

To change a CLI output, follow the steps in the root `../AGENTS.md` ("When modifying the CLI's JSON output schema"); the `rocky-codegen` skill has the detail and pitfalls. In short: edit the struct, run `just codegen` from the monorepo root, and commit the schema and bindings with the Rust change, or `codegen-drift.yml` fails the PR. A new command also registers its type in `crates/rocky-cli/src/commands/export_schemas.rs::schemas()`; the full checklist is the `rocky-new-cli-command` skill.

Every `--output json` payload has a typed output struct. To find one, open `../schemas/<command>.schema.json` (its `title` is the struct name) or grep `output.rs`. The registry of exported types is `crates/rocky-cli/src/commands/export_schemas.rs::schemas()`. Full workflow and pitfalls: the `rocky-codegen` skill.

Plus one shared schema: `adapter_config.schema.json` — generated from `AdapterConfig` (derives `JsonSchema`). It's embedded in other command outputs rather than emitted standalone.

Test fixtures for the dagster integration are captured from the live binary by `../scripts/regen_fixtures.sh` (runs against `examples/playground/pocs/00-foundations/01-replication-basics/` — the replication-shape POC; `00-playground-default` is now a transformation pipeline).

## Configuration

Rocky reads `rocky.toml`. Env vars substituted at parse time: `${VAR_NAME}` (with defaults: `${VAR:-default}`). Two top-level sections: `[adapter]` (warehouse connection) and `[pipeline.<name>]` (pipeline definition). For the full config reference, schema, and examples, see the `rocky-config` skill at the monorepo root (`.agents/skills/rocky-config/SKILL.md`).

Section reference:

- `[adapter]` — warehouse connection (type: duckdb/databricks/snowflake/bigquery/trino, auth, host, http_path). Unnamed `[adapter]` auto-wraps as `adapter.default`.
- `[pipeline.<name>]` — pipeline definition (strategy, timestamp_column, metadata_columns)
  - `[pipeline.<name>.source]` — type (fivetran/manual), API credentials, schema_pattern
  - `[pipeline.<name>.target]` — catalog_template, schema_template (with `{variable}` placeholders)
  - `[pipeline.<name>.checks]` — row_count, column_match, freshness, null_rate, anomaly_threshold_pct; plus `fail_on_error`, `[[assertions]]` blocks (DQX parity: `not_null`, `unique`, `accepted_values`, `relationships`, `expression`, `row_count_range`, `in_range`, `regex_match`, `aggregate`, `composite`, `not_in_future`, `older_than_n_days` — each supports `severity` and `filter`), and `[quarantine]` (`mode = "split" | "tag" | "drop"`)
  - `[pipeline.<name>.execution]` — concurrency, fail_fast, error_rate_abort_pct, table_retries
- `[pipeline.<name>.target.governance]` — auto_create_catalogs, auto_create_schemas, tags, isolation, grants (per target; there is no top-level `[governance]` table)
- `[cost]` — storage_cost_per_gb_month, compute_cost_per_dbu, warehouse_size
- `[state]` — backend (local/s3/gcs/valkey/tiered)
- `[cache.schemas]` — the schema cache: enabled, ttl_seconds, replicate, trusted_max_age_seconds, strict_sources (the last two drive the E041/W041 missing-source-column check, and `strict_sources` also escalates the W045 missing-source-table warning to E045; there is no other `[cache]` key)

**Key defaults** (omit when redundant):
- `pipeline.type` → `"replication"`, unnamed `[adapter]` → `adapter.default`
- `[state] backend = "local"`, `auto_create_catalogs = false`
- Model sidecar `name` → filename stem, `target.table` → `name`
- `models/_defaults.toml` provides directory-level `[target]` defaults

## Hooks System

Rocky supports lifecycle hooks configured in `rocky.toml`. Hooks fire shell commands or webhooks on pipeline events.

```toml
[hook.on_pipeline_start]
command = "scripts/notify.sh"
timeout_ms = 5000
on_failure = "warn"  # or "abort" or "ignore" (silent); "warn" is the default
```

`rocky run` ignores every hook outcome: each call site discards the result of `fire`, so a failing hook is logged and the run continues whatever `on_failure` says. Only `rocky hooks test` acts on `abort`. A hook cannot gate a run today.

Implementation in `crates/rocky-core/src/hooks/` (mod.rs, webhook.rs, template.rs, presets.rs).

## LSP Server

`rocky lsp` runs as a Language Server Protocol server over stdio. Used by the VS Code extension at `../editors/vscode/`.

Provides: diagnostics, hover, go-to-definition, find-references, rename, completion, signature help, document symbols, code actions, inlay hints, semantic tokens.

Implementation in `crates/rocky-server/`.

## When Helping Users

- **Config issues** → check `rocky.toml` syntax, env vars, schema pattern
- **SQL generation** → `rocky plan` shows exactly what SQL will run
- **New tables not appearing** → check source type, schema prefix, connector status
- **Check failures** → look at JSON output `check_results` for details
- **DAG errors** → check `depends_on` in model TOML files for cycles or unknown refs
- **Drift** → safe type widenings become `ALTER COLUMN TYPE`; unsafe changes trigger full refresh

## Sibling subprojects in the monorepo

- `../integrations/dagster/` — Dagster integration (Python, wraps the Rocky CLI as a `ConfigurableResource`). The CLI also emits the [Dagster Pipes](https://docs.dagster.io/concepts/dagster-pipes) wire protocol when the dagster client sets `DAGSTER_PIPES_CONTEXT` + `DAGSTER_PIPES_MESSAGES` — see `crates/rocky-cli/src/pipes.rs` and `../docs/src/content/docs/dagster/pipes.md`.
- `../editors/vscode/` — VS Code extension (TypeScript, LSP client over stdio)
- `../examples/playground/` — Sample DuckDB pipeline used for smoke tests and benchmarks (no credentials needed)

When changing the CLI's JSON output schema or the Rocky DSL syntax, see the cascade rules in `../AGENTS.md` — those changes touch sibling subprojects in lockstep.
