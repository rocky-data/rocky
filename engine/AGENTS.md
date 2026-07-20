# Rocky

Rust SQL transformation engine. Replaces dbt's core responsibilities (DAG resolution, incremental logic, SQL generation, schema management) with a compiled, type-safe approach. No Jinja. No manifest. No parse step.

Rocky is a typed-program layer above the warehouse. It owns the DAG: compile-time types, branches + replay, column-level lineage, drift handling, incremental logic, and per-model cost attribution. Storage and compute stay with the warehouse (Databricks, Snowflake, BigQuery, DuckDB).

**License:** Apache 2.0

## Key Concepts

- **Adapter-based ELT** — source adapters discover what data is available (metadata only, no extraction); warehouse adapters execute SQL and manage governance. Rocky operates on data already in the warehouse — it does not extract from external systems
- **No model files for raw layer** — Rocky discovers tables at runtime from source adapters (Fivetran, manual config), no SQL files needed
- **Silver layer models** — either a `.sql` + `.toml` sidecar pair (pure SQL, no templating) or a `.rocky` DSL file; SQL stays first-class
- **Config-driven** — `rocky.toml` defines source, warehouse, target patterns, checks, and contracts
- **Schema patterns** — configurable prefix/separator/components parse schema names into structured data for routing

## Engine Repository Structure

Cargo workspace with 23 library crates + 2 binary crates (`rocky` + `rocky-lsp`) — 25 members total. Rust edition 2024, MSRV 1.88:

The legacy `Plan` enum is gone; `ModelIr` is the sole transformation intermediate, dispatched via `ModelIrVariant`. The IR data types (`ModelIr`, `ModelIrVariant`, `ProjectIr`, lakehouse format/options, column lineage, masks, time grains, `RockyType`) live in their own `rocky-ir` crate; `rocky-core` keeps the runtime surface (adapter traits, DAG executor, state store, drift, SQL generation, breaking-change classifier, ci-diff).

```
engine/                         # this directory, inside the rocky monorepo
├── Cargo.toml                  # Workspace manifest
├── crates/
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
│   │       └── e2e.rs          # E2E integration tests (DuckDB-backed, no credentials needed)
│   ├── rocky-sql/              # SQL parsing + typed AST (sqlparser-rs)
│   │   └── src/
│   │       ├── parser.rs       # sqlparser-rs wrapper with typed extensions
│   │       ├── dialect.rs      # Databricks SQL dialect
│   │       ├── validation.rs   # SQL identifier validation, injection prevention
│   │       ├── transpile.rs    # SQL transpilation between dialects
│   │       └── lineage.rs      # SQL-level column lineage extraction
│   ├── rocky-lang/             # Rocky DSL parser (.rocky files)
│   │   └── src/                # Lexer (logos) + parser → IR lowering
│   ├── rocky-compiler/         # Type checking + semantic analysis
│   │   └── src/                # Semantic graph, contract validation, diagnostics
│   ├── rocky-ai/               # AI intent layer
│   │   └── src/                # explain, sync, test, generate (requires ANTHROPIC_API_KEY)
│   ├── rocky-server/           # HTTP API + LSP server
│   │   └── src/                # axum REST + tower-lsp for IDE integration
│   ├── rocky-engine/           # Local execution engine
│   │   └── src/                # DuckDB-backed execution (DataFusion + Arrow)
│   ├── rocky-adapter-sdk/      # Adapter SDK for building custom warehouse adapters
│   │   └── src/
│   ├── rocky-databricks/       # Databricks warehouse adapter
│   │   └── src/
│   │       ├── connector.rs    # SQL Statement Execution REST API (with is_transient/is_rate_limit)
│   │       ├── throttle.rs     # Adaptive concurrency (AIMD algorithm)
│   │       ├── catalog.rs      # Unity Catalog CRUD + tagging + isolation
│   │       ├── permissions.rs  # GRANT/REVOKE + SHOW GRANTS
│   │       ├── workspace.rs    # Workspace binding (isolation)
│   │       ├── auth.rs         # PAT + OAuth M2M auto-detection
│   │       └── batch.rs        # information_schema batching (UNION ALL)
│   ├── rocky-fivetran/         # Fivetran source adapter
│   │   └── src/
│   │       ├── adapter.rs      # Adapter trait implementation
│   │       ├── client.rs       # Async REST client (reqwest + Basic Auth)
│   │       ├── connector.rs    # Connector discovery + filtering
│   │       ├── schema.rs       # Schema config parsing (nested JSON)
│   │       └── sync.rs         # Sync detection (timestamp comparison)
│   ├── rocky-cache/            # Three-tier caching (memory → Valkey → API)
│   │   └── src/
│   │       ├── memory.rs       # In-process LRU with TTL
│   │       ├── valkey.rs       # Valkey/Redis distributed cache (feature-gated: `valkey`)
│   │       └── tiered.rs       # Fallback chain: memory → valkey → source
│   ├── rocky-snowflake/        # Snowflake warehouse adapter
│   │   └── src/
│   │       ├── auth.rs         # OAuth, password, RS256 key-pair JWT auth
│   │       ├── connector.rs    # Snowflake REST client
│   │       └── dialect.rs      # Snowflake SQL dialect
│   ├── rocky-duckdb/           # DuckDB local execution adapter
│   │   └── src/
│   │       ├── adapter.rs      # Adapter trait implementation
│   │       ├── dialect.rs      # DuckDB SQL dialect specifics
│   │       ├── discovery.rs    # Local schema/table discovery
│   │       ├── seed.rs         # Seed data loading (data/seed.sql)
│   │       ├── test_harness.rs # In-memory DuckDB test harness
│   │       └── types.rs        # DuckDB type mapping
│   ├── rocky-bigquery/          # BigQuery warehouse adapter
│   │   └── src/
│   │       ├── adapter.rs      # Adapter trait implementation
│   │       ├── dialect.rs      # BigQuery SQL dialect (backtick quoting, DML transactions)
│   │       └── auth.rs         # GCP Service Account / ADC authentication
│   ├── rocky-trino/            # Trino warehouse adapter (REST /v1/statement polling, Basic + JWT auth)
│   │   └── src/                # Adapter trait impl + Docker-backed conformance harness behind `trino-conformance`
│   ├── rocky-airbyte/          # Airbyte source adapter
│   │   └── src/                # Airbyte protocol integration
│   ├── rocky-iceberg/          # Apache Iceberg table format adapter
│   │   └── src/                # Iceberg metadata + snapshot management
│   ├── rocky-catalog-core/     # Shared catalog primitives across warehouse adapters
│   │   └── src/                # Catalog/schema lifecycle, isolation policies, grant primitives
│   ├── rocky-mcp/              # MCP server (rmcp, stdio transport) — backs `rocky mcp` for AI agents
│   │   └── src/
│   ├── rocky-wasm/             # WebAssembly exports
│   │   └── src/                # WASM-compatible entry points for browser/edge execution
│   ├── rocky-observe/          # Observability
│   │   └── src/
│   │       ├── metrics.rs      # In-process counters + histograms
│   │       ├── events.rs       # Event bus (tokio::broadcast) for pipeline lifecycle events
│   │       └── tracing_setup.rs # Structured JSON logging
│   └── rocky-cli/              # CLI framework
│       └── src/
│           ├── commands/       # init, validate, discover, plan, run, state, doctor, + ~50 more (one .rs file per subcommand)
│           │   └── doctor.rs   # Aggregate health checks (config, state, adapters, pipelines)
│           ├── pipes.rs        # Dagster Pipes protocol emitter — activates when DAGSTER_PIPES_CONTEXT/MESSAGES env vars set
│           └── output.rs       # JSON / table formatters (incl. MaterializationMetadata + sql_fingerprint helper)
├── rocky/                      # Binary crate (the `rocky` CLI)
│   └── src/
│       └── main.rs
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

### Modeling
```bash
rocky compile                        # Type-check models, validate contracts, column-level diagnostics
rocky lineage <model> [--column col] # Trace column-level lineage
rocky test                           # Run local tests via DuckDB
rocky ci                             # Combined compile + test for CI pipelines
```

### AI
```bash
rocky ai "<intent>"                  # Generate a model from natural language
rocky ai-explain <model>             # Generate intent description from code
rocky ai-sync                        # Propose intent-guided updates for schema changes
rocky ai-test <model>                # Generate test assertions from intent
```

### Development
```bash
rocky playground [path]              # Create a sample DuckDB project
rocky import-dbt --dbt-project <p>   # Convert a dbt project to Rocky
rocky serve                          # HTTP API server with watch mode
rocky lsp                            # Language Server Protocol for IDE integration
rocky mcp                            # Model Context Protocol server over stdio (for AI agents)
rocky init-adapter <name>            # Scaffold a new warehouse adapter crate
```

### Administration
```bash
rocky history                        # Run history with trend analysis
rocky metrics <model>                # Quality metrics with alerts
rocky optimize                       # Materialization cost recommendations
rocky compact                        # Generate OPTIMIZE/VACUUM SQL
rocky profile-storage                # Column encoding recommendations
rocky archive                        # Partition archival
rocky doctor                         # Aggregate health checks
rocky compare                        # Shadow vs production comparison
```

Schema drift is not a standalone verb — there is no `rocky drift` command. Drift
detection runs inside `rocky run` / `rocky plan` (surfaced on `RunOutput.drift`);
see `drift.rs`.

Global flags: `--config` (default: `rocky.toml`), `--output` (`json`|`table`), `--state-path` (default: `models/.rocky-state.redb`; an existing `.rocky-state.redb` in the current directory still works and emits a one-time deprecation warning)

## Git Conventions

- **Never** include `Co-Authored-By` trailers in commit messages
- Use conventional commits: `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`
- Scope by crate when relevant: `feat(rocky-databricks): add OAuth M2M auth`

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
- `engine-bench.yml` — Benchmark on PRs labeled `perf` touching `engine/crates/**` or `engine/Cargo.*` (120% alert threshold).
- `engine-docs.yml` — Build + deploy Astro docs from `../docs/` to GitHub Pages.

## Schema Pattern

Rocky uses a configurable schema pattern to map source schemas to target catalogs/schemas:

- **Source:** `<source_catalog>.src__{tenant}__{regions...}__{source}.table`
- **Target:** `{catalog_template}.raw__{regions}__{source}.table`
- Example: `src__acme__us_west__shopify` -> `acme_warehouse.raw__us_west__shopify`

## SQL Patterns Rocky Generates

**Incremental copy (the core operation):** watermark-filtered INSERT via `WHERE _fivetran_synced > TIMESTAMP '<prior>'` — the runner reads the previous run's `MAX(ts) FROM source` from the state store, threads it into SQL gen as a literal, and re-queries source post-execute to record the next watermark. See `sql_gen.rs` and `commands/run.rs::query_source_max_timestamp`.

**Schema drift:** `DESCRIBE TABLE` source vs target → safe type widening (`ALTER COLUMN TYPE`) or full refresh. See `drift.rs:is_safe_type_widening()`.

**Materialized views / dynamic tables:** `CREATE OR REPLACE MATERIALIZED VIEW` (Databricks) or `DYNAMIC TABLE ... TARGET_LAG` (Snowflake).

**Time-interval materialization:** partition-keyed via `@start_date`/`@end_date` placeholders. CLI: `rocky run --partition KEY` / `--from KEY --to KEY` / `--latest` / `--missing`. Per-partition state in the `PARTITIONS` redb table. Compiler diagnostics E020-E026 + W003. See `../docs/src/content/docs/concepts/time-interval.md`.

**Databricks-specific SQL, REST APIs, and auth:** use the engine-local `databricks` skill when the active agent client exposes it; otherwise start with `crates/rocky-databricks/src/` and the adapter's referenced API documentation.

**Snowflake auth:** OAuth (highest priority) → RS256 key-pair JWT → password. See `crates/rocky-snowflake/src/auth.rs`.

## Validation Rules

- **SQL identifiers** — `^[a-zA-Z0-9_]+$`. Never interpolate unvalidated strings into SQL.
- **Principal names** — `^[a-zA-Z0-9_ \-\.@]+$`. Always wrap in backticks in SQL.

## JSON Output Schema

Rocky's CLI output is the interface contract with orchestrators (e.g., Dagster). Every command's `--output json` payload is backed by a typed Rust struct in `crates/rocky-cli/src/output.rs` (or `crates/rocky-cli/src/commands/doctor.rs` for doctor) that derives `JsonSchema` via the `schemars` crate. JSON Schemas are exported to `../schemas/` and used to autogenerate Pydantic models for the dagster integration and TypeScript interfaces for the VS Code extension.

To change a CLI output:
1. Edit the relevant `*Output` struct in `crates/rocky-cli/src/output.rs`.
2. From the monorepo root, run `just codegen` (or `cd .. && cargo run --bin rocky -- export-schemas schemas/` if `just` isn't installed). This regenerates `../schemas/<command>.schema.json`, the Pydantic models in `../sdk/python/src/rocky_sdk/types_generated/` (re-exported by `dagster_rocky.types`), and the TypeScript interfaces in `../editors/vscode/src/types/generated/`.
3. Commit the schema and regenerated bindings together with the Rust change.
4. The `codegen-drift` CI workflow fails any PR where the committed bindings don't match what the engine produces locally — see `../.github/workflows/codegen-drift.yml`.

To add a new command schema, register the output type in `crates/rocky-cli/src/commands/export_schemas.rs::schemas()` and re-run `just codegen`.

Every Rocky CLI command that emits `--output json` has a typed Rust output struct deriving `JsonSchema`. The table below is a non-exhaustive snapshot — newer schemas (e.g. `branch_*`, `catalog`, `compliance`, `cost`, `preview_*`, `replay`, `retention_status`, `state_*` subcommands, `trace`) live in `schemas/` but aren't enumerated here yet. Run `ls schemas/*.schema.json | wc -l` for the current count.

| Command | Output struct |
|---|---|
| `rocky discover --output json` | `DiscoverOutput` (+ `ChecksConfigOutput` when pipeline declares `[checks]`) |
| `rocky run --output json` | `RunOutput` (materializations, check results, drift, permissions, anomalies; supports `--resume`/`--resume-latest`) |
| `rocky plan --output json` | `PlanOutput` |
| `rocky state --output json` | `StateOutput` (watermarks) |
| `rocky doctor --output json` | `DoctorOutput` (config, state, adapters, pipelines, state_sync) |
| `rocky drift --output json` | `DriftOutput` |
| `rocky compile --output json` | `CompileOutput` (typed model schemas, diagnostics, per-phase timings) |
| `rocky test --output json` | `TestOutput` (DuckDB-backed assertions) |
| `rocky ci --output json` | `CiOutput` (compile + test combined) |
| `rocky lineage <model>` | `LineageOutput` |
| `rocky lineage <model> --column <col>` | `ColumnLineageOutput` |
| `rocky history --output json` | `HistoryOutput` (recent runs) |
| `rocky history --model <name> --output json` | `ModelHistoryOutput` |
| `rocky metrics <model> --output json` | `MetricsOutput` (quality snapshots, alerts, column trend) |
| `rocky optimize --output json` | `OptimizeOutput` (materialization-strategy recommendations) |
| `rocky compare --output json` | `CompareOutput` (shadow vs production) |
| `rocky compact --output json` | `CompactOutput` (OPTIMIZE/VACUUM SQL plan) |
| `rocky archive --output json` | `ArchiveOutput` (DELETE + VACUUM SQL plan) |
| `rocky profile-storage --output json` | `ProfileStorageOutput` (encoding recommendations) |
| `rocky import-dbt --output json` | `ImportDbtOutput` (dbt → Rocky migration result) |
| `rocky validate-migration --output json` | `ValidateMigrationOutput` |
| `rocky test-adapter --output json` | `TestAdapterOutput` (conformance suite results) |
| `rocky hooks list --output json` | `HooksListOutput` |
| `rocky hooks test <event> --output json` | `HooksTestOutput` |
| `rocky ai "<intent>"` | `AiGenerateOutput` |
| `rocky ai-sync --output json` | `AiSyncOutput` |
| `rocky ai-explain --output json` | `AiExplainOutput` |
| `rocky ai-test --output json` | `AiTestOutput` |
| `rocky validate --output json` | `ValidateOutput` |
| `rocky compact --measure-dedup --output json` | `CompactDedupOutput` |
| `rocky seed --output json` | `SeedOutput` |
| `rocky dag --output json` | `DagOutput` |
| `rocky dag --run --output json` | `DagRunOutput` |
| `rocky ci --diff --output json` | `CiDiffOutput` |
| `rocky estimate --output json` | `EstimateOutput` |
| `rocky load --output json` | `LoadOutput` |

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
- `[governance]` — auto_create_catalogs, auto_create_schemas, tags, isolation, grants
- `[cost]` — storage_cost_per_gb_month, compute_cost_per_dbu, warehouse_size
- `[state]` — backend (local/s3/valkey/tiered)
- `[cache]` — valkey_url

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
on_failure = "warn"  # or "error"
```

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
