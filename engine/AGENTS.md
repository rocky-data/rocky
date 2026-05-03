# Rocky — Agent Instructions

Rocky is a Rust-based control plane for warehouse SQL pipelines. It owns the DAG: compile-time types, branches + replay, column-level lineage, drift handling, incremental logic, and per-model cost attribution. Storage and compute stay with the warehouse (Databricks, Snowflake, BigQuery, DuckDB).

## Key Concepts

- **Adapter-based ELT** — source adapters discover what data is available (metadata only, no extraction); warehouse adapters execute SQL and manage governance. Rocky operates on data already in the warehouse — it does not extract from external systems
- **No model files for raw layer** — Rocky discovers tables at runtime from source adapters (Fivetran, manual config), no SQL files needed
- **Silver layer uses `.sql` + `.toml` sidecar files** — pure SQL (no templating) with TOML config for strategy/dependencies
- **Config-driven** — `rocky.toml` defines source, warehouse, target patterns, checks, and contracts
- **Schema patterns** — configurable prefix/separator/components parse schema names into structured data for routing

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
rocky drift                          # Schema drift detection
```

Global flags: `--config` (default: `rocky.toml`), `--output` (`json`|`table`), `--state-path` (default: `models/.rocky-state.redb`; an existing `.rocky-state.redb` in the current directory still works and emits a one-time deprecation warning)

## Project Structure

```
my-project/
├── rocky.toml          # Pipeline configuration
└── models/                # Silver layer transformations (optional)
    ├── dim_customers.sql   # Pure SQL
    ├── dim_customers.toml  # Config (name, depends_on, strategy, target)
    ├── fct_orders.sql
    └── fct_orders.toml
```

## Configuration Format

`rocky.toml` uses `${ENV_VAR}` for environment variable substitution (with optional defaults: `${VAR:-default}`). Two main sections:

- `[adapter]` — warehouse connection (type: duckdb/databricks/snowflake, auth, host, http_path). Unnamed `[adapter]` auto-wraps as `adapter.default`.
- `[pipeline.<name>]` — pipeline definition (strategy, timestamp_column, metadata_columns)
  - `[pipeline.<name>.source]` — type (fivetran/manual), API credentials, schema_pattern
  - `[pipeline.<name>.target]` — catalog_template, schema_template (with `{variable}` placeholders)
  - `[pipeline.<name>.checks]` — row_count, column_match, freshness, null_rate, anomaly_threshold_pct; plus `fail_on_error`, `[[assertions]]` blocks (DQX parity: `not_null`, `unique`, `accepted_values`, `relationships`, `expression`, `row_count_range`, `in_range`, `regex_match`, `aggregate`, `composite`, `not_in_future`, `older_than_n_days` — each supports `severity` and `filter`), and `[quarantine]` (`mode = "split" | "tag" | "drop"`)
  - `[pipeline.<name>.execution]` — concurrency, fail_fast, error_rate_abort_pct, table_retries
- `[governance]` — auto_create_catalogs, auto_create_schemas, tags, isolation, grants
- `[cost]` — storage_cost_per_gb_month, compute_cost_per_dbu, warehouse_size
- `[state]` — backend (local/s3/valkey/tiered)
- `[cache]` — valkey_url

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

## Crate Architecture

20-crate Cargo workspace (Rust edition 2024, MSRV 1.85):

```
rocky-core         — Warehouse-agnostic engine: IR, adapter traits, DAG, models, checks, contracts, config, state
rocky-sql          — SQL parsing, validation, dialect support, transpilation, lineage
rocky-lang         — Rocky DSL parser (.rocky files) — lexer (logos) + parser → IR lowering
rocky-compiler     — Type checking, semantic analysis, contract validation, diagnostics
rocky-ai           — AI intent layer (explain, sync, test, generate — requires ANTHROPIC_API_KEY)
rocky-server       — HTTP API (axum) + LSP server (tower-lsp) for IDE integration
rocky-engine       — Local execution engine (DataFusion + Arrow, DuckDB-backed)
rocky-adapter-sdk  — Adapter SDK for building custom warehouse adapters
rocky-databricks   — Databricks warehouse adapter (SQL execution, Unity Catalog governance, permissions, workspace isolation)
rocky-snowflake    — Snowflake warehouse adapter (SQL execution, OAuth/key-pair/password auth)
rocky-bigquery     — BigQuery warehouse adapter (connector, auth, dialect)
rocky-fivetran     — Fivetran source adapter (REST API discovery of connectors/tables — metadata only, no extraction)
rocky-duckdb       — DuckDB warehouse adapter (local dev/testing, schema discovery, seed loading)
rocky-cache        — Three-tier caching (memory LRU → Valkey → API)
rocky-observe      — Structured JSON logging, metrics, tracing
rocky-airbyte      — Airbyte source adapter (protocol integration)
rocky-iceberg      — Apache Iceberg table format adapter (metadata + snapshot management)
rocky-cli          — CLI commands + JSON/table output formatters + Dagster Pipes protocol
rocky-wasm         — WebAssembly exports for browser/edge execution
rocky              — CLI binary
```

## When Helping Users

- **Config issues** → check `rocky.toml` syntax, env vars, schema pattern
- **SQL generation** → `rocky plan` shows exactly what SQL will run
- **New tables not appearing** → check source type, schema prefix, connector status
- **Check failures** → look at JSON output `check_results` for details
- **DAG errors** → check `depends_on` in model TOML files for cycles or unknown refs
- **Drift** → column type changes between source and target trigger full refresh
