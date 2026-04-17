---
title: All Features
description: Complete list of Rocky capabilities across compilation, execution, governance, and developer experience
sidebar:
  order: 11
---

## Warehouse Support

| Warehouse | Status | Auth methods |
|---|---|---|
| **Databricks** | Production | Unity Catalog, SQL Statement API, OAuth M2M |
| **Snowflake** | Beta | Key-pair JWT, OAuth, password |
| **BigQuery** | Beta | Service account, application default |
| **DuckDB** | Production | Embedded (local dev/test, no credentials) |

Source adapters: **Fivetran** (REST API discovery), **DuckDB** (information_schema), **Manual** (config-defined).

## Materialization Strategies (10)

| Strategy | Description |
|---|---|
| **full_refresh** | Drop and recreate table (default) |
| **incremental** | Append past a timestamp watermark |
| **merge** | Upsert via MERGE INTO with unique key |
| **time_interval** | Partition-keyed with per-partition state, lookback, parallel execution |
| **snapshot** | SCD Type 2 with valid_from/valid_to and hard delete tracking |
| **materialized_view** | Warehouse-managed materialized view (Databricks) |
| **dynamic_table** | Snowflake dynamic table with lag-based refresh |
| **ephemeral** | Inlined as CTE in downstream queries, no table created |
| **microbatch** | Alias for time_interval with hourly defaults (dbt-compatible) |
| **delete_insert** | Delete by partition key, then insert fresh data |

### Time Interval Materialization

Partition-keyed materialization with:
- `--partition KEY` — run a single partition
- `--from / --to` — run a date range
- `--latest` — run the partition containing now()
- `--missing` — discover and fill gaps from state store
- `--lookback N` — recompute N previous partitions for late-arriving data
- `--parallel N` — concurrent partition processing
- Per-partition state tracking (Computed / Failed / InProgress)
- Atomic writes: Databricks uses `INSERT INTO ... REPLACE WHERE`; Snowflake/DuckDB use transactional DELETE + INSERT

## Type-Safe Compiler

- **Static type inference** across the full DAG at compile time
- **Column type tracking** through JOINs, GROUP BYs, window functions
- **35+ diagnostic codes** (E001-E026, W001-W003, W010-W011, E010-E013, V001-V020) with actionable fix suggestions
- **Safe type widening detection**: INT → BIGINT, FLOAT → DOUBLE, VARCHAR expansion — handled via zero-copy ALTER TABLE
- **NULL-safe equality**: `!=` compiles to `IS DISTINCT FROM`
- **SELECT * expansion** with deduplication
- **Parallel type checking** via rayon across DAG execution layers
- **Data contracts** with required columns, protected columns (prevent removal), allowed type changes (widening whitelist), and nullability constraints

## Column-Level Lineage

- Computed at **compile time** — no warehouse query, no catalog rebuild
- Per-column trace through SQL and Rocky DSL transformations
- Transform tracking: Direct, Cast, Aggregation, Expression
- Output formats: **JSON**, **Graphviz DOT**, human-readable text
- CLI: `rocky lineage model.column`

## Schema Drift Detection

- **Automatic detection** at compile time
- **Safe type widening**: ALTER TABLE for compatible changes (INT → BIGINT, FLOAT → DOUBLE, VARCHAR expansion, numeric → STRING)
- **Graduated response**: ALTER for safe changes, full refresh for unsafe changes
- **Column addition/removal** detected with recommended actions
- **Shadow mode**: Write to `_rocky_shadow` tables for validation before production

## Data Quality Checks

### Pipeline-level checks

| Check | Description |
|---|---|
| **Row count** | Source vs target row count match |
| **Column match** | Missing/extra column detection (case-insensitive, with exclusion list) |
| **Freshness** | Timestamp lag against configurable threshold |
| **Null rate** | Per-column null percentage with TABLESAMPLE support |
| **Custom SQL** | Threshold-based checks with `{target}` placeholder |
| **Anomaly detection** | Row count deviation from historical baselines |

### Declarative assertions (DQX parity)

Model-level `[[assertions]]` blocks cover the full DQX surface:

| Kind | Description |
|---|---|
| `not_null`, `unique` | Null / duplicate detection |
| `accepted_values` | Value set membership |
| `relationships` | Referential integrity against another table |
| `expression` | Custom SQL boolean per row |
| `row_count_range` | Table-level row-count bounds |
| `in_range` | Numeric column bounds |
| `regex_match` | Dialect-specific regex (REGEXP / RLIKE / REGEXP_LIKE / REGEXP_CONTAINS) |
| `aggregate` | `SUM/COUNT/AVG/MIN/MAX(col) cmp value` |
| `composite` | Multi-column uniqueness |
| `not_in_future`, `older_than_n_days` | Time-window sugar |

Every assertion supports `severity` (`error` / `warning`), `fail_on_error` (pipeline-level override), and a per-check `filter` SQL predicate.

### Row quarantine

Row-level assertions can route failing rows with `[quarantine] mode = "split" | "tag" | "drop"`. `split` writes a sibling `<target>__quarantine` table; `tag` adds `__dqx_valid` boolean; `drop` discards.

All checks run inline during pipeline execution, not as a separate step. Full reference: [Data Quality Checks](/features/data-quality-checks/).

## Rocky DSL

Pipeline-oriented syntax that compiles to standard SQL:

| Step | Description |
|---|---|
| `from` | Load from source model or table |
| `where` | Filter rows |
| `derive` | Create computed columns |
| `group` | Aggregate with grouping |
| `select` | Project columns |
| `join` | Join another model |
| `sort` | Order rows |
| `take` | Limit row count |
| `distinct` | Remove duplicates |

Plus: window functions with PARTITION BY / ORDER BY / frame specs, `match` expressions (→ CASE WHEN), date literals (`@2025-01-01`), IS NULL / IS NOT NULL, IN lists.

## CLI Commands (38+)

### Core Pipeline
`init` · `validate` · `discover` · `plan` · `run` · `compare` · `state`

### Modeling & Compilation
`compile` · `lineage` · `test` · `ci` · `export-schemas`

### AI
`ai` (generate) · `ai-sync` · `ai-explain` · `ai-test`

### Development
`playground` · `serve` · `lsp` · `init-adapter` · `test-adapter` · `import-dbt` · `validate-migration`

### Administration
`doctor` · `history` · `metrics` · `optimize` · `compact` · `profile-storage` · `archive` · `bench` · `hooks list` · `hooks test`

## IDE / Language Server (11 capabilities)

Full LSP implementation via `rocky lsp`:

- Diagnostics (live compile errors/warnings)
- Hover (column types and lineage)
- Go to Definition
- Find References
- Completions (models, columns, functions)
- Rename (across all files)
- Code Actions (quick fixes)
- Inlay Hints (inline type annotations)
- Semantic Tokens (syntax highlighting)
- Signature Help (function parameters)
- Document Symbols (outline)

Published VS Code extension with TextMate grammar + semantic tokens.

## Dagster Integration

- **dagster-rocky** package with `RockyResource` and `RockyComponent`
- **Auto-discovery**: Rocky discover → Dagster asset definitions
- **Dagster Pipes protocol**: Hand-rolled emitter (no external dependency) — reports materializations, check results, drift observations, anomaly alerts
- **28 typed JSON output schemas** with auto-generated Pydantic v2 models and TypeScript interfaces via `rocky export-schemas`
- **Freshness policies** auto-attached from `[checks.freshness]` config
- **Column lineage** attached to derived model asset metadata

## Governance

- **Catalog lifecycle**: Auto-create catalogs with configurable tags
- **Schema lifecycle**: Auto-create schemas with tags
- **RBAC**: 6 permission types with declarative GRANT management
- **Permission reconciliation**: SHOW GRANTS → diff → GRANT/REVOKE
- **Workspace isolation**: Databricks catalog binding (READ_WRITE, READ_ONLY)
- **Resource tagging**: Component-derived tags (client, region, source) + static tags
- **Multi-tenant patterns**: Schema pattern routing (`src__client__region__connector`)

## State Management

Embedded **redb** state store (`.rocky-state.redb`) with:

| Table | Purpose |
|---|---|
| WATERMARKS | Per-table incremental progress |
| PARTITIONS | Per-partition lifecycle (Computed/Failed/InProgress) |
| CHECK_HISTORY | Row count snapshots for anomaly detection |
| RUN_HISTORY | Full run execution records |
| QUALITY_HISTORY | Check results and metrics |
| DAG_SNAPSHOTS | Compilation snapshots |
| RUN_PROGRESS | In-flight run progress (checkpoint/resume) |

Remote sync to **S3** or **Valkey/Redis** (tiered backend). No manifest file.

## Lifecycle Hooks (18 events)

Configurable shell commands or webhooks triggered on:

- **Pipeline**: start, discover_complete, compile_complete, complete, error
- **Table**: before_materialize, after_materialize, materialize_error
- **Model**: before_model_run, after_model_run, model_error
- **Checks**: before_checks, check_result, after_checks
- **State**: drift_detected, anomaly_detected, state_synced

Each hook supports `command`, `timeout_ms`, and `on_failure` behavior. Webhook hooks send POST with JSON templates.

## AI Features

Powered by Claude (via `ANTHROPIC_API_KEY`):

| Command | Description |
|---|---|
| `rocky ai <intent>` | Generate a model from natural language |
| `rocky ai-sync` | Detect schema changes and update models guided by stored intent |
| `rocky ai-explain` | Generate intent description from existing code |
| `rocky ai-test` | Generate test assertions from intent |

All AI commands are CLI-native, not cloud-dependent. Intent is stored as metadata in model TOML files.

## Performance

| Metric (10k models) | Value |
|---|---:|
| Compile | **1.00 s** |
| Per-model cost | **100 µs** |
| Peak memory | **147 MB** |
| Lineage | **0.84 s** |
| Warm compile | **0.72 s** |
| Startup | **14 ms** |
| Config validation | **15 ms** |
| SQL generation | **200 ms** (50k tables/sec) |

Linear scaling verified from 1k to 50k models. See [benchmarks](/features/benchmarks/) for full comparison with dbt-core and dbt-fusion.
