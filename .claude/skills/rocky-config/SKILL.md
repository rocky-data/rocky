---
name: rocky-config
description: Canonical `rocky.toml` authoring reference. Use when writing or reviewing a Rocky pipeline config — covers the 4 pipeline types (replication, transformation, quality, snapshot), adapter variants (duckdb/databricks/snowflake/fivetran), minimal-config defaults, env-var substitution, governance, checks, hooks, and the ${VAR:-default} syntax.
---

# rocky.toml authoring reference

Rocky reads **one** config file — `rocky.toml` — for everything: adapters, pipelines, governance, state backend, cache. The Rust source of truth is `engine/crates/rocky-core/src/config.rs`. This skill is the canonical authoring reference.

## When to use this skill

- Writing a new `rocky.toml` from scratch (POC, example, production)
- Migrating an old config to current shape (watch for `[source]` / `[warehouse]` — those are pre-Phase-2 and no longer work)
- Deciding which pipeline type to use (replication vs transformation vs quality vs snapshot)
- Looking up an adapter's required fields (databricks needs host + http_path, snowflake needs auth variants, etc.)
- Adding governance, hooks, state sync, or checks to an existing config

## Top-level structure

Two mandatory sections (`[adapter]` + at least one `[pipeline.<name>]`) plus optional global blocks:

```toml
[adapter]                # Warehouse / source connection
# …

[pipeline.<name>]        # One or more pipelines — discriminated by `type` (default: replication)
# …

# Optional globals:
[state]                  # Embedded state store backend
[cache]                  # Valkey cache for adapter responses
[cost]                   # Cost model for `rocky optimize`
[governance]             # Tags, grants, workspace bindings (Databricks)
[hook.<event>]           # Lifecycle hooks (one per event)
```

## Env var substitution

Every string value supports `${VAR_NAME}` at parse time, with optional default:

```toml
token     = "${DATABRICKS_TOKEN}"              # hard-required
namespace = "${ROCKY_NAMESPACE:-default}"      # default when unset
workspace = "${WORKSPACE_IDS:-}"               # default to empty string
```

Substitution happens in `rocky-core/src/config.rs` before serde sees the value.

## Adapter variants

An unnamed `[adapter]` with a `type` key auto-wraps as `adapter.default`. Pipeline adapter refs default to `"default"` — so you can omit `adapter = "default"` lines everywhere.

### DuckDB (no credentials)

```toml
[adapter]
type = "duckdb"
path = "playground.duckdb"        # omit for in-memory; required if also used for discovery
```

### Databricks (PAT first, OAuth M2M fallback)

```toml
[adapter]
type      = "databricks"
host      = "${DATABRICKS_HOST}"                 # e.g. dbc-xxxx.cloud.databricks.com (no https://)
http_path = "${DATABRICKS_HTTP_PATH}"            # /sql/1.0/warehouses/<warehouse_id>

[adapter.auth]
token         = "${DATABRICKS_TOKEN}"            # PAT (tried first)
# client_id     = "${DATABRICKS_CLIENT_ID}"      # OAuth M2M (fallback)
# client_secret = "${DATABRICKS_CLIENT_SECRET}"
```

### Snowflake (OAuth > RS256 key-pair > password priority)

```toml
[adapter]
type     = "snowflake"
account  = "${SNOWFLAKE_ACCOUNT}"
username = "${SNOWFLAKE_USER}"

[adapter.auth]
# OAuth (pre-supplied token, highest priority):
# token = "${SNOWFLAKE_OAUTH_TOKEN}"

# RS256 key-pair JWT (preferred for service principals):
private_key_path = "${SNOWFLAKE_KEY_PATH}"

# Password (lowest priority):
# password = "${SNOWFLAKE_PASSWORD}"
```

### Fivetran (source adapter)

```toml
[adapter.fivetran]
type           = "fivetran"
destination_id = "${FIVETRAN_DESTINATION_ID}"
api_key        = "${FIVETRAN_API_KEY}"
api_secret     = "${FIVETRAN_API_SECRET}"
```

### Named adapters (multi-adapter configs)

```toml
[adapter.warehouse]
type = "databricks"
# …

[adapter.source]
type = "fivetran"
# …

[pipeline.raw]
[pipeline.raw.source]
adapter = "source"                 # ref by name

[pipeline.raw.target]
adapter = "warehouse"
```

## Pipeline types

Pipelines are discriminated by `type`. Default is `"replication"`, so a pipeline block with no `type` is a replication pipeline.

| `type` | What it does | Canonical use |
|---|---|---|
| `replication` (default) | Copies tables from source to target with schema-pattern discovery + incremental/full-refresh strategy | Raw/Bronze layer |
| `transformation` | Runs user SQL models against the warehouse and materializes them | Silver/Gold layer |
| `quality` | Runs data quality checks against existing tables | Standalone QA runs |
| `snapshot` | SCD-Type-2 snapshots with history tracking | Slowly-changing dimensions |

All four share `execution`, `checks`, `depends_on`, and `target.adapter`. They differ in what `source`/`target` shapes look like.

### Replication pipeline

```toml
[pipeline.raw]
# type = "replication"               # optional, this is the default
strategy         = "incremental"     # or "full_refresh"
timestamp_column = "_fivetran_synced"
metadata_columns = [
    { name = "_loaded_by", type = "STRING", value = "'rocky'" },
]

[pipeline.raw.source]
# adapter = "default"                # optional — first adapter by default
[pipeline.raw.source.schema_pattern]
prefix     = "src__"
separator  = "__"
components = ["client", "regions...", "connector"]
# "name"    = single segment
# "name..." = variable-length (1+)

[pipeline.raw.target]
catalog_template = "{client}_warehouse"
schema_template  = "staging__{regions}__{connector}"
```

### Transformation pipeline

```toml
[pipeline.silver]
type        = "transformation"
models_dir  = "models/silver"
contracts_dir = "contracts/silver"
depends_on  = ["raw"]

[pipeline.silver.target]
catalog = "analytics"
schema  = "marts"
```

### Quality pipeline

```toml
[pipeline.qa]
type       = "quality"
depends_on = ["silver"]

[pipeline.qa.target]
catalog = "analytics"
schema  = "marts"

[pipeline.qa.checks]
row_count    = true
column_match = true
freshness    = { threshold_seconds = 86400 }
```

### Snapshot pipeline

```toml
[pipeline.dim_history]
type       = "snapshot"
depends_on = ["silver"]

[pipeline.dim_history.source]
catalog = "analytics"
schema  = "marts"
table   = "dim_customers"

[pipeline.dim_history.target]
catalog = "analytics_history"
schema  = "snapshots"
```

## Minimal-config defaults (omit these)

The parser applies sane defaults — keep configs lean by omitting anything that matches the default:

| Field | Default | Omit unless |
|---|---|---|
| `pipeline.type` | `"replication"` | You need transformation/quality/snapshot |
| `adapter = "default"` (in pipeline source/target) | First adapter | Multi-adapter config |
| `[state]\nbackend = "local"` | local (embedded redb) | Using S3, Valkey, or tiered state sync |
| `auto_create_catalogs = false` | false | You want Rocky to CREATE CATALOG |
| `auto_create_schemas = false` | false | You want Rocky to CREATE SCHEMA |
| Model `name` in sidecar `.toml` | filename stem | You want a different logical name |
| Model `target.table` | `name` | Renaming on write |
| Directory-level `target` | `models/_defaults.toml` inherited | Overriding per-model |

## Checks (all four pipeline types)

```toml
[pipeline.<name>.checks]
enabled      = true
row_count    = true                                      # source vs target row count
column_match = true                                      # source vs target column list
freshness    = { threshold_seconds = 86400 }             # max staleness of newest row
null_rate    = { columns = ["email"], threshold = 0.05, sample_percent = 10 }

[[pipeline.<name>.checks.custom]]
name      = "no_future_dates"
sql       = "SELECT COUNT(*) FROM {target} WHERE created_at > CURRENT_TIMESTAMP()"
threshold = 0                                            # max failing rows
```

## Contracts

```toml
[pipeline.<name>.contracts]
required_columns = [
    { name = "id", type = "BIGINT", nullable = false },
]
protected_columns    = ["id", "email"]                   # may not be removed from source
allowed_type_changes = [
    { from = "INT", to = "BIGINT" },                     # widening allowlist
]
```

## Execution & adaptive concurrency

```toml
[pipeline.<name>.execution]
concurrency           = 16
fail_fast             = false
error_rate_abort_pct  = 50
table_retries         = 1
# adaptive_concurrency is planned but not yet a config field.
# The AIMD throttle primitive exists but isn't wired. See Plan 23.
```

## Governance (Databricks Unity Catalog)

```toml
[governance]
auto_create_catalogs = true
auto_create_schemas  = true

[governance.tags]
managed_by = "rocky"

[[governance.grants]]
principal   = "data-readers"
permissions = ["BROWSE", "USE CATALOG", "USE SCHEMA", "SELECT"]

[governance.isolation]
enabled       = true
workspace_ids = "${WORKSPACE_IDS:-}"
```

Permissions handled: BROWSE, USE CATALOG, USE SCHEMA, SELECT, MANAGE, MODIFY. Skipped: OWNERSHIP, ALL PRIVILEGES, CREATE SCHEMA (non-managed).

## State backend

```toml
# Embedded redb (default — no config needed)
[state]
backend = "local"

# S3-backed state sync
[state]
backend = "s3"
bucket  = "my-rocky-state"
prefix  = "prod/"
region  = "us-east-1"

# Tiered: local redb + S3 for durability
[state]
backend = "tiered"
# … S3 fields plus local path
```

## Cache (Valkey/Redis)

```toml
[cache]
valkey_url = "${VALKEY_URL}"
```

Feature-gated behind `valkey` Cargo feature in `rocky-cache`. Used for three-tier caching: memory → Valkey → source.

## Cost model (for `rocky optimize`)

```toml
[cost]
storage_cost_per_gb_month = 0.023
compute_cost_per_dbu      = 0.40
warehouse_size            = "Medium"
min_history_runs          = 5
```

## Hooks

```toml
[hook.on_pipeline_start]
command    = "scripts/notify.sh"
timeout_ms = 5000
on_failure = "warn"                  # or "error"

[hook.on_pipeline_fail]
url        = "${SLACK_WEBHOOK_URL}"   # webhook instead of command
template   = "default"                # built-in preset
```

Events: `on_pipeline_start`, `on_pipeline_complete`, `on_pipeline_fail`, `on_model_start`, `on_model_complete`, `on_model_fail`, `on_check_fail`, `on_drift_detected`.

## Model sidecar files (`.sql` + `.toml`)

Model files live under `models/` and use a sidecar pattern:

```toml
# models/marts/dim_customers.toml
name       = "dim_customers"
depends_on = ["stg_customers"]

[strategy]
type       = "merge"                 # full_refresh | incremental | merge
unique_key = ["customer_id"]
# update_columns = ["name", "email"] # omit for UPDATE SET *

[target]
catalog = "analytics"
schema  = "marts"
table   = "dim_customers"

[[sources]]                          # optional: declare source tables
catalog = "analytics"
schema  = "staging"
table   = "customers"
```

```sql
-- models/marts/dim_customers.sql
-- Pure SQL. No Jinja. No templating.
SELECT customer_id, name, email, updated_at
FROM {{ analytics.staging.customers }}        -- Rocky expands refs at compile time, not via Jinja
```

Directory-level defaults via `models/<dir>/_defaults.toml`:

```toml
[target]
catalog = "analytics"
schema  = "marts"
```

## Validation

SQL identifiers (catalog, schema, table, tenant, region, source names) must match `^[a-zA-Z0-9_]+$`. Rocky rejects anything else. Principal names for GRANT/REVOKE allow the broader `^[a-zA-Z0-9_ \-\.@]+$` pattern and are always wrapped in backticks in generated SQL.

## Time-interval (partition-keyed) materialization

For transformation models that need partition-by-partition execution:

```toml
# models/fact_events.toml
[strategy]
type              = "time_interval"
time_column       = "event_date"
granularity       = "day"                   # hour | day | month | year
lookback          = 7                       # re-run last 7 partitions
batch_size        = 4                       # partitions per concurrent batch
first_partition   = "2024-01-01"
```

Model SQL uses `@start_date` and `@end_date` placeholders — Rocky substitutes per partition:

```sql
SELECT * FROM raw.events
WHERE event_date >= @start_date AND event_date < @end_date
```

CLI: `rocky run --partition KEY` / `--from KEY --to KEY` / `--latest` / `--missing` / `--lookback N` / `--parallel N`.

## Full canonical example

See `examples/playground/pocs/00-foundations/00-playground-default/rocky.toml` for the minimal DuckDB case, or `engine/examples/multi-layer/rocky.toml` for a full Bronze/Silver/Gold setup.

## What NOT to write (pre-Phase-2 legacy)

The following keys **do not work** anymore — they were the pre-Phase-2 config shape and will be rejected by the parser:

| ❌ Legacy | ✅ Current |
|---|---|
| `[source]` top-level | `[pipeline.<name>.source]` |
| `[warehouse]` | `[adapter]` |
| `[replication]` top-level | `[pipeline.<name>]` with `strategy = "incremental"` |
| `[checks]` top-level | `[pipeline.<name>.checks]` |
| `[target]` top-level | `[pipeline.<name>.target]` |

If you see any of these in a config you're editing, the config is stale — migrate it.

## Reference

- `engine/crates/rocky-core/src/config.rs` — Rust source of truth for every field
- `engine/CLAUDE.md` — "Configuration" section with full annotated example
- `editors/vscode/schemas/rocky-config.schema.json` — JSON Schema for IDE autocompletion (autogenerated; keep in sync with `config.rs`)
- `examples/playground/CLAUDE.md` — POC-specific minimal-config idioms
