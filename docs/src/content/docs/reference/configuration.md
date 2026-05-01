---
title: Configuration
description: Complete rocky.toml reference
sidebar:
  order: 2
---

Rocky reads a single `rocky.toml` file for all configuration. The file uses **named adapters** (`[adapter.NAME]`) and **named pipelines** (`[pipeline.NAME]`), so a single config can host multiple sources, warehouses, and pipelines side by side.

## Config Inference and Defaults

Rocky applies sensible defaults to minimize boilerplate. Many fields can be omitted:

| What | Default | When to omit |
|---|---|---|
| `pipeline.type` | `"replication"` | Always (unless using a different type) |
| Unnamed `[adapter]` with a `type` key | Auto-wraps as `adapter.default` | Single-adapter projects |
| Pipeline `adapter` refs | `"default"` | When only one adapter is defined |
| `[state]\nbackend = "local"` | `"local"` | Local development (always the default) |
| `auto_create_catalogs` / `auto_create_schemas` | `false` | When you don't need auto-creation |
| Model sidecar `name` | Filename stem | When file is `fct_orders.toml` and name is `fct_orders` |
| Model sidecar `target.table` | Same as `name` | When table name matches model name |
| `models/_defaults.toml` | N/A | Provides directory-level `[target]` defaults for `catalog` and `schema` |

### Directory-level defaults

Create a `models/_defaults.toml` to avoid repeating `[target]` in every model:

```toml
# models/_defaults.toml
[target]
catalog = "analytics"
schema = "warehouse"
```

Individual models inherit these defaults and only need to override what differs.

## At a glance

```toml
# Define one or more adapter instances by name
[adapter.local]
type = "duckdb"
path = "warehouse.duckdb"

# Define one or more pipelines and reference adapters by name
[pipeline.replication]
type = "replication"
strategy = "full_refresh"

[pipeline.replication.source]
adapter = "local"

[pipeline.replication.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.replication.target]
adapter = "local"
catalog_template = "warehouse"
schema_template = "analytics"

[state]
backend = "local"
```

The same config can declare additional adapters (`[adapter.prod_databricks]`, `[adapter.prod_fivetran]`) and additional pipelines, and pipelines select which adapters to use via the `adapter = "..."` field on `source`/`target`.

## Environment Variables

Environment variables can be referenced anywhere in the config using `${VAR_NAME}` syntax. They are substituted at parse time before TOML is evaluated.

```toml
[adapter.prod]
type = "databricks"
host = "${DATABRICKS_HOST}"
token = "${DATABRICKS_TOKEN}"
```

If a referenced variable is not set, Rocky returns a parse error listing the missing variable.

### Default Values

Use `${VAR_NAME:-default}` to provide a fallback when a variable is unset or empty:

```toml
[state]
backend = "${ROCKY_STATE_BACKEND:-local}"
s3_bucket = "${ROCKY_STATE_BUCKET:-}"
```

If `ROCKY_STATE_BACKEND` is not set, it defaults to `"local"`. If `ROCKY_STATE_BUCKET` is not set, it defaults to an empty string.

---

## `[adapter.NAME]`

Each `[adapter.NAME]` block defines one adapter instance. The `name` is arbitrary — pipelines reference adapters by this name. The `type` field selects which adapter implementation handles the connection.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | Adapter type. One of `"databricks"`, `"snowflake"`, `"duckdb"`, `"fivetran"`, `"manual"`. |
| `retry` | table | No | Retry policy (see [`[adapter.NAME.retry]`](#adapternameretry)). |

The remaining fields depend on the adapter type.

### `type = "duckdb"`

Local in-process execution adapter. Use as a warehouse, source, or both — the same adapter instance can handle discovery and execution because they share the same database.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `path` | string | No | (in-memory) | Path to a persistent DuckDB file. Required when using the same DuckDB adapter for both discovery and execution, so the discovery side sees rows written by the warehouse side. |

```toml
# In-memory DuckDB
[adapter.local]
type = "duckdb"

# Persistent DuckDB file
[adapter.local]
type = "duckdb"
path = "warehouse.duckdb"
```

### `type = "databricks"`

Databricks SQL warehouse adapter. Executes SQL via the Statement Execution REST API and manages Unity Catalog governance.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `host` | string | Yes | Workspace hostname (e.g., `"workspace.cloud.databricks.com"`). |
| `http_path` | string | Yes | SQL warehouse HTTP path (e.g., `"/sql/1.0/warehouses/abc123"`). |
| `token` | string | No | Personal Access Token. Tried first if set. |
| `client_id` | string | No | OAuth M2M client ID (service principal). Used as fallback when `token` is not set. |
| `client_secret` | string | No | OAuth M2M client secret. Required if `client_id` is set. |
| `timeout_secs` | integer | No | Statement execution timeout in seconds (default `120`). Increase for large full-refresh queries. |

```toml
[adapter.prod]
type = "databricks"
host = "${DATABRICKS_HOST}"
http_path = "${DATABRICKS_HTTP_PATH}"
token = "${DATABRICKS_TOKEN}"
```

OAuth M2M instead of PAT:

```toml
[adapter.prod]
type = "databricks"
host = "${DATABRICKS_HOST}"
http_path = "${DATABRICKS_HTTP_PATH}"
client_id = "${DATABRICKS_CLIENT_ID}"
client_secret = "${DATABRICKS_CLIENT_SECRET}"
```

### `type = "snowflake"`

Snowflake warehouse adapter. Supports OAuth, key-pair (RS256 JWT), and password authentication.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `account` | string | Yes | Snowflake account identifier (e.g., `"org-account"`). |
| `warehouse` | string | Yes | Warehouse name for query execution. |
| `database` | string | No | Default database. |
| `schema` | string | No | Default schema. |
| `role` | string | No | Role to assume. |
| `username` | string | No | Username for key-pair or password auth. |
| `password` | string | No | Password for password auth. |
| `private_key_path` | string | No | Path to PKCS#8 PEM private key for key-pair JWT auth. |
| `oauth_token` | string | No | Pre-supplied OAuth token from an IdP. |

Authentication priority: OAuth (highest) > Key-pair JWT > Password (lowest).

```toml
# Key-pair JWT auth
[adapter.snow]
type = "snowflake"
account = "${SNOWFLAKE_ACCOUNT}"
warehouse = "COMPUTE_WH"
username = "${SNOWFLAKE_USER}"
private_key_path = "${SNOWFLAKE_KEY_PATH}"

# Password auth
[adapter.snow]
type = "snowflake"
account = "${SNOWFLAKE_ACCOUNT}"
warehouse = "COMPUTE_WH"
username = "${SNOWFLAKE_USER}"
password = "${SNOWFLAKE_PASSWORD}"
```

### `type = "fivetran"`

Fivetran source adapter. Calls the Fivetran REST API to discover connectors and tables. **Metadata only** — Rocky never moves data through this adapter.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `destination_id` | string | Yes | Fivetran destination ID. |
| `api_key` | string | Yes | Fivetran API key (Basic Auth). |
| `api_secret` | string | Yes | Fivetran API secret (Basic Auth). |

```toml
[adapter.fivetran]
type = "fivetran"
destination_id = "${FIVETRAN_DESTINATION_ID}"
api_key = "${FIVETRAN_API_KEY}"
api_secret = "${FIVETRAN_API_SECRET}"
```

### `type = "manual"`

Lets you define source schemas and tables inline in `rocky.toml` instead of discovering them from an API. Useful for tests and small fixed sources.

### `[adapter.NAME.retry]`

Retry policy for transient errors (HTTP 429/503, rate limits, timeouts). Uses exponential backoff with optional jitter.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_retries` | integer | `3` | Maximum retry attempts. Set to 0 to disable. |
| `initial_backoff_ms` | integer | `1000` | Initial backoff in milliseconds. |
| `max_backoff_ms` | integer | `30000` | Maximum backoff cap in milliseconds. |
| `backoff_multiplier` | float | `2.0` | Multiplier applied after each retry. |
| `jitter` | bool | `true` | Add random jitter to prevent thundering herd. |
| `circuit_breaker_threshold` | integer | `5` | Trip after this many consecutive failures. Set to 0 to disable. |
| `circuit_breaker_recovery_timeout_secs` | integer | `null` | When set, the breaker auto-recovers after this many seconds: it enters half-open, admits a single trial request, and either closes on success or re-opens on failure. When unset, a tripped breaker stays tripped for the rest of the run (manual-reset behaviour). |
| `max_retries_per_run` | integer | `null` | Per-adapter cross-statement retry budget for a single run. Use the top-level [`[retry]`](#retry) block when you want one shared budget across every adapter instead. |

```toml
[adapter.prod.retry]
max_retries = 5
initial_backoff_ms = 500
max_backoff_ms = 60000
circuit_breaker_recovery_timeout_secs = 30
```

When the breaker trips, Rocky emits a `circuit_breaker_tripped` pipeline event; on auto-recovery it emits `circuit_breaker_recovered`. Hook subscribers can observe both without polling the adapter. See [Hooks](/concepts/hooks/).

---

## `[pipeline.NAME]`

Each `[pipeline.NAME]` block defines a pipeline. The `name` is arbitrary — Rocky CLI commands accept `--pipeline NAME` to select one when multiple are defined.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `type` | string | No | `"replication"` | Pipeline type. One of `"replication"`, `"transformation"`, `"quality"`, `"snapshot"`, `"load"`. The remaining fields depend on the type — the fields below apply to `"replication"` (the default). |
| `strategy` | string | No | `"incremental"` | Replication strategy: `"incremental"` or `"full_refresh"`. |
| `timestamp_column` | string | No | `"_fivetran_synced"` | Watermark column for incremental strategy. |
| `metadata_columns` | list | No | `[]` | Extra columns to add to copied data (see below). |

```toml
[pipeline.bronze]
type = "replication"
strategy = "incremental"
timestamp_column = "_fivetran_synced"
metadata_columns = [
    { name = "_loaded_by", type = "STRING", value = "NULL" },
    { name = "_loaded_at", type = "TIMESTAMP", value = "CURRENT_TIMESTAMP()" },
]
```

The `value` field is inserted as a SQL expression. Use `"NULL"` for null values and SQL function calls like `"CURRENT_TIMESTAMP()"` for computed values.

### `[pipeline.NAME.source]`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `adapter` | string | Yes | Name of the adapter that owns the source data. Must match a `[adapter.NAME]` key. |
| `catalog` | string | No | Source catalog name (used by warehouse-resident sources like Databricks). |

```toml
[pipeline.bronze.source]
adapter = "fivetran"
```

### `[pipeline.NAME.source.discovery]`

Optional override for the adapter that lists schemas/tables. Useful when the source is discovered from one system (e.g., DuckDB) but its data lives somewhere else.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `adapter` | string | Yes | Adapter name to use for discovery. |

```toml
[pipeline.bronze.source.discovery]
adapter = "fivetran"
```

If omitted, Rocky uses the source `adapter` for discovery.

### `[pipeline.NAME.source.schema_pattern]`

Defines how source schema names are parsed into structured components.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `prefix` | string | Yes | Prefix that identifies managed schemas (e.g., `"src__"`). |
| `separator` | string | Yes | Separator between components (e.g., `"__"`). |
| `components` | list of strings | Yes | Ordered list of component names. A trailing `"..."` marks a component as multi-valued. |

```toml
[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["client", "regions...", "connector"]
```

Given `src__acme__us_west__us_east__shopify`, this pattern extracts:

| Component | Value |
|-----------|-------|
| `client` | `"acme"` |
| `regions` | `["us_west", "us_east"]` |
| `connector` | `"shopify"` |

### `[pipeline.NAME.target]`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `adapter` | string | Yes | Name of the warehouse adapter. Must match a `[adapter.NAME]` key. |
| `catalog_template` | string | Yes | Template for the target catalog name. Uses `{component}` placeholders (or `{component:SEP}` to pin the join separator for variadic components). |
| `schema_template` | string | Yes | Template for the target schema name. Uses `{component}` placeholders (or `{component:SEP}` to pin the join separator for variadic components). |

```toml
[pipeline.bronze.target]
adapter = "prod"
catalog_template = "warehouse"
schema_template = "stage__{source}"
```

Given `source=shopify`:

| Template | Result |
|----------|--------|
| `warehouse` | `warehouse` (static — no substitution) |
| `stage__{source}` | `stage__shopify` |

For multi-tenant setups with per-tenant catalogs, use `{component}` placeholders in `catalog_template` — see [Schema Patterns](/concepts/schema-patterns/) for the full pattern reference (e.g. `catalog_template = "{tenant}_warehouse"` with `components = ["tenant", "regions...", "source"]`).

### `[pipeline.NAME.target.governance]`

Catalog/schema lifecycle, tagging, grants, and isolation. These features are implemented against Databricks Unity Catalog APIs and apply only when the target adapter is Databricks.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `auto_create_catalogs` | bool | `false` | Create target catalogs if they do not exist. |
| `auto_create_schemas` | bool | `false` | Create target schemas if they do not exist. |
| `tags` | table | `{}` | Tags applied to managed catalogs, schemas, and tables. |
| `grants` | list | `[]` | Catalog-level grants. Each entry has `principal` (string) and `permissions` (list of strings). |
| `schema_grants` | list | `[]` | Schema-level grants. Same format as `grants`. |
| `isolation` | table | | Workspace isolation settings (see below). |

```toml
[pipeline.bronze.target.governance]
auto_create_catalogs = true
auto_create_schemas = true

[pipeline.bronze.target.governance.tags]
managed_by = "rocky"
environment = "production"

[[pipeline.bronze.target.governance.grants]]
principal = "group:data_engineers"
permissions = ["USE CATALOG", "MANAGE"]

[[pipeline.bronze.target.governance.schema_grants]]
principal = "group:data_engineers"
permissions = ["USE SCHEMA", "SELECT", "MODIFY"]
```

### `[pipeline.NAME.target.governance.isolation]`

Workspace isolation for Databricks Unity Catalog. Binds managed catalogs to specific workspaces and optionally enables isolated mode.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Set catalog isolation mode to `ISOLATED`. |
| `workspace_ids` | list of tables | `[]` | Workspace bindings — see below. |

Each entry in `workspace_ids` is a table with two fields:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | integer | required | Databricks workspace ID. |
| `binding_type` | string | `"READ_WRITE"` | Access level: `"READ_WRITE"` or `"READ_ONLY"`. |

```toml
[pipeline.bronze.target.governance.isolation]
enabled = true

[[pipeline.bronze.target.governance.isolation.workspace_ids]]
id = 7474656540609532
binding_type = "READ_WRITE"

[[pipeline.bronze.target.governance.isolation.workspace_ids]]
id = 7474647537929812
binding_type = "READ_ONLY"
```

The binding type maps to the Databricks API values `BINDING_TYPE_READ_WRITE` and `BINDING_TYPE_READ_ONLY`.

### `[pipeline.NAME.checks]`

Rocky runs quality checks inline during replication. Two surfaces share this section: pipeline-level switches (row count, column match, freshness, null rate, custom SQL, anomaly detection) and model-level `[[assertions]]` blocks covering the DQX parity surface (`not_null`, `unique`, `in_range`, `regex_match`, etc.). Full semantic reference: [Data Quality Checks](/features/data-quality-checks/).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Master switch for the pipeline-level checks below. |
| `fail_on_error` | bool | `true` | When `false`, downgrades every `error`-severity assertion to a non-fatal result. |
| `row_count` | bool | `false` | Compare row counts between source and target. |
| `column_match` | bool | `false` | Verify source and target have the same column sets. |
| `freshness` | table | | `{ threshold_seconds = N, overrides = { ... } }`. |
| `null_rate` | table | | `{ columns = [...], threshold = 0.0–1.0, sample_percent = 10 }`. |
| `custom` | list | `[]` | Custom SQL checks. Each entry has `name`, `sql`, and optional `threshold`. |
| `anomaly_threshold_pct` | float | `50.0` | Row count deviation percentage that triggers an anomaly. Set to 0 to disable. |
| `quarantine` | table | | `{ mode = "split" \| "tag" \| "drop" }`. See below. |
| `assertions` | list | `[]` | Repeated `[[assertions]]` blocks (DQX parity). See below. |

```toml
[pipeline.bronze.checks]
enabled = true
row_count = true
column_match = true
freshness = { threshold_seconds = 86400 }
anomaly_threshold_pct = 50.0
```

#### `[[pipeline.NAME.checks.assertions]]`

Declarative model-level assertions. Each block declares a `type` and type-specific parameters. All assertions share the same base fields:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | (required) | One of: `not_null`, `unique`, `accepted_values`, `relationships`, `expression`, `row_count_range`, `in_range`, `regex_match`, `aggregate`, `composite`, `not_in_future`, `older_than_n_days`. |
| `column` | string | | Required for row-level column kinds (`not_null`, `unique`, `accepted_values`, `relationships`, `in_range`, `regex_match`, `not_in_future`, `older_than_n_days`). |
| `severity` | string | `"error"` | `error` fails the pipeline (subject to `fail_on_error`); `warning` reports but never fails. |
| `filter` | string | | SQL boolean predicate that scopes the assertion to a subset of rows. |

Type-specific parameters:

| Type | Additional fields |
|---|---|
| `accepted_values` | `values: [String]` |
| `relationships` | `to_table: String`, `to_column: String` |
| `expression` | `expression: String` (SQL boolean predicate) |
| `row_count_range` | `min: u64?`, `max: u64?` |
| `in_range` | `min: String?`, `max: String?` (numeric literals) |
| `regex_match` | `pattern: String` (dialect-specific regex; no single quotes, backticks, or semicolons) |
| `aggregate` | `op: sum\|count\|avg\|min\|max`, `cmp: lt\|lte\|gt\|gte\|eq\|ne`, `value: String` |
| `composite` | `kind: "unique"`, `columns: [String]` (≥2) |
| `older_than_n_days` | `days: u32` |

```toml
[[pipeline.silver.checks.assertions]]
type = "not_null"
column = "order_id"

[[pipeline.silver.checks.assertions]]
type = "accepted_values"
column = "status"
values = ["pending", "shipped", "delivered"]
severity = "warning"

[[pipeline.silver.checks.assertions]]
type = "in_range"
column = "amount_cents"
min = "0"
filter = "region = 'US' AND status != 'cancelled'"

[[pipeline.silver.checks.assertions]]
type = "aggregate"
op = "sum"
cmp = "gt"
value = "0"
column = "amount_cents"

[[pipeline.silver.checks.assertions]]
type = "composite"
kind = "unique"
columns = ["order_id", "line_item_id"]
```

#### `[pipeline.NAME.checks.quarantine]`

Route failing rows from row-level assertions into a dedicated table or column instead of just reporting a count.

| Mode | Behavior |
|---|---|
| `split` | Materializes `<target>` (valid rows) and `<target>__quarantine` (failing rows). Downstream models see only the clean table. |
| `tag` | Adds `__dqx_valid` boolean column; failing rows stay with `__dqx_valid = FALSE`. |
| `drop` | Drops failing rows from `<target>`. |

Set-based and table-level assertions (`unique`, `composite`, `row_count_range`, `aggregate`) run as post-hoc checks regardless of mode.

### `[pipeline.NAME.execution]`

Parallelism and error handling.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `concurrency` | integer | `8` | Maximum number of tables processed in parallel. |
| `fail_fast` | bool | `false` | Abort all remaining tables on first error. |
| `error_rate_abort_pct` | integer | `50` | Abort if error rate exceeds this percentage (0–100). Set to 0 to disable. |
| `table_retries` | integer | `1` | Times to retry failed tables after the initial parallel phase. Set to 0 to disable. |

```toml
[pipeline.bronze.execution]
concurrency = 8
fail_fast = false
error_rate_abort_pct = 50
table_retries = 1
```

---

## `[state]`

Global state persistence — where Rocky stores watermarks, run history, and checkpoint progress.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `backend` | string | `"local"` | Storage backend: `"local"`, `"s3"`, `"gcs"`, `"valkey"`, or `"tiered"`. |
| `s3_bucket` | string | | S3 bucket name. Required when `backend` is `"s3"` or `"tiered"`. |
| `s3_prefix` | string | `"rocky/state/"` | S3 key prefix for state files. |
| `gcs_bucket` | string | | GCS bucket name. Required when `backend` is `"gcs"`. |
| `gcs_prefix` | string | `"rocky/state/"` | GCS object prefix for state files. |
| `valkey_url` | string | | Valkey/Redis connection URL. Required when `backend` is `"valkey"` or `"tiered"`. |
| `valkey_prefix` | string | `"rocky:state:"` | Valkey key prefix for state entries. |
| `transfer_timeout_seconds` | int | `300` | Wall-clock budget for each transfer (upload *or* download). Retries share this budget rather than extending it — raise for large state or slow networks. |
| `on_upload_failure` | string | `"skip"` | What to do when upload exhausts retries + circuit-breaker. `"skip"` logs a warning and continues (state goes stale, next run re-derives); `"fail"` propagates the error. |

**Local (default):**

```toml
[state]
backend = "local"
```

**S3 (durable, for ephemeral environments):**

```toml
[state]
backend = "s3"
s3_bucket = "${ROCKY_STATE_BUCKET}"
s3_prefix = "rocky/state/"
```

**Valkey (low-latency, shared):**

```toml
[state]
backend = "valkey"
valkey_url = "${VALKEY_URL}"
```

**Tiered (Valkey + S3 fallback):**

```toml
[state]
backend = "tiered"
valkey_url = "${VALKEY_URL}"
s3_bucket = "${ROCKY_STATE_BUCKET}"
```

Tiered downloads from Valkey first (fast), falls back to S3 (durable). Uploads to both.

### `[state.retry]`

Retry policy applied to transient state-transfer failures (network hiccups, transient 5xx, hung endpoints that hit the per-request HTTP timeout). Same shape as [`[adapter.NAME.retry]`](#adapternameretry) so both layers share one mental model. Retries share the outer `transfer_timeout_seconds` budget — the total wall-clock ceiling is unchanged.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_retries` | int | `3` | Maximum retry attempts per transfer. Set to `0` to disable retries. |
| `initial_backoff_ms` | int | `1000` | Initial backoff before the first retry. |
| `max_backoff_ms` | int | `30000` | Cap on exponential backoff growth. |
| `backoff_multiplier` | float | `2.0` | Multiplier applied between retries (e.g. 2.0 = doubling). |
| `jitter` | bool | `true` | Add random jitter to prevent concurrent runs from retrying in lockstep. |
| `circuit_breaker_threshold` | int | `5` | Trip the breaker after this many consecutive failures. `0` disables. |
| `circuit_breaker_recovery_timeout_secs` | int \| null | `null` | Seconds in `Open` before a half-open trial is allowed. `null` = manual reset only. |
| `max_retries_per_run` | int \| null | `null` | Cross-transfer retry budget for a single run. `null` = unbounded (per-transfer `max_retries` is the only cap). |

```toml
[state]
backend = "s3"
s3_bucket = "${ROCKY_STATE_BUCKET}"
on_upload_failure = "fail"    # strict: treat state durability as required

[state.retry]
max_retries = 5
circuit_breaker_threshold = 3
circuit_breaker_recovery_timeout_secs = 30
```

Terminal outcomes surface as structured `outcome` fields on `state.upload` / `state.download` events — `ok`, `absent`, `timeout`, `error_then_fresh`, `skipped_after_failure`, `transient_exhausted`, `circuit_open`, `budget_exhausted`. Grep those instead of the free-form log message when building alerts.

### `[state.idempotency]`

Tuning knobs for `rocky run --idempotency-key <KEY>` dedup. All fields are optional with the shown defaults; the block is a no-op on runs that don't pass `--idempotency-key`. Unknown fields are rejected.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `retention_days` | integer | `30` | Lifetime of a terminal idempotency stamp before garbage collection. GC runs during the state upload sweep — no separate cron. |
| `dedup_on` | string | `"success"` | Which terminal statuses count as "already processed". `"success"` only stamps successful runs (failures stay claimable for retries); `"any"` stamps every terminal status. |
| `in_flight_ttl_hours` | integer | `24` | Hours after which an `InFlight` claim is treated as a crashed-pod corpse and adopted by a fresh caller. Informational on Valkey/tiered backends, which set the TTL server-side via `SET NX EX`. |

```toml
[state.idempotency]
retention_days = 30
dedup_on = "success"
in_flight_ttl_hours = 24
```

Stamps live in the `IDEMPOTENCY_KEYS` redb table and replicate on tiered backends so sibling pods see the same entry. See [`rocky run --idempotency-key`](/reference/cli/#rocky-run) for the three possible outcomes (`fresh_run`, `skipped_idempotent`, `skipped_in_flight`).

#### Bucket-native lifecycle for object-store backends

Rocky's built-in sweep deletes idempotency stamps from `state.redb` after `retention_days`. On `s3` / `gcs` / `tiered` backends, the sweep is correct but pays a per-key delete during state upload. For projects that emit thousands of stamps per day, configuring a bucket-native lifecycle rule is faster, cheaper, and keeps GC running even when no Rocky process is active.

Both rules below match the default `state.s3_prefix` / `state.gcs_prefix` of `rocky/state/`. Adjust the prefix if you've overridden it. The retention window should match — or be larger than — `[state.idempotency] retention_days` so Rocky's own sweep doesn't try to delete an object the bucket has already removed.

**S3 — `s3api put-bucket-lifecycle-configuration` payload:**

```json
{
  "Rules": [
    {
      "ID": "rocky-idempotency-stamps-30d",
      "Status": "Enabled",
      "Filter": { "Prefix": "rocky/state/" },
      "Expiration": { "Days": 30 }
    }
  ]
}
```

```bash
aws s3api put-bucket-lifecycle-configuration \
  --bucket "$ROCKY_STATE_BUCKET" \
  --lifecycle-configuration file://rocky-lifecycle.json
```

The same rule works for any object Rocky writes under `rocky/state/`, including state-store snapshots. If you want to retain snapshots longer than stamps, namespace them under separate prefixes via `state.s3_prefix` and configure two rules.

**GCS — `gcloud storage buckets update` lifecycle JSON:**

```json
{
  "lifecycle": {
    "rule": [
      {
        "action": { "type": "Delete" },
        "condition": {
          "age": 30,
          "matchesPrefix": ["rocky/state/"]
        }
      }
    ]
  }
}
```

```bash
gcloud storage buckets update "gs://$ROCKY_STATE_BUCKET" \
  --lifecycle-file=rocky-lifecycle.json
```

**Terraform equivalents:**

```hcl
# S3
resource "aws_s3_bucket_lifecycle_configuration" "rocky_state" {
  bucket = aws_s3_bucket.rocky_state.id

  rule {
    id     = "rocky-idempotency-stamps-30d"
    status = "Enabled"

    filter { prefix = "rocky/state/" }
    expiration { days = 30 }
  }
}

# GCS
resource "google_storage_bucket" "rocky_state" {
  name     = "rocky-state"
  location = "US"

  lifecycle_rule {
    action    { type = "Delete" }
    condition {
      age            = 30
      matches_prefix = ["rocky/state/"]
    }
  }
}
```

**Operational notes:**

- **Bucket lifecycle does not replace `[state.idempotency] retention_days`.** The local redb mirror on each pod still has its own copy of the stamp; Rocky's sweep is what evicts that. Bucket lifecycle handles the durable copy.
- **In-flight claims (`InFlight`) are TTL-bounded by `in_flight_ttl_hours`, not by the lifecycle rule.** Don't set the lifecycle window shorter than `in_flight_ttl_hours` (default 24) or you risk reaping a live claim.
- **Tiered backends already serve hits from Valkey first.** A bucket lifecycle that's slightly behind `retention_days` is harmless — Valkey's own TTL evicts the hot copy long before the cold S3/GCS copy expires.

---

## `[ai]`

Configuration for the AI intent layer (`rocky ai`, `rocky ai-explain`, `rocky ai-sync`, `rocky ai-test`). Unknown fields are rejected.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_tokens` | integer | `4096` | Per-request `max_tokens` sent to the Anthropic Messages API **and** the cumulative output-token budget enforced across the compile-verify retry loop. When the running total of `output_tokens` returned by the LLM across attempts exceeds this value, Rocky fail-stops with a `TokenBudgetExceeded` error instead of issuing another retry. The default preserves Rocky's pre-1.x hard-coded behaviour. Increase only when generations legitimately need more headroom (large model surfaces, verbose tests). |

```toml
[ai]
max_tokens = 8192
```

The `[ai]` block is read by every `rocky ai*` command. The API key itself is **not** read from `rocky.toml` — it must come from the `ANTHROPIC_API_KEY` environment variable so it never lands on disk in a project file.

---

## `[cache]`

Project-level cache configuration. Today this is the schema cache
(Arc 7 wave 2 wave-2) — a persisted cache of `DESCRIBE TABLE` results that
lets `rocky compile` / `rocky lsp` typecheck leaf models against real
warehouse column types without paying a live round-trip on every call.

### `[cache.schemas]`

Controls the schema cache.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true` | Enable schema cache reads + writes. Set to `false` for strict CI where every typecheck should resolve against the current warehouse. |
| `ttl_seconds` | integer | `86400` | TTL for cache entries in seconds (default 24h). Lower for high-DDL-churn teams. |
| `replicate` | bool | `false` | Replicate the schema cache via `[state]` sync. Default is off — a fresh clone should warm its cache from its own `rocky run`, not inherit another machine's stale types. |

```toml
[cache.schemas]
ttl_seconds = 3600   # 1h TTL for teams with high-DDL churn
replicate = true     # opt in to share cache via the remote state backend
```

Note: a separate Valkey-backed runtime cache (`ValkeyCacheConfig`) exists in
the codebase for future three-tier caching of API responses but is not
wired into `rocky.toml` today. When that surface lands it'll live under a
sibling key (e.g. `[cache.valkey]`).

---

## `[cost]`

Cost assumptions used by `rocky optimize` when recommending materialization strategies.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `storage_cost_per_gb_month` | float | `0.023` | Storage cost per GB-month. |
| `compute_cost_per_dbu` | float | `0.40` | Compute cost per DBU. |
| `warehouse_size` | string | `"Medium"` | Warehouse size for cost estimation (e.g., `"Small"`, `"Medium"`, `"Large"`). |
| `min_history_runs` | integer | `5` | Minimum runs before cost recommendations are emitted. |

```toml
[cost]
storage_cost_per_gb_month = 0.023
compute_cost_per_dbu = 0.40
warehouse_size = "Medium"
```

---

## `[budget]`

Declarative run-level cost, duration, and scan-volume limits. When a run exceeds a configured limit, Rocky emits a `budget_breach` pipeline event and fires the `HookEvent::BudgetBreach` hook. With `on_breach = "error"` the run also exits non-zero. Unknown fields are rejected.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_usd` | float | | Maximum allowed run cost in USD. Cost is computed from per-materialization `cost_usd` values on `RunOutput.cost_summary`. `None` on runs where no adapter produced cost data (e.g. a BigQuery job with no bytes billed). |
| `max_duration_ms` | integer | | Maximum allowed run wall time in milliseconds. |
| `max_bytes_scanned` | integer | | Maximum allowed total bytes scanned across every materialization in the run. Useful for CI gates on scan volume even when the dollar cost stays inside `max_usd` (e.g. a BigQuery query that stops pruning partitions). Aggregated from the per-model `bytes_scanned` figures the adapter reports — today that's BigQuery's `totalBytesBilled`; Databricks / Snowflake / DuckDB still inherit `None` and skip the dimension rather than treating "no data" as zero. |
| `on_breach` | string | `"warn"` | Either `"warn"` (fire the event, keep the run successful) or `"error"` (also fail the run). |

```toml
[budget]
max_usd = 25.0
max_duration_ms = 900000          # 15 minutes
max_bytes_scanned = 1099511627776 # 1 TiB
on_breach = "error"
```

All three limits are independent and composed with all-OR — any single dimension breach trips the `budget_breach` event (and, with `on_breach = "error"`, fails the run). They evaluate once per run against observed totals; per-model budgets are a follow-up. Subscribe to `on_budget_breach` under `[hook.*]` to route breaches into a notification system.

Each [`BudgetBreachOutput`](./json-output) carries a `limit_type` tag — `"max_usd"`, `"max_duration_ms"`, or `"max_bytes_scanned"` — so consumers can branch on the breached dimension without string-matching the human message.

---

## `[portability]`

Project-wide configuration for Rocky's dialect-portability lint (**P001**). When `target_dialect` is set, every `rocky compile` (and LSP-driven in-editor check) runs the lint against that target. The CLI flag `rocky compile --target-dialect <DIALECT>` overrides this block when both are present. Unknown fields are rejected.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `target_dialect` | string | | `"databricks"`, `"snowflake"`, `"bigquery"`, or `"duckdb"`. When unset, no lint runs (wave-1 "flag opt-in" behavior). |
| `allow` | list of string | `[]` | Project-wide allow-list of construct labels (case-insensitive). Useful when a project standardizes on a non-portable extension like `QUALIFY`. Prefer per-model `-- rocky-allow: …` pragmas for targeted exemptions. |

```toml
[portability]
target_dialect = "bigquery"
allow = ["QUALIFY"]
```

Precedence for the effective target dialect:

1. `rocky compile --target-dialect <DIALECT>` flag (wins if set).
2. `[portability] target_dialect`.
3. Unset — no lint.

See [Linters](/features/linters/) for the full list of covered constructs and the per-model pragma syntax.

---

## `[retry]`

Optional top-level cross-adapter retry budget. When set, `AdapterRegistry` builds one shared `Arc<AtomicI64>` budget and wires it through every adapter for this run — once exhausted, no adapter retries further. Prevents one failing endpoint from burning the whole budget pool that other adapters could have used.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_retries_per_run` | integer | | Total retries allowed across every adapter for this run. Omit the block to keep per-adapter budgets (each `[adapter.NAME.retry]` remains in isolation). |

```toml
[retry]
max_retries_per_run = 50
```

---

## `[mask]`

Workspace-default column-masking strategies keyed by classification tag. Each value is a short string selecting how the warehouse should render masked reads of any column the sidecar tags with that classification. See [Governance](/guides/governance/) for the narrative.

| Strategy | Behavior |
|---|---|
| `"hash"` | SHA-256 hex digest of the column value. Deterministic, one-way. |
| `"redact"` | Replace the value with the literal string `'***'`. |
| `"partial"` | Keep the first and last two characters; replace the middle with `***`. Values shorter than 5 chars are fully replaced with `'***'`. |
| `"none"` | Explicit identity — no masking applied. Useful as a per-env override to unmask a column that defaults to masked at the workspace level. |

```toml
# models/customers.toml tags email + ssn with these classifications
[mask]
pii = "hash"
confidential = "redact"
```

Unknown strategies (e.g. `"mask"`) hard-fail at config load — Rocky never silently accepts a spelling it can't emit SQL for.

:::note[Adapter support]
Masking is implemented today against **Databricks** Unity Catalog via column tags + `CREATE MASK` / `SET MASKING POLICY` (one statement per column — UC rejects multi-column masking DDL). Snowflake, BigQuery, and DuckDB default-unsupported until demand. Masks are applied after a successful DAG run, best-effort — failures emit `warn!` and don't abort the pipeline (same semantics as grants).
:::

### `[mask.<env>]`

Per-environment overrides that win over the workspace default for the matching env name. Rocky resolves `classification → strategy` by taking the `[mask]` defaults, then layering `[mask.<env>]` on top when the active env matches.

```toml
[mask]
pii = "hash"
confidential = "redact"

[mask.prod]
pii = "none"            # unmask pii in prod (e.g. service principal reads)
confidential = "partial"

[mask.staging]
pii = "partial"         # staging gets a softer mask than the dev default
```

Resolution precedence:

1. `[mask.<env>]` entry for the active env (when supplied to `rocky run --env <env>`).
2. `[mask]` workspace default.
3. Unmatched tag — W004 warning unless the tag is listed in [`[classifications] allow_unmasked`](#classifications).

---

## `[classifications]`

Advisory settings for the column-classification feature. Distinct from the per-model `[classification]` sidecar block (see [Model Format](/reference/model-format/#classification)).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `allow_unmasked` | list of strings | `[]` | Classification tags allowed to appear in a model sidecar without a matching `[mask]` strategy. Suppresses the W004 compiler warning. |

```toml
[classifications]
allow_unmasked = ["internal", "lineage_only"]
```

Use this escape hatch for tags that exist only for discovery or lineage tracking — Rocky still surfaces them on `rocky compliance` reports, just without enforcing a masking strategy.

### `[classifications.allow_unmasked]` on the compliance resolver

`rocky compliance` suppresses exceptions for every tag in `allow_unmasked`. The flag is advisory — it doesn't pretend those columns are enforced, it just keeps them off the exception list. See [`rocky compliance`](/reference/cli/#rocky-compliance).

---

## `[role.<name>]`

Hierarchical role declarations reconciled against the warehouse's native role/group system. Each `[role.<name>]` block declares one role; Rocky flattens the inheritance DAG at reconcile time (DFS walk with cycle detection + unknown-parent errors at config-load).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `inherits` | list of strings | `[]` | Immediate parent role names. Rocky unions permissions transitively across every ancestor. Cycles and unknown parents are rejected at config-load time. |
| `permissions` | list of strings | `[]` | Permissions this role grants. Canonical uppercase spellings (e.g. `"SELECT"`, `"USE CATALOG"`, `"USE SCHEMA"`, `"MODIFY"`, `"MANAGE"`). Empty lists are legal — pure grouping roles exist only to aggregate children. |

```toml
[role.reader]
permissions = ["SELECT", "USE CATALOG", "USE SCHEMA"]

[role.analyst]
inherits = ["reader"]
permissions = ["MODIFY"]

[role.admin]
inherits = ["analyst"]
permissions = ["MANAGE"]
```

Rocky flattens the graph into `admin → {SELECT, USE CATALOG, USE SCHEMA, MODIFY, MANAGE}` and forwards the resolved set to `GovernanceAdapter::reconcile_role_graph` after a successful DAG.

:::caution[v1 is log-only]
The v1 Databricks implementation validates each `rocky_role_<name>` principal against the identifier grammar and emits a `debug!` trace. SCIM group creation and per-catalog GRANT emission are deferred as a follow-up. The resolver still catches cycles and unknown parents at config-load regardless of adapter capability — so invalid graphs fail fast even before reconcile runs.
:::

---

## Full Example

A complete Fivetran → Databricks pipeline with governance:

```toml
# ──────────────────────────────────────────────────
# Adapters: connections to source and warehouse
# ──────────────────────────────────────────────────
[adapter.fivetran]
type = "fivetran"
destination_id = "${FIVETRAN_DESTINATION_ID}"
api_key = "${FIVETRAN_API_KEY}"
api_secret = "${FIVETRAN_API_SECRET}"

[adapter.prod]
type = "databricks"
host = "${DATABRICKS_HOST}"
http_path = "${DATABRICKS_HTTP_PATH}"
token = "${DATABRICKS_TOKEN}"

# ──────────────────────────────────────────────────
# Pipeline: bronze layer replication
# ──────────────────────────────────────────────────
[pipeline.bronze]
type = "replication"
strategy = "incremental"
timestamp_column = "_fivetran_synced"
metadata_columns = [
    { name = "_loaded_by", type = "STRING", value = "NULL" },
]

[pipeline.bronze.source]
adapter = "fivetran"

[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["source"]

[pipeline.bronze.target]
adapter = "prod"
catalog_template = "warehouse"
schema_template = "stage__{source}"

[pipeline.bronze.target.governance]
auto_create_catalogs = true
auto_create_schemas = true

[pipeline.bronze.target.governance.tags]
managed_by = "rocky"
environment = "production"

[[pipeline.bronze.target.governance.grants]]
principal = "group:data_engineers"
permissions = ["USE CATALOG", "MANAGE"]

[[pipeline.bronze.target.governance.schema_grants]]
principal = "group:data_engineers"
permissions = ["USE SCHEMA", "SELECT", "MODIFY"]

[pipeline.bronze.target.governance.isolation]
enabled = true

[[pipeline.bronze.target.governance.isolation.workspace_ids]]
id = 123456789
binding_type = "READ_WRITE"

[pipeline.bronze.checks]
enabled = true
row_count = true
column_match = true
freshness = { threshold_seconds = 86400 }

[pipeline.bronze.execution]
concurrency = 8
fail_fast = false
table_retries = 1

# ──────────────────────────────────────────────────
# State: persistence backend for watermarks
# ──────────────────────────────────────────────────
[state]
backend = "${ROCKY_STATE_BACKEND:-local}"
# s3_bucket = "${ROCKY_STATE_BUCKET}"
# valkey_url = "${VALKEY_URL}"
```

A credential-free DuckDB pipeline (good for examples and tests). Using config inference, many defaults can be omitted:

```toml
[adapter.local]
type = "duckdb"
path = "warehouse.duckdb"

[pipeline.demo]
strategy = "full_refresh"

[pipeline.demo.source]
adapter = "local"

[pipeline.demo.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.demo.target]
adapter = "local"
catalog_template = "warehouse"
schema_template = "analytics"

[pipeline.demo.checks]
row_count = true

[pipeline.demo.execution]
concurrency = 4
```

Note: `pipeline.type = "replication"` (default), `auto_create_catalogs = false` (default), `auto_create_schemas = false` (default), and `[state]\nbackend = "local"` (default) are all omitted.

With a single adapter, this can be even more minimal using the unnamed adapter shorthand:

```toml
[adapter]
type = "duckdb"
path = "warehouse.duckdb"

[pipeline.demo]
strategy = "full_refresh"

[pipeline.demo.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.demo.target]
catalog_template = "warehouse"
schema_template = "analytics"
```

---

## Hooks

Configure shell scripts and webhooks to run at pipeline lifecycle events.

```toml
# Shell hooks — run a command, pipe JSON context to stdin
[[hook.pipeline_start]]
command = "bash scripts/notify.sh"
timeout_ms = 5000
on_failure = "warn"    # abort | warn | ignore

[[hook.materialize_error]]
command = "bash scripts/pagerduty.sh"
on_failure = "ignore"

# Webhooks — HTTP POST with template body
[hook.webhooks.pipeline_complete]
url = "https://hooks.slack.com/services/T.../B.../xxx"
preset = "slack"
secret = "${WEBHOOK_SECRET}"

[hook.webhooks.materialize_error]
url = "https://events.pagerduty.com/v2/enqueue"
preset = "pagerduty"
```

### Hook Events

| Event | Trigger |
|-------|---------|
| `pipeline_start` | Pipeline execution begins |
| `discover_complete` | Source discovery finishes |
| `compile_complete` | Compilation finishes |
| `pipeline_complete` | Pipeline execution succeeds |
| `pipeline_error` | Pipeline execution fails |
| `before_materialize` | Before a table is materialized |
| `after_materialize` | After a table is materialized |
| `materialize_error` | Table materialization fails |
| `before_model_run` | Before a compiled model runs |
| `after_model_run` | After a compiled model runs |
| `model_error` | Compiled model execution fails |
| `check_result` | A quality check completes |
| `drift_detected` | Schema drift detected |
| `anomaly_detected` | Row count anomaly detected |
| `state_synced` | State store sync completes |

### Hook Config Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `command` | string | required | Shell command to execute |
| `timeout_ms` | number | 30000 | Max execution time in milliseconds |
| `on_failure` | string | `"warn"` | Behavior on failure: `abort`, `warn`, or `ignore` |
| `env` | object | {} | Extra environment variables |

### Webhook Config Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | required | Webhook endpoint URL |
| `preset` | string | — | Built-in preset: `slack`, `pagerduty`, `datadog`, `teams` |
| `method` | string | `"POST"` | HTTP method |
| `headers` | object | {} | Additional HTTP headers |
| `body_template` | string | — | Mustache-style template (`{{event}}`, `{{model}}`, `{{error}}`) |
| `secret` | string | — | HMAC-SHA256 signing key |
| `timeout_ms` | number | 5000 | Request timeout |
| `async_mode` | boolean | false | Fire-and-forget (don't wait for response) |
| `on_failure` | string | `"warn"` | Behavior on failure |
| `retry_count` | number | 0 | Number of retries |
| `retry_delay_ms` | number | 1000 | Delay between retries |
