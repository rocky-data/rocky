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

```toml
[adapter.prod.retry]
max_retries = 5
initial_backoff_ms = 500
max_backoff_ms = 60000
```

---

## `[pipeline.NAME]`

Each `[pipeline.NAME]` block defines a pipeline. The `name` is arbitrary — Rocky CLI commands accept `--pipeline NAME` to select one when multiple are defined.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `type` | string | Yes | | Pipeline type. Currently `"replication"`. |
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
| `catalog_template` | string | Yes | Template for the target catalog name. Uses `{component}` placeholders. |
| `schema_template` | string | Yes | Template for the target schema name. Uses `{component}` placeholders. |

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

Post-replication data quality checks.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Master switch to enable or disable all checks. |
| `row_count` | bool | `false` | Compare row counts between source and target. |
| `column_match` | bool | `false` | Verify source and target have the same column sets. |
| `freshness` | table | | `{ threshold_seconds = N, overrides = { ... } }`. |
| `null_rate` | table | | `{ columns = [...], threshold = 0.0–1.0, sample_percent = 10 }`. |
| `custom` | list | `[]` | Custom SQL checks. Each entry has `name`, `sql`, and optional `threshold`. |
| `anomaly_threshold_pct` | float | `50.0` | Row count deviation percentage that triggers an anomaly. Set to 0 to disable. |

```toml
[pipeline.bronze.checks]
enabled = true
row_count = true
column_match = true
freshness = { threshold_seconds = 86400 }
anomaly_threshold_pct = 50.0
```

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
| `backend` | string | `"local"` | Storage backend: `"local"`, `"s3"`, `"valkey"`, or `"tiered"`. |
| `s3_bucket` | string | | S3 bucket name. Required when `backend` is `"s3"` or `"tiered"`. |
| `s3_prefix` | string | `"rocky/state/"` | S3 key prefix for state files. |
| `valkey_url` | string | | Valkey/Redis connection URL. Required when `backend` is `"valkey"` or `"tiered"`. |
| `valkey_prefix` | string | `"rocky:state:"` | Valkey key prefix for state entries. |

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

---

## `[cache]`

Optional caching configuration. Rocky uses a three-tier cache (memory, Valkey, API) to reduce redundant warehouse calls.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `valkey_url` | string | | Valkey/Redis URL for distributed caching. |

```toml
[cache]
valkey_url = "${VALKEY_URL}"
```

When configured, Rocky caches metadata queries (table descriptions, schema lookups) in Valkey to avoid repeated warehouse API calls across runs. Without Valkey, only in-memory caching is used (effective within a single run).

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
