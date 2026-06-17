---
title: Model Format
description: SQL and TOML model file specification
sidebar:
  order: 4
---

Rocky models define SQL transformations and their materialization behavior. Each model is a SQL query paired with configuration that tells Rocky how to materialize it, what it depends on, and where to write the output.

Rocky supports two model formats: **sidecar** (recommended) and **inline** (legacy).

## Sidecar Format (Recommended)

The sidecar format keeps SQL and configuration in separate files with matching names:

```
models/
├── fct_orders.sql          <- pure SQL
├── fct_orders.toml         <- configuration
├── stg_customers.sql
├── stg_customers.toml
├── dim_products.sql
└── dim_products.toml
```

This separation keeps SQL files clean and editable by any SQL tool without needing to understand Rocky-specific syntax.

### SQL File

The `.sql` file contains a plain SQL query. No templating, no Jinja, no special markers.

```sql
-- models/fct_orders.sql
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.total_amount,
    c.customer_name,
    c.segment
FROM analytics.staging.orders AS o
JOIN analytics.staging.customers AS c
    ON o.customer_id = c.customer_id
WHERE o.order_date >= '2024-01-01'
```

### TOML Config File

The `.toml` file specifies the model name, dependencies, materialization strategy, and target table.

**Fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Model identifier. Must be unique across all models. |
| `depends_on` | list of strings | No | Names of upstream models that must run before this one. Defaults to `[]`. |
| `retention` | string | No | Data retention policy for this model. Grammar `^\d+[dy]$` — e.g. `"90d"` or `"1y"`. See [Retention](#retention). |

**`[strategy]`** -- Materialization configuration:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | `"full_refresh"` | Materialization type. One of `"full_refresh"`, `"incremental"`, `"merge"`, `"time_interval"`, `"ephemeral"`, `"delete_insert"`, `"microbatch"`, `"content_addressed"`. |
| `timestamp_column` | string | | Column used as the incremental watermark. Required when `type = "incremental"` or `type = "microbatch"`. |
| `unique_key` | list of strings | | Key columns for merge matching. Required when `type = "merge"`. |
| `update_columns` | list of strings | | Columns to update on merge match. Defaults to all non-key columns if omitted. |
| `partition_by` | list of strings | | Column(s) identifying the partition to delete. Required when `type = "delete_insert"`. |
| `time_column` | string | | Partition column for time-interval processing. Required when `type = "time_interval"`. |
| `granularity` | string | `"hour"` (microbatch) | Partition granularity: `"hour"`, `"day"`, `"month"`, or `"year"`. Required when `type = "time_interval"`; optional default for `"microbatch"`. |
| `lookback` | integer | `0` | Number of past partitions to reprocess. Optional for `"time_interval"`. |
| `batch_size` | integer | `1` | Max partitions per batch. Optional for `"time_interval"`. |
| `first_partition` | string | | Earliest partition key (e.g., `"2024-01-01"`). Optional for `"time_interval"`. |
| `storage_prefix` | string | | Object-store key prefix that holds `_delta_log/` + Parquet files for the target table (e.g. `"s3://bucket/path/table"`). Required when `type = "content_addressed"`. |
| `partition_columns` | list of strings | `[]` | Logical partition columns for content-addressed tables. Empty for unpartitioned tables. Optional for `"content_addressed"`. |

:::note[Lakehouse formats]
Warehouse-managed table shapes (**Delta tables**, **Iceberg tables**, **materialized views**, **streaming tables**, **plain views**) are modeled as a separate `format` axis on the `LakehouseFormat` enum. `[strategy]` controls how Rocky writes data into the table; `format` controls the physical table shape. The two are orthogonal. The engine-side DDL generator (`rocky-core::lakehouse::generate_lakehouse_ddl`) handles each format; end-to-end TOML wiring varies by adapter, so consult the per-adapter guides before committing to one.
:::

**`[target]`** -- Output table:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `catalog` | string | Yes | Target catalog name. |
| `schema` | string | Yes | Target schema name. |
| `table` | string | Yes | Target table name. |

**`[[sources]]`** -- Input tables (optional, for documentation and lineage):

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `catalog` | string | Yes | Source catalog name. |
| `schema` | string | Yes | Source schema name. |
| `table` | string | Yes | Source table name. |

### `[skip]`

Per-model overrides for the opt-in `--skip-unchanged` model-skip gate. The gate is conservative by default: a model is auto-skip-eligible only when a static scan finds its SQL deterministic and it uses a plain materialization strategy. This block lets an owner override that decision per model. Omit it entirely to follow the automatic rules.

| Field | Type | Default | Description |
|---|---|---|---|
| `eligible` | bool \| null | `null` | Explicit eligibility override. `false` ⇒ this model **always builds**, even when the gate is on and everything looks unchanged (use for a known-volatile model the static scan might miss). `true` ⇒ the model is eligible, subject to the other gate clauses. `null` ⇒ fall back to the automatic rules. |
| `deterministic` | bool \| null | `null` | Owner assertion about the SQL's purity. `true` is the only way a model the static non-determinism scan flagged (timestamps, randomness, unresolved UDFs, order-unstable aggregates) becomes skip-eligible — an explicit, auditable opt-in. `false` forces the model to be treated as non-deterministic (never auto-skipped). `null` ⇒ trust the static scan. |

```toml
name = "fct_orders"

[skip]
eligible = false        # opt this model out — always rebuild
```

```toml
name = "dim_dates"

[skip]
deterministic = true    # owner asserts the SQL is pure → re-eligible despite the scan
```

**Fail-safe rules.** The gate exists to avoid silent production staleness, so it builds on any doubt. Beyond `[skip]`, a model is **never** auto-skip-eligible (it always rebuilds) when:

- its SQL is **non-deterministic**: it calls a volatile builtin (`CURRENT_TIMESTAMP`, `NOW`, `RANDOM`, `UUID`, `CURRENT_USER`, `CURRENT_CATALOG`, …), an order/tie-break-unstable aggregate (`ANY_VALUE`, `ARRAY_AGG`, `COLLECT_LIST`, `COLLECT_SET`, `MODE`), an unordered `LIMIT`/`TOP`/`FETCH`, or any function not on Rocky's pure-function allowlist;
- its **lineage isn't provably complete**: anything beyond a single plain `SELECT` over bare tables (CTEs, sub-queries in `FROM`, `PIVOT`/`UNNEST`/nested joins, `IN (SELECT …)`/`EXISTS`/scalar sub-selects, or set operations) forces a rebuild;
- it uses a `content_addressed` or `time_interval` strategy (a `full_refresh` model **is** eligible).

`deterministic = true` overrides only the first bullet. Even an eligible model is skipped only when its logic and every upstream's data are both unchanged. See [Skip Unchanged Models and Defer to Prod](/guides/skip-and-defer/) for the full workflow and the `[run]` tuning knobs.

### Environment variables

Sidecar `.toml` files (and `models/_defaults.toml`) go through the same `${VAR}` / `${VAR:-default}` substitution as `rocky.toml`. This lets an orchestrator inject per-model `[target]` values via subprocess env without templating the sidecar:

```toml
# models/customer_facts.toml
[target]
catalog = "${ROCKY_TARGET_CATALOG:-warehouse}"
schema  = "${ROCKY_TARGET_SCHEMA:-marts}"
table   = "${ROCKY_TABLE_OVERRIDE:-customer_facts}"
```

See [Environment Variables](/reference/configuration/#environment-variables) for the canonical syntax reference and [`examples/playground/pocs/00-foundations/07-config-layering/`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/00-foundations/07-config-layering) for a runnable three-layer example.

### Config groups

When many models share the same routing and materialization, define a **config group** once and have each model opt in by name. A group lives in `models/groups/<name>.toml` (the file stem is the group name) and supplies a `schema_template` and a `strategy`:

```toml
# models/groups/daily_marts.toml
schema_template = "mart_{region}"

[strategy]
type = "merge"
unique_key = ["id"]
update_columns = ["amount", "status"]
```

A model joins the group with `group = "<name>"` and fills the template's placeholders from its own `[args]`:

```toml
# models/fct_orders.toml
group = "daily_marts"

[target]
catalog = "warehouse"   # schema comes from the group template

[args]
region = "emea"         # fills {region} -> schema "mart_emea"
```

Precedence is **per-model sidecar > group > `_defaults.toml`**: a model can still pin its own `schema` or `strategy` to override the group, and the group in turn overrides directory defaults. A `group` that names no definition, or a `schema_template` placeholder the model doesn't supply, fails the load with a clear error rather than routing a model to the wrong place.

The model loader does not recurse into subdirectories, so `models/groups/` is never mistaken for model files.

A group currently carries `schema_template` and `strategy`. Shared tags and per-model computed keys are planned additions; until then, an unrecognized key in a group file is rejected at load so typos surface immediately.

### `[classification]`

Per-column classification tags. Keys are column names, values are free-form classification strings. Rocky resolves each value against `[mask]` / `[mask.<env>]` in `rocky.toml` to pick the masking strategy, then applies both the column tag and the mask via the governance adapter after a successful DAG.

| Key pattern | Value type | Description |
|---|---|---|
| `<column_name>` | string | Free-form classification tag (e.g. `"pii"`, `"confidential"`, `"internal"`). Matched case-insensitively against `[mask]` keys in `rocky.toml`. Tags without a matching strategy emit the W004 compiler warning unless listed in [`[classifications] allow_unmasked`](/reference/configuration/#classifications). |

```toml
# models/customers.toml
name = "customers"

[classification]
email = "pii"
phone = "pii"
ssn = "confidential"
```

Tags are free-form strings (no enum), so teams can coin new classifications without touching the engine. See [Governance](/guides/governance/) for the end-to-end story (classify → mask → audit → compliance rollup) and [`[mask]`](/reference/configuration/#mask) for the resolver semantics.

:::note[Adapter support]
Classification tags + masking policies are applied today against **Databricks** Unity Catalog (column tags + `CREATE MASK` / `SET MASKING POLICY`, one statement per column). Snowflake, BigQuery, and DuckDB default-unsupported until demand. Best-effort: failures emit `warn!` and don't abort the run.
:::

### Retention

Top-level `retention` key on the sidecar declares a data-retention policy for the model. Parsed at load time into a typed `RetentionPolicy { duration_days: u32 }`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `retention` | string \| null | `null` (disabled) | Grammar `^\d+[dy]$`. `"d"` = days verbatim, `"y"` = years flattened at 365 days per year (no leap-year semantics). Zero (`"0d"`, `"0y"`) is rejected — use `null` to disable. |

```toml
# models/fct_orders.toml
name = "fct_orders"
retention = "90d"

[strategy]
type = "incremental"
timestamp_column = "_fivetran_synced"

[target]
catalog = "analytics"
schema = "warehouse"
table = "fct_orders"
```

Applied by `GovernanceAdapter::apply_retention_policy` after a successful DAG run:

| Adapter | SQL emitted |
|---|---|
| **Databricks (Delta)** | `ALTER TABLE ... SET TBLPROPERTIES ('delta.logRetentionDuration' = '{N} days', 'delta.deletedFileRetentionDuration' = '{N} days')` — both keys written together. |
| **Snowflake** | `ALTER TABLE ... SET DATA_RETENTION_TIME_IN_DAYS = {N}`. |
| **BigQuery / DuckDB** | Default-unsupported — those warehouses lack a first-class retention knob at the config level. |

Garbage inputs (`"abc"`, `"90"`, `"-3d"`, `"1.5d"`, leading signs, exponents) are rejected at sidecar parse time with a `ModelError::InvalidRetention` diagnostic naming the offending value. Inspect resolved policies + warehouse state with [`rocky retention-status`](/reference/cli/#rocky-retention-status).

---

## Inline Format (Legacy)

The inline format embeds TOML configuration directly in the SQL file using a `---toml` / `---` fenced block at the top of the file:

```sql
---toml
name = "stg_orders"
depends_on = []

[target]
catalog = "analytics"
schema = "staging"
table = "orders"
---

SELECT
    order_id,
    customer_id,
    order_date,
    total_amount
FROM raw_catalog.src__acme__us_west__shopify.orders
```

The inline format uses the same fields as the sidecar TOML file. The SQL query follows the closing `---` marker.

The frontmatter block supports the same `${VAR}` / `${VAR:-default}` substitution as sidecar `.toml` files (see [Environment Variables](/reference/configuration/#environment-variables)); the SQL body below the closing `---` is **not** substituted, so any `${VAR}` token in the query stays literal.

This format is supported for backward compatibility. The sidecar format is preferred because it keeps SQL files portable and free of non-SQL syntax.

---

## Strategy Examples

### Full Refresh

Drops and recreates the target table on every run. Use this for small dimension tables or when you need a clean rebuild.

**SQL** (`models/dim_products.sql`):

```sql
SELECT
    product_id,
    product_name,
    category,
    price,
    is_active
FROM raw_catalog.src__acme__us_west__shopify.products
WHERE _fivetran_deleted = false
```

**Config** (`models/dim_products.toml`):

```toml
name = "dim_products"
depends_on = []

[strategy]
type = "full_refresh"

[target]
catalog = "analytics"
schema = "warehouse"
table = "dim_products"

[[sources]]
catalog = "raw_catalog"
schema = "src__acme__us_west__shopify"
table = "products"
```

Generated SQL:

```sql
CREATE OR REPLACE TABLE analytics.warehouse.dim_products AS
SELECT
    product_id,
    product_name,
    category,
    price,
    is_active
FROM raw_catalog.src__acme__us_west__shopify.products
WHERE _fivetran_deleted = false
```

---

### Incremental

Appends only new rows based on a watermark column. Use this for large fact tables where full refresh is too slow.

**SQL** (`models/fct_orders.sql`):

```sql
SELECT
    order_id,
    customer_id,
    order_date,
    total_amount,
    _fivetran_synced
FROM raw_catalog.src__acme__us_west__shopify.orders
```

**Config** (`models/fct_orders.toml`):

```toml
name = "fct_orders"
depends_on = ["dim_products"]

[strategy]
type = "incremental"
timestamp_column = "_fivetran_synced"

[target]
catalog = "analytics"
schema = "warehouse"
table = "fct_orders"

[[sources]]
catalog = "raw_catalog"
schema = "src__acme__us_west__shopify"
table = "orders"
```

Generated SQL (on incremental runs):

```sql
INSERT INTO analytics.warehouse.fct_orders
SELECT
    order_id,
    customer_id,
    order_date,
    total_amount,
    _fivetran_synced
FROM raw_catalog.src__acme__us_west__shopify.orders
WHERE _fivetran_synced > (
    SELECT COALESCE(MAX(_fivetran_synced), TIMESTAMP '1970-01-01')
    FROM analytics.warehouse.fct_orders
)
```

On the first run (when the target table does not exist), Rocky performs a full refresh automatically.

---

### Merge

Upserts rows based on a unique key. Matching rows are updated; non-matching rows are inserted. Use this for slowly changing dimensions or tables with late-arriving updates.

**SQL** (`models/dim_customers.sql`):

```sql
SELECT
    customer_id,
    customer_name,
    email,
    segment,
    lifetime_value,
    updated_at
FROM raw_catalog.src__acme__us_west__shopify.customers
WHERE _fivetran_deleted = false
```

**Config** (`models/dim_customers.toml`):

```toml
name = "dim_customers"
depends_on = []

[strategy]
type = "merge"
unique_key = ["customer_id"]
update_columns = ["customer_name", "email", "segment", "lifetime_value", "updated_at"]

[target]
catalog = "analytics"
schema = "warehouse"
table = "dim_customers"

[[sources]]
catalog = "raw_catalog"
schema = "src__acme__us_west__shopify"
table = "customers"
```

Generated SQL:

```sql
MERGE INTO analytics.warehouse.dim_customers AS target
USING (
    SELECT
        customer_id,
        customer_name,
        email,
        segment,
        lifetime_value,
        updated_at
    FROM raw_catalog.src__acme__us_west__shopify.customers
    WHERE _fivetran_deleted = false
) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE SET
    target.customer_name = source.customer_name,
    target.email = source.email,
    target.segment = source.segment,
    target.lifetime_value = source.lifetime_value,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT *
```

When `update_columns` is omitted, Rocky updates all non-key columns.

---

### Ephemeral

An ephemeral model is never materialized; Rocky inlines it as a CTE in every downstream consumer. Useful for lightweight intermediate transformations you don't want to persist.

**Config** (`models/stg_recent_orders.toml`):

```toml
name = "stg_recent_orders"
depends_on = []

[strategy]
type = "ephemeral"

[target]
catalog = "analytics"
schema = "staging"
table = "stg_recent_orders"
```

No DDL runs for ephemeral models. The SQL body is injected as a `WITH stg_recent_orders AS (…)` CTE wherever the model is referenced.

---

### Delete + Insert

Deletes matching rows by partition key, then inserts fresh data. A lower-overhead alternative to `merge` when the partition key identifies the rows being rewritten.

**Config** (`models/fct_daily_activity.toml`):

```toml
name = "fct_daily_activity"
depends_on = []

[strategy]
type = "delete_insert"
partition_by = ["activity_date"]

[target]
catalog = "analytics"
schema = "warehouse"
table = "fct_daily_activity"
```

---

### Microbatch

An alias for `time_interval` with `hour`-granularity defaults. dbt-compatible naming for partition-based incremental processing.

**Config** (`models/fct_hourly_events.toml`):

```toml
name = "fct_hourly_events"
depends_on = []

[strategy]
type = "microbatch"
timestamp_column = "event_at"   # TIMESTAMP column on the model output
# granularity = "hour"           # optional — defaults to hour

[target]
catalog = "analytics"
schema = "warehouse"
table = "fct_hourly_events"
```

---

### Content-Addressed

Writes the model's SELECT result to a Delta UniForm table as content-addressed Parquet (blake3-hashed file names) plus a Delta log commit. Designed for cross-engine reads from DuckDB, Trino, Spark, and any Iceberg-compatible reader: Rocky owns the writer, and the consumers read directly from the object store. See [Content-Addressed Materialization](/concepts/content-addressed/) for the why and when.

**Config** (`models/fct_events.toml`):

```toml
name = "fct_events"
depends_on = []

[strategy]
type = "content_addressed"
storage_prefix = "s3://${ROCKY_BUCKET}/marts/fct_events"
partition_columns = ["event_date"]

[target]
catalog = "analytics"
schema = "marts"
table = "fct_events"
```

The runtime executes the model SQL, converts the result to Arrow, hashes the Parquet bytes, uploads to `storage_prefix`, and emits a Delta log commit. `partition_columns` may be omitted for unpartitioned tables. Backed by the `rocky-iceberg` writer (shipped in engine v1.30.0 across Phases 1–5: discover, write, sync, partitioned, rowTracking, schema evolution).

---

### Time Interval

Partition-keyed materialization for time-series data. The model SQL uses `@start_date` and `@end_date` placeholders that the runtime substitutes per partition.

**SQL** (`models/fct_daily_events.sql`):

```sql
SELECT
    event_date,
    event_type,
    COUNT(*) AS event_count
FROM raw_catalog.events.page_views
WHERE event_date >= @start_date
  AND event_date < @end_date
GROUP BY event_date, event_type
```

**Config** (`models/fct_daily_events.toml`):

```toml
name = "fct_daily_events"
depends_on = []

[strategy]
type = "time_interval"
time_column = "event_date"
granularity = "day"
lookback = 3
first_partition = "2024-01-01"

[target]
catalog = "analytics"
schema = "warehouse"
table = "fct_daily_events"
```

**CLI flags** for time-interval models. Every flag below is accepted on both `rocky plan` and the `rocky run` single-step alias, which fuses plan + apply into one invocation for local iteration and automation. The canonical, auditable form is `rocky plan` followed by `rocky apply <plan-id>`.

```bash
# Process a specific partition
rocky plan --partition 2026-04-01 && rocky apply <plan-id>

# Process a date range
rocky plan --from 2026-03-01 --to 2026-04-01 && rocky apply <plan-id>

# Process the latest partition
rocky plan --latest && rocky apply <plan-id>

# Discover and process missing partitions
rocky plan --missing && rocky apply <plan-id>

# Set lookback window
rocky plan --lookback 7 && rocky apply <plan-id>

# Parallelize partition processing
rocky plan --parallel 4 && rocky apply <plan-id>
```

Per-partition state is tracked in the state store. The `--missing` flag consults stored partition records to discover gaps.

---

## DAG Resolution

Rocky automatically resolves the execution order of models based on their `depends_on` declarations. Models are executed in topological order, meaning every upstream dependency runs before its downstream dependents.

During `rocky validate`, the DAG is checked for cycles. If a cycle is detected (e.g., model A depends on B, B depends on A), validation fails with an error listing the cycle.

```
!!  dag_validation — cycle detected: fct_orders -> dim_customers -> fct_orders
```

Models with no dependencies run first. Models at the same depth in the DAG may run concurrently in future versions.
