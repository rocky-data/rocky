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

**`[strategy]`** -- Materialization configuration:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | `"full_refresh"` | Materialization type. One of `"full_refresh"`, `"incremental"`, `"merge"`, `"time_interval"`, `"ephemeral"`, `"delete_insert"`, `"microbatch"`. |
| `timestamp_column` | string | | Column used as the incremental watermark. Required when `type = "incremental"` or `type = "microbatch"`. |
| `unique_key` | list of strings | | Key columns for merge matching. Required when `type = "merge"`. |
| `update_columns` | list of strings | | Columns to update on merge match. Defaults to all non-key columns if omitted. |
| `partition_by` | list of strings | | Column(s) identifying the partition to delete. Required when `type = "delete_insert"`. |
| `time_column` | string | | Partition column for time-interval processing. Required when `type = "time_interval"`. |
| `granularity` | string | `"hour"` (microbatch) | Partition granularity: `"hour"`, `"day"`, `"month"`, or `"year"`. Required when `type = "time_interval"`; optional default for `"microbatch"`. |
| `lookback` | integer | `0` | Number of past partitions to reprocess. Optional for `"time_interval"`. |
| `batch_size` | integer | `1` | Max partitions per batch. Optional for `"time_interval"`. |
| `first_partition` | string | | Earliest partition key (e.g., `"2024-01-01"`). Optional for `"time_interval"`. |

:::note[Lakehouse formats]
Warehouse-managed materializations like **Delta tables**, **Iceberg tables**, **materialized views**, **streaming tables**, and **plain views** are selected via the top-level `format` key on the model TOML (e.g. `format = "materialized_view"`), not via `[strategy]`. `[strategy]` controls how Rocky writes the data; `format` controls the physical table shape.
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

An ephemeral model is never materialized — Rocky inlines it as a CTE in every downstream consumer. Useful for lightweight intermediate transformations you don't want to persist.

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

**CLI flags** for time-interval models:

```bash
# Process a specific partition
rocky run --partition 2026-04-01

# Process a date range
rocky run --from 2026-03-01 --to 2026-04-01

# Process the latest partition
rocky run --latest

# Discover and process missing partitions
rocky run --missing

# Set lookback window
rocky run --lookback 7

# Parallelize partition processing
rocky run --parallel 4
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
