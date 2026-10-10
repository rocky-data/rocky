---
title: Silver Layer (Models)
description: SQL transformation models, and the TOML sidecar that configures each one
sidebar:
  order: 3
---

The silver layer is where you write your own SQL transformations. A model is one SQL query plus a TOML file. The TOML declares the model's dependencies, its materialization strategy, and its target table.

:::tip[Models are plain SQL]
Rocky models are plain SQL files. Dependencies and materialization live in a sidecar TOML file. Your `.sql` is what the warehouse sees.
:::

```
  models/fct_orders.sql     the query you wrote
  models/fct_orders.toml    name, depends_on, strategy, target
            │
            │ rocky run
            ▼
  ┌─────────────────────────────────────────────────────┐
  │ the strategy decides the statement                  │
  │   full_refresh → CREATE OR REPLACE TABLE …          │
  │   merge        → MERGE INTO … USING (…)             │
  │   … and the other strategies listed below           │
  └──────────────────────────┬──────────────────────────┘
                             ▼
           acme_warehouse.analytics.fct_orders
```

## Model formats

### Sidecar format (recommended)

Each model is two files with the same base name:

```
models/
├── fct_orders.sql    # Pure SQL; opens cleanly in any SQL editor
└── fct_orders.toml   # Configuration
```

### Inline format (legacy)

A single SQL file with TOML frontmatter at the top. Rocky still reads it. Prefer the sidecar format, because embedded TOML breaks SQL editor tooling.

```sql
---toml
name = "fct_orders"
depends_on = ["stg_orders"]

[strategy]
type = "full_refresh"

[target]
catalog = "acme_warehouse"
schema = "analytics"
table = "fct_orders"
---

SELECT ...
```

## Configuration

Model TOML fields (full reference: [Model Format](/reference/model-format/)):

| Field | Required | Description |
|---|---|---|
| `name` | No | Model identifier, used in `depends_on` references; defaults to the file name |
| `depends_on` | No | List of upstream model names (execution order) |
| `[strategy]` | No | Materialization config (see below); defaults to `full_refresh` |
| `[target]` | Yes | Output table: `{ catalog, schema, table }`. A config group or `_defaults.toml` can supply `catalog` and `schema`; `table` defaults to the model name |
| `[[sources]]` | No | Input tables (for documentation and lineage) |

### `[strategy]`

A sidecar declares at most one `[strategy]`. If it declares none, Rocky takes one from the config group, then from the directory defaults, and otherwise uses `full_refresh`. Pick the block that matches what you need.

**Merge.** `update_columns` is optional and defaults to all non-key columns.

```toml
[strategy]
type = "merge"
unique_key = ["customer_id"]
update_columns = ["name", "email", "updated_at"]
```

## Example: sidecar model

**models/fct_orders.toml**

```toml
name = "fct_orders"
depends_on = ["stg_orders", "dim_customers"]

[strategy]
type = "full_refresh"

[target]
catalog = "acme_warehouse"
schema = "analytics"
table = "fct_orders"
```

**models/fct_orders.sql**

```sql
SELECT
    o.order_id,
    o.customer_id,
    c.customer_name,
    o.total_amount,
    o.order_date
FROM acme_warehouse.staging__us_west__shopify.orders o
JOIN acme_warehouse.analytics.dim_customers c
    ON o.customer_id = c.customer_id
WHERE o.order_date >= '2024-01-01'
```

## Example: merge model

**models/dim_customers.toml**

```toml
name = "dim_customers"
depends_on = ["stg_customers"]

[strategy]
type = "merge"
unique_key = ["customer_id"]

[target]
catalog = "acme_warehouse"
schema = "analytics"
table = "dim_customers"
```

**models/dim_customers.sql**

```sql
SELECT
    customer_id,
    customer_name,
    email,
    signup_date,
    current_timestamp() AS updated_at
FROM acme_warehouse.staging__us_west__shopify.customers
```

This generates a `MERGE` statement:

```sql
MERGE INTO acme_warehouse.analytics.dim_customers AS target
USING (
    SELECT
        customer_id,
        customer_name,
        email,
        signup_date,
        current_timestamp() AS updated_at
    FROM acme_warehouse.staging__us_west__shopify.customers
) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

## Materialization strategies

A model takes one of these `[strategy] type` values. The
[strategy examples](/reference/model-format/#strategy-examples) in the model
format reference show each one in full.

| Strategy | When to use | Known warehouse limits |
|---|---|---|
| [`full_refresh`](#full_refresh-default) | Small tables, complex transforms, guaranteed consistency | — |
| [`incremental`](#incremental) | Append only the rows newer than the watermark | — |
| [`merge`](#merge) | SCDs, upserts by key | Not on Trino or ClickHouse |
| `delete_insert` | Rewrite the rows that match a partition key | — |
| [`time_interval`](/concepts/time-interval/) | Partition-keyed reprocessing with `@start_date` / `@end_date` | — |
| `microbatch` | `time_interval` with hourly defaults | — |
| `view` | A view, no stored rows | — |
| `ephemeral` | Never built; inlined into the models that read it | — |
| `materialized_view` | Warehouse-managed view refresh | Databricks, Snowflake, BigQuery, PostgreSQL, Redshift |
| `dynamic_table` | Target-lag managed tables | Snowflake |
| [`content_addressed`](/concepts/content-addressed/) | Hash-named Parquet files with a Delta log commit | See its page |
| `snapshot` | Keep row history (SCD Type 2) | See the model format reference |

### full_refresh (default)

Rocky rebuilds the whole table on every run:

```sql
CREATE OR REPLACE TABLE target AS SELECT ...
```

### incremental

A silver model loads only rows newer than its target's watermark. Declare the watermark column with `timestamp_column`, and put `@incremental_filter` where the filter belongs in the SQL. Rocky resolves the placeholder to `TRUE` on the first run and on `rocky run --full-refresh`. On every later run it becomes a comparison with `MAX(<watermark>)` read from the target. A model with no watermark is refused with `E037`. See [Incremental](/reference/model-format/#incremental) in the model format reference.

In the [bronze layer](/concepts/bronze-layer/), `incremental` copies source tables and filters each copy on a watermark in the state store.

### merge

Rocky upserts by unique key:

```sql
MERGE INTO target USING (...) AS source
ON target.key = source.key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

## Validation

Run `rocky validate` to load every model and check the dependency graph before you execute anything:

```bash
rocky validate
```

It checks that:
- Every model file parses
- Every `depends_on` reference points to a model that exists
- No model depends on itself, directly or through a cycle
- Every target table identifier passes SQL validation
