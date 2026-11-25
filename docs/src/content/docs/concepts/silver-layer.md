---
title: Silver Layer (Models)
description: SQL transformation models, and the TOML sidecar that configures each one
sidebar:
  order: 3
---

The silver layer is where you write your own SQL transformations. A model is one SQL file plus a TOML file that declares its dependencies, materialization strategy and target table. Your `.sql` is what the warehouse sees.

```
  models/fct_orders.sql   the query you wrote
  models/fct_orders.toml  name, depends_on, strategy, target
       │ rocky run: the strategy decides the statement (full_refresh → CREATE OR REPLACE TABLE …, merge → MERGE INTO …)
       ▼
  acme_warehouse.analytics.fct_orders
```

## Model formats and configuration

Each model is a `.sql` file with a same-named `.toml` sidecar (recommended). A legacy inline format puts the TOML in `---toml` frontmatter at the top of the SQL file: prefer the sidecar, because embedded TOML breaks SQL editor tooling. The sidecar fields (`name`, `depends_on`, `[strategy]`, `[target]`, `[[sources]]`) and their defaults are in [Model Format](/reference/model-format/). A sidecar declares at most one `[strategy]`. With none, Rocky takes one from the config group, then the directory defaults, then `full_refresh`.

For a `merge` strategy, `update_columns` is optional. Without it a matched row updates every column, keys included. DuckDB, Snowflake and BigQuery have no `UPDATE SET *`: when Rocky cannot resolve the column list for them, the run fails and asks you to declare `update_columns`.

## Examples

```toml
# models/fct_orders.toml
name = "fct_orders"
depends_on = ["stg_orders", "dim_customers"]

[strategy]
type = "full_refresh"

[target]
catalog = "acme_warehouse"
schema = "analytics"
table = "fct_orders"
```

```sql
-- models/fct_orders.sql
SELECT o.order_id, o.customer_id, c.customer_name, o.total_amount, o.order_date
FROM acme_warehouse.staging__us_west__shopify.orders o
JOIN acme_warehouse.analytics.dim_customers c ON o.customer_id = c.customer_id
WHERE o.order_date >= '2024-01-01'
```

For a merge model, set `type = "merge"` and `unique_key = ["customer_id"]`. Rocky wraps the `SELECT` in `MERGE INTO ... USING (...) AS source ON target.customer_id = source.customer_id`. See [SQL Generation](/concepts/sql-generation/#merge-upsert).

## Materialization strategies

A model takes one `[strategy] type`. The [strategy examples](/reference/model-format/#strategy-examples) show each in full.

| Strategy | When to use | Known warehouse limits |
|---|---|---|
| `full_refresh` (default) | Small tables, complex transforms, guaranteed consistency | — |
| `incremental` | Append only the rows newer than the watermark | — |
| `merge` | SCDs, upserts by key | Not on Trino or ClickHouse |
| `delete_insert` | Rewrite the rows that match a partition key | — |
| [`time_interval`](/concepts/time-interval/) | Partition-keyed reprocessing with `@start_date` / `@end_date` | — |
| `microbatch` | `time_interval` with hourly defaults | — |
| `view` | A view, no stored rows | — |
| `ephemeral` | Never built; inlined into the models that read it | — |
| `materialized_view` | Warehouse-managed view refresh | Databricks, Snowflake, BigQuery, PostgreSQL, Redshift |
| `dynamic_table` | Target-lag managed tables | Snowflake |
| [`content_addressed`](/concepts/content-addressed/) | Hash-named Parquet files with a Delta log commit | See its page |
| `snapshot` | Keep row history (SCD Type 2) | See the model format reference |

`full_refresh` rebuilds the table every run with `CREATE OR REPLACE TABLE target AS SELECT ...`. Each strategy's statement is in [SQL Generation](/concepts/sql-generation/#transformation-sql).

**incremental.** A silver model loads only rows newer than its target's watermark. Declare the column with `timestamp_column` and put `@incremental_filter` where the filter belongs in the SQL. Rocky resolves it to `TRUE` on the first run and on `rocky run --full-refresh`. Later it becomes a comparison with `MAX(<watermark>)` read from the target. A model with no watermark is refused with `E037`. See [Incremental](/reference/model-format/#incremental). In the [bronze layer](/concepts/bronze-layer/), `incremental` filters on a watermark in the state store instead.

## Validation

`rocky validate` loads every model and checks the dependency graph before anything executes: every file parses, every `depends_on` points to an existing model, no model depends on itself or through a cycle, and every target table identifier passes SQL validation.
