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
  │   incremental  → INSERT INTO … WHERE ts > watermark │
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
| `name` | Yes | Model identifier, used in `depends_on` references |
| `depends_on` | No | List of upstream model names (execution order) |
| `[strategy]` | No | Materialization config (see below); defaults to `full_refresh` |
| `[target]` | Yes | Output table: `{ catalog, schema, table }` |
| `[[sources]]` | No | Input tables (for documentation and lineage) |

### `[strategy]`

```toml
# Incremental
[strategy]
type = "incremental"
timestamp_column = "updated_at"

# Merge
[strategy]
type = "merge"
unique_key = ["customer_id"]
update_columns = ["name", "email", "updated_at"]  # optional, defaults to all non-key columns
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

| Strategy | When to use | Adapters |
|---|---|---|
| [`full_refresh`](#full_refresh-default) | Small tables, complex transforms, guaranteed consistency | All |
| [`incremental`](#incremental) | Large append-mostly tables, timestamped events | All |
| [`merge`](#merge) | SCDs, upserts by key | All |
| [`time_interval`](/concepts/time-interval/) | Partition-keyed reprocessing with `@start_date` / `@end_date` | All |
| `materialized_view` | Warehouse-managed view refresh | Databricks, Snowflake, BigQuery |
| `dynamic_table` | Target-lag managed tables | Snowflake |

### full_refresh (default)

Rocky rebuilds the whole table on every run:

```sql
CREATE OR REPLACE TABLE target AS SELECT ...
```

### incremental

Rocky appends the rows that arrived after the stored watermark:

```sql
INSERT INTO target SELECT ... WHERE updated_at > :watermark
```

A watermark is the timestamp of the newest row Rocky has already loaded. Watermarks live in Rocky's embedded [state store](/concepts/state-management/) and advance after each successful run.

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
