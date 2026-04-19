---
title: Silver Layer (Models)
description: Custom SQL transformation models with TOML configuration
sidebar:
  order: 3
---

The silver layer is where you write custom SQL transformations — the equivalent of dbt models. Each model is a SQL query paired with TOML configuration that declares dependencies, materialization strategy, and target table.

:::tip[SQL stays first-class]
Rocky models are plain SQL files. No Jinja, no `{{ ref() }}` macros, no templating. Dependencies and materialization live in a sidecar TOML file — your `.sql` is what the warehouse sees.
:::

## Model formats

### Sidecar format (recommended)

Each model is two files with the same base name:

```
models/
├── fct_orders.sql    # Pure SQL — opens cleanly in any SQL editor
└── fct_orders.toml   # Configuration
```

### Inline format (legacy)

A single SQL file with TOML frontmatter:

```sql
---toml
name = "fct_orders"
depends_on = ["stg_orders", "dim_customers"]
strategy = "full_refresh"

[target]
catalog = "acme_warehouse"
schema = "analytics"
table = "fct_orders"
---

SELECT
    o.order_id,
    o.customer_id,
    c.customer_name,
    o.total_amount,
    o.order_date
FROM acme_warehouse.staging__us_west__shopify.orders o
JOIN acme_warehouse.analytics.dim_customers c
    ON o.customer_id = c.customer_id
```

This format works but is not recommended because embedded TOML breaks SQL editor tooling.

## Model configuration

The TOML configuration (whether sidecar or inline) supports these fields:

| Field | Required | Description |
|---|---|---|
| `name` | Yes | Model identifier, used in `depends_on` references |
| `depends_on` | No | List of upstream model names (determines execution order) |
| `strategy` | No | Materialization strategy: `full_refresh` (default), `incremental`, or `merge` |
| `target` | Yes | Output table: `{ catalog, schema, table }` |
| `sources` | No | Input tables (for documentation and validation) |

### Strategy-specific fields

For `incremental`:

```toml
strategy = "incremental"
timestamp_column = "updated_at"
```

For `merge`:

```toml
strategy = "merge"
unique_key = ["customer_id"]
update_columns = ["customer_name", "email", "updated_at"]  # optional, defaults to all
```

## Example: sidecar model

**models/fct_orders.toml**

```toml
name = "fct_orders"
depends_on = ["stg_orders", "dim_customers"]
strategy = "full_refresh"

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
strategy = "merge"
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
| [`time_interval`](/features/time-interval/) | Partition-keyed reprocessing with `@start_date` / `@end_date` | All |
| `materialized_view` | Warehouse-managed view refresh | Databricks |
| `dynamic_table` | Target-lag managed tables | Snowflake |

### full_refresh (default)

Rebuilds the entire table on every run:

```sql
CREATE OR REPLACE TABLE target AS SELECT ...
```

### incremental

Appends new rows past the stored watermark:

```sql
INSERT INTO target SELECT ... WHERE updated_at > :watermark
```

Watermarks live in Rocky's embedded state store ([state management](/concepts/state-management/)) and advance after each successful run.

### merge

Upserts by unique key:

```sql
MERGE INTO target USING (...) AS source
ON target.key = source.key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

## Validation

Run `rocky validate` to load all models and validate the DAG before execution:

```bash
rocky validate --models ./models
```

This checks:
- All model files parse correctly
- All `depends_on` references point to existing models
- No circular dependencies exist
- Target table identifiers pass SQL validation
