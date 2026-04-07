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
| `name` | string | No | Model identifier. **Defaults to the filename stem** (e.g., `fct_orders.toml` → `name = "fct_orders"`). Must be unique across all models. |
| `depends_on` | list of strings | No | Names of upstream models that must run before this one. Defaults to `[]`. |

**`[strategy]`** -- Materialization configuration:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | `"full_refresh"` | Materialization type: `"full_refresh"`, `"incremental"`, or `"merge"`. |
| `timestamp_column` | string | | Column used as the incremental watermark. Required when `type = "incremental"`. |
| `unique_key` | list of strings | | Key columns for merge matching. Required when `type = "merge"`. |
| `update_columns` | list of strings | | Columns to update on merge match. Defaults to all non-key columns if omitted. |

**`[target]`** -- Output table:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `catalog` | string | No* | Target catalog name. Can be inherited from `_defaults.toml`. |
| `schema` | string | No* | Target schema name. Can be inherited from `_defaults.toml`. |
| `table` | string | No | Target table name. **Defaults to `name`** (which itself defaults to filename stem). |

*Required unless provided by a `_defaults.toml` file in the same directory (see [Directory Defaults](#directory-defaults) below).

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

## DAG Resolution

Rocky automatically resolves the execution order of models based on their `depends_on` declarations. Models are executed in topological order, meaning every upstream dependency runs before its downstream dependents.

During `rocky validate`, the DAG is checked for cycles. If a cycle is detected (e.g., model A depends on B, B depends on A), validation fails with an error listing the cycle.

```
!!  dag_validation — cycle detected: fct_orders -> dim_customers -> fct_orders
```

Models with no dependencies run first. Models at the same depth in the DAG may run concurrently in future versions.

---

## Directory Defaults

Place a `_defaults.toml` file in a models directory to set shared defaults for all models in that directory. Individual sidecars override defaults field-by-field.

```
models/
├── _defaults.toml          <- shared target + strategy
├── dim_customers.sql
├── dim_customers.toml      <- only the merge config
├── fct_orders.sql
└── fct_orders.toml         <- can be empty
```

### Supported fields

```toml
# models/_defaults.toml
[target]
catalog = "analytics"
schema = "warehouse"

[strategy]
type = "full_refresh"

# Optional: shared intent for AI features
# intent = "Silver-layer business models"
```

Fields that are per-model (`name`, `depends_on`, `sources`) are **rejected** in `_defaults.toml` — they must be set in individual sidecars.

### Precedence

1. **Explicit sidecar field** — highest priority
2. **`_defaults.toml`** — fills in missing fields
3. **Filename inference** — `name` defaults to filename stem, `target.table` defaults to `name`

### Minimal sidecar example

With a `_defaults.toml` that sets `target.catalog` and `target.schema`, a model that uses the default `full_refresh` strategy needs only:

```toml
# models/fct_orders.toml — empty or just depends_on
depends_on = ["stg_orders"]
```

Everything else (name, target catalog/schema/table, strategy) is inferred.

---

## Contract Auto-Discovery

If a file named `<model>.contract.toml` exists alongside `<model>.sql`, Rocky automatically binds it to that model during compilation — no `--contracts` flag needed.

```
models/
├── revenue_summary.sql
├── revenue_summary.toml
└── revenue_summary.contract.toml    <- auto-discovered
```

Explicit `--contracts contracts/` still works and takes precedence over auto-discovered contracts when the same model name appears in both.

Use `rocky validate` to see which contracts were auto-discovered (reported as `L008` lint messages).
