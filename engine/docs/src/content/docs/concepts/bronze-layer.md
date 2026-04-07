---
title: Bronze Layer
description: Config-driven data replication from sources to warehouse
sidebar:
  order: 2
---

The bronze layer is Rocky's config-driven replication within the warehouse. No SQL files needed — Rocky discovers what tables are available, generates the SQL, and copies data from the ingestion catalog into structured target catalogs/schemas.

:::note
Rocky does not extract data from external systems. It operates on data that has already been landed in your warehouse by an ingestion tool (Fivetran, Airbyte, manual loads, etc.). The "discover" step finds what is available — it does not move data.
:::

## The flow

```
rocky discover  →  rocky plan  →  rocky run
```

1. **Discover** — Finds what schemas and tables are available for processing. For `fivetran` adapters, calls the Fivetran REST API to list connectors and enabled tables. For `duckdb` adapters, queries `information_schema`. For `manual` adapters, reads inline schema/table definitions. Discovery is metadata-only — it identifies what exists, it does not extract data.
2. **Plan** — Parses source schema names, resolves target catalogs/schemas, generates SQL statements. Shows what will happen without executing.
3. **Run** — Executes the plan: creates catalogs/schemas, copies data, runs quality checks, updates watermarks.

## Schema pattern parsing

Source schemas follow a naming convention. Rocky parses these into structured components using a configurable pattern:

```
src__acme__us_west__shopify
│    │     │        │
│    │     │        └── source (connector name)
│    │     └── regions (variable-length)
│    └── tenant
└── prefix (stripped)
```

The pattern is defined under the pipeline source in `rocky.toml`:

```toml
[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["tenant", "regions...", "source"]
```

Given `src__acme__us_west__shopify`, Rocky extracts:
- `tenant = "acme"`
- `regions = ["us_west"]`
- `source = "shopify"`

## Target mapping

Templates on the pipeline target determine where data lands:

```toml
[pipeline.bronze.target]
adapter = "prod"
catalog_template = "{tenant}_warehouse"
schema_template = "staging__{regions}__{source}"
```

Using the parsed components:
- `{tenant}_warehouse` resolves to `acme_warehouse`
- `staging__{regions}__{source}` resolves to `staging__us_west__shopify`

So `fivetran_catalog.src__acme__us_west__shopify.orders` is copied to `acme_warehouse.staging__us_west__shopify.orders`.

## Auto-creation

When `auto_create_catalogs = true` and `auto_create_schemas = true`, Rocky creates target catalogs and schemas before copying data:

```sql
CREATE CATALOG IF NOT EXISTS acme_warehouse;
CREATE SCHEMA IF NOT EXISTS acme_warehouse.staging__us_west__shopify;
```

Catalogs are tagged (e.g., `managed_by = "rocky"`) so Rocky can later discover which catalogs it manages.

## Incremental strategy

On the first run (no watermark), Rocky performs a full refresh. On subsequent runs, it only copies rows where the timestamp column exceeds the last known watermark:

```sql
INSERT INTO acme_warehouse.staging__us_west__shopify.orders
SELECT *, CAST(NULL AS STRING) AS _loaded_by
FROM fivetran_catalog.src__acme__us_west__shopify.orders
WHERE _fivetran_synced > (
    SELECT COALESCE(MAX(_fivetran_synced), TIMESTAMP '1970-01-01')
    FROM acme_warehouse.staging__us_west__shopify.orders
)
```

The `_fivetran_synced` column is Fivetran's built-in timestamp that records when each row was synced. Rocky uses it as the watermark column by default (configurable via `timestamp_column`).

If schema drift is detected (column type mismatch between source and target), Rocky falls back to a full refresh: it drops the target table and recreates it.

## Metadata columns

Rocky can add metadata columns to replicated tables. They are declared on the pipeline alongside `strategy` and `timestamp_column`:

```toml
[pipeline.bronze]
type = "replication"
strategy = "incremental"
timestamp_column = "_fivetran_synced"
metadata_columns = [
    { name = "_loaded_by", type = "STRING", value = "NULL" }
]
```

These are appended to the SELECT: `SELECT *, CAST(NULL AS STRING) AS _loaded_by`.

## Filtering

Scope execution to a specific tenant:

```bash
rocky run --config rocky.toml --filter tenant=acme
```

This processes only schemas where the parsed tenant component matches `acme`.

## Comparison to dbt

In dbt, you write one staging model per source table:

```sql
-- models/staging/shopify/stg_orders.sql
SELECT * FROM {{ source('shopify', 'orders') }}
```

Multiply that by every table, every source, every tenant. For a multi-tenant setup with 50 connectors and 20 tables each, that's 1,000 SQL files that all look the same.

In Rocky, the entire bronze layer is config-driven. Zero SQL files. The `rocky.toml` configuration handles all source-to-target mapping, and Rocky generates the appropriate SQL at runtime.
