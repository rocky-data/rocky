---
title: Bronze Layer
description: Copy source tables into structured warehouse catalogs from config alone, with no SQL files.
sidebar:
  order: 2
---

The bronze layer is Rocky's config-driven replication inside the warehouse. You
write no SQL. Rocky finds which tables are available, writes the SQL itself, and
copies rows from the ingestion catalog into the target catalogs and schemas you
declare.

:::note
Rocky does not extract data from external systems. It works on data an ingestion
tool has already landed in your warehouse (Fivetran, Airbyte, a manual load). The
discover step finds what is there. It does not move anything.
:::

## The flow

```
rocky discover  →  rocky plan  →  rocky apply
```

1. **Discover.** Finds the schemas and tables available for processing. A `fivetran` adapter calls the Fivetran REST API for connectors and enabled tables. A `duckdb` adapter queries `information_schema`. A `manual` adapter reads schema and table definitions written inline in the config.
2. **Plan.** Parses the source schema names, resolves target catalogs and schemas, and writes the SQL statements. Records the result as a deterministic plan, keyed by `plan_id`.
3. **Apply.** Runs one plan by id. It creates catalogs and schemas, copies data, runs quality checks, and updates watermarks. `rocky run` collapses plan and apply into one call, for local work and automation.

## Schema pattern parsing

Source schemas follow a naming convention. Rocky splits the name into parts using
a pattern you configure.

```
src__acme__us_west__shopify
│    │     │        │
│    │     │        └── source  = "shopify"   (the connector)
│    │     └── regions = ["us_west"]          (variable-length)
│    └── tenant  = "acme"
└── prefix "src__", stripped
```

Declare the pattern under the pipeline source in `rocky.toml`:

```toml
[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["tenant", "regions...", "source"]
```

## Target mapping

Templates on the pipeline target decide where the rows land.

```toml
[pipeline.bronze.target]
adapter = "prod"
catalog_template = "warehouse"
schema_template = "stage__{source}"
```

Rocky fills those templates from the parts it just parsed.

```
  fivetran_catalog.src__acme__us_west__shopify.orders    ← source table
                   └───────────┬─────────────┘
                               │ schema_pattern splits the schema name
                               ▼
                    tenant  = "acme"
                    regions = ["us_west"]
                    source  = "shopify"
                               │ templates fill from those parts
                               │   catalog_template = "warehouse"  (no variable)
                               │   schema_template  = "stage__{source}"
                               ▼
  warehouse.stage__shopify.orders                        ← target table
```

For a multi-tenant setup where each tenant gets its own catalog, see
[Schema Patterns](/concepts/schema-patterns/) for the `{tenant}_warehouse` plus
`components = ["tenant", "regions...", "source"]` pattern.

## Auto-creation

Set `auto_create_catalogs = true` and `auto_create_schemas = true`, and Rocky
creates the target catalog and schema before it copies anything:

```sql
CREATE CATALOG IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS warehouse.stage__shopify;
```

Rocky tags the catalogs it creates (for example `managed_by = "rocky"`) so it can
find which catalogs it manages later.

## Incremental strategy

The first run has no watermark (the timestamp of the newest row Rocky has already
copied), so it does a full refresh. Every run after that copies only the rows
whose timestamp is newer than the stored watermark:

```sql
INSERT INTO warehouse.stage__shopify.orders
SELECT *, CAST(NULL AS STRING) AS _loaded_by
FROM fivetran_catalog.src__acme__us_west__shopify.orders
WHERE _fivetran_synced > TIMESTAMP '2026-04-17 09:30:00'
```

The literal in that `WHERE` is the previous run's `MAX(_fivetran_synced)`. Rocky
keeps it in the [state store](/reference/glossary/#state-store) and writes it into
the query. It does not read the value back from the target with a subquery.

`_fivetran_synced` is Fivetran's built-in column recording when each row was
synced. Rocky uses it as the watermark column by default. Change it with
`timestamp_column`.

When Rocky detects schema drift, it responds in graded steps. A safe type
widening becomes `ALTER COLUMN TYPE`. A new column becomes
`ALTER TABLE ADD COLUMN`. Only an unsafe type change falls back to a full
refresh, which drops and recreates the target table.

## Metadata columns

Rocky can add metadata columns to the copied tables. Declare them on the pipeline,
alongside `strategy` and `timestamp_column`:

```toml
[pipeline.bronze]
type = "replication"
strategy = "incremental"
timestamp_column = "_fivetran_synced"
metadata_columns = [
    { name = "_loaded_by", type = "STRING", value = "NULL" }
]
```

Rocky appends them to the SELECT: `SELECT *, CAST(NULL AS STRING) AS _loaded_by`.

## Filtering

Scope a run to one tenant:

```bash
plan_id=$(rocky --config rocky.toml plan --filter tenant=acme --output json | jq -r .plan_id)
rocky apply "$plan_id"
```

This processes only the schemas whose parsed `tenant` component is `acme`.

## Related

- [Schema Patterns](/concepts/schema-patterns/) — the full pattern and template reference
- [Incremental Loads](/concepts/incremental/) — how the watermark advances
- [Schema Drift](/concepts/schema-drift/) — what Rocky does when a source column changes
- [Configuration](/reference/configuration/) — every `rocky.toml` key
