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

## Schema pattern parsing and target mapping

Source schemas follow a naming convention. A `schema_pattern` on the pipeline source splits the name into parts, and templates on the pipeline target fill from them:

```toml
[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["tenant", "regions...", "source"]

[pipeline.bronze.target]
adapter = "prod"
catalog_template = "warehouse"
schema_template = "stage__{source}"
```

```
  fivetran_catalog.src__acme__us_west__shopify.orders    ← source table
      schema_pattern: tenant="acme", regions=["us_west"], source="shopify"
      templates:      catalog "warehouse" (no variable), schema "stage__{source}"
  warehouse.stage__shopify.orders                        ← target table
```

[Schema Patterns](/concepts/schema-patterns/) has the full pattern and template reference, including a per-tenant catalog.

## Auto-creation

Set `auto_create_catalogs` and `auto_create_schemas` under the target's
`governance` block. Rocky then creates the target catalog and schema before it
copies anything:

```toml
[pipeline.bronze.target.governance]
auto_create_catalogs = true
auto_create_schemas = true

[pipeline.bronze.target.governance.tags]
managed_by = "rocky"
```

```sql
CREATE CATALOG IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS warehouse.stage__shopify;
```

Rocky applies the tags you declare in `governance.tags` to the catalogs it
manages. A tag such as `managed_by` lets you find those catalogs later.

## Incremental strategy

The first run has no watermark (the timestamp of the newest row Rocky has already copied), so it copies every row except the newest batch. Every later run copies only rows newer than the stored watermark. Rocky keeps the watermark in the [state store](/reference/glossary/#state-store), holds back the newest batch until a later run, and uses `_fivetran_synced` as the watermark column by default (change it with `timestamp_column`). [SQL Generation](/concepts/sql-generation/#incremental-copy) shows the statement. See [Incremental Processing](/concepts/incremental/#what-the-hold-back-changes).

On schema drift, a safe type widening becomes `ALTER COLUMN TYPE`, a new column becomes `ALTER TABLE ADD COLUMN`, and only an unsafe type change falls back to a full refresh. See [Schema Drift](/concepts/schema-drift/).

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

Because `value` becomes part of that statement, Rocky checks it when the config loads: it must be one expression, and any function it calls must be on the scalar-function allowlist. See [`metadata_columns`](/reference/configuration/#pipelinename) for what passes and what does not.

## Filtering

Scope a run to one tenant:

```bash
plan_id=$(rocky --config rocky.toml plan --filter tenant=acme --output json | jq -r .plan_id)
rocky apply "$plan_id"
```

This processes only the schemas whose parsed `tenant` component is `acme`.

## Related

- [Schema Patterns](/concepts/schema-patterns/): the full pattern and template reference
- [Configuration](/reference/configuration/): every `rocky.toml` key
