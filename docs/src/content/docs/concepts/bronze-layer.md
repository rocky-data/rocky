---
title: Bronze Layer
description: Copy source tables into structured warehouse catalogs from config alone, with no SQL files.
sidebar:
  order: 2
---

The bronze layer is Rocky's config-driven replication inside the warehouse. You write no SQL. Rocky finds the available tables, writes the SQL, and copies rows from the ingestion catalog into the target catalogs and schemas you declare.

:::note
Rocky does not extract data from external systems. It works on data an ingestion tool (Fivetran, Airbyte, a manual load) has already landed in your warehouse. Discover finds what is there and moves nothing.
:::

## The flow

```
rocky discover  →  rocky plan  →  rocky apply
```

1. **Discover.** Finds the schemas and tables available for processing. A `fivetran` adapter calls the Fivetran REST API for connectors and enabled tables. A `duckdb` adapter queries `information_schema`. A `manual` adapter reads schema and table definitions written inline in the config.
2. **Plan.** Parses the source schema names, resolves target catalogs and schemas, and writes the SQL statements. Records the result as a deterministic plan, keyed by `plan_id`.
3. **Apply.** Runs one plan by id. It creates catalogs and schemas, copies data, runs quality checks, and updates watermarks. `rocky run` collapses plan and apply into one call, for local work and automation.

## Schema pattern parsing and target mapping

A `schema_pattern` on the pipeline source splits a source schema name into parts, and `catalog_template` and `schema_template` on the pipeline target fill from them. Example: `fivetran_catalog.src__acme__us_west__shopify.orders` with `schema_template = "stage__{source}"` and `catalog_template = "warehouse"` lands in `warehouse.stage__shopify.orders`. Full reference: [Schema Patterns](/concepts/schema-patterns/).

## Auto-creation

Set `auto_create_catalogs` and `auto_create_schemas` under the target's `governance` block. Rocky then creates the target catalog and schema before it copies anything:

```toml
[pipeline.bronze.target.governance]
auto_create_catalogs = true
auto_create_schemas = true

[pipeline.bronze.target.governance.tags]
managed_by = "rocky"
```

Rocky applies `governance.tags` to the catalogs it manages, so a tag such as `managed_by` finds them later.

## Incremental strategy

The first run has no watermark, so it copies every row except the newest batch. Later runs copy rows newer than the stored watermark. The watermark column is `_fivetran_synced` by default (`timestamp_column` changes it). See [SQL Generation](/concepts/sql-generation/#incremental-copy) and [Incremental Processing](/concepts/incremental/#what-the-hold-back-changes).

On schema drift, a safe type widening becomes `ALTER COLUMN TYPE`, a new column becomes `ALTER TABLE ADD COLUMN`, and only an unsafe type change falls back to a full refresh. See [Schema Drift](/concepts/schema-drift/).

## Metadata columns

Declare metadata columns on the pipeline, beside `strategy` and `timestamp_column`:

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

```bash
plan_id=$(rocky --config rocky.toml plan --filter tenant=acme --output json | jq -r .plan_id)
rocky apply "$plan_id"
```

This processes only the schemas whose parsed `tenant` is `acme`.

## Related

- [Schema Patterns](/concepts/schema-patterns/): the full pattern and template reference
- [Configuration](/reference/configuration/): every `rocky.toml` key
