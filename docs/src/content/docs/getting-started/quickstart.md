---
title: Quickstart
description: Get a Rocky pipeline running in 5 minutes
sidebar:
  order: 3
---

This guide walks you through setting up a Rocky pipeline that replicates Fivetran-landed sources into Databricks. If you do not have warehouse credentials handy, the [Playground guide](/guides/playground/) does the same thing end-to-end against a local DuckDB file with no setup.

## 1. Initialize a Project

```bash
rocky init my-pipeline
cd my-pipeline
```

This creates:

```
my-pipeline/
├── rocky.toml      # Pipeline configuration (named adapters + named pipelines)
└── models/         # Transformation models (SQL + TOML)
```

## 2. Configure Your Pipeline

Edit `rocky.toml` to declare a Fivetran source adapter, a Databricks warehouse adapter, and a pipeline that wires them together:

```toml
[adapter.fivetran]
type = "fivetran"
destination_id = "${FIVETRAN_DESTINATION_ID}"
api_key = "${FIVETRAN_API_KEY}"
api_secret = "${FIVETRAN_API_SECRET}"

[adapter.prod]
type = "databricks"
host = "${DATABRICKS_HOST}"
http_path = "${DATABRICKS_HTTP_PATH}"
token = "${DATABRICKS_TOKEN}"

[pipeline.bronze]
type = "replication"
strategy = "incremental"
timestamp_column = "_fivetran_synced"
metadata_columns = [
    { name = "_loaded_by", type = "STRING", value = "NULL" },
]

[pipeline.bronze.source]
adapter = "fivetran"

[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["source"]

[pipeline.bronze.target]
adapter = "prod"
catalog_template = "warehouse"
schema_template = "stage__{source}"

[pipeline.bronze.target.governance]
auto_create_catalogs = true
auto_create_schemas = true

[pipeline.bronze.checks]
enabled = true
row_count = true
column_match = true
freshness = { threshold_seconds = 86400 }

[state]
backend = "local"
```

The `[adapter.NAME]` blocks define connections; the `[pipeline.NAME]` block ties them together. You can declare additional adapters and pipelines in the same file and select between them with `--pipeline NAME`.

Set the environment variables:

```bash
export DATABRICKS_HOST="your-workspace.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/abc123"
export DATABRICKS_TOKEN="dapi..."
export FIVETRAN_DESTINATION_ID="your_destination_id"
export FIVETRAN_API_KEY="your_api_key"
export FIVETRAN_API_SECRET="your_api_secret"
```

## 3. Validate Your Config

```bash
rocky validate
```

Expected output:

```
  ok  Config syntax valid (v2 format)
  ok  adapter.fivetran: fivetran
  ok  adapter.prod: databricks (auth configured)
  ok  pipeline.bronze: schema pattern parseable
  ok  pipeline.bronze: replication / incremental -> warehouse / stage__{source}

Validation complete.
```

`rocky validate` only checks the config — it does not call the Fivetran or Databricks APIs.

## 4. Discover Sources

```bash
rocky -o table discover
```

This calls the Fivetran API and lists all connectors that match the schema pattern:

```
connector_abc | tenant=acme regions=[us_west] source=shopify | 12 tables
connector_def | tenant=acme regions=[us_west] source=stripe  | 8 tables
connector_ghi | tenant=globex regions=[emea] source=hubspot  | 15 tables
```

## 5. Preview the SQL

```bash
rocky plan --filter tenant=acme
```

This shows the SQL Rocky will generate without executing it:

```sql
-- create_catalog (acme_warehouse)
CREATE CATALOG IF NOT EXISTS acme_warehouse;

-- create_schema (acme_warehouse.staging__us_west__shopify)
CREATE SCHEMA IF NOT EXISTS acme_warehouse.staging__us_west__shopify;

-- incremental_copy (acme_warehouse.staging__us_west__shopify.orders)
INSERT INTO acme_warehouse.staging__us_west__shopify.orders
SELECT *, CAST(NULL AS STRING) AS _loaded_by
FROM source_catalog.src__acme__us_west__shopify.orders
WHERE _fivetran_synced > (
    SELECT COALESCE(MAX(_fivetran_synced), TIMESTAMP '1970-01-01')
    FROM acme_warehouse.staging__us_west__shopify.orders
);
```

## 6. Run the Pipeline

```bash
rocky run --filter tenant=acme
```

This executes the full pipeline:

1. Discovers sources from Fivetran
2. Creates catalogs and schemas as needed and applies governance
3. Detects schema drift between source and target
4. Copies data incrementally (or full refresh if drift forces it)
5. Runs data quality checks (row count, column match, freshness)

The JSON output includes materializations, check results, drift actions, and permissions:

```json
{
  "version": "0.1.0",
  "command": "run",
  "filter": "tenant=acme",
  "duration_ms": 45200,
  "tables_copied": 20,
  "materializations": [...],
  "check_results": [...],
  "permissions": { "catalogs_created": 1, "schemas_created": 2 },
  "drift": { "tables_checked": 20, "tables_drifted": 0 }
}
```

If a run fails partway through, you can resume from the last checkpoint instead of rerunning everything:

```bash
rocky run --filter tenant=acme --resume-latest
```

## 7. Check State

```bash
rocky state
```

Shows stored watermarks for each table:

```
acme_warehouse.staging__us_west__shopify.orders | 2026-03-30T10:00:00Z | 2026-03-30T10:01:32Z
acme_warehouse.staging__us_west__shopify.customers | 2026-03-30T10:00:00Z | 2026-03-30T10:01:35Z
```

## Next Steps

- Try the [playground](/guides/playground/) for a credential-free DuckDB version of this flow
- Learn about [schema patterns](/concepts/schema-patterns/) to customize source-to-target mapping
- Add [transformation models](/concepts/silver-layer/) for custom SQL
- Configure [data quality checks](/features/data-quality-checks/)
- Set up [permissions](/features/permissions/) for RBAC
- Integrate with [Dagster](/dagster/introduction/) for orchestration
