---
title: Quickstart
description: Get a Rocky pipeline running in 5 minutes
sidebar:
  order: 3
---

This walks through a Rocky pipeline that replicates Fivetran-landed sources into Databricks. No credentials? The [Playground guide](/guides/playground/) runs the same flow against a local DuckDB file.

## 1. Scaffold

```bash
rocky init my-pipeline
cd my-pipeline
```

Creates `rocky.toml` and an empty `models/` directory.

## 2. Configure

Edit `rocky.toml` to declare a Fivetran source, a Databricks target, and a pipeline that wires them together.

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

`[adapter.*]` blocks define connections. `[pipeline.*]` blocks tie them together. Select between pipelines with `--pipeline NAME`.

Export the referenced environment variables before running Rocky (`DATABRICKS_HOST`, `FIVETRAN_API_KEY`, etc.).

## 3. Validate

```bash
rocky validate
```

Checks config syntax and adapter wiring. Does not call external APIs.

## 4. Discover

```bash
rocky -o table discover
```

Calls the Fivetran API and lists connectors matching the schema pattern.

## 5. Preview the SQL

```bash
rocky plan --filter tenant=acme
```

Shows the SQL Rocky will generate, without executing it.

## 6. Run

```bash
rocky run --filter tenant=acme
```

Executes the full pipeline: discover → create catalogs/schemas → detect drift → copy data → run checks. Outputs a versioned JSON result with materializations, check results, drift actions, and permissions.

Resume from the last checkpoint after a failure:

```bash
rocky run --filter tenant=acme --resume-latest
```

## 7. Inspect state

```bash
rocky state
```

Shows stored watermarks for every table.

## Next steps

- [Playground](/guides/playground/) — credential-free DuckDB version
- [Schema patterns](/concepts/schema-patterns/) — customize source-to-target mapping
- [Silver layer](/concepts/silver-layer/) — add transformation models
- [Data quality checks](/features/data-quality-checks/)
- [Dagster integration](/dagster/introduction/)
