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

## 5. Plan

```bash
plan_id=$(rocky plan --filter tenant=acme --output json | jq -r .plan_id)
```

Compiles the pipeline, runs drift detection, and records a deterministic plan keyed by `plan_id`. Inspect it (SQL, drift actions, checks) before committing to a run.

## 6. Apply

```bash
rocky apply "$plan_id"
```

Executes the plan: discover → create catalogs/schemas → apply drift → copy data → run checks. Outputs a versioned JSON result with materializations, check results, drift actions, and permissions.

`rocky plan` + `rocky apply` is the auditable two-step for production and PR gating — the persisted plan is reviewable before anything writes. For local iteration and automation, `rocky run` does the same work in a single step:

```bash
rocky run --filter tenant=acme
```

Either path resumes from the last checkpoint after a failure with `--resume-latest`:

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
- See the combo in action — POC #17 (trace + cost + replay against the same run_id): [examples/playground/pocs/06-developer-experience/17-trace-replay-cost-combo](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/06-developer-experience/17-trace-replay-cost-combo)
