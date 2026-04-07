# 04-tagging-lifecycle — `[governance.tags]` propagation

> **Category:** 04-governance
> **Credentials:** `DATABRICKS_HOST` + `DATABRICKS_TOKEN` required
> **Runtime:** depends on Databricks API
> **Rocky features:** `[governance.tags]`, `ALTER ... SET TAGS`

## What it shows

Tags declared in `[governance.tags]` are applied to catalogs/schemas/tables
during `rocky run` via `ALTER ... SET TAGS`. Schema-pattern components
(e.g., `tenant`, `regions`) are also auto-applied as tags so you can
filter by them in Unity Catalog.

## Run

```bash
export DATABRICKS_HOST="..."
export DATABRICKS_TOKEN="..."
./run.sh
```
