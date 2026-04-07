# 03-workspace-isolation — Catalog isolation + workspace bindings

> **Category:** 04-governance
> **Credentials:** `DATABRICKS_HOST` + `DATABRICKS_TOKEN` required
> **Runtime:** depends on Databricks API
> **Rocky features:** `[governance.isolation]`, workspace bindings, ISOLATED catalog mode

## What it shows

Set Databricks Unity Catalog catalogs to ISOLATED mode and bind them to
specific workspace IDs. This is the standard Databricks security pattern
for multi-tenant data platforms.

## Run

```bash
export DATABRICKS_HOST="..."
export DATABRICKS_TOKEN="..."
./run.sh
```
