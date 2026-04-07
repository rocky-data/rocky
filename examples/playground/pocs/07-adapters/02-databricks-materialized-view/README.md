# 02-databricks-materialized-view — Databricks Materialized View

> **Category:** 07-adapters
> **Credentials:** Databricks (host + token + http_path) required
> **Runtime:** depends on Databricks API
> **Rocky features:** `MaterializationStrategy::MaterializedView`

## What it shows

A Rocky transformation model materialized as a **Databricks Materialized
View** instead of a regular table. Rocky generates
`CREATE OR REPLACE MATERIALIZED VIEW ...` and Databricks manages refresh
based on its own scheduler.

## Run

```bash
export DATABRICKS_HOST="..."
export DATABRICKS_TOKEN="..."
export DATABRICKS_HTTP_PATH="..."
./run.sh
```
