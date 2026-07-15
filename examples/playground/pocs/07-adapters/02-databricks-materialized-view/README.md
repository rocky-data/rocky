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

## Why it's distinctive

- **Databricks-native materialization** as a named strategy alongside
  full-refresh, incremental, and merge.
- The same model code can be retargeted to Snowflake (dynamic table)
  or DuckDB (table) by changing the strategy in the `.toml`.

## Run

```bash
export DATABRICKS_HOST="..."
export DATABRICKS_TOKEN="..."
export DATABRICKS_HTTP_PATH="..."
./run.sh
```

`run.sh` validates the config and compiles the model (both credential-free
once the env vars are set) to show the `materialized_view` strategy parse
and the target DDL Rocky emits. Live execution against Databricks requires
a real warehouse; the compile step is the credential-free checkpoint.
