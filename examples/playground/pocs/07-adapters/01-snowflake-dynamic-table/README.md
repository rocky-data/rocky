# 01-snowflake-dynamic-table — Snowflake Dynamic Tables

> **Category:** 07-adapters
> **Credentials:** Snowflake account + auth (key-pair JWT recommended)
> **Runtime:** depends on Snowflake API
> **Rocky features:** `MaterializationStrategy::DynamicTable { target_lag }`, Snowflake dialect

## What it shows

A Rocky transformation model materialized as a **Snowflake Dynamic Table**
with a `target_lag` of `1 hour`. Snowflake auto-refreshes the table to
keep it within the lag threshold — Rocky generates the
`CREATE OR REPLACE DYNAMIC TABLE ... TARGET_LAG = '1 hour' WAREHOUSE = ...` SQL
from a model TOML sidecar.

## Why it's distinctive

- **Snowflake-native materialization** as a first-class strategy.
- The same model code can be retargeted to Databricks (materialized view)
  or DuckDB (table) by changing the strategy in the .toml.

## Run

```bash
export SNOWFLAKE_ACCOUNT="..."
export SNOWFLAKE_USERNAME="..."
export SNOWFLAKE_PRIVATE_KEY_PATH="~/.ssh/snowflake.p8"
./run.sh
```
