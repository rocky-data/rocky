# 01-snowflake-dynamic-table — Snowflake Dynamic Tables

> **Category:** 07-adapters
> **Credentials:** none for this smoke — `SNOWFLAKE_ACCOUNT` + `SNOWFLAKE_USERNAME` only satisfy run.sh's fail-fast guard; live execution needs a real Snowflake account + auth (key-pair JWT recommended)
> **Runtime:** offline (validate + compile only)
> **Rocky features:** `StrategyConfig::DynamicTable { target_lag }` → `MaterializationStrategy::DynamicTable`, Snowflake dialect

## What it shows

A Rocky transformation model that declares the **`dynamic_table`**
materialization strategy with a `target_lag` of `1 hour`. `run.sh` runs
`rocky validate` + `rocky compile` **fully offline** — it never connects to
Snowflake. The point is that the compiler parses and type-checks the
Snowflake-native strategy: `rocky compile` surfaces
`strategy = { type = "dynamic_table", target_lag = "1 hour" }` on the model.

At live-run time (Snowflake adapter, currently beta) this strategy lowers to:

```sql
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.MARTS.customer_revenue
  TARGET_LAG = '1 hour'
  WAREHOUSE = ANALYTICS_WH
  AS <model SQL>
```

Snowflake then auto-refreshes the table to keep it within the lag threshold.
The `target_lag` comes from the model sidecar; the `WAREHOUSE` comes from
`[adapter.snowflake].warehouse` in `rocky.toml` (not from the sidecar).

## Why it's distinctive

- **Snowflake-native materialization** as a named strategy alongside
  full-refresh, incremental, and materialized view.
- The same model code can be retargeted to Databricks (materialized view)
  or DuckDB (table) by changing the strategy in the `.toml`.

## Layout

```
rocky.toml                     # snowflake + manual-discovery adapters, one replication pipeline
models/customer_revenue.sql    # SELECT ... GROUP BY customer_id
models/customer_revenue.toml   # [strategy] type = "dynamic_table", target_lag = "1 hour"
run.sh                         # rocky validate + rocky compile (offline)
```

## Run

```bash
# The two vars below only satisfy run.sh's fail-fast guard; the validate +
# compile smoke runs offline, so any placeholder values work.
export SNOWFLAKE_ACCOUNT="demo_account"
export SNOWFLAKE_USERNAME="demo_user"
./run.sh
```

## Expected output

`rocky validate` reports the config valid — 2 adapters (`snowflake`,
`local_discovery`), 1 pipeline (`poc`, replication / full_refresh), 1 model,
DAG valid — plus a few `lint`-severity "you can omit the default" hints.
`rocky compile` writes `expected/compile.json` with one model,
`has_errors: false`, no diagnostics, and:

```json
{
  "name": "customer_revenue",
  "strategy": { "type": "dynamic_table", "target_lag": "1 hour" },
  "target": { "catalog": "ANALYTICS", "schema": "MARTS", "table": "customer_revenue" }
}
```

The run ends with:

```
POC complete: dynamic_table strategy parsed; live execution requires the Snowflake adapter beta.
```

## Related

- `02-databricks-materialized-view` — the same idea against Databricks (`materialized_view`).
