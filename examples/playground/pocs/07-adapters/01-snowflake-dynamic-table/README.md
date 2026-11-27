# 01-snowflake-dynamic-table — Snowflake Dynamic Tables

> **Credentials:** none for this smoke. `SNOWFLAKE_ACCOUNT` + `SNOWFLAKE_USERNAME` only satisfy run.sh's fail-fast guard; live execution needs a real Snowflake account + auth (key-pair JWT recommended)
> **Runtime:** offline (validate + compile only)

## What it shows

A model declaring the `dynamic_table` strategy with a `target_lag` of `1 hour`. `run.sh` runs `rocky validate` + `rocky compile` offline and never connects to Snowflake.

At live-run time (Snowflake adapter, currently beta) the strategy lowers to:

```sql
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.MARTS.customer_revenue
  TARGET_LAG = '1 hour'
  WAREHOUSE = ANALYTICS_WH
  AS <model SQL>
```

`target_lag` comes from the model sidecar. `WAREHOUSE` comes from `[adapter.snowflake].warehouse` in `rocky.toml`, not the sidecar.

## Run

```bash
# Any placeholder values work.
export SNOWFLAKE_ACCOUNT="demo_account"
export SNOWFLAKE_USERNAME="demo_user"
./run.sh
```

## Expected output

`rocky validate` reports the config valid: 2 adapters (`snowflake`, `local_discovery`), 1 pipeline (`poc`, replication / full_refresh), 1 model, DAG valid. `rocky compile` writes `expected/compile.json` with one model, `has_errors: false`, and:

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

- [`02-databricks-materialized-view`](../02-databricks-materialized-view/): the same idea on Databricks.
