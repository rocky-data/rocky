# 02-databricks-materialized-view — Databricks Materialized View

> **Credentials:** Databricks (host + token + http_path) required
> **Runtime:** depends on Databricks API

## What it shows

A model materialized as a Databricks Materialized View. Rocky generates `CREATE OR REPLACE MATERIALIZED VIEW ...` and Databricks schedules the refresh.

## Run

```bash
export DATABRICKS_HOST="..."
export DATABRICKS_TOKEN="..."
export DATABRICKS_HTTP_PATH="..."
./run.sh
```

`run.sh` only validates the config and compiles the model; neither needs a live warehouse once the env vars are set. Live execution requires a real Databricks warehouse.

## Expected output

`expected/compile.json` has one model, `customer_revenue`, whose `strategy` is `{ "type": "materialized_view" }`. The run ends with:

```
POC complete: materialized_view strategy parsed; live execution requires the Databricks adapter.
```

## Related

- [`01-snowflake-dynamic-table`](../01-snowflake-dynamic-table/): the same idea on Snowflake.
