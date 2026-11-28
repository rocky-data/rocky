# 06-schema-drift-recover — Drift detection on incremental runs

![Two rocky run invocations sandwiching an ALTER TABLE, the second run reports "Drift: 1/1 tables drifted"](../../../../../docs/public/demo-drift-recover.gif)

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `DESCRIBE TABLE` source vs target diff, `DROP+RECREATE` recovery

## What it shows

Rocky compares source and target schemas on incremental runs and recovers from an unsafe type change:

1. Run 1: `amount` is `DECIMAL(10,2)`; full refresh succeeds, watermark recorded.
2. `ALTER TABLE raw__orders.orders ALTER COLUMN amount TYPE VARCHAR`.
3. Run 2: drift detection sees `DECIMAL → VARCHAR`, drops the target, and full-refreshes.

The result is in the `drift` block of the `rocky run` JSON output.

## Run

```bash
./run.sh
```
