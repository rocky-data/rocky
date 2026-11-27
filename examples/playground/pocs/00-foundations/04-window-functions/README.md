# 04-window-functions — DSL window syntax

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `over (partition ..., sort ..., rows unbounded..current)`

## What it shows

DSL window functions with `over (...)`: `partition`, `sort` (`-` means descending) and an optional frame. The model adds three columns per order:

- `running_total`: cumulative revenue per customer over time
- `latest_per_customer`: `row_number()` per customer by date descending
- `prev_amount`: `lag(amount, 1)` per customer

## Run

```bash
./run.sh
```

## Expected output

`rocky validate` prints its JSON report (config valid, 2 models, DAG valid), then the script ends with:

```text
POC complete: window functions compile and execute.
```

`rocky compile` and `rocky test` write JSON to `expected/compile.json` and `expected/test.json`; `test.json` reports `"total": 2, "passed": 2, "failed": 0` (`raw_orders` and `window_demo`).
