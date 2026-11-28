# 04-column-propagation — Column-level lineage across a model chain

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky lineage --column`, semantic graph

## What it shows

Column-level lineage over `raw_orders` → `stg_orders` → `fct_status_summary` answers "which downstream models break if I drop `amount`?". `fct_status_summary` groups on `status` only and never reads `amount`. `--format dot` outputs Graphviz.

## Run

```bash
./run.sh
```

## Expected output

`run.sh` validates, compiles, tests, then emits whole-model lineage and two column traces. Tracing `amount` from `stg_orders` walks upstream with no downstream consumers:

```json
{
  "command": "lineage",
  "model": "stg_orders",
  "column": "amount",
  "direction": "upstream",
  "trace": [
    { "source": { "model": "raw_orders",   "column": "amount" },
      "target": { "model": "stg_orders",   "column": "amount" }, "transform": "direct" },
    { "source": { "model": "seeds.orders", "column": "amount" },
      "target": { "model": "raw_orders",   "column": "amount" }, "transform": "direct" }
  ]
}
```

Tracing `status` gives an equivalent upstream trace plus a `downstream_consumers` list containing `fct_status_summary`. Output lands in `expected/`: `lineage_stg_orders_amount.json`, `lineage_stg_orders_status.json`, whole-model `lineage_*.json`, and `lineage.dot`.
