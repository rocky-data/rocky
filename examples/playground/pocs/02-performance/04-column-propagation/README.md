# 04-column-propagation — Column-level lineage pruning

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky lineage --column`, semantic graph

## What it shows

A 3-model chain — `raw_orders` → `stg_orders` → `fct_status_summary` —
where each downstream carries forward a different subset of the upstream
columns. `stg_orders` reads `amount` and `status`; `fct_status_summary`
groups on `status` only and never touches `amount`. Column-level lineage
lets you answer "which downstream models break if I drop `amount`?"
without reading every model's SQL.

## Why it's distinctive

- **Column-level**, not table-level. dbt's lineage graph stops at table
  edges; Rocky traces every column transformation.
- Tracing `status` from `stg_orders` reports a `downstream_consumers`
  entry for `fct_status_summary`; tracing `amount` reports none — the
  column dead-ends there.
- Outputs Graphviz `dot` format for rendering.

## Layout

```
.
├── README.md
├── rocky.toml
├── run.sh
├── models/
│   ├── _defaults.toml
│   ├── raw_orders.sql            SELECT from seeds.orders
│   ├── stg_orders.rocky          carries amount + status forward
│   ├── fct_status_summary.rocky  groups on status only
│   └── *.toml                    strategy sidecars
└── data/seed.sql
```

## Run

```bash
./run.sh
```

## Expected output

`run.sh` validates, compiles, tests, then emits whole-model lineage plus
two per-column traces. Tracing `amount` from `stg_orders` walks upstream
to `raw_orders` and `seeds.orders` with no downstream consumers:

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

Tracing `status` from the same model produces an equivalent upstream
trace but adds a `downstream_consumers` list containing
`fct_status_summary` — the column propagates one hop further than
`amount` does. Captured golden files land in `expected/`:
`lineage_stg_orders_amount.json`, `lineage_stg_orders_status.json`,
whole-model `lineage_*.json`, and `lineage.dot`.
