# 13-estimate-explain-cost — Cost estimation via warehouse EXPLAIN

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `rocky estimate`, warehouse-native EXPLAIN

## What it shows

`rocky estimate` runs each compiled model's `SELECT` through the warehouse's
native `EXPLAIN` and returns the planner's projection (estimated rows, scan
strategy, join type, filter pushdown) **without materializing any table**.
On DuckDB this is the real DuckDB query planner; on a cloud warehouse it is
that engine's `EXPLAIN`. It answers "what will this model cost to build?"
before you spend the compute to build it.

The POC seeds 5000 order lines and 200 customers, compiles two models (a
filtered staging model and a grouped join), then estimates both and confirms
the target schema is still empty.

## Why it's distinctive

- The estimate comes from the **warehouse's own planner**, not a Rocky guess:
  row counts, the `HASH_JOIN` node, and the `status='completed'` filter pushdown
  in the plan are exactly what the engine will execute.
- It is a true dry-run: no table is created, so you can estimate the cost of a
  change in CI on every PR without touching production compute.

## Layout

```
.
├── README.md                 this file
├── rocky.toml                transformation pipeline config
├── run.sh                    seed → compile → estimate → assert dry-run
├── data/seed.sql             raw__sales.orders (5000) + raw__sales.customers (200)
└── models/
    ├── _defaults.toml        shared target catalog/schema
    ├── stg_orders.sql/.toml  filtered staging model
    └── revenue_by_tier.sql/.toml  grouped join over orders × customers
```

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI for seeding (`brew install duckdb`)
- `jq` for reading the JSON output

## Run

```bash
./run.sh
```

## Expected output

```text
==> 2. rocky estimate — run each model's SELECT through DuckDB EXPLAIN
    total_models    : 2
    - revenue_by_tier
    - stg_orders

==> 3. Estimated rows per scan from the join model's plan (revenue_by_tier):
│        ~2,192 rows        │
│         HASH_JOIN         │
│     status='completed'    │
│        ~2,500 rows        │  │         ~200 rows         │

==> 4. Nothing was materialized — estimate is a dry-run
    tables in target schema 'demo' : 0  (expected 0)
```

## What happened

1. The seed creates the two raw tables that the models read from.
2. `rocky compile` type-checks both models without a warehouse.
3. `rocky estimate` sends each model's `SELECT` to DuckDB's `EXPLAIN`. The join
   model's plan shows the planner expects ~2,500 completed orders after filter
   pushdown, a hash join against 200 customers, and ~2,192 joined rows.
4. A direct DuckDB check confirms the target schema is empty; `estimate` never
   wrote anything.

## Related

- Companion: [`02-performance/05-optimize-recommendations`](../05-optimize-recommendations), where `rocky optimize` recommends strategy changes from run history
- Companion: [`02-performance/10-cost-budgets`](../10-cost-budgets), with `[budget]` blocks + per-run `cost_summary` after execution
