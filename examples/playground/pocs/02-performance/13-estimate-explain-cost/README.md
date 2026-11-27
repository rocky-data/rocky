# 13-estimate-explain-cost — Cost estimation via warehouse EXPLAIN

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `rocky estimate`, warehouse-native EXPLAIN

## What it shows

`rocky estimate` runs each compiled model's `SELECT` through the warehouse's native `EXPLAIN` and returns the planner's projection (rows, scan strategy, join type, filter pushdown) without materializing any table, so you can run it in CI.

The POC seeds 1000 order lines and 200 customers, estimates a filtered staging model and a grouped join, and confirms the target schema is still empty.

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
~5 rows
~5 rows
~414 rows
~414 rows
Hash Join
~414 rows
Table: poc.raw__sales.orders Table: poc.raw__sales.customers
Filters: status = 'completed' ~200 rows
~500 rows

==> 4. Nothing was materialized — estimate is a dry-run
    tables in target schema 'demo' : 0  (expected 0)
```

The join plan expects ~500 completed orders after filter pushdown, a hash join against 200 customers, and ~414 joined rows. `run.sh` strips the plan's box-drawing frame with `sed`.

Related: [`05-optimize-recommendations`](../05-optimize-recommendations), [`10-cost-budgets`](../10-cost-budgets).
