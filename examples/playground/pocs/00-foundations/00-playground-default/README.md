# 00-playground-default — Stock `rocky playground` scaffold

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** transformation pipeline, model DAG, Rocky DSL aggregation, contract, materialize + inspect

## What it shows

This is the **exact output** of `rocky playground my-project` — a small, runnable
transformation DAG you can materialize locally and inspect end-to-end. It doubles as
the known-good baseline for the catalog: if `rocky run` or `rocky test` ever fails here
after a binary upgrade, the playground generator regressed.

The pipeline (all models materialize into the default schema `playground.main`):

```
raw__orders.orders   →  raw_orders        →  customer_orders   →  revenue_summary
(seeded source)         (SQL passthrough)    (Rocky DSL group)    (SQL aggregation)
```

Because every model targets the database's default schema, a model can reference an
upstream by name (`from raw_orders`) and it resolves both when materialized by
`rocky run` and in the in-memory `rocky test` run. `depends_on` in each sidecar pins
the execution order.

## Why it's distinctive

- **The only POC that intentionally mirrors the binary's stock output.** Every other POC covers a feature the generator doesn't show.
- Materializes a real multi-model DAG with `rocky run`, then `rocky preview rows` / `rocky profile` (and the VS Code Inspector) read the materialized tables — the simplest end-to-end "see your data" loop.

## Layout

```
.
├── README.md
├── rocky.toml
├── data/
│   └── seed.sql          # seeds raw__orders.orders
├── models/
│   ├── raw_orders.sql        # FROM raw__orders.orders
│   ├── raw_orders.toml
│   ├── customer_orders.rocky # from raw_orders (Rocky DSL group-by)
│   ├── customer_orders.toml  # depends_on = ["raw_orders"]
│   ├── revenue_summary.sql   # FROM customer_orders
│   └── revenue_summary.toml  # depends_on = ["customer_orders"]
└── contracts/
    └── revenue_summary.contract.toml
```

## Prerequisites

- `rocky` CLI on PATH
- `duckdb` CLI (`brew install duckdb`)

## Run

```bash
./run.sh
# or, by hand:
duckdb playground.duckdb < data/seed.sql   # seed the source
rocky run                                  # materialize raw_orders → customer_orders → revenue_summary
rocky preview rows --model customer_orders # peek at real rows
rocky profile customer_orders              # observed per-column stats
rocky test --models models --contracts contracts
```

## Expected output

```text
materialized in playground.main: customer_orders, raw_orders, revenue_summary
test result: 3 passed, 0 failed
```

## What happened

1. `duckdb … < data/seed.sql` seeds the `raw__orders.orders` source.
2. `rocky run` materializes the three models in DAG order into `playground.main`.
3. `rocky preview rows` / `rocky profile` read those materialized tables — the same data the VS Code Inspector surfaces.
4. `rocky test` re-executes the models against an in-memory DuckDB and verifies the `revenue_summary` contract.

## Related

- Source of the scaffold: `engine/crates/rocky-cli/src/commands/playground_data/`
- Companion: [`01-replication-basics`](../01-replication-basics) — the source→staging **replication** pattern (schema-pattern routing), which this transformation playground doesn't show.
