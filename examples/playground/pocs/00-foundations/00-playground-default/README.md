# 00-playground-default — Stock `rocky playground` scaffold

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** transformation pipeline, model DAG, Rocky DSL aggregation, contract, materialize + inspect

## What it shows

This POC holds the same models, contract and config as `rocky playground my-project`.
Only comments differ. It is a small transformation DAG that you materialize and inspect locally.
It is also the baseline for the catalog. If `rocky run` or `rocky test` fails here
after a binary upgrade, the playground generator regressed.

`rocky playground` seeds `playground.duckdb` for you when it scaffolds. This POC
seeds it with the `duckdb` CLI in `run.sh`.

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
- Materializes a real multi-model DAG with `rocky run`. Then `rocky preview rows` and `rocky profile` read the materialized tables. The browser UI (`rocky serve --ui`) and the VS Code Inspector show the same rows.

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
rocky preview rows --model customer_orders # peek at real rows (not in run.sh)
rocky profile customer_orders              # observed per-column stats
rocky test --models models --contracts contracts
```

`run.sh` also runs `rocky validate`, `rocky compile` and `rocky lineage revenue_summary`.
It writes each JSON result to `expected/`.

## Expected output

```text
materialized in playground.main:
customer_orders, raw_orders, revenue_summary
```

`rocky test` writes its result to `expected/test.json`
(`"total": 3, "passed": 3, "failed": 0`) rather than printing a text summary.

## What happened

1. `duckdb … < data/seed.sql` seeds the `raw__orders.orders` source.
2. `rocky validate` and `rocky compile` check the config and type-check the models.
3. `rocky run` materializes the three models in DAG order into `playground.main`.
4. `rocky test` runs the models again against an in-memory DuckDB. It verifies the `revenue_summary` contract.
5. `rocky profile` reads the materialized `customer_orders` table.
6. `rocky lineage revenue_summary` traces the column edges back to the source.

## Related

- Source of the scaffold: `engine/crates/rocky-cli/src/commands/playground_data/`
- Companion: [`01-replication-basics`](../01-replication-basics) — the source→staging **replication** pattern (schema-pattern routing), which this transformation playground doesn't show.
