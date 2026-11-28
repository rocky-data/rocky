# 00-playground-default — Stock `rocky playground` scaffold

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** transformation pipeline, model DAG, Rocky DSL aggregation, contract, materialize + inspect

## What it shows

The same models, contract and config as `rocky playground my-project` (only comments differ): a small transformation DAG you materialize and inspect locally. It is the catalog baseline. If `rocky run` or `rocky test` fails here after a binary upgrade, the playground generator regressed.

`rocky playground` seeds `playground.duckdb` when it scaffolds. This POC seeds it with the `duckdb` CLI in `run.sh`.

All models materialize into the default schema `playground.main`:

```
raw__orders.orders   →  raw_orders        →  customer_orders   →  revenue_summary
(seeded source)         (SQL passthrough)    (Rocky DSL group)    (SQL aggregation)
```

Because of that, a model can reference an upstream by name (`from raw_orders`) under both `rocky run` and the in-memory `rocky test`. `depends_on` in each sidecar pins the execution order.

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

`run.sh` also runs `rocky validate`, `rocky compile` and `rocky lineage revenue_summary`, and writes JSON results to `expected/`.

## Expected output

```text
materialized in playground.main:
customer_orders, raw_orders, revenue_summary
```

`rocky test` writes its result to `expected/test.json` (`"total": 3, "passed": 3, "failed": 0`) rather than printing a text summary.

## Related

- Scaffold source: `engine/crates/rocky-cli/src/commands/playground_data/`
- [`01-replication-basics`](../01-replication-basics): the source→staging **replication** pattern.
