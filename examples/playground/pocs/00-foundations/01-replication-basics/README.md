# 01-replication-basics — source → staging replication via schema patterns

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** replication pipeline, source schema-pattern routing, `{source}` target templating

## What it shows

A **replication** pipeline: Rocky discovers source schemas matching a pattern
(`raw__<source>`) and copies each source table to a templated target schema
(`staging__<source>`). No transformation models run — replication moves data, it
doesn't reshape it.

```
raw__orders.orders   ──(replicate)──▶   staging__orders.orders
(seeded source)                          (templated target)
```

The `[pipeline.playground.source.schema_pattern]` parses `raw__orders` into
`{source = "orders"}`, and `schema_template = "staging__{source}"` routes the copy
to `staging__orders`. This is the pattern multi-tenant ingestion uses to fan many
source schemas into a consistent target layout.

The folder also ships the three sample models (`raw_orders`, `customer_orders`,
`revenue_summary`) so `rocky compile` / `rocky test` / `rocky lineage` have a model
graph to exercise — but `rocky run` here replicates the *source*, it does not
materialize those models. For a runnable, materializing model DAG see the companion
[`00-playground-default`](../00-playground-default).

## Why it's distinctive

- The catalog's reference **replication** example: schema-pattern discovery + `{source}` target templating, which the transformation playground doesn't show.
- Backs the dagster integration's replication-shape test fixtures (stable `discover` / `plan` / `run` / `state` output).

## Layout

```
.
├── README.md
├── rocky.toml
├── data/
│   └── seed.sql          # seeds raw__orders.orders
├── models/               # sample graph for compile/test/lineage
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
duckdb playground.duckdb < data/seed.sql        # seed the source
rocky discover                                  # list discovered sources
rocky plan --filter source=orders               # preview the replication SQL
rocky run --filter source=orders                # replicate raw__orders → staging__orders
rocky state                                     # inspect watermarks
```

## Expected output

```text
discover: 1 source (raw__orders), 1 table (orders)
run: raw__orders.orders → playground.staging__orders.orders
```

## Related

- [`00-playground-default`](../00-playground-default) — the runnable transformation model DAG (this POC's companion).
- [`04-governance/02-schema-patterns-multi-tenant`](../../04-governance/02-schema-patterns-multi-tenant) — schema patterns with multi-component tenant routing.
