# 01-replication-basics — source → staging replication via schema patterns

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** replication pipeline, source schema-pattern routing, `{source}` target templating

## What it shows

A **replication** pipeline: Rocky discovers source schemas matching a pattern (`raw__<source>`) and copies each table to a templated target (`staging__<source>`) without reshaping it.

```
raw__orders.orders   ──(replicate)──▶   staging__orders.orders
(seeded source)                          (templated target)
```

`[pipeline.playground.source.schema_pattern]` parses `raw__orders` into `{source = "orders"}`, and `schema_template = "staging__{source}"` routes the copy to `staging__orders`.

The folder also ships three sample models (`raw_orders`, `customer_orders`, `revenue_summary`) and a contract for `rocky compile` / `rocky test` / `rocky lineage`. `rocky run` here does **not** materialize them; see [`00-playground-default`](../00-playground-default) for that.

This POC also backs the dagster integration's replication-shape test fixtures.

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

- [`04-governance/02-schema-patterns-multi-tenant`](../../04-governance/02-schema-patterns-multi-tenant): multi-component tenant routing.
