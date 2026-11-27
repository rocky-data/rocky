# 07-auto-create-schemas — `auto_create_schemas` on transformation pipelines

> **Category:** 04-governance
> **Credentials:** none (DuckDB)
> **Runtime:** &lt; 10s
> **Rocky features:** transformation pipelines, `[…target.governance] auto_create_schemas`

## What it shows

A transformation pipeline materialises two models into `poc.mart`, a schema that
does not exist when the run starts. With `auto_create_schemas = true` on
`[pipeline.transform.target.governance]`, Rocky creates it between compile and
execute. Before engine 1.29.0, transformation pipelines ignored this setting and
the run failed with `Catalog Error: Schema with name mart does not exist`.

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI for seeding (`brew install duckdb`)

## Run

```bash
./run.sh
```

## Expected output

`run.sh` shows `mart` absent, runs, then shows the schema, both tables and the
revenue rows.

```text
=== run — pipeline creates 'poc.mart' before materializing the two models ===
  2 model(s) materialized:
    - poc.mart.stg_orders (full_refresh): ok
    - poc.mart.order_revenue_by_customer (full_refresh): ok
```

The `run` step reports `ok`, not a row count: transformation materializations
don't populate `rows_copied` (a replication field). `stg_orders` keeps 6 of 8
seeded rows (2 cancelled) and `order_revenue_by_customer` has 4 customers.

## Related

- [`[…target.governance]` reference](https://rocky-data.dev/reference/configuration/#pipelinenametargetgovernance)
