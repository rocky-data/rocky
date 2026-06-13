# 07-auto-create-schemas — `auto_create_schemas` on transformation pipelines

> **Category:** 04-governance
> **Credentials:** none (DuckDB)
> **Runtime:** &lt; 10s
> **Rocky features:** transformation pipelines, `[…target.governance] auto_create_schemas`

## What it shows

A transformation pipeline materialises two models into a target schema (`poc.mart`) that does not exist when the run starts. With `auto_create_schemas = true` set on the pipeline target's governance block, Rocky creates the schema between compile and execute, then materialises the models into it.

## Why it's distinctive

- **Engine v1.29.0 fix.** Pre-v1.29.0 the setting was silently a no-op on transformation pipelines; only the replication path honoured it. Runs failed at execute time with `Catalog Error: Schema with name mart does not exist`. This POC is the regression guard: the pre-condition asserts the target schema is absent, and the run succeeds end-to-end.
- **One config knob, zero `CREATE SCHEMA` boilerplate.** Without `auto_create_schemas`, users hand-author the schema before every fresh deploy.

## Layout

```
.
├── README.md                                    this file
├── rocky.toml                                   transformation pipeline w/ auto_create_schemas = true
├── run.sh                                       end-to-end demo
├── models/
│   ├── stg_orders.sql                           filter source on status; target poc.mart
│   ├── stg_orders.toml                          full_refresh -> poc.mart.stg_orders
│   ├── order_revenue_by_customer.sql            depends on stg_orders; aggregate
│   └── order_revenue_by_customer.toml           full_refresh -> poc.mart.order_revenue_by_customer
└── seeds/
    └── seed.sql                                 8 orders in raw__orders.orders (2 cancelled)
```

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI for seeding (`brew install duckdb`)

## Run

```bash
./run.sh
```

## Expected output

```text
=== precondition — 'mart' schema does NOT exist in DuckDB ===
  (expected: empty result — no 'poc.mart' schema yet)

=== run — pipeline creates 'poc.mart' before materializing the two models ===
  2 model(s) materialized:
    - poc.mart.stg_orders: 6 rows
    - poc.mart.order_revenue_by_customer: 4 rows

=== postcondition — 'mart' schema now exists, and both models live in it ===
mart
order_revenue_by_customer
stg_orders
```

## What happened

1. `seeds/seed.sql` populates `raw__orders.orders` with 8 rows; the `poc.mart` target schema is intentionally not pre-created.
2. The precondition query against `information_schema.schemata` confirms `poc.mart` is absent.
3. `rocky run` compiles the two models, then (because `auto_create_schemas = true` is set on `[pipeline.transform.target.governance]`) pre-creates the unique `(catalog, schema)` pairs declared by the model set (here, just `poc.mart`) before executing per-model SQL.
4. Both models materialise: `stg_orders` filters the cancelled orders out (6 of 8 rows), `order_revenue_by_customer` aggregates by `customer_id` (4 distinct customers).
5. The postcondition queries show the schema now exists and contains both tables.

## Related

- Rocky configuration reference: [`[…target.governance]`](https://rocky-data.dev/reference/configuration/#pipelinenametargetgovernance)
- Source: `engine/crates/rocky-cli/src/commands/run_local.rs` (transformation path) + `engine/crates/rocky-cli/src/commands/run.rs` (replication path, pre-existing parity).
