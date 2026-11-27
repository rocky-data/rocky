# 12-init-scaffolding — Project scaffolding with `rocky init`

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `rocky init`, project templates

## What it shows

`rocky init` scaffolds a project from a template: `duckdb` (default), `databricks-fivetran`, `snowflake`, `bigquery` or `trino`. The DuckDB project validates and compiles immediately, with no credentials.

The demo scaffolds into `./scaffolded/`, runs `validate` and `compile` there, then scaffolds a Snowflake project into `./scaffolded-snowflake/`. Both directories are gitignored and regenerated on every run. The top-level `rocky.toml` is only a placeholder.

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI (`brew install duckdb`)
- `jq`

## Run

```bash
./run.sh
```

## Expected output

```text
==> 1. rocky init — scaffold a fresh DuckDB project into ./scaffolded/
Initialized Rocky project in scaffolded

  rocky.toml           — pipeline config (DuckDB)
  models/_defaults.toml — shared model defaults
  models/stg_orders.*  — sample transformation model
  seeds/seed.sql       — sample data

==> 2. Scaffolded tree:
    ./models/_defaults.toml
    ./models/stg_orders.sql
    ./models/stg_orders.toml
    ./rocky.toml
    ./seeds/seed.sql

==> 3. Run the quickstart the scaffold prints — it works out of the box.
      valid : true
      models compiled : 1
```

## Related

- [`00-playground-default`](../00-playground-default): the `rocky playground` scaffold
- [`11-plan-apply-workflow`](../11-plan-apply-workflow): next step
