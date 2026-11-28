# 10-route-by-tenant — Fan one Parquet file out to per-tenant DuckDB schemas

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** load pipeline + transformation pipeline, model sidecar `target.schema` override, `pipeline.depends_on` chaining

## What it shows

A two-stage pipeline that splits one Parquet file of rows for three accounts into per-account DuckDB schemas:

1. **Ingest** (`type = "load"`) reads `data/events.parquet` (20 rows: `acme`, `beta`, `ceres`) into `main.events`.
2. **Route** (`type = "transformation"`, `depends_on = ["ingest"]`) runs one SQL model per account, filtering `WHERE account_id = '<id>'` into `account_acme.events`, `account_beta.events`, `account_ceres.events`.

Each target is set by `target.schema = "account_<id>"` in the model's sidecar, not in `rocky.toml`. Add tenants by adding `models/events_<id>.{sql,toml}` files (generate them from a tenant registry if the set is dynamic). `rocky dag` shows the connected graph.

`run.sh` pre-creates the three account schemas. This POC does not set `auto_create_schemas = true` (see [`04-governance/07-auto-create-schemas`](../../04-governance/07-auto-create-schemas)).

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI (`brew install duckdb`)

## Run

```bash
./run.sh
```

## Expected output

```text
=== 1. ingest pipeline — Parquet -> main.events ===
{ "files_loaded": 1, "total_rows": 20, ... }

=== 2. compile route models ===
   events_acme    -> poc.account_acme.events
   events_beta    -> poc.account_beta.events
   events_ceres   -> poc.account_ceres.events

=== query per-tenant schemas ===
┌──────────────────────┬───────────┐
│      table_name      │ row_count │
├──────────────────────┼───────────┤
│ account_acme.events  │         8 │
│ account_beta.events  │         6 │
│ account_ceres.events │         6 │
│ main.events          │        20 │
└──────────────────────┴───────────┘
```

## Related

- Source: [`load.rs`](../../../../../engine/crates/rocky-cli/src/commands/load.rs), [`run.rs`](../../../../../engine/crates/rocky-cli/src/commands/run.rs) (`execute_models`)
- [`09-files-to-duckdb`](../09-files-to-duckdb): multi-format ingest without fan-out.
