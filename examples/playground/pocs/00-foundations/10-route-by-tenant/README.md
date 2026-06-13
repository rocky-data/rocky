# 10-route-by-tenant — Fan one Parquet file out to per-tenant DuckDB schemas

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** load pipeline + transformation pipeline, model sidecar `target.schema` override, `pipeline.depends_on` chaining

## What it shows

A two-stage pipeline that takes a single Parquet file with rows for many
accounts and routes each account's rows into its own DuckDB schema:

1. **Ingest** (`type = "load"`) reads `data/events.parquet` into a single
   staging table at `main.events` (20 rows mixing 3 accounts: `acme`, `beta`,
   `ceres`).
2. **Route** (`type = "transformation"`, `depends_on = ["ingest"]`) runs three
   SQL models (one per account) that filter `WHERE account_id = '<id>'` and
   each materialize to its own schema: `account_acme.events`,
   `account_beta.events`, `account_ceres.events`.

The per-tenant target is set in each model's sidecar `.toml` (`target.schema =
"account_<id>"`), not in `rocky.toml`. The transformation pipeline doesn't
know or care how many tenants exist; it just runs every model under
`models/`.

## Why it's distinctive

- **One config, two pipelines, three outputs.** `pipeline.depends_on` makes the
  ordering explicit; `rocky dag` shows the connected graph end-to-end
  (`load → events_acme + events_beta + events_ceres`).
- **Per-model `target.schema` override** is just two lines of TOML next to the
  SQL, no Jinja, no `{% if tenant == ... %}` branching.
- The same shape extends to N tenants by adding more `models/events_<id>.{sql,toml}`
  files (each ~5 lines). For dynamic-N production, generate the model files
  from a tenant registry as a build step.

## Layout

```
.
├── README.md                       this file
├── rocky.toml                      ingest (load) + route (transformation) pipelines
├── run.sh                          mint Parquet → load → run → query
├── models/
│   ├── _defaults.toml              shared `[strategy]` + `target.catalog`
│   ├── events_acme.sql + .toml     WHERE account_id = 'acme'  → account_acme.events
│   ├── events_beta.sql  + .toml    WHERE account_id = 'beta'  → account_beta.events
│   └── events_ceres.sql + .toml    WHERE account_id = 'ceres' → account_ceres.events
├── seeds/events.csv                20 rows, 3 accounts mixed
└── data/                           staged at run time (events.parquet)
```

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI (`brew install duckdb`) — mints `events.parquet` from the seed
  CSV, pre-creates the per-tenant schemas, and runs the final verification
  query

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

## What happened

1. `duckdb :memory:` mints `data/events.parquet` from the seed CSV.
2. `rocky load --pipeline ingest` writes all 20 rows into `main.events` via the
   DuckDB adapter's native `read_parquet()` + `CREATE TABLE`.
3. `run.sh` pre-creates `account_acme`, `account_beta`, `account_ceres`
   schemas; transformation pipelines don't auto-create schemas at run time
   today.
4. `rocky run --pipeline route` compiles the three models, resolves their
   `[target]` from the sidecar `.toml`, and executes a per-model
   `CREATE OR REPLACE TABLE poc.account_<id>.events AS SELECT ... WHERE
   account_id = '<id>'`.
5. The final DuckDB query proves the row counts add up (8 + 6 + 6 = 20).

## Related

- `rocky load` source: [`engine/crates/rocky-cli/src/commands/load.rs`](../../../../engine/crates/rocky-cli/src/commands/load.rs)
- `execute_models` (transformation runtime): [`engine/crates/rocky-cli/src/commands/run.rs`](../../../../engine/crates/rocky-cli/src/commands/run.rs)
- Sibling: [`09-files-to-duckdb`](../09-files-to-duckdb) — Parquet/CSV/JSONL
  ingest in one `rocky load` (no fan-out).
