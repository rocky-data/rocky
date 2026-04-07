# 02-merge-upsert — Merge strategy via custom MERGE SQL

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `strategy.type = "merge"` model strategy

## What it shows

Demonstrates the MERGE materialization strategy declared at the **model**
level (sidecar `.toml`) rather than at the pipeline replication level.
Models with `[strategy] type = "merge"` use `unique_key` + `update_columns`
to upsert rather than truncate-and-reload.

```toml
[strategy]
type = "merge"
unique_key = ["customer_id"]
update_columns = ["name", "email", "tier"]
```

## Why it's distinctive

- **Model-level MERGE** avoids hand-writing `MERGE INTO` SQL.
- Update column list is explicit so unrelated columns aren't overwritten.
- Compile-time validation of `unique_key` ensures the column exists.

## Layout

```
.
├── README.md
├── rocky.toml
├── run.sh
├── models/
│   ├── raw_customers.sql
│   ├── raw_customers.toml
│   ├── dim_customers.sql
│   └── dim_customers.toml    type = "merge", unique_key, update_columns
└── data/seed.sql
```

## Run

```bash
./run.sh
```

## Status

The merge strategy is parsed and the model.toml schema is validated by
`rocky compile`. Local DuckDB execution (`rocky test`) doesn't currently
generate MERGE SQL — `executor.rs` always emits `CREATE OR REPLACE TABLE AS`.
This POC is **scaffold + spec**: shows the user-facing config + intended
behavior. End-to-end MERGE on DuckDB awaits a follow-up to `rocky-engine`.
