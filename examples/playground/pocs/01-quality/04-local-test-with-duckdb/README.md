# 04-local-test-with-duckdb — `rocky test` against in-memory DuckDB

> **Category:** 01-quality
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky test`, model-level execution, contract validation, auto-loaded `data/seed.sql`

## What it shows

The fastest feedback loop in Rocky: `rocky test` compiles every model,
executes it against an in-memory DuckDB instance, and validates the output
against any contracts — all without ever touching a real warehouse.

This POC ships 2 models (raw → mart) with no contracts attached, since the
strict-contracts diagnostic surface is covered separately in
`01-data-contracts-strict`.

## Why it's distinctive

- **No warehouse required.** No Databricks token, no Snowflake key, no
  network calls. Cargo-style sub-30-second feedback for SQL.
- `data/seed.sql` is loaded automatically — no manual setup script.
- Failure messages cite the offending column with diagnostic codes (`E012` etc.).

## Layout

```
.
├── README.md
├── rocky.toml
├── run.sh
├── models/
├── contracts/
└── data/seed.sql
```

## Run

```bash
./run.sh
```

The script runs `rocky test` once with the full set, then again with
`--model good_mart` to show single-model targeting.
