# 04-local-test-with-duckdb — `rocky test` against in-memory DuckDB

> **Category:** 01-quality
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky test`, model-level execution, contract validation, auto-loaded `data/seed.sql`

## What it shows

`rocky test` compiles every model, runs it on an in-memory DuckDB (auto-loading `data/seed.sql`) and validates output against any contracts, with no warehouse. Failures cite the column with diagnostic codes (`E012` etc.).

The two models (raw → mart) have no contracts; see `01-data-contracts-strict` for that.

## Run

```bash
./run.sh
```

The script runs `rocky test` on the full set, then with `--model good_mart` to target one model.
