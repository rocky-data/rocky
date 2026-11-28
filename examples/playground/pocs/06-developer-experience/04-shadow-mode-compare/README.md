# 04-shadow-mode-compare — `rocky run --shadow` + `rocky compare`

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `--shadow`, `--keep-shadow`, `rocky compare`

## What it shows

`rocky run --shadow` writes to `<table>_rocky_shadow`, leaving the real table untouched, and compares it with production before cleanup. The diff reports row count delta, schema delta and a verdict per table.

## Run

```bash
./run.sh
```

## What to expect

`run.sh` exits 0 on the local DuckDB path:

- `rocky run --filter source=orders` creates the target `poc.staging__orders.orders` (100 rows).
- The first `--shadow` run reads the unsuffixed source `raw__orders.orders`, writes `poc.staging__orders.orders_rocky_shadow`, then drops it. Its verdict is in `expected/run_shadow_once.json`.
- The second run reuses the name with `--keep-shadow`, so `rocky compare` can inspect the table. It succeeds without manual cleanup; its in-run verdict is in `expected/run_shadow.json`.
- `expected/compare.json` reports 100 rows in both tables, `schema_match: true` and `overall_verdict: "pass"`.
