# 04-shadow-mode-compare — `rocky run --shadow` + `rocky compare`

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `--shadow`, `--keep-shadow`, `rocky compare`

## What it shows

`rocky run --shadow` writes to `<table>_rocky_shadow` and compares it with
production before cleanup. This POC uses `--keep-shadow` so a separate
`rocky compare` can inspect the same tables.

## Why it's distinctive

- **Safe to run on prod data** — no overwriting of the real tables.
- The diff is structured (row count delta and schema delta, with a verdict per table).

## Run

```bash
./run.sh
```

## Status

The POC runs end to end on the local DuckDB path, and `run.sh` exits 0:

- **The prod run works.** `rocky run --filter source=orders` creates the
  real target `poc.staging__orders.orders` (100 rows).
- **The `--shadow` run works.** It reads the unsuffixed source
  `raw__orders.orders` and writes `poc.staging__orders.orders_rocky_shadow`.
  The real target is not touched. The result is in
  `expected/run_shadow.json`.
- **Both comparisons pass.** `expected/run_shadow.json` contains the in-run
  verdict. `expected/compare.json` reports 100 rows in
  both tables, `schema_match: true` and `overall_verdict: "pass"`.
