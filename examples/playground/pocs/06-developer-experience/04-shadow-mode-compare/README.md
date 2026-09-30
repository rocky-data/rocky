# 04-shadow-mode-compare — `rocky run --shadow` + `rocky compare`

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `--shadow`, `--shadow-suffix`, `rocky compare`

## What it shows

Shadow mode lets you safely test pipeline changes against production:
`rocky run --shadow` writes to `<table>_rocky_shadow` instead of the real
target. Then `rocky compare` diffs row counts and schemas between shadow
and prod.

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
- **`rocky compare` passes.** `expected/compare.json` reports 100 rows in
  both tables, `schema_match: true` and `overall_verdict: "pass"`.

`run.sh` still ends the shadow and compare steps with `|| true`, so it does
not fail if either step fails. Read the two JSON files to see the result.
