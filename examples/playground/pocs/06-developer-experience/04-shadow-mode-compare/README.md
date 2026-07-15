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
- The diff is structured (row count delta, schema delta, sample mismatches).

## Run

```bash
./run.sh
```

## Status

Partially end-to-end on the local DuckDB path — the POC runs to
completion (`run.sh` exits 0), but the shadow half is blocked by an
engine bug:

- **Prod run works.** `rocky run --filter source=orders` creates the
  real target `poc.staging__orders.orders` (100 rows).
- **`--shadow` run currently fails.** On the DuckDB replication path the
  shadow suffix is appended to the table identity that source *and*
  target share, so the copy tries to read from the non-existent suffixed
  **source** table (`FROM raw__orders.orders_rocky_shadow`) instead of
  writing to a suffixed target. The suffix should apply to the target
  write only. `run.sh` tolerates this (`|| true`) and captures the
  failure in `expected/run_shadow.json`.
- **`rocky compare` runs**, but because the shadow table was never
  written it reports 100 (prod) vs 0 (shadow) with `verdict: fail` in
  `expected/compare.json`.

The `--shadow` / `--shadow-suffix` / `--shadow-schema` flags and the
`rocky compare` command all exist and are wired; the demo becomes fully
green once the replication run path stops suffixing the source read.
