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

Shadow mode + compare are implemented in the Databricks run path.
The DuckDB local execution path doesn't yet rewrite targets in shadow
mode. This POC ships the config + flags as a spec; the demo will become
end-to-end once `run_local.rs` honors `--shadow`.
