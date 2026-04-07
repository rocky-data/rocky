# 03-anomaly-detection — Row count anomalies via state history

> **Category:** 01-quality
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `rocky history`, `rocky metrics`, state-store-backed row count snapshots

## What it shows

Demonstrates anomaly detection via the embedded state store. The POC:

1. Runs the same pipeline 3 times against gradually shrinking source data.
2. Each run records a row count snapshot in the redb state store.
3. The 4th run uses **truncated** source data (5 rows instead of 500).
4. `rocky history` and `rocky metrics` surface the deviation from baseline.

## Why it's distinctive

- **Stateful** anomaly tracking that persists between runs without an
  external metrics store. Just `rocky history` against the local `.rocky-state.redb`.

## Layout

```
.
├── README.md
├── rocky.toml
├── run.sh                  Runs the pipeline 4 times, then queries history
├── models/
└── data/
    └── seed.sql            Generates 500 rows
```

## Run

```bash
./run.sh
```

## Note

This POC ships the structure and shows how the history would look. Full
anomaly alerting is currently part of the Databricks run path; the DuckDB
local path (`run_local.rs`) records run summaries but does not yet wire
into the `[anomaly]` config block. The `rocky history` command works
against any populated state store.
