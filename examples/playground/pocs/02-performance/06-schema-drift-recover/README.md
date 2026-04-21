# 06-schema-drift-recover — Drift detection on incremental runs

![Two rocky run invocations sandwiching an ALTER TABLE — the second run reports "Drift: 1/1 tables drifted"](../../../../../docs/public/demo-drift-recover.gif)

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `DESCRIBE TABLE` source vs target diff, `DROP+RECREATE` recovery

## What it shows

Two `rocky run` invocations against the same source, with the source's
column type changed in between:

1. Run 1 — `amount` is `DECIMAL(10,2)`, full refresh succeeds, watermark recorded.
2. Source schema mutated: `ALTER TABLE raw__orders.orders ALTER COLUMN amount TYPE VARCHAR`.
3. Run 2 — drift detection runs as part of the incremental path, detects an
   unsafe type change (`DECIMAL → VARCHAR`), drops the target, and full-refreshes.

## Why it's distinctive

- **Silent in dbt** — dbt doesn't compare source vs target schema between
  runs. A type drift can produce a successful run with corrupted data.
- The drift result is surfaced in the `rocky run` JSON output's `drift` block.

## Layout

```
.
├── README.md
├── rocky.toml
├── run.sh                  Two runs sandwiching a deliberate ALTER TABLE
└── data/seed.sql
```

## Run

```bash
./run.sh
```
