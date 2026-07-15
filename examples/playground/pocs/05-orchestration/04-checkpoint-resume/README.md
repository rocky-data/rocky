# 04-checkpoint-resume — `rocky run --resume-latest` after a failure

> **Category:** 05-orchestration
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `--resume-latest`, per-table run progress in the state store

## What it shows

`rocky run` records per-table progress in the state store as it processes the
pipeline. If a run stops before every table is copied, `rocky run
--resume-latest` reads the previous run's checkpoint and skips the tables that
already succeeded rather than starting over.

## How it works

The replication run path (used by the DuckDB and Databricks adapters alike)
initialises run progress up front and checkpoints each table as it finishes —
success or failure — into the local state store (`.rocky-state.redb`). On
`--resume-latest`, the next run loads the most recent run's progress, collects
the table keys that reached `Success`, and drops them from the work list. The
output records the replayed run under `resumed_from`.

## Layout

```
.
├── README.md
├── rocky.toml
├── run.sh           Demonstrates the --resume-latest flag
└── data/seed.sql
```

## Run

```bash
./run.sh
```

## Expected output

- `Run 1 (orders)` copies **1** table (`orders`) and checkpoints it.
- `Run 2 (--resume-latest)` has all three sources in scope but skips the
  already-completed `orders`, copying **2** tables (`customers`, `products`).
  `expected/run2.json` carries a non-null `resumed_from` pointing at run 1.
