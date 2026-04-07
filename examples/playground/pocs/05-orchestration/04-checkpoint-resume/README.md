# 04-checkpoint-resume — `rocky run --resume-latest` after a failure

> **Category:** 05-orchestration
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `--resume-latest`, run progress in state store

## What it shows

`rocky run` records per-table progress in the state store as it processes
the pipeline. If a run fails partway through, `rocky run --resume-latest`
picks up where it left off rather than starting over.

## Status

`--resume-latest` and `--resume <run_id>` are implemented in the Databricks
run path. The DuckDB local execution path (`run_local.rs`) doesn't yet
record per-table progress, so `--resume-latest` is a no-op there. This POC
ships the config + flag invocation as a spec.

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
