# 05-snapshot-scd2 — Snapshot Pipeline (SCD Type 2)

> **Category:** 01-quality
> **Credentials:** none (DuckDB)
> **Runtime:** < 15s
> **Rocky features:** `type = "snapshot"`, `unique_key`, `updated_at`, `invalidate_hard_deletes`, SCD-2 history tracking

## What it shows

A `type = "snapshot"` pipeline for SCD Type 2 history on one explicit source/target table (models can also use a snapshot strategy; this shows the pipeline form). It:

1. Compares source rows with open snapshot rows using `unique_key` + `updated_at`
2. Inserts new versions with `valid_from` / `valid_to`
3. Closes previous versions by setting `valid_to`
4. With `invalidate_hard_deletes = true`, closes rows that disappeared from the source

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI (`brew install duckdb`)

## Run

```bash
./run.sh
```

## Expected output

After run 2, `snapshots.customers_history` should hold:

1. **Alice** (customer_id=1): `updated_at` changed, so the old record is closed and a new one inserted.
2. **Bob** (customer_id=2): unchanged.
3. **Charlie** (customer_id=3): gone from the source, so the open record is closed.
4. **Dave** (customer_id=4): new, with `valid_to = NULL`.

Rocky sets `valid_from` and `valid_to` from `CURRENT_TIMESTAMP` at run time, not from the source `updated_at`.

## Status of the local DuckDB path

Engines before 1.75.0 emitted invalid snapshot MERGE SQL on DuckDB (`INSERT (*) VALUES (source.*, …)` and an unbound `target` alias), so the history table stayed empty. 1.75.0 (#2012) and 1.76.0 (#2212) fixed both, and `engine/crates/rocky-cli/tests/snapshot_hard_delete_run.rs` now runs a DuckDB snapshot through `rocky run`.

`run.sh` still tolerates a failed `rocky run` with `|| true`. Its comments and its final line still describe the old failure. If the history table is empty, check `expected/run1.json` and `expected/run2.json`.

## Related

- [`engine/examples/snapshot`](../../../../../engine/examples/snapshot)
- Source: `engine/crates/rocky-core/src/config.rs` (SnapshotPipelineConfig), `engine/crates/rocky-cli/src/commands/run_local.rs`
