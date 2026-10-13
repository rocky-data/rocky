# 05-snapshot-scd2 — Snapshot Pipeline (SCD Type 2)

> **Category:** 01-quality
> **Credentials:** none (DuckDB)
> **Runtime:** < 15s
> **Rocky features:** `type = "snapshot"`, `unique_key`, `updated_at`, `invalidate_hard_deletes`, SCD-2 history tracking

## What it shows

Rocky's snapshot pipeline type is a dedicated SCD Type 2 slowly-changing dimension tracker. Unlike replication (bulk copy) or transformation (model execution), the snapshot pipeline:

1. Compares source rows against the open snapshot rows using `unique_key` + `updated_at`
2. Inserts new versions with `valid_from` / `valid_to` columns
3. Closes previous versions by setting `valid_to` on the old record
4. Optionally invalidates hard deletes (`invalidate_hard_deletes = true`)

## Why it's distinctive

- **Dedicated pipeline type** — a `type = "snapshot"` pipeline. (A model can also use a snapshot strategy; this POC shows the pipeline form.)
- **Single-table focus** — explicit source/target table refs (not pattern-based discovery)
- **Hard delete tracking** — `invalidate_hard_deletes = true` closes records when rows disappear from source

## Layout

```
.
├── README.md         this file
├── rocky.toml        snapshot pipeline config
├── run.sh            end-to-end demo (two snapshot runs)
└── data/
    ├── seed_v1.sql   initial customer data (3 rows)
    └── seed_v2.sql   changed data (Alice upgraded, Charlie deleted, Dave added)
```

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI (`brew install duckdb`)

## Run

```bash
./run.sh
```

## Expected output

After run 2, `snapshots.customers_history` should hold this history:

1. **Alice** (customer_id=1): `updated_at` changed. The old record is closed
   (`valid_to` set) and a new record is inserted.
2. **Bob** (customer_id=2): unchanged. No action.
3. **Charlie** (customer_id=3): missing from the source. With
   `invalidate_hard_deletes = true` the open record is closed.
4. **Dave** (customer_id=4): new. Inserted with `valid_to = NULL`.

Rocky sets `valid_from` and `valid_to` from `CURRENT_TIMESTAMP` at run time,
not from the source `updated_at`.

## Status of the local DuckDB path

Engines before 1.75.0 emitted invalid snapshot MERGE SQL on DuckDB
(`INSERT (*) VALUES (source.*, …)` and an unbound `target` alias), so the
history table stayed empty. 1.75.0 (#2012) and 1.76.0 (#2212) fixed both, and
`engine/crates/rocky-cli/tests/snapshot_hard_delete_run.rs` now runs a DuckDB
snapshot through `rocky run`.

`run.sh` still tolerates a failed `rocky run` with `|| true`. Its comments and
its final line still describe the old failure. Check `expected/run1.json` and
`expected/run2.json` if the history table is empty.

## Related

- Engine starter with the same shape: [`engine/examples/snapshot`](../../../../../engine/examples/snapshot)
- Snapshot pipeline config: `engine/crates/rocky-core/src/config.rs` (SnapshotPipelineConfig)
- Snapshot execution: `engine/crates/rocky-cli/src/commands/run_local.rs`
