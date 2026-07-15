# 05-snapshot-scd2 — Snapshot Pipeline (SCD Type 2)

> **Category:** 01-quality
> **Credentials:** none (DuckDB)
> **Runtime:** < 15s
> **Rocky features:** `type = "snapshot"`, `unique_key`, `updated_at`, `invalidate_hard_deletes`, SCD-2 history tracking

## What it shows

Rocky's snapshot pipeline type is a dedicated SCD Type 2 slowly-changing dimension tracker. Unlike replication (bulk copy) or transformation (model execution), the snapshot pipeline:

1. Compares source rows against the latest snapshot using `unique_key` + `updated_at`
2. Inserts new versions with `valid_from` / `valid_to` columns
3. Closes previous versions by setting `valid_to` on the old record
4. Optionally invalidates hard deletes (`invalidate_hard_deletes = true`)

## Why it's distinctive

- **Dedicated pipeline type** — not a strategy on a model, but a `type = "snapshot"` pipeline
- **Single-table focus** — explicit source/target table refs (not pattern-based discovery)
- **Hard delete tracking** — `invalidate_hard_deletes = true` closes records when rows disappear from source
- **dbt comparison:** dbt snapshots require Jinja macros and source freshness; Rocky uses declarative TOML

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

```text
Source v1: 3 customers
(validate: config syntax valid, snapshot pipeline customer_history resolved)
Snapshot run 1 (initial)
    history rows: 0
Source v2: Alice upgraded, Charlie deleted, Dave added
    source rows: 3
Snapshot run 2 (changes detected)
History table:
    (empty)
POC complete: snapshot SCD-2 pipeline configured and validated.
```

The config **validates** cleanly and the snapshot pipeline resolves, but the
history table stays **empty** on the local DuckDB path — see the known limitation
below.

## Known limitation (local DuckDB path)

On the local DuckDB execution path the engine currently emits invalid MERGE SQL
for snapshots, so neither run populates `snapshots.customers_history`
(`history rows: 0`, empty history table). Both `rocky run` invocations exit
non-zero and are intentionally tolerated with `|| true` in `run.sh` so the demo
still walks through the full validate → run → inspect flow. The captured
`expected/run1.json` / `expected/run2.json` show the two underlying DuckDB
errors:

- `Parser Error: syntax error at or near "*"` on
  `... WHEN NOT MATCHED THEN INSERT (*) VALUES (source.*, CURRENT_TIMESTAMP, NULL)`
- `Binder Error: Referenced table "target" not found!` in the hard-delete
  invalidation subquery (`WHERE target.customer_id = source.customer_id`)

The `[pipeline.customer_history]` config (`unique_key`, `updated_at`,
`invalidate_hard_deletes`, explicit source/target refs) is the declarative
SCD-2 surface this POC demonstrates; end-to-end SCD-2 history is exercised
against a real warehouse adapter (Databricks/Snowflake), not the local DuckDB
snapshot path.

## What it would produce (once the DuckDB snapshot path is fixed)

1. **Run 1:** All 3 source rows inserted as new snapshot records with `valid_from = updated_at`, `valid_to = NULL`
2. **Run 2:** Rocky compares source vs latest snapshot:
   - **Alice** (customer_id=1): `updated_at` changed → old record closed (`valid_to` set), new record inserted
   - **Bob** (customer_id=2): unchanged → no action
   - **Charlie** (customer_id=3): missing from source + `invalidate_hard_deletes = true` → record closed
   - **Dave** (customer_id=4): new row → inserted with `valid_to = NULL`

## Related

- Snapshot pipeline config: `engine/crates/rocky-core/src/config.rs` (SnapshotPipelineConfig)
- Snapshot execution: `engine/crates/rocky-cli/src/commands/run_local.rs`
