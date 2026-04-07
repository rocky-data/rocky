# 05-snapshot-scd2 — Snapshot Pipeline (SCD Type 2)

> **Category:** 01-quality
> **Credentials:** none (DuckDB)
> **Runtime:** < 15s
> **Rocky features:** `type = "snapshot"`, `unique_key`, `updated_at`, `invalidate_hard_deletes`, SCD-2 history tracking

## What it shows

Rocky's snapshot pipeline type — a dedicated SCD Type 2 slowly-changing dimension tracker. Unlike replication (bulk copy) or transformation (model execution), the snapshot pipeline:

1. Compares source rows against the latest snapshot using `unique_key` + `updated_at`
2. Inserts new versions with `valid_from` / `valid_to` columns
3. Closes previous versions by setting `valid_to` on the old record
4. Optionally invalidates hard deletes (`invalidate_hard_deletes = true`)

## Why it's distinctive

- **First-class pipeline type** — not a strategy on a model, but a dedicated `type = "snapshot"` pipeline
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
Snapshot run 1 (initial)
    history rows: 3
Source v2: Alice upgraded, Charlie deleted, Dave added
    source rows: 3
Snapshot run 2 (changes detected)
History table:
    customer_id | name    | tier     | valid_from          | valid_to
    1           | Alice   | gold     | 2026-01-15 10:00:00 | 2026-04-01 08:00:00
    1           | Alice   | platinum | 2026-04-01 08:00:00 | NULL
    2           | Bob     | silver   | 2026-02-01 09:30:00 | NULL
    3           | Charlie | bronze   | 2026-03-10 14:00:00 | 2026-04-01 ...
    4           | Dave    | bronze   | 2026-04-05 11:00:00 | NULL
POC complete.
```

## What happened

1. **Run 1:** All 3 source rows inserted as new snapshot records with `valid_from = updated_at`, `valid_to = NULL`
2. **Run 2:** Rocky compared source vs latest snapshot:
   - **Alice** (customer_id=1): `updated_at` changed → old record closed (`valid_to` set), new record inserted
   - **Bob** (customer_id=2): unchanged → no action
   - **Charlie** (customer_id=3): missing from source + `invalidate_hard_deletes = true` → record closed
   - **Dave** (customer_id=4): new row → inserted with `valid_to = NULL`

## Related

- Snapshot pipeline config: `engine/crates/rocky-core/src/config.rs` (SnapshotPipelineConfig)
- Snapshot execution: `engine/crates/rocky-cli/src/commands/run_local.rs`
