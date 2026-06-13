# 11-strategy-showcase — Three materialization strategies side-by-side

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `strategy = "full_refresh"`, `strategy = "incremental"`, model-level `[strategy] type = "merge"`, `auto_create_schemas`

## What it shows

A cheat-sheet POC: one source table fed into three pipelines, each using a
different materialization strategy. Run them once, mutate the source, run
them again, then compare what each strategy did with the same delta:

```
                          ┌── pipeline.fr  (full_refresh)  → poc.fr.orders
raw__orders.orders ──────┼── pipeline.inc (incremental)   → poc.inc.orders
                          └── pipeline.merge (transformation, type="merge") → poc.merge.orders
```

Two of the strategies (`full_refresh`, `incremental`) live on **replication
pipelines**; `merge` is declared at the **model level** inside a
**transformation pipeline**, because Rocky's replication strategy enum is
limited to full_refresh + incremental. The user-facing TOML is shown in
each pipeline's section of `rocky.toml`.

## Why it's distinctive

- One config, three strategies, one source: see the differential behavior
  on the same dataset rather than reading three separate POCs.
- Surfaces the watermark-incremental tradeoff: `incremental` is fast and
  cheap but can't catch in-place UPDATEs to existing rows. `merge` and
  `full_refresh` can; only `merge` does so without rewriting unchanged
  rows.
- Where to look elsewhere:
  [`02-performance/03-partition-checksum`](../03-partition-checksum) for
  `time_interval` partitioning;
  [`07-adapters/02-databricks-materialized-view`](../../07-adapters/02-databricks-materialized-view)
  for `materialized_view`;
  [`07-adapters/01-snowflake-dynamic-table`](../../07-adapters/01-snowflake-dynamic-table)
  for `dynamic_table`;
  [`01-quality/05-snapshot-scd2`](../../01-quality/05-snapshot-scd2) for
  SCD-2 snapshots.

## Cheat sheet

| Strategy       | First run | Second run with the same delta             | Best for |
|----------------|-----------|--------------------------------------------|----------|
| `full_refresh` | 100 rows  | rebuild → 120 rows; **all** updates seen   | small tables, source-of-truth dimensions |
| `incremental`  | 100 rows  | append-only → 120 rows; **no** UPDATEs    | large append-mostly facts (logs, events) |
| `merge`        | 100 rows  | upsert → 120 rows; UPDATEs **seen**        | facts with late updates (orders, status) |

## Layout

```
.
├── README.md
├── rocky.toml                     three pipelines, one adapter
├── run.sh                         seed → run #1 (×3) → mutate → run #2 (×3) → diff
├── data/
│   ├── seed.sql                   100 orders in raw__orders.orders
│   └── delta.sql                  UPDATE order 2 amount, UPDATE order 5 status, INSERT 20
└── models/
    ├── orders.sql                 SELECT … FROM raw__orders.orders (used by pipeline.merge)
    └── orders.toml                [strategy] type = "merge", unique_key, update_columns
```

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI (`brew install duckdb`), which seeds the source, applies the
  delta, and runs the side-by-side verification queries
- Engine ≥ 1.29 (or this branch). `auto_create_schemas` for transformation
  pipelines landed in PR #448; if your binary predates it, either pre-create
  `poc.merge` via duckdb or wait for the next release.

## Run

```bash
./run.sh
```

## Expected output

```text
=== differential behavior on the two updated rows (order_id 2 + 5) ===
-- full_refresh table (rebuilt entirely; sees all updates) --
order_id |  status    | amount
       2 | completed  |  1000.0
       5 | completed  |    18.5

-- incremental table (no UPDATE; existing rows untouched) --
order_id |  status    | amount
       2 | completed  |    13.4   ← pre-delta
       5 | pending    |    18.5   ← pre-delta

-- merge table (UPDATE on match; both rows reflect the new values) --
order_id |  status    | amount
       2 | completed  |  1000.0
       5 | completed  |    18.5
```

Row counts after the second run are identical (120) across all three
strategies; the differential is *which existing rows changed*, not the
total count.

## What happened

1. `data/seed.sql` populates `raw__orders.orders` with 100 rows.
2. First `rocky run` for each pipeline:
   - `pipeline.fr` does a CTAS into `poc.fr.orders` (100 rows).
   - `pipeline.inc` does the same for the first run (the watermark is
     unset so the full source is copied) into `poc.inc.orders`.
   - `pipeline.merge` runs the transformation model: Rocky bootstraps an
     empty `poc.merge.orders` from the model schema and MERGE-inserts all
     100 rows.
3. `data/delta.sql` mutates the source: change `status` on order 5,
   change `amount` on order 2, insert 20 new orders dated `2026-05-01+`.
4. Second `rocky run` for each pipeline:
   - `pipeline.fr` rebuilds → 120 rows; **both** UPDATEs visible.
   - `pipeline.inc` appends only rows where `ordered_at > MAX(target.ordered_at)`
     → 120 rows; the 20 inserts are picked up but the two UPDATEs to
     existing rows are silently skipped (their `ordered_at` is below the
     watermark).
   - `pipeline.merge` runs MERGE → 120 rows; UPDATE on match catches the
     two existing-row mutations, INSERT on no-match catches the 20 new.
5. The final query proves the differential.

## Related

- Replication strategy parsing:
  [`engine/crates/rocky-core/src/config.rs`](../../../../engine/crates/rocky-core/src/config.rs)
- Strategy SQL generation:
  [`engine/crates/rocky-core/src/sql_gen.rs`](../../../../engine/crates/rocky-core/src/sql_gen.rs)
  (`generate_create_table_as_sql`, watermark `WHERE` injection,
  `generate_transformation_sql` MERGE arm)
- DuckDB MERGE dialect:
  [`engine/crates/rocky-duckdb/src/dialect.rs`](../../../../engine/crates/rocky-duckdb/src/dialect.rs)
