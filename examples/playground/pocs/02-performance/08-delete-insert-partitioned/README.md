# 08-delete-insert-partitioned — Delete+Insert Strategy

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `strategy = "delete_insert"`, `partition_by`, partition replacement

## What it shows

The `delete_insert` materialization strategy is an alternative to MERGE for partition-level updates. Instead of row-by-row matching (MERGE), delete+insert:

1. Deletes all rows in the target matching the partition key(s)
2. Inserts fresh data for those partitions

This is ideal for late-arriving data, daily/regional aggregates, and scenarios where MERGE's row-matching overhead isn't needed.

**Limit:** on DuckDB the DELETE and the INSERT run as two separate
statements, not one transaction. The DELETE commits first, so a failed INSERT
leaves the partition empty until the next run. Some adapters (PostgreSQL,
Redshift) join them into one transaction.

`run.sh` only validates and compiles the model. It does not execute it.

## Why it's distinctive

- **No duplicate risk** — unlike incremental INSERT, delete+insert clears the partition first
- **Simpler than MERGE** — no `WHEN MATCHED / WHEN NOT MATCHED` logic
- **Partition-scoped** — only touches rows matching `partition_by` keys, not the entire table
- **dbt comparison:** dbt's `incremental` with `delete+insert` strategy requires Jinja config blocks; Rocky uses `partition_by` in TOML

## Layout

```
.
├── README.md                this file
├── rocky.toml               pipeline config
├── run.sh                   end-to-end demo
├── data/
│   └── seed.sql             daily sales across 3 regions (300 rows)
└── models/
    ├── _defaults.toml       shared target (poc.analytics)
    ├── regional_sales.sql   aggregated revenue by date + region
    └── regional_sales.toml  strategy = "delete_insert", partition_by = ["region"]
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
=== Compiled model ===
  regional_sales — delete_insert (partition_by: [region])

Delete+Insert strategy:
  1. DELETE FROM target WHERE region IN (affected partitions)
  2. INSERT INTO target SELECT ... FROM source WHERE region IN (...)
  This avoids MERGE overhead and prevents duplicates from late-arriving data.

POC complete: delete_insert strategy parsed and compiled.
```

> `rocky compile` also emits an `I002` info diagnostic (`4 column(s) have unknown types`)
> because the model reads from a raw seed table with no declared source schema — the
> aggregate still compiles cleanly (`has_errors: false`).

## What happened

1. `rocky compile` parsed the model and recognized `delete_insert` strategy with `partition_by = ["region"]`
2. At execution time, Rocky generates two statements (shape from
   `SqlDialect::delete_partitions_sql` in `engine/crates/rocky-core/src/traits.rs`):
   ```sql
   DELETE FROM poc.analytics.regional_sales WHERE (region) IN (
     SELECT DISTINCT region FROM (<model SQL>) AS _rocky_incoming);
   INSERT INTO poc.analytics.regional_sales <model SQL>;
   ```
3. Only the regions present in the new output are deleted and re-inserted.
   Other regions in the target stay as they are.

## Related

- Merge strategy: [`02-performance/02-merge-upsert`](../02-merge-upsert/)
- Incremental watermark: [`02-performance/01-incremental-watermark`](../01-incremental-watermark/)
- Time-interval partitioning: [`02-performance/03-partition-checksum`](../03-partition-checksum/)
