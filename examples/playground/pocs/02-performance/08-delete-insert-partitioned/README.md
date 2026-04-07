# 08-delete-insert-partitioned — Delete+Insert Strategy

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `strategy = "delete_insert"`, `partition_by`, atomic partition replacement

## What it shows

The `delete_insert` materialization strategy — an alternative to MERGE for partition-level updates. Instead of row-by-row matching (MERGE), delete+insert:

1. Deletes all rows in the target matching the partition key(s)
2. Inserts fresh data for those partitions

This is ideal for late-arriving data, daily/regional aggregates, and scenarios where MERGE's row-matching overhead isn't needed.

## Why it's distinctive

- **No duplicate risk** — unlike incremental INSERT, delete+insert clears the partition first
- **Simpler than MERGE** — no `WHEN MATCHED / WHEN NOT MATCHED` logic
- **Partition-scoped** — only touches rows matching `partition_by` keys, not the entire table
- **dbt comparison:** dbt's `incremental` with `delete+insert` strategy requires Jinja config blocks; Rocky uses `partition_by` in declarative TOML

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
Compiled model:
  regional_sales — delete_insert (partition_by: [region])

Delete+Insert strategy:
  1. DELETE FROM target WHERE region IN (affected partitions)
  2. INSERT INTO target SELECT ... FROM source WHERE region IN (...)
POC complete.
```

## What happened

1. `rocky compile` parsed the model and recognized `delete_insert` strategy with `partition_by = ["region"]`
2. At execution time, Rocky would generate:
   ```sql
   DELETE FROM poc.analytics.regional_sales WHERE region IN ('us_east', 'us_west', 'eu_west');
   INSERT INTO poc.analytics.regional_sales
   SELECT sale_date, region, COUNT(*), SUM(amount), AVG(amount)
   FROM seeds.daily_sales GROUP BY sale_date, region;
   ```
3. Only affected partitions are touched — unmodified regions remain untouched

## Related

- Merge strategy: [`02-performance/02-merge-upsert`](../02-merge-upsert/)
- Incremental watermark: [`02-performance/01-incremental-watermark`](../01-incremental-watermark/)
- Time-interval partitioning: [`02-performance/03-partition-checksum`](../03-partition-checksum/)
