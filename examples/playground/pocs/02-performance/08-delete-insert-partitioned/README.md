# 08-delete-insert-partitioned — Delete+Insert Strategy

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `strategy = "delete_insert"`, `partition_by`, partition replacement

## What it shows

The `delete_insert` strategy (`partition_by = ["region"]`) replaces whole partitions: delete the target rows matching the partition keys, then insert fresh data. It suits late-arriving data and daily or regional aggregates, without MERGE's row matching.

**Limit:** on DuckDB the DELETE and the INSERT are two separate statements, not one transaction. The DELETE commits first, so a failed INSERT leaves the partition empty until the next run. Some adapters (PostgreSQL, Redshift) join them into one transaction.

`run.sh` does not execute the strategy against the warehouse: it validates, compiles, and runs a local `rocky test`.

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

> `rocky compile` also emits an `I002` info diagnostic (`4 column(s) have unknown types`) because the model reads a raw seed table with no declared source schema; `has_errors` is `false`.

## SQL at execution time

Shape from `SqlDialect::delete_partitions_sql` in `engine/crates/rocky-core/src/traits.rs`:

```sql
DELETE FROM poc.analytics.regional_sales WHERE (region) IN (
  SELECT DISTINCT region FROM (<model SQL>) AS _rocky_incoming);
INSERT INTO poc.analytics.regional_sales <model SQL>;
```

Only regions present in the new output are replaced; others stay as they are.
