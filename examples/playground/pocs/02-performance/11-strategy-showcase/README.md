# 11-strategy-showcase — Three materialization strategies side-by-side

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `strategy = "full_refresh"`, `strategy = "incremental"`, model-level `[strategy] type = "merge"`, `auto_create_schemas`

## What it shows

One source table feeds three pipelines with different strategies. Run them, mutate the source, run again, and compare:

```
                          ┌── pipeline.fr  (full_refresh)  → poc.fr.orders
raw__orders.orders ──────┼── pipeline.inc (incremental)   → poc.inc.orders
                          └── pipeline.merge (transformation, type="merge") → poc.merge.orders
```

`full_refresh` and `incremental` are replication pipelines; `merge` is declared on the model in a transformation pipeline. (A replication pipeline also accepts `strategy = "merge"` with `merge_keys`.)

`incremental` is cheap but cannot catch in-place UPDATEs; `merge` and `full_refresh` can, and only `merge` does so without rewriting unchanged rows.

Other strategies: [`03-partition-checksum`](../03-partition-checksum) (`time_interval`), [`07-adapters/02-databricks-materialized-view`](../../07-adapters/02-databricks-materialized-view), [`07-adapters/01-snowflake-dynamic-table`](../../07-adapters/01-snowflake-dynamic-table), [`01-quality/05-snapshot-scd2`](../../01-quality/05-snapshot-scd2).

## Cheat sheet

| Strategy       | First run | Second run with the same delta             | Best for |
|----------------|-----------|--------------------------------------------|----------|
| `full_refresh` | 100 rows  | rebuild → 120 rows; **all** updates seen   | small tables, source-of-truth dimensions |
| `incremental`  | 99 rows   | append-only → 119 rows; **no** UPDATEs    | large append-mostly facts (logs, events) |
| `merge`        | 100 rows  | upsert → 120 rows; UPDATEs **seen**        | facts with late updates (orders, status) |

## Prerequisites

- `rocky` ≥ 1.29.0 on PATH (`auto_create_schemas` for transformation pipelines, PR #448). On an older binary, pre-create `poc.merge` with duckdb.
- `duckdb` CLI (`brew install duckdb`)

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

After the second run `full_refresh` and `merge` hold 120 rows. `incremental` holds 119, because it holds back the newest source row (see [`01-incremental-watermark`](../01-incremental-watermark)). The cheat sheet `run.sh` prints still says 100 and 120 for `incremental`.

## What happened

The seed has 100 orders. `data/delta.sql` changes `amount` on order 2 and `status` on order 5, and inserts 20 orders. On run 2, `incremental` appends only rows with `ordered_at > MAX(target.ordered_at)`, so both UPDATEs are silently skipped.
