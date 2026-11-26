---
title: Time Interval Materialization
description: Partition-keyed materialization with idempotent re-runs and late-arriving data correction
sidebar:
  order: 10.5
---

The `time_interval` strategy partitions a model by a time column at a fixed granularity: `hour`, `day`, `month`, or `year`. A run targets named partitions instead of appending past a watermark. So a re-run of one partition is idempotent, a late-arriving row is picked up on the next run of its partition, and a backfill walks the range in order.

Use it when the model aggregates by date and a day must be recomputed as a whole, or when you want cost and row counts per partition. A transformation `incremental` model needs no partition column. See [Incremental](/reference/model-format/#incremental).

## TOML reference

```toml
# models/fct_daily_orders.toml
[strategy]
type = "time_interval"
time_column = "order_date"        # column on the model output
granularity = "day"               # hour | day | month | year
lookback = 0                      # optional: recompute the previous N partitions
batch_size = 1                    # optional: combine N partitions per SQL statement
first_partition = "2024-01-01"    # required if you use --missing
```

| Field | Type | Default | Meaning |
|---|---|---|---|
| `time_column` | string | required | Column on the **model output** that holds the partition value. The compiler validates it exists, has a date/timestamp type (when known), is non-nullable, and passes SQL identifier validation. A nullable upstream column passes the non-null check when the model's `WHERE` compares that same column (for example `order_date >= @start_date`), because the comparison drops every row with a NULL key. |
| `granularity` | enum | required | One of `hour`, `day`, `month`, `year`. Determines the canonical partition key format. |
| `lookback` | u32 | `0` | Recompute the previous N partitions on each run, in addition to whichever partitions the CLI selected. The standard pattern for late-arriving data without re-running the whole table. |
| `batch_size` | NonZeroU32 | `1` | Combine N consecutive partitions into a single SQL statement when backfilling. `1` is atomic per partition (recommended). `>1` trades atomicity for backfill throughput. |
| `first_partition` | string | none | Lower bound for `--missing` discovery and for the first-run fill, in canonical key format (e.g. `"2024-01-01"` for daily). Required when using `--missing`; otherwise optional. |

## SQL placeholders

The model SQL must reference both `@start_date` and `@end_date`. Rocky
substitutes them at compile time per partition with quoted timestamp
literals (`'2026-04-07 00:00:00'`):

```sql
SELECT
    DATE_TRUNC('day', order_at) AS order_date,
    customer_id,
    COUNT(*) AS order_count,
    SUM(amount) AS revenue
FROM stg_orders
WHERE order_at >= @start_date
  AND order_at <  @end_date
GROUP BY 1, 2
```

Each window is **half-open**. A row at exactly `@start_date` is in the window. A
row at exactly `@end_date` belongs to the next partition. 

```
  granularity = "day"

   04-06 00:00      04-07 00:00      04-08 00:00      04-09 00:00
        │                │                │                │
   ─────┼────────────────┼────────────────┼────────────────┼─────► time
        │  partition     │  partition     │  partition     │
        │  2026-04-06    │  2026-04-07    │  2026-04-08    │
        └────────────────┴────────────────┴────────────────┘
                         ▲
                         └── @end_date of partition 2026-04-06,
                             and @start_date of partition 2026-04-07

   running partition 2026-04-07 substitutes:
       @start_date → '2026-04-07 00:00:00'   a row here is included
       @end_date   → '2026-04-08 00:00:00'   a row here is excluded
```

The same rule applies at every grain. For `granularity = "hour"`, key `2026-04-07T13` substitutes `@start_date` → `'2026-04-07 13:00:00'` and `@end_date` → `'2026-04-07 14:00:00'`.

The compiler emits `E024` if either placeholder does not filter the rows, or when it cannot parse the SQL. Each placeholder must appear in a `WHERE`, `HAVING` or `QUALIFY` clause, or in the `ON` of an inner join. A placeholder in a comment, in a longer string or only in the `SELECT` list does not count.

Inside that clause, a placeholder counts only in a comparison that bounds a column and is joined to the rest of the clause by `AND`.

| Counts | Does not count |
|---|---|
| `<col> >= @start_date` (or `>`, `=`) and `<col> < @end_date` (or `<=`, `=`), either operand order | An `OR` with an unbounded branch, for example `ts >= @start_date OR 1 = 1` or `@start_date IS NULL OR ...` |
| `<col> BETWEEN @start_date AND @end_date` | A bound that faces the wrong way (`ts <= @start_date`) |
| A cast or function on the column (`DATE(ts)`), or a cast or constant shift on the placeholder (`@start_date - INTERVAL 1 DAY`) | `NOT BETWEEN`, `NOT IN`, `EXISTS`, any comparison under `NOT` |
| `<col> IN (SELECT ...)` when the subquery's rows are bounded | A placeholder inside a subquery bounds that subquery's rows, not the clause around it |
| An `OR` where every branch is bounded by these rules | |

The filter must reach every row the model emits:

| Shape | Rule |
|---|---|
| `UNION` / `UNION ALL` | Each branch needs its own filter, or one filter wraps the whole union. |
| `INTERSECT` / `EXCEPT` | `INTERSECT` needs one filtered side. `EXCEPT` needs a filtered left side. |
| CTE or subquery | A filter counts only when the output reads it. A filter in a CTE nothing reads, or in a scalar subquery in the `SELECT` list, counts for nothing. |
| Join | An inner or semi join is bounded when either side is. `LEFT` is bounded by its left side, `RIGHT` by its right side. `FULL OUTER` needs both. |
| Date spine | `GENERATE_SERIES(@start_date, @end_date, INTERVAL 1 DAY)` does not count on its own. Its end is inclusive in most warehouses, so it emits a row of the next partition. Add `WHERE d >= @start_date AND d < @end_date` on the spine column. |

## Partition key format

Canonical ISO format, derived from `granularity`. These keys appear in
the state store, the `--partition` / `--from` / `--to` CLI flags, and the
JSON output:

| Granularity | Format | Example |
|---|---|---|
| `hour` | `%Y-%m-%dT%H` | `2026-04-07T13` |
| `day` | `%Y-%m-%d` | `2026-04-07` |
| `month` | `%Y-%m` | `2026-04` |
| `year` | `%Y` | `2026` |

Rocky rejects `2026-04-7`, which drops the zero padding.

## CLI flags

Every flag below is accepted on both `rocky plan` and `rocky run`. The selection flags (`--partition`, `--from/--to`, `--latest`, `--missing`) are mutually exclusive:

| Flag | Behavior |
|---|---|
| `--partition KEY` | Run exactly one partition by canonical key. Errors if the format doesn't match the model's grain. |
| `--from FROM --to TO` | Run a closed inclusive range. Both bounds must align to the grain. |
| `--latest` | Run the partition containing `now()` (UTC). Default for `time_interval` models when no other selection flag is given, except on the first run (see below). |
| `--missing` | Compute the diff between expected partitions (`first_partition` → `now()`) and what's recorded as `Computed` in the `PARTITIONS` state-store table; run only the gaps. Errors if `first_partition` is unset. |
| `--lookback N` | Recompute the previous N partitions in addition to the selected ones. CLI override beats the model's TOML `lookback`. |
| `--parallel N` | Run N partitions concurrently (default 4; pass `--parallel 1` for serial). This bounds warehouse queries only: state writes serialize through redb's single-writer lock. **Caveat:** DuckDB's adapter holds a connection mutex, so partitions always run serially against DuckDB, whatever `--parallel` says. Snowflake and Databricks parallelize up to N. |

### The first run

A model that sets `first_partition` and has no recorded partition has never run. With no selection flag, its first run fills every partition from `first_partition` up to now, as if you had passed `--missing`. The fill adds no `lookback` partitions before `first_partition`.

The fill has a limit of 1000 partitions. If `first_partition` is further back, the first run builds only the latest partition and logs a warning. Pass `--missing`, or `--from` and `--to`, to fill the history. A model with no `first_partition` always builds the latest partition. So does a first run whose `first_partition` is after the current partition.

`rocky plan` reports each fill before it runs, in the JSON field `first_run_fills` (`model`, `partitions`, `from`, `to`, and `fills`, which is `false` when the range is over the limit). The text output prints a `-- first-run fills --` block. A plan with a partition flag or `--lookback` reports none.

The plan also records which models fill. An agent's `rocky apply` refuses a fill its plan did not record, before any model writes. Plan again and review the new plan. A plan from an older Rocky records no fill, so an agent's apply refuses a never-run model that sets `first_partition`. A recorded fill does not fix its end: applied on a later day, it reaches that day's partition. An agent's apply of a `--dag` plan is refused outright.

```bash
rocky run --partition 2026-04-07                          # one partition
rocky run --from 2026-04-01 --to 2026-04-07               # closed range
rocky run --missing                                       # gaps in the state store
rocky run --lookback 3                                    # today + previous 3 (late data)
```

## Per-warehouse SQL

Each adapter implements `insert_overwrite_partition` and returns a `Vec<String>`, so the runtime can issue more than one statement. Atomicity holds per partition. If a statement fails mid-batch, the runtime issues `ROLLBACK` and marks the partition `Failed` in the state store.

Before it runs any partition, Rocky checks that the target table exists. If not, it creates it by running the model SQL over an empty window, so the model reads no upstream rows. A model with an ungrouped aggregate still writes one row; every other shape writes none. The table's columns come from that query.

| Adapter | Statements |
|---|---|
| Databricks | One `INSERT INTO ... REPLACE WHERE`. It works on un-partitioned tables too. |
| Snowflake, DuckDB | `BEGIN`, `DELETE` the window, `INSERT`, `COMMIT`. Snowflake's REST API runs one statement per call, so each is a separate `POST /api/v2/statements`. |
| BigQuery | One `BEGIN TRANSACTION` / `COMMIT TRANSACTION` script in a single job. BigQuery's REST API is stateless, so separate `BEGIN` and `COMMIT` statements fail with "Transaction control statements are supported only in scripts or sessions." |
| PostgreSQL, Redshift | `DELETE` then `INSERT` in one transaction. |

Databricks example, with Delta's atomic `REPLACE WHERE`:

```sql
INSERT INTO `warehouse`.`marts`.`fct_daily_orders`
REPLACE WHERE order_date >= '2026-04-07 00:00:00' AND order_date < '2026-04-08 00:00:00'
SELECT ... FROM `warehouse`.`raw`.`stg_orders`
WHERE order_at >= '2026-04-07 00:00:00'
  AND order_at <  '2026-04-08 00:00:00'
GROUP BY 1, 2
```

## State store

Per-partition state lives in its own `PARTITIONS` table in the redb [state store](/concepts/state-management/) (`<models>/.rocky-state.redb` by default), one row per `(model_name, partition_key)`.

| Field | Type |
|---|---|
| `model_name`, `partition_key`, `run_id` | `String` |
| `status` | `Computed`, `Failed`, `InProgress` or `Interrupted` |
| `computed_at` | `DateTime<Utc>` |
| `row_count`, `duration_ms` | `u64` |
| `checksum` | `Option<u64>` |

`--missing` reads this table to compute the gap between expected and actual. `RUN_HISTORY` stays the source of truth for whole-run success. `PARTITIONS` is the source of truth for per-partition success. The `run_id` field cross-references the two.

## Timezone

**Partitions are UTC only.** Rocky reads the `time_column` as UTC, and
`--latest` resolves to the partition that contains the current UTC time. If
your data lives in another timezone, convert in your model SQL or accept UTC
partition boundaries. The strategy has no `timezone` field.

## JSON output

After a `time_interval` run, `rocky run --output json` adds a `partition` object (`key`, `start`, `end`, `batched_with`) to each materialization and a `partition_summaries` list:

```json
"partition_summaries": [
  { "model": "fct_daily_orders", "partitions_planned": 1,
    "partitions_succeeded": 1, "partitions_failed": 0 }
]
```

The dagster-rocky integration maps the strategy to a Dagster `DailyPartitionsDefinition` (or `Hourly`, `Monthly`, or `TimeWindow` for year) through the generated `PartitionInfo` and `PartitionSummary` models. See [the dagster partitions guide](/dagster/partitions/).

## Comparison with `incremental`

| | `incremental` | `time_interval` |
|---|---|---|
| Unit of work | Rows newer than the target's `MAX(<watermark>)` | One partition window per statement |
| SQL marker | `@incremental_filter` | `@start_date` and `@end_date` |
| Late data | `lookback` with `unique_key` | `lookback` partitions |
| Backfill a range | `rocky run --full-refresh` rebuilds all | `--from` / `--to` |
| State | Read from the target | `PARTITIONS` table in the state store |

## Limitations

These are not supported yet:

- **Rocky DSL placeholder syntax** — `@start_date` / `@end_date` are
  recognized in `.sql` files only. The `.rocky` parser has no placeholder
  syntax.
- **Native partitions on PostgreSQL and Redshift** — both adapters run
  `time_interval` as `DELETE` then `INSERT` in one transaction. A
  declarative-partition (child-table truncate) route is a follow-up.
- **Sub-day granularities** below `hour` — belongs in streaming systems.
- **Multi-column partitions** — a single time column only.
- **Partition column transformations** — `time_column` must be a real
  output column, not an expression.
- **`--batch-size` CLI flag** — `batch_size` is TOML-only.

## See also

- Partition-checksum POC: `examples/playground/pocs/02-performance/03-partition-checksum/` (`time_interval` with late-arriving data correction)
- [Dagster partitions guide](/dagster/partitions/)
