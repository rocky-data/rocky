---
title: Time Interval Materialization
description: Partition-keyed materialization with idempotent re-runs and late-arriving data correction
sidebar:
  order: 10.5
---

The `time_interval` materialization strategy partitions a model by a time
column at a fixed granularity: `hour`, `day`, `month`, or `year`. A run targets
named partitions instead of appending past a watermark. Three things follow from
that. Re-running the same partition is idempotent. A late-arriving row is picked
up on the next run of its partition. A backfill walks the range in order.

Reach for this strategy when:

- Your model aggregates by date and you need per-day rebuilds
- You want cost and row counts reported per partition

For a pure append-only pattern, where a row never arrives late, `incremental`
stays the simpler choice.

## TOML reference

```toml
# models/fct_daily_orders.toml
name = "fct_daily_orders"
depends_on = ["stg_orders"]

[[sources]]
catalog = "warehouse"
schema = "raw"
table = "stg_orders"

[strategy]
type = "time_interval"
time_column = "order_date"        # column on the model output
granularity = "day"               # hour | day | month | year
lookback = 0                      # optional: recompute the previous N partitions
batch_size = 1                    # optional: combine N partitions per SQL statement
first_partition = "2024-01-01"    # required if you use --missing

[target]
catalog = "warehouse"
schema = "marts"
table = "fct_daily_orders"
```

| Field | Type | Default | Meaning |
|---|---|---|---|
| `time_column` | string | required | Column on the **model output** that holds the partition value. The compiler validates it exists, has a date/timestamp type (when known), is non-nullable, and passes SQL identifier validation. |
| `granularity` | enum | required | One of `hour`, `day`, `month`, `year`. Determines the canonical partition key format. |
| `lookback` | u32 | `0` | Recompute the previous N partitions on each run, in addition to whichever partitions the CLI selected. The standard pattern for late-arriving data without re-running the whole table. |
| `batch_size` | NonZeroU32 | `1` | Combine N consecutive partitions into a single SQL statement when backfilling. `1` is atomic per partition (recommended). `>1` trades atomicity for backfill throughput. |
| `first_partition` | string | none | Lower bound for `--missing` discovery, in canonical key format (e.g. `"2024-01-01"` for daily). Required when using `--missing`; otherwise optional. |

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
row at exactly `@end_date` belongs to the next partition. This matches Spark,
BigQuery, and Postgres `tsrange` conventions.

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

The same rule applies at every grain. For `granularity = "hour"`, partition key
`2026-04-07T13` substitutes:
- `@start_date` → `'2026-04-07 13:00:00'`
- `@end_date` → `'2026-04-07 14:00:00'`

The compiler emits `E024` if neither placeholder is present, and `W003`
if only one of the two is referenced (the partition window would be
unbounded on one side).

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

Rocky restricts the format on purpose. It rejects `2026-04-7`, which drops the
zero padding, even though `chrono` would parse it.

## CLI flags

> Note: the canonical, auditable form is `rocky plan` followed by `rocky apply <plan-id>`. The `rocky run` single-step alias fuses plan + apply into one invocation for local iteration and automation. Every partition-selection flag below is accepted on both `rocky plan` and `rocky run`.

`rocky run` accepts seven new flags for selecting and modifying which
partitions to compute. The selection flags are mutually exclusive (`clap`
enforces it):

| Flag | Behavior |
|---|---|
| `--partition KEY` | Run exactly one partition by canonical key. Errors if the format doesn't match the model's grain. |
| `--from FROM --to TO` | Run a closed inclusive range. Both bounds must align to the grain. |
| `--latest` | Run the partition containing `now()` (UTC). Default for `time_interval` models when no other selection flag is given. |
| `--missing` | Compute the diff between expected partitions (`first_partition` → `now()`) and what's recorded as `Computed` in the `PARTITIONS` state-store table; run only the gaps. Errors if `first_partition` is unset. |
| `--lookback N` | Recompute the previous N partitions in addition to the selected ones. CLI override beats the model's TOML `lookback`. |
| `--parallel N` | Run N partitions concurrently (default 4; pass `--parallel 1` for serial). Driven by `futures::stream::buffer_unordered` so the per-partition futures are polled in the same task — no spawn, no `Send` constraint. Warehouse-query parallelism only: state writes serialize through redb's single-writer lock. **Caveat:** DuckDB's adapter holds a connection mutex and runs `execute_statement` synchronously, so partitions always run serially against DuckDB regardless of `--parallel`. Snowflake and Databricks (REST-based async I/O) parallelize up to N as expected. |

```bash
# Run today's partition
rocky run --filter client=acme

# Run a specific historical partition
rocky run --filter client=acme --partition 2026-04-07

# Backfill a closed range
rocky run --filter client=acme --from 2026-04-01 --to 2026-04-07

# Find and run gaps in the state store
rocky run --filter client=acme --missing

# Re-run today + the previous 3 days (for late-arriving data)
rocky run --filter client=acme --lookback 3
```

## Per-warehouse SQL

Each adapter implements `insert_overwrite_partition` in its own way. Each one
returns a `Vec<String>`, so the runtime can issue more than one statement.
Atomicity holds per partition. If a statement fails mid-batch, the runtime
issues `ROLLBACK` and marks the partition `Failed` in the state store.

### Databricks (Delta Lake)

Single statement using Delta's atomic `REPLACE WHERE`:

```sql
INSERT INTO `warehouse`.`marts`.`fct_daily_orders`
REPLACE WHERE order_date >= '2026-04-07 00:00:00' AND order_date < '2026-04-08 00:00:00'
SELECT
    DATE_TRUNC('day', order_at) AS order_date,
    customer_id,
    COUNT(*) AS order_count,
    SUM(amount) AS revenue
FROM `warehouse`.`raw`.`stg_orders`
WHERE order_at >= '2026-04-07 00:00:00'
  AND order_at <  '2026-04-08 00:00:00'
GROUP BY 1, 2
```

Works on un-partitioned tables too; `REPLACE WHERE` doesn't require physical
partitioning.

### Snowflake

Four-statement vec via explicit transaction. Snowflake's REST API runs one
statement per call by default, so the runtime issues each as a separate
`POST /api/v2/statements`:

```sql
BEGIN;
DELETE FROM "warehouse"."marts"."fct_daily_orders"
WHERE order_date >= '2026-04-07 00:00:00' AND order_date < '2026-04-08 00:00:00';
INSERT INTO "warehouse"."marts"."fct_daily_orders"
SELECT ... ;
COMMIT;
```

### DuckDB

Same shape as Snowflake; keeps the abstraction symmetric across warehouses:

```sql
BEGIN;
DELETE FROM "marts"."fct_daily_orders"
WHERE order_date >= '2026-04-07 00:00:00' AND order_date < '2026-04-08 00:00:00';
INSERT INTO "marts"."fct_daily_orders"
SELECT ... ;
COMMIT;
```

## State store

Per-partition state lives in its own `PARTITIONS` table in the redb
[state store](/concepts/state-management/) (`<models>/.rocky-state.redb` by
default). There is one row per `(model_name, partition_key)`:

```rust
PartitionRecord {
    model_name: String,
    partition_key: String,
    status: PartitionStatus,   // Computed | Failed | InProgress | Interrupted
    computed_at: DateTime<Utc>,
    row_count: u64,
    duration_ms: u64,
    run_id: String,
    checksum: Option<u64>,
}
```

`--missing` reads this table to compute the gap between expected and actual.
`RUN_HISTORY` stays the source of truth for whole-run success. `PARTITIONS` is
the source of truth for per-partition success. The `run_id` field
cross-references the two when you investigate a failure.

## Timezone

**v1 is UTC-only.** The `time_column` is interpreted as UTC and `--latest`
resolves to "the partition containing `chrono::Utc::now()`". If your data
lives in a non-UTC timezone, convert in your model SQL or accept UTC
partition boundaries. A `timezone` field on the strategy config is a v2
candidate.

## JSON output

After a `time_interval` run, `rocky run --output json` includes per-partition
materialization entries and a per-model summary:

```json
{
  "materializations": [
    {
      "asset_key": ["warehouse", "marts", "fct_daily_orders"],
      "duration_ms": 1234,
      "metadata": { "strategy": "time_interval", "watermark": null },
      "partition": {
        "key": "2026-04-07",
        "start": "2026-04-07T00:00:00Z",
        "end": "2026-04-08T00:00:00Z",
        "batched_with": []
      }
    }
  ],
  "partition_summaries": [
    {
      "model": "fct_daily_orders",
      "partitions_planned": 1,
      "partitions_succeeded": 1,
      "partitions_failed": 0
    }
  ]
}
```

The dagster-rocky integration reads this through the autogenerated Pydantic
`PartitionInfo` and `PartitionSummary` models. It translates the strategy
discriminator into a Dagster `DailyPartitionsDefinition`, or `Hourly`,
`Monthly`, or `TimeWindow` for year. See
[the dagster partitions guide](/dagster/partitions/) for the full mapping.

## Comparison with `incremental`

| Aspect | `[strategy] type = "incremental"` | `[strategy] type = "time_interval"` |
|---|---|---|
| State | Single watermark per table | Per-partition `PARTITIONS` records |
| Filter | `WHERE ts > MAX(ts_in_target)` | `WHERE ts >= @start_date AND ts < @end_date` |
| Late data | Missed (watermark already past) | Picked up on partition re-run |
| Idempotent re-run | No (depends on watermark state) | Yes (DELETE+INSERT) |
| Backfill | `full_refresh` only | `--from / --to` walks the range |
| Per-partition observability | No | Yes (`PartitionInfo` in JSON) |

## Limitations (v1)

The following are deferred:

- **Rocky DSL placeholder syntax** — `@start_date` / `@end_date` are
  recognized in `.sql` files only. The `.rocky` parser will gain `@var`
  syntax in v1.1.
- **Bootstrap on first run** — the target table must exist before the
  first partition runs. The runtime currently emits `DELETE` against the
  target, which fails if the table is missing. Either pre-create the
  table once (recommended for now) or `full_refresh` an empty version
  via a one-time migration. Bootstrap-on-first-run is a planned follow-up.
- **BigQuery and Postgres adapters** — these adapters don't exist yet.
  When they ship, BigQuery will use `MERGE ... WHEN NOT MATCHED BY SOURCE`
  and Postgres will route via parent table + child partition truncate.
- **Sub-day granularities** below `hour` — belongs in streaming systems.
- **Multi-column partitions** — single time column only in v1.
- **Partition column transformations** — `time_column` must be a real
  output column, not an expression.
- **`--batch-size` CLI flag** — TOML-only in v1; promote to CLI when
  there's demand.

## See also

- Partition-checksum POC — runnable end-to-end demo of `time_interval`
  with late-arriving data correction
  (`examples/playground/pocs/02-performance/03-partition-checksum/`)
- [Dagster partitions guide](/dagster/partitions/) — how the dagster-rocky
  integration translates `time_interval` into Dagster `PartitionsDefinition`
- `rocky run --help` — full CLI reference for the seven new flags
