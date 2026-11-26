---
title: Incremental Processing
description: How Rocky reprocesses only what changed, using watermarks, per-partition state, and the skip-unchanged gate.
sidebar:
  order: 10
---

How Rocky decides what "changed" means: watermarks, per-partition state, and the skip-unchanged gate.

## Materialization strategies

Every model declares a materialization strategy. It decides the SQL Rocky generates. [SQL Generation](/concepts/sql-generation/#transformation-sql) lists every strategy and its statement, and [Model Format](/reference/model-format/) has the configuration.

| Strategy | Use case |
|---|---|
| `full_refresh` | Small tables, schema changes, initial loads |
| `incremental` | Source tables with a reliable timestamp. Replication keeps the watermark in the state store. A transformation model reads it from its target with `@incremental_filter` |
| `merge` | Mutable data with a unique key |
| `time_interval` | Time-series data with partition-level reprocessing (`microbatch` is its hourly alias) |
| `delete_insert` | Partition-replace when MERGE overhead is not needed |
| `view` | An intermediate other models read, with no copied data |

## Watermark-based incremental

This is the default incremental strategy for replication. A **watermark** is the timestamp of the newest row Rocky has already loaded (see the [glossary](/reference/glossary/)). Rocky keeps one per table and reads only rows newer than it.

The strategy needs a timestamp column whose values only ever increase, typically `_fivetran_synced`.

### How Rocky advances the watermark

Each run copies the rows above the watermark and below the newest source timestamp. Before the copy, Rocky reads `MAX(<timestamp_column>)` over the source once. The rows at that maximum, the newest batch, wait for a later run.

```
 source rows, by their _fivetran_synced timestamp
   r1     r2     r3     r4       r5     r6       r7     r8
  08:00  08:30  09:10  09:45    10:20  10:55    11:30  11:55
 ───┴──────┴──────┴──────┴────────┴──────┴────────┴──────┴──────►

  run 1 at 09:50       run 2 at 11:00         run 3 at 12:00
  no watermark yet     ts > 09:10             ts > 10:20
  ts < 09:45           AND ts < 10:55         AND ts < 11:55
  copies r1..r3        copies r4, r5          copies r6, r7
  holds back r4        holds back r6          holds back r8
        │                     │                      │
        ▼                     ▼                      ▼
  watermark = 09:10    watermark = 10:20      watermark = 11:30

  Rocky flushes watermarks after copying. A committed copy whose
  watermark flush fails leaves recovery intent for the next attempt.
```

Rocky keys the watermark by the fully qualified table name (`catalog.schema.table`). After the copy, it stores `MAX(<timestamp_column>)` over the target, the newest row it loaded.

Rocky holds back the newest batch because a sync can land in parts. If run 1 copied the first rows stamped `09:45`, the watermark would become `09:45`, and the strict `>` would skip the rest forever. Held back, the whole batch lands on run 2.

The first run has no watermark. It copies every row below the newest source timestamp, plus the rows whose timestamp is NULL, and establishes the baseline. A model with a lakehouse `format` (`delta_table` / `iceberg_table`) gets that shape on this first materialization, as do `delete_insert`, `microbatch` and `time_interval`. See [Lakehouse formats](/reference/model-format/).

Every later run filters on the stored watermark and the newest source timestamp. [SQL Generation](/concepts/sql-generation/#incremental-copy) shows the statement. `rocky plan` shows it without the second bound, because Rocky reads the newest source timestamp only when the copy runs.

### What the hold-back changes

The hold-back applies to `strategy = "incremental"` on a replication pipeline. `microbatch`, `merge` and transformation models do not use it. What it changes:

- **The newest batch lands one run later.** This includes the first run, and a rebuild after schema drift.
- **A source that stops changing keeps its last batch.** The batch lands when a row with a newer timestamp arrives. A run that copied nothing says so in its materialization note: `N rows held back until newer data arrives`.
- **`row_count` counts the source rows below the cutoff,** plus the rows whose timestamp is NULL. Those are the rows the copy could land, so a real mismatch still fails the check.
- **NULL timestamps behave as before.** A full copy keeps them. An incremental copy skips them, because `ts > watermark` is not true for NULL.
- **`freshness` and `row_count_anomaly` lag one run.** Both read the target, which does not have the newest batch yet.
- **Each incremental table costs one to three more queries per run:** the source `MAX`, a count of the held-back rows for the note, and a source count for `row_count` when that check is on. This matters on a warehouse that bills per query or per byte scanned, such as BigQuery.
- **An empty source, or one whose timestamps are all NULL, has no maximum.** Rocky copies it as before, with no bound.

### Configuration

Replication strategy and watermark column live on the pipeline:

```toml
[pipeline.bronze]
type = "replication"
strategy = "incremental"
timestamp_column = "_fivetran_synced"
```

The timestamp column must exist in the source table, and its values must only increase. If the source system backfills history with old timestamps, a watermark run misses those rows. Rocky does not detect that case today. Run a full refresh to pick them up.

The filter is a strict `>`. The hold-back above keeps a late row with the same timestamp as the newest batch from being skipped. A row that arrives with a timestamp at or below the stored watermark, after a newer batch already landed, is still skipped. Replication has no `lookback` setting for this case. A transformation model can set `lookback`; the compiler warns (`W056`) when it does not.

### Recovering an interrupted replication

Rocky records recovery intent before it copies: the source, target, timestamp column and prior watermark. Support depends on the adapter:

| Adapters | Recovery |
|---|---|
| DuckDB, Databricks, Snowflake, BigQuery, PostgreSQL, SQL Server | Yes. Each INSERT commits atomically. Fabric Warehouse uses the SQL Server adapter, so it recovers too, but no live test covers it. Snowflake needs a pinned database or catalog: an unpinned session-default namespace warns and disables recovery. |
| Redshift, ClickHouse, Spark, Trino, process adapters | No. After an interrupted copy, the next run can append the same rows again. |

```text
record intent -> publish remote intent -> copy -> capture target MAX
                                                  |
                               flush watermarks + confirm intent

unconfirmed intent -> read target MAX -> repair watermark -> next copy
```

- An immediate retry reconciles its target before it appends again. A fresh run reconciles earlier unconfirmed runs before it copies or prunes unchanged tables.
- Changing a filter or renaming a pipeline cannot hide an older run. Rocky also reconciles an unconfirmed run of another pipeline on the same target endpoint when this run plans every target it wrote.
- Rocky commits recovered watermarks and their confirmation together. History cleanup keeps unresolved recovery records until confirmation.
- A missing watermark still selects full refresh. A missing target clears its watermark so Rocky recreates it. A failed recovery query stops the replay.
- Rocky refuses recovery when it cannot verify an older source or timestamp contract. Run the affected tables with `strategy = "full_refresh"` without a resume flag, then restore the incremental strategy. Keep the state file: it holds the evidence Rocky needs to explain the interrupted run.
- Keep `timestamp_column` configured for the source you restore, so returning to incremental mode cannot inherit a cursor ahead of the source.
- Checkpoints from Rocky 1.75.0 and earlier carry no recovery records. Recovery ignores them. If one crashed after its INSERT committed, the next incremental append can copy those rows again. Repair the cursor with [`rocky state reconcile-watermark`](/reference/commands/administration/#rocky-state-reconcile-watermark) first.

Remote backends publish intent before INSERT using their upload policy. With `on_upload_failure = "skip"`, Rocky warns and continues after an unavailable upload. Recovery then depends on the local ledger, and losing it removes the fresh-pod recovery guarantee. Set `on_upload_failure = "fail"` when remote durability is required. Governed runs require durable publication too.

Rules for writers:

- Serialize runs that append to the same targets. State compare-and-swap detects ledger conflicts. It does not lock warehouse tables.
- Upgrade every writer first. Older binaries do not reconcile the recovery descriptors.
- After an ambiguous transport failure, wait for earlier warehouse statements to finish, or cancel them, before you retry.

`--resume` refuses a checkpoint whose planned tables all copied but whose run record is missing. See [Recovering replication checkpoints](/reference/commands/core-pipeline/#recovering-replication-checkpoints).

## Merge strategy

Use the merge strategy for data whose rows change after they are first written. It matches rows on a unique key and updates them in place:

```toml
[strategy]
type = "merge"
unique_key = ["customer_id"]
update_columns = ["name", "email", "updated_at"]
```

The SQL is in [SQL Generation](/concepts/sql-generation/).

If `update_columns` is omitted, all columns are updated on match.

A merge does not drop columns. When the model's SQL stops selecting a column, the column stays in the table. Rocky prints a warning that names the column, and the same text is in the model's `notes`. Rocky does not refuse the run. On DuckDB, the MERGE succeeds and new rows hold `NULL` there. Other warehouses are not verified. Databricks (`INSERT *`) and BigQuery (`INSERT ROW`) may refuse the MERGE when the model lacks a column of the target. A column added to the table by hand, outside the model, also triggers the warning. While a stale column stays in the table, each run of the model adds one zero-row query of the model to confirm it. `--full-refresh` does not rebuild a merge model, so it does not remove the column. To keep the column, select it again. To remove it, drop it from the table in the warehouse (`ALTER TABLE ... DROP COLUMN`).

## Skipping unchanged models

The strategies above decide *how* a model rebuilds. The `--skip-unchanged` gate decides *whether* a transformation model rebuilds at all. It skips a model only when both its logic and its upstream data look unchanged since its last successful build.

Treat it as a cost saving, not a promise. It does **not** guarantee that two runs produce identical rows. The gate is **default-off**. Turn it on with `--skip-unchanged` or `[run] skip_unchanged = true`.

Every ambiguous input resolves to *build*. A flaky freshness probe, a model with no prior successful build, and an unparseable SQL body all rebuild. Non-deterministic SQL, lineage that is not provably complete, and the `content_addressed` and `time_interval` strategies are never skip-eligible. A `full_refresh` model is eligible. `--force-rebuild` always rebuilds. The two conditions, the full ineligible list and the `[skip]` overrides are in [Skip Unchanged Models and Defer to Prod](/guides/skip-and-defer/).

## Time-interval processing

The `time_interval` strategy processes one partition at a time and records per-partition state. See [Time Interval Materialization](/concepts/time-interval/).

## Full refresh fallback

Rocky falls back to a full refresh in two cases:

- **Schema drift.** On a type mismatch the drift detector triggers `DropAndRecreate`, because an insert with an incompatible type would fail. See [Schema Drift](/concepts/schema-drift/).
- **Missing watermark.** The table is new, the state backend was wiped, or the table was renamed. Rocky treats the run as a first run and sets a new baseline watermark.

## State store

Rocky keeps watermarks, check and run history, per-partition records and DAG snapshots in an embedded [redb](https://github.com/cberner/redb) store, scoped per environment. See [State Management](/concepts/state-management/).
