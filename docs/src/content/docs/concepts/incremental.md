---
title: Incremental Processing
description: How Rocky reprocesses only what changed, using watermarks, per-partition state, and the skip-unchanged gate.
sidebar:
  order: 10
---

Rocky reprocesses only what changed. This page covers how it decides what "changed" means: watermarks, per-partition state, and the skip-unchanged gate.

## Materialization strategies

Every model declares a materialization strategy. It decides the SQL Rocky generates. [SQL Generation](/concepts/sql-generation/) shows the statements, [Architecture](/concepts/architecture/#materializationstrategy) lists every strategy, and [Model Format](/reference/model-format/) has the configuration.

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

Rocky holds back the newest batch because a sync can land in parts. Say a sync writes some rows stamped `09:45` before run 1 and the rest after it. If run 1 copied the first part, the watermark would become `09:45`, and the strict `>` would skip the rest forever. Held back, the whole `09:45` batch lands on run 2.

The first run has no watermark. It copies every row below the newest source timestamp, plus the rows whose timestamp is NULL, and establishes the baseline. When the model declares a lakehouse `format` (`delta_table` / `iceberg_table`) with `[format_options]`, that format is applied on this first materialization. The baseline table is created in the requested Delta or Iceberg shape rather than as a plain table. The same holds for the `delete_insert`, `microbatch`, and `time_interval` strategies. See [Lakehouse formats](/reference/model-format/) in the model format reference.

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

Each copy with a cutoff says how many rows it held back, in the `notes` of its materialization.

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

For DuckDB, Databricks, Snowflake, BigQuery, PostgreSQL, and SQL Server, Rocky
records recovery intent before copying. It records the source, target, timestamp
column, and prior watermark. These adapters commit each INSERT atomically.
Fabric Warehouse uses the SQL Server adapter, so it recovers too, but no
live test covers it.
Redshift, ClickHouse, Spark, Trino, and process adapters do not recover: after
an interrupted copy, the next run can append the same rows again.
Snowflake recovery requires a pinned database or catalog. Fresh runs with an
unpinned session-default namespace warn and disable recovery.

```text
record intent -> publish remote intent -> copy -> capture target MAX
                                                  |
                               flush watermarks + confirm intent

unconfirmed intent -> read target MAX -> repair watermark -> next copy
```

An immediate retry reconciles its target before appending again. A fresh run
reconciles earlier unconfirmed runs before copying or pruning unchanged tables.
Changing a filter cannot hide an older run. Renaming a pipeline cannot hide one
either: Rocky also reconciles an unconfirmed run of another pipeline on the same
target endpoint when this run plans every target it wrote. Rocky commits
recovered watermarks and their confirmation together.
History cleanup keeps new unresolved recovery records until confirmation, even
when they exceed the configured history age limit.

A missing watermark still selects full refresh. A missing target clears its
watermark so Rocky recreates it. A failed recovery query stops the replay.
Trino and process adapters do not use target-MAX recovery.

Rocky refuses recovery when it cannot verify an older source or timestamp
contract. Run the affected tables with `strategy = "full_refresh"` without a
resume flag, then restore their incremental strategy. Keep the state file;
deleting it also deletes the evidence Rocky needs to explain the interrupted run.

Checkpoints written by Rocky 1.75.0 and earlier carry no recovery records.
Recovery ignores them, so an upgrade does not change how those runs behave. If
one of them crashed after its INSERT committed, the next incremental append can
copy those rows again, as in earlier releases.

Keep `timestamp_column` configured for the source you are restoring. Recovery
replacements establish its target MAX before confirmation, so returning to
incremental mode cannot inherit a cursor ahead of the source.

Remote backends publish intent before INSERT using their configured upload
policy and concurrency checks. With `on_upload_failure = "skip"`, Rocky warns
and continues after an unavailable upload. Recovery then depends on preserving
the local ledger. Losing that ledger before a successful upload removes the
fresh-pod recovery guarantee. Set `on_upload_failure = "fail"` when remote
durability is required. Governed runs require durable publication too.

Serialize runs that append to the same targets. State compare-and-swap detects
ledger conflicts; it does not lock warehouse tables against concurrent writers.
Upgrade every writer before relying on this recovery protocol. Older binaries
do not reconcile the new recovery descriptors.
Wait for earlier warehouse statements to finish, or cancel them, before retrying
after an ambiguous transport failure.

### Resuming after every table copied

Rocky refuses `--resume` and `--resume-latest` when every planned table copied
but the terminal run record is missing. Skipping those tables would also skip
their post-copy checks and could report false success.

Follow the recovery route in the refusal. A matching fresh run does not prune targets
from a complete checkpoint without a run record. It copies those targets and
runs their checks, even when the source marker is unchanged. A later matching
run can use another pipeline name. Its target endpoint must match, and its plan
must contain every target in the checkpoint.

After that run finishes checks and writes its run record, Rocky marks the old
checkpoint superseded. The next run can prune unchanged targets. A recorded
check failure still completes this step.

With supported recovery records, the fresh run also re-derives watermarks from
the target before copying. An old checkpoint alone never fails a fresh run.

A checkpoint from Rocky 1.75.0 or earlier cannot show that its watermarks were
saved. Switch the affected tables to `strategy = "full_refresh"`, then run
`rocky run --pipeline <name> --no-prune` without a resume flag. That replaces
their data and runs the checks without appending the same rows twice. Keep the
full-refresh strategy until you repair the incremental cursor from the
replacement target:

```sh
rocky state reconcile-watermark --pipeline <name> --dry-run
rocky state reconcile-watermark --pipeline <name>
```

Repeat `--table catalog.schema.table` to select affected targets. The command
reads `MAX(timestamp_column)` from each target and saves it through the
configured state backend. For DuckDB, use `--table .schema.table`.
An empty target has its cursor cleared. The next incremental run replaces it.
Confirm that each target's effective `timestamp_column` still identifies the copied rows. Run the command only
after the replacement and any earlier warehouse writes have finished.
The command applies table-specific timestamp overrides.
It refuses a connector-specific override if the source connector cannot be identified.
Then restore `strategy = "incremental"`.

Rocky 1.75.0 and earlier did not record recovery descriptors. A crash after
an INSERT but before its watermark flush can leave a stale cursor. The first
run after upgrading can append those rows again. Repair the cursor from the
target before that run if the old flush is uncertain.

Other unsupported checkpoints require full refresh. Keep that strategy until
`rocky state reconcile-watermark --pipeline <name>` sets the replacement
target's cursor. Switching back to
incremental with a wall-clock refresh cursor can skip later source arrivals.
Incomplete crash checkpoints remain resumable.

## Merge strategy

Use the merge strategy for data whose rows change after they are first written. It matches rows on a unique key and updates them in place:

```toml
[strategy]
type = "merge"
unique_key = ["customer_id"]
update_columns = ["name", "email", "updated_at"]
```

This generates:

```sql
MERGE INTO target_catalog.target_schema.customers AS target
USING (SELECT ... FROM source WHERE ...) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE SET
    name = source.name,
    email = source.email,
    updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT *
```

If `update_columns` is omitted, all columns are updated on match.

A merge does not drop columns. When the model's SQL stops selecting a column, the column stays in the table. Rocky prints a warning that names the column, and the same text is in the model's `notes`. Rocky does not refuse the run. On DuckDB, the MERGE succeeds and new rows hold `NULL` there. Other warehouses are not verified. Databricks (`INSERT *`) and BigQuery (`INSERT ROW`) may refuse the MERGE when the model lacks a column of the target. A column added to the table by hand, outside the model, also triggers the warning. While a stale column stays in the table, each run of the model adds one zero-row query of the model to confirm it. `--full-refresh` does not rebuild a merge model, so it does not remove the column. To keep the column, select it again. To remove it, drop it from the table in the warehouse (`ALTER TABLE ... DROP COLUMN`).

## Skipping unchanged models

The strategies above decide *how* a model rebuilds. The `--skip-unchanged` gate decides *whether* a transformation model rebuilds at all. It skips a model only when both its logic and its upstream data look unchanged since its last successful build.

Treat it as a cost saving, not a promise. It does **not** guarantee that two runs produce identical rows. The gate is **default-off**. Turn it on with `--skip-unchanged` or `[run] skip_unchanged = true`.

Every ambiguous input resolves to *build*. A flaky freshness probe, a model with no prior successful build, and an unparseable SQL body all rebuild. Non-deterministic SQL, lineage that is not provably complete, and the `content_addressed` and `time_interval` strategies are never skip-eligible. A `full_refresh` model is eligible. `--force-rebuild` always rebuilds. The two conditions, the full ineligible list and the `[skip]` overrides are in [Skip Unchanged Models and Defer to Prod](/guides/skip-and-defer/).

## Time-interval processing

The `time_interval` strategy processes one partition at a time. The model SQL uses `@start_date` and `@end_date`, which Rocky replaces with quoted timestamp literals for each partition. Rocky records per-partition state, which makes `--missing` gap discovery possible. `rocky run` fuses plan and apply into one step. The partition flags (`--partition`, `--from/--to`, `--latest`, `--missing`, `--lookback`, `--parallel`) work on both. See [Time Interval Materialization](/concepts/time-interval/).

## Full refresh fallback

Rocky falls back to a full refresh in two cases:

- **Schema drift.** On a type mismatch the drift detector triggers `DropAndRecreate`, because an insert with an incompatible type would fail. See [Schema Drift](/concepts/schema-drift/).
- **Missing watermark.** The table is new, the state backend was wiped, or the table was renamed. Rocky treats the run as a first run and sets a new baseline watermark.

## State store

Rocky keeps watermarks, check and run history, per-partition records and DAG snapshots in an embedded [redb](https://github.com/cberner/redb) store, scoped per environment. See [State Management](/concepts/state-management/).
