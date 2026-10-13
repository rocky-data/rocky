---
title: Incremental Processing
description: How Rocky reprocesses only what changed, using watermarks, per-partition state, and the skip-unchanged gate.
sidebar:
  order: 10
---

Rocky reprocesses only what changed. This page covers how it decides what "changed" means: watermarks, per-partition state, and the skip-unchanged gate. It also marks two building blocks that `rocky run` does not use yet: partition checksums and column-level propagation.

## Materialization strategies

Every model, replication or transformation, declares a materialization strategy. The strategy decides the SQL Rocky generates:

| Strategy | Behavior | Use case |
|----------|----------|----------|
| `full_refresh` | `CREATE OR REPLACE TABLE ... AS SELECT ...` | Small tables, schema changes, initial loads |
| `incremental` | `INSERT INTO ... SELECT ... WHERE ts > watermark` (or `MERGE` with a `unique_key`). Replication keeps the watermark in the state store. A transformation model reads it from its target with `@incremental_filter` | Source tables with a reliable timestamp |
| `merge` | `MERGE INTO ... USING ... ON key WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT` | Mutable data with a unique key |
| `time_interval` | Per-partition `INSERT OVERWRITE` with `@start_date`/`@end_date` placeholders | Time-series data with partition-level reprocessing |
| `microbatch` | `time_interval` alias with hourly defaults | dbt-compatible partition processing |
| `view` | `CREATE OR REPLACE VIEW ... AS SELECT ...` | An intermediate other models read, with no copied data |
| `delete_insert` | `DELETE WHERE partition_key IN (...); INSERT ...` | Partition-replace when MERGE overhead isn't needed |

See [Model Format](/reference/model-format/) for the full configuration of each strategy.

## Watermark-based incremental

This is the default incremental strategy for replication. A **watermark** is the timestamp of the newest row Rocky has already loaded (see the [glossary](/reference/glossary/)). Rocky keeps one per table and reads only rows newer than it.

The strategy needs a timestamp column whose values only ever increase, typically `_fivetran_synced`.

### How Rocky advances the watermark

```
 source rows, by their _fivetran_synced timestamp
   r1     r2     r3     r4       r5     r6       r7     r8
  08:00  08:30  09:10  09:45    10:20  10:55    11:30  11:55
 ───┴──────┴──────┴──────┴────────┴──────┴────────┴──────┴──────►

  run 1 at 09:50      run 2 at 11:00      run 3 at 12:00
  no watermark yet    WHERE ts > 09:45    WHERE ts > 10:55
  full refresh:       copies r5, r6       copies r7, r8
  copies r1..r4
        │                    │                    │
        ▼                    ▼                    ▼
  watermark = 09:45    watermark = 10:55    watermark = 11:55

  Rocky flushes watermarks after copying. A committed copy whose
  watermark flush fails leaves recovery intent for the next attempt.
```

Rocky keys the watermark by the fully qualified table name (`catalog.schema.table`), and stores the maximum timestamp it saw in the batch.

The first run has no watermark, so it copies every row and establishes the baseline. When the model declares a lakehouse `format` (`delta_table` / `iceberg_table`) with `[format_options]`, that format is applied on this first materialization. The baseline table is created in the requested Delta or Iceberg shape rather than as a plain table. The same holds for the `delete_insert`, `microbatch`, and `time_interval` strategies. See [Lakehouse formats](/reference/model-format/) in the model format reference.

Every later run filters on the stored watermark:

```sql
SELECT *, CAST(NULL AS STRING) AS _loaded_by
FROM source_catalog.source_schema.orders
WHERE _fivetran_synced > TIMESTAMP '2025-03-15T14:30:00Z'
```

### Configuration

Replication strategy and watermark column live on the pipeline:

```toml
[pipeline.bronze]
type = "replication"
strategy = "incremental"
timestamp_column = "_fivetran_synced"
```

The timestamp column must exist in the source table, and its values must only increase. If the source system backfills history with old timestamps, a watermark run misses those rows. Rocky does not detect that case today. Run a full refresh to pick them up.

### Recovering an interrupted replication

For DuckDB, Databricks, Snowflake, and BigQuery, Rocky records recovery intent
before copying. It records the source, target, timestamp column, and prior
watermark. These adapters commit each INSERT atomically.
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

## Partition-level checksums

:::caution[Not wired into runs]
`rocky run` does not compare partition checksums today. The `incremental` module of `rocky-core` holds the building blocks (`diff_checksums`, `generate_checksum_sql`). No run path calls them, and the state store keeps no checksums. Only `rocky compact --measure-dedup` computes table checksums.
:::

A watermark only finds appended rows. Partition checksums are designed to find changes to rows that are already there.

### How the checksum comparison works

1. Each partition of a model, for example one partition per date, gets a checksum: a hash of the partition contents and its row count.
2. On the next run, Rocky compares the current checksums against the stored ones.
3. Rocky reprocesses only the partitions whose checksum changed. It skips the rest entirely.

```
Previous run:  { "2026-03-28": 0xABCD, "2026-03-29": 0x1234 }
Current run:   { "2026-03-28": 0xABCD, "2026-03-29": 0x5678, "2026-03-30": 0x9999 }

Result:        Changed: ["2026-03-29", "2026-03-30"]
               Unchanged: ["2026-03-28"]
```

This would catch what watermarks miss: backfills, late-arriving corrections, and retroactive edits to historical data.

## Column-level change propagation

:::caution[Not wired into runs]
`rocky run` does not skip models by column-level propagation today. `compute_propagation` in `rocky-core` holds the logic, but no run path calls it. To skip unchanged models, use the [skip-unchanged gate](#skipping-unchanged-models).
:::

The compiler's semantic graph (see [The Rocky Compiler](/concepts/compiler/)) tracks column-level lineage across the whole DAG. Lineage is the map of which columns feed which. The propagation logic reads it to find downstream models that do not depend on any changed column.

### Example

Consider three models:

```
orders (source) → orders_summary (uses: amount, customer_id)
                → orders_audit   (uses: status, updated_at)
```

If an upstream schema change only affects the `status` column, the logic decides:

- `orders_summary` does not depend on `status`, so it is skipped
- `orders_audit` depends on `status`, so it is recomputed

This is a `PropagationDecision`: either `Recompute` or `Skip { reason }`.

## Skipping unchanged models

The strategies above decide *how* a model rebuilds. The `--skip-unchanged` gate decides *whether* a transformation model rebuilds at all. It skips re-materializing a model when both its logic and its upstream data look unchanged since the model's last successful build.

Treat it as a cost saving, not a promise. It does **not** guarantee that two runs produce identical rows.

The gate is **default-off**. A plain `rocky run` behaves exactly as it did before the gate existed. Turn it on per invocation with `--skip-unchanged`, or project-wide with `[run] skip_unchanged = true`.

### The two conditions: logic and data

Rocky skips a model only when **both** conditions hold. Skipping on logic alone, while upstream data has moved, is the staleness bug the gate exists to prevent.

- **B2 — logic unchanged.** The model's logic key matches the one recorded on the prior successful build. The key is a hash of the normalised SQL plus typed structural facts, so reformatting the SQL is not a change. Altering what it computes is.
- **B3 — upstream data unchanged.** Every upstream is provably stable. That means an upstream Rocky model that was *skipped* this run, whose output is unchanged by definition. Or it means a raw source whose `MAX(<timestamp>)` matches the signature recorded on the prior build. Behind the `skip_rowcount_fallback` opt-in, `COUNT(*)` counts too.

### Every ambiguous input resolves to *build*

A wrong skip is silent production staleness, the worst failure a transformation engine can have. So exactly one code path yields a skip, and everything else resolves the other way: **build**. A flaky freshness probe rebuilds. A model with no prior successful build rebuilds. An unparseable SQL body rebuilds. `--force-rebuild` always rebuilds.

### Models that are never skip-eligible

Eligibility is a conservative static check. These always rebuild:

- **Non-deterministic SQL** — any model calling a volatile builtin (`CURRENT_TIMESTAMP`, `NOW`, `RANDOM`, `UUID`, `CURRENT_USER`, `CURRENT_CATALOG`, …) or any function not on Rocky's pure-function allowlist. The aggregates `ANY_VALUE`, `ARRAY_AGG`, `COLLECT_LIST`, `COLLECT_SET`, and `MODE` are excluded too. Without a `WITHIN GROUP (ORDER BY …)` their output can differ run to run.
- **Models whose lineage isn't provably complete** — anything beyond a single plain `SELECT` over bare tables. That covers CTEs, sub-queries in `FROM`, and `PIVOT` / `UNNEST` / nested-join table factors. It also covers `IN (SELECT …)` / `EXISTS` / scalar sub-selects, and set operations (`UNION` / `INTERSECT` / `EXCEPT`). Each could read an upstream the freshness walk never examined, so the model rebuilds.
- **`content_addressed` and `time_interval` strategies** — these use the content-addressed and per-partition paths, not the skip gate. A `full_refresh` model **is** eligible.

A model owner can override the automatic decision per model with a `[skip]` sidecar block (`eligible` / `deterministic`). For the flags, the `[run]` knobs, and the `[skip]` overrides, see [Skip Unchanged Models and Defer to Prod](/guides/skip-and-defer/).

## Time-interval processing

The `time_interval` strategy processes time-series data one partition at a time. The model SQL uses `@start_date` and `@end_date` placeholders:

```sql
SELECT event_date, event_type, COUNT(*) AS event_count
FROM events.page_views
WHERE event_date >= @start_date AND event_date < @end_date
GROUP BY event_date, event_type
```

```toml
[strategy]
type = "time_interval"
time_column = "event_date"
granularity = "day"
lookback = 3
```

### How Rocky processes one partition

1. Rocky decides which partitions to process from the CLI flags (`--partition`, `--from/--to`, `--latest`, `--missing`, `--lookback`).
2. For each partition, it replaces `@start_date` and `@end_date` with quoted timestamp literals.
3. The generated SQL uses `INSERT OVERWRITE` semantics. That is one atomic statement on Databricks via Delta, and a multi-statement transaction on Snowflake.
4. Rocky tracks per-partition state in the state store, which is what makes gap discovery (`--missing`) possible.

### Per-warehouse SQL

- **Databricks**: `INSERT INTO <target> REPLACE WHERE <filter> <select>` (single atomic statement via Delta)
- **Snowflake**: `BEGIN; DELETE FROM <target> WHERE <filter>; INSERT INTO <target> <select>; COMMIT;` (4 statements)
- **DuckDB**: Same shape as Snowflake

### CLI flags

> Note: the canonical, auditable form is `rocky plan` followed by `rocky apply <plan-id>`. Every partition-selection flag below is accepted on both `rocky plan` and the `rocky run` single-step alias. `rocky run` fuses plan and apply into one invocation, for local iteration and automation.

```bash
rocky plan --partition 2026-04-01 && rocky apply <plan-id>          # Process one partition
rocky plan --from 2026-03-01 --to 2026-04-01 && rocky apply <plan-id>  # Date range
rocky plan --latest && rocky apply <plan-id>                        # Most recent partition
rocky plan --missing && rocky apply <plan-id>                       # Discover and fill gaps
rocky plan --lookback 7 && rocky apply <plan-id>                    # Reprocess last N partitions
rocky plan --parallel 4 && rocky apply <plan-id>                    # Parallelize partitions
```

## Full refresh fallback

Rocky falls back to a full refresh in two situations.

### Schema drift

The schema drift detector (in `rocky-core/drift.rs`) compares source and target column types. On a type mismatch it triggers `DropAndRecreate`: Rocky drops the target table and rebuilds it from scratch. It has to, because inserting rows with an incompatible type would fail at the warehouse.

```
Source: orders.amount (DECIMAL(10,2))
Target: orders.amount (STRING)
→ Schema drift detected → DROP TABLE → Full refresh
```

### Missing watermark

The state store may hold no watermark for a table. The table is new, the state backend was wiped, or the table was renamed. Rocky then treats the next run as a first run: a full refresh that establishes a new baseline watermark.

## State store

Rocky keeps watermarks and partition checksums in an embedded key-value store backed by [redb](https://github.com/cberner/redb). For the remote persistence backends and the state lifecycle, see [State Management](/concepts/state-management/).

The state store tracks:

- **Watermarks:** last successfully replicated timestamp per table
- **Check history:** historical row counts for anomaly detection
- **Run history:** metadata about previous runs
- **Partition records:** per-partition state for `time_interval` models, which `--missing` reads
- **DAG snapshots:** previous DAG structure for change detection

All state is scoped per environment. Dev, staging, and prod maintain independent state with no cross-environment coordination.
