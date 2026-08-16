---
title: Incremental Processing
description: How Rocky reprocesses only what changed, using watermarks, partition checksums, and column-level change propagation.
sidebar:
  order: 10
---

Rocky reprocesses only what changed. This page covers the mechanisms it uses to decide what "changed" means: watermarks, partition checksums, column-level propagation, and the skip-unchanged gate.

## Materialization strategies

Every model, replication or transformation, declares a materialization strategy. The strategy decides the SQL Rocky generates:

| Strategy | Behavior | Use case |
|----------|----------|----------|
| `full_refresh` | `CREATE OR REPLACE TABLE ... AS SELECT ...` | Small tables, schema changes, initial loads |
| `incremental` | `INSERT INTO ... SELECT ... WHERE ts > watermark` | Append-only data with a reliable timestamp |
| `merge` | `MERGE INTO ... USING ... ON key WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT` | Mutable data with a unique key |
| `time_interval` | Per-partition `INSERT OVERWRITE` with `@start_date`/`@end_date` placeholders | Time-series data with partition-level reprocessing |
| `microbatch` | `time_interval` alias with hourly defaults | dbt-compatible partition processing |
| `ephemeral` | No table; inlined as CTE in downstream models | Lightweight intermediate transformations |
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

  Each watermark is written to the state store only after the copy
  succeeds. A failed copy leaves the previous watermark in place.
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

The timestamp column must exist in the source table, and its values must only increase. If the source system backfills history with old timestamps, a watermark run misses those rows. Partition checksums, below, catch that case.

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

A watermark only finds appended rows. Partition checksums find changes to rows that are already there. The `incremental` module of `rocky-core` implements them.

### How Rocky compares partition checksums

1. Each partition of a model, for example one partition per date, gets a checksum: a hash of the partition contents and its row count.
2. On the next run, Rocky compares the current checksums against the stored ones.
3. Rocky reprocesses only the partitions whose checksum changed. It skips the rest entirely.

```
Previous run:  { "2026-03-28": 0xABCD, "2026-03-29": 0x1234 }
Current run:   { "2026-03-28": 0xABCD, "2026-03-29": 0x5678, "2026-03-30": 0x9999 }

Result:        Changed: ["2026-03-29", "2026-03-30"]
               Unchanged: ["2026-03-28"]
```

This catches what watermarks miss: backfills, late-arriving corrections, and retroactive edits to historical data.

## Column-level change propagation

The compiler's semantic graph (see [The Rocky Compiler](/concepts/compiler/)) tracks column-level lineage across the whole DAG. Lineage is the map of which columns feed which. Rocky reads it to skip downstream models that do not depend on any changed column.

### Example

Consider three models:

```
orders (source) → orders_summary (uses: amount, customer_id)
                → orders_audit   (uses: status, updated_at)
```

If an upstream schema change only affects the `status` column, Rocky determines:

- `orders_summary` does not depend on `status`, so it is skipped
- `orders_audit` depends on `status`, so it is recomputed

This is a `PropagationDecision`: either `Recompute` or `Skip { reason }`. Rocky logs the skip reason so you can check the decision.

Column propagation works with any incremental strategy. It runs on top of watermarks or checksums and prunes the DAG to what actually needs to change.

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
- **Partition checksums:** per-partition hashes for checksum-based incremental
- **DAG snapshots:** previous DAG structure for change detection

All state is scoped per environment. Dev, staging, and prod maintain independent state with no cross-environment coordination.
