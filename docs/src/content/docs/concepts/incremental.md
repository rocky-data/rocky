---
title: Incremental Processing
description: Watermarks, partition checksums, and change propagation
sidebar:
  order: 10
---

Rocky supports multiple incremental processing strategies to avoid reprocessing data that has not changed. The simplest is watermark-based append, where only rows newer than a stored timestamp are copied. More advanced strategies include partition-level checksums and column-level change propagation.

## Materialization strategies

Every model (replication or transformation) declares a materialization strategy:

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

The default incremental strategy for replication. It relies on a monotonically increasing timestamp column (typically `_fivetran_synced`) to determine which rows are new.

### Lifecycle

1. **Read watermark.** Rocky reads the last stored watermark for the table from the state store. The watermark is keyed by the fully qualified table name (`catalog.schema.table`).

2. **No watermark (first run).** If no watermark exists, Rocky performs a full refresh and copies all rows from the source. This establishes the baseline.

3. **Watermark exists.** Rocky generates an incremental query that filters to rows newer than the watermark:

```sql
SELECT *, CAST(NULL AS STRING) AS _loaded_by
FROM source_catalog.source_schema.orders
WHERE _fivetran_synced > TIMESTAMP '2025-03-15T14:30:00Z'
```

4. **Update watermark.** After a successful copy, Rocky writes the new watermark (the maximum timestamp seen in this batch) to the state store. The next run picks up from this point.

### Configuration

Replication strategy and watermark column live on the pipeline:

```toml
[pipeline.bronze]
type = "replication"
strategy = "incremental"
timestamp_column = "_fivetran_synced"
```

The timestamp column must exist in the source table and contain monotonically increasing values. If the source system backfills historical data with old timestamps, those rows will be missed by incremental processing.

## Merge strategy

For mutable data where rows can be updated after initial insert, the merge strategy uses a unique key to upsert:

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

Beyond watermark-based incremental (which only handles appends), Rocky supports partition-level checksums to detect changes in existing rows. This is implemented in the `incremental` module of `rocky-core`.

### How it works

1. Each partition of a model (e.g., one partition per date) gets a checksum computed from its data (a hash of the partition contents and row count).
2. On subsequent runs, Rocky compares current checksums against stored checksums from the previous run.
3. Only partitions with changed checksums are reprocessed. Unchanged partitions are skipped entirely.

```
Previous run:  { "2026-03-28": 0xABCD, "2026-03-29": 0x1234 }
Current run:   { "2026-03-28": 0xABCD, "2026-03-29": 0x5678, "2026-03-30": 0x9999 }

Result:        Changed: ["2026-03-29", "2026-03-30"]
               Unchanged: ["2026-03-28"]
```

This catches scenarios that watermarks miss: backfills, late-arriving corrections, and retroactive updates to historical data.

## Column-level change propagation

The compiler's semantic graph (see [The Rocky Compiler](/concepts/compiler)) tracks column-level lineage across the entire DAG. Rocky uses this lineage to skip downstream models that do not depend on any changed columns.

### Example

Consider three models:

```
orders (source) → orders_summary (uses: amount, customer_id)
                → orders_audit   (uses: status, updated_at)
```

If an upstream schema change only affects the `status` column, Rocky determines:

- `orders_summary` does not depend on `status`, so it is skipped
- `orders_audit` depends on `status`, so it is recomputed

This is a `PropagationDecision`: either `Recompute` or `Skip { reason }`. The skip reason is logged so you can verify the decision.

Column propagation works with any incremental strategy. It adds a layer of intelligence on top of watermarks or checksums by pruning the DAG execution to only what actually needs to change.

## Skipping unchanged models

The strategies above decide *how* a model rebuilds. The opt-in `--skip-unchanged` gate decides *whether* a transformation model needs to rebuild at all: it skips re-materializing a model when both its logic and its upstream data look unchanged since the model's last successful build. It is a best-effort cost-saving optimization, **not** a guarantee that two runs produce identical rows.

The gate is **default-off** — a plain `rocky run` is byte-identical to one without it. Turn it on per invocation with `--skip-unchanged`, or project-wide with `[run] skip_unchanged = true`.

### The two gates: logic and data

A model is skipped only when **both** of these hold (skipping on logic alone — ignoring whether upstream data changed — is precisely the staleness bug the gate exists to prevent):

- **B2 — logic unchanged.** The model's cosmetic-invariant logic key (a hash of its normalised SQL plus typed structural facts) matches the one recorded on the prior successful build. Reformatting the SQL is not a change; altering what it computes is.
- **B3 — upstream data unchanged.** Every upstream is provably stable: an upstream Rocky model that was *skipped* this run (its output is unchanged by definition), or a raw source whose `MAX(<timestamp>)` (and, behind the `skip_rowcount_fallback` opt-in, `COUNT(*)`) matches the signature recorded on the prior build.

### Fail-safe to *build*

A wrong skip is silent production staleness — the worst failure a transformation engine can have. So the gate has exactly one code path that yields a skip, and every missing, unreadable, or ambiguous input resolves the other way: **build**. A flaky freshness probe, a model with no prior successful build, an unparseable SQL body — all force a rebuild rather than risk a wrong skip. `--force-rebuild` always rebuilds.

### Models that are never skip-eligible

Eligibility is a conservative static check. These always rebuild:

- **Non-deterministic SQL** — any model calling a volatile builtin (`CURRENT_TIMESTAMP`, `NOW`, `RANDOM`, `UUID`, `CURRENT_USER`, `CURRENT_CATALOG`, …) or any function not on Rocky's pure-function allowlist. The order/tie-break-unstable aggregates `ANY_VALUE`, `ARRAY_AGG`, `COLLECT_LIST`, `COLLECT_SET`, and `MODE` are deliberately excluded too — without a `WITHIN GROUP (ORDER BY …)` their output can differ run to run.
- **Models whose lineage isn't provably complete** — anything beyond a single plain `SELECT` over bare tables. CTEs, sub-queries in `FROM`, `PIVOT` / `UNNEST` / nested-join table factors, `IN (SELECT …)` / `EXISTS` / scalar sub-selects, and set operations (`UNION` / `INTERSECT` / `EXCEPT`) could read an upstream the freshness walk never examined, so the model rebuilds.
- **`content_addressed` and `time_interval` strategies** — these use the content-addressed and per-partition paths, not the skip gate. A `full_refresh` model **is** eligible.

A model owner can override the automatic decision per model with a `[skip]` sidecar block (`eligible` / `deterministic`). The full how-to — flags, the `[run]` knobs, and the `[skip]` overrides — is in [Skip Unchanged Models and Defer to Prod](/guides/skip-and-defer/).

## Time-interval processing

For time-series data that requires partition-level control, the `time_interval` strategy processes data in discrete time partitions. The model SQL uses `@start_date` and `@end_date` placeholders:

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

### How it works

1. Rocky determines which partitions to process based on CLI flags (`--partition`, `--from/--to`, `--latest`, `--missing`, `--lookback`).
2. For each partition, it substitutes `@start_date` and `@end_date` with quoted timestamp literals.
3. The generated SQL uses `INSERT OVERWRITE` semantics (atomic on Databricks via Delta, multi-statement transaction on Snowflake).
4. Per-partition state is tracked in the state store for gap discovery (`--missing`).

### Per-warehouse SQL

- **Databricks**: `INSERT INTO <target> REPLACE WHERE <filter> <select>` (single atomic statement via Delta)
- **Snowflake**: `BEGIN; DELETE FROM <target> WHERE <filter>; INSERT INTO <target> <select>; COMMIT;` (4 statements)
- **DuckDB**: Same shape as Snowflake

### CLI flags

> Note: the canonical, auditable form is `rocky plan` followed by `rocky apply <plan-id>`. Every partition-selection flag below is accepted on both `rocky plan` and the `rocky run` single-step alias, which fuses plan + apply into one invocation for local iteration and automation.

```bash
rocky plan --partition 2026-04-01 && rocky apply <plan-id>          # Process one partition
rocky plan --from 2026-03-01 --to 2026-04-01 && rocky apply <plan-id>  # Date range
rocky plan --latest && rocky apply <plan-id>                        # Most recent partition
rocky plan --missing && rocky apply <plan-id>                       # Discover and fill gaps
rocky plan --lookback 7 && rocky apply <plan-id>                    # Reprocess last N partitions
rocky plan --parallel 4 && rocky apply <plan-id>                    # Parallelize partitions
```

## Full refresh fallback

Rocky falls back to full refresh in two situations:

### Schema drift

When the schema drift detector (in `rocky-core/drift.rs`) finds a type mismatch between source and target columns, it triggers `DropAndRecreate`. The target table is dropped and rebuilt from scratch. This is necessary because inserting rows with incompatible types would fail at the warehouse level.

```
Source: orders.amount (DECIMAL(10,2))
Target: orders.amount (STRING)
→ Schema drift detected → DROP TABLE → Full refresh
```

### Missing watermark

If the state store has no watermark for a table (the table is new, the state backend was wiped, or the table was renamed), Rocky treats the next run as a first run and performs a full refresh, establishing a new baseline watermark.

## State store

Watermarks and partition checksums are stored in an embedded key-value store backed by [redb](https://github.com/cberner/redb). See the [State Management](/concepts/state-management) page for details on the state store, remote persistence backends, and the state lifecycle.

The state store tracks:

- **Watermarks:** last successfully replicated timestamp per table
- **Check history:** historical row counts for anomaly detection
- **Run history:** metadata about previous runs
- **Partition checksums:** per-partition hashes for checksum-based incremental
- **DAG snapshots:** previous DAG structure for change detection

All state is scoped per environment. Dev, staging, and prod maintain independent state with no cross-environment coordination.
