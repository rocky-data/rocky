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
| `FullRefresh` | `CREATE OR REPLACE TABLE ... AS SELECT ...` | Small tables, schema changes, initial loads |
| `Incremental` | `INSERT INTO ... SELECT ... WHERE ts > watermark` | Append-only data with a reliable timestamp |
| `Merge` | `MERGE INTO ... USING ... ON key WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT` | Mutable data with a unique key |

## Watermark-based incremental

The default incremental strategy for replication. It relies on a monotonically increasing timestamp column (typically `_fivetran_synced`) to determine which rows are new.

### Lifecycle

1. **Read watermark.** Rocky reads the last stored watermark for the table from the state store. The watermark is keyed by the fully qualified table name (`catalog.schema.table`).

2. **No watermark (first run).** If no watermark exists, Rocky performs a full refresh -- it copies all rows from the source. This establishes the baseline.

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

- `orders_summary` does not depend on `status` -- skip
- `orders_audit` depends on `status` -- recompute

This is a `PropagationDecision`: either `Recompute` or `Skip { reason }`. The skip reason is logged so you can verify the decision.

Column propagation works with any incremental strategy. It adds a layer of intelligence on top of watermarks or checksums by pruning the DAG execution to only what actually needs to change.

## Full refresh fallback

Rocky falls back to full refresh in two situations:

### Schema drift

When the schema drift detector (in `rocky-core/drift.rs`) finds a type mismatch between source and target columns, it triggers `DropAndRecreate`. The target table is dropped and rebuilt from scratch. This is necessary because inserting rows with incompatible types would fail at the warehouse level.

```
Source: orders.amount (DECIMAL(10,2))
Target: orders.amount (STRING)
→ Schema drift detected → DROP TABLE → Full refresh
```

### Deleted watermark

Removing a table's watermark from the state store causes Rocky to treat it as a first run:

```bash
rocky state delete --table "warehouse.staging.orders"
```

The next run performs a full refresh for that table, establishing a new baseline watermark.

## State store

Watermarks and partition checksums are stored in an embedded key-value store backed by [redb](https://github.com/cberner/redb). See the [State Management](/concepts/state-management) page for details on the state store, remote persistence backends, and the state lifecycle.

The state store tracks:

- **Watermarks** -- last successfully replicated timestamp per table
- **Check history** -- historical row counts for anomaly detection
- **Run history** -- metadata about previous runs
- **Partition checksums** -- per-partition hashes for checksum-based incremental
- **DAG snapshots** -- previous DAG structure for change detection

All state is scoped per environment. Dev, staging, and prod maintain independent state with no cross-environment coordination.
