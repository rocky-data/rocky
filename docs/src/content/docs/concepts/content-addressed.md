---
title: Content-Addressed Materialization
description: Cross-engine Delta + UniForm writes with content-hashed Parquet files
sidebar:
  order: 16
---

`materialization = "content_addressed"` is a write strategy that lands the model's SELECT result as **content-addressed Parquet files plus a Delta log commit** under an object-store prefix you control. Cross-engine readers (DuckDB `iceberg_scan`, Trino, Spark, anything that reads Iceberg or Delta) read the same files directly without going through Rocky.

Shipped end-to-end in engine v1.30.0 across Phases 1–5 of the `rocky-iceberg` writer: scaffold, bootstrap-commit discovery, `write_batch()`, `sync_iceberg_metadata()`, partitioned tables, rowTracking, and post-ALTER schema evolution.

## When to use it

Use content-addressed materialization when you want Rocky to own the *writer* but explicitly **not** own the readers. Typical fits:

- **Multiple query engines read the same table.** DuckDB analysts, Trino dashboards, and Spark batch jobs all need to read the marts your pipeline writes. Pointing each engine at object storage avoids round-tripping every read through one warehouse.
- **You're committing into a managed Delta or Iceberg catalog.** Unity Catalog managed tables with UniForm exposed, Iceberg REST catalogs, etc.
- **You want stable, deduplicatable file names.** Content-hashing the Parquet bytes means the same logical batch produces the same physical file name, which is useful for replay, audit, and storage de-dup against an external lake.

Stick with `full_refresh` / `incremental` / `merge` when there's a single warehouse, or when you don't have direct object-store access from the runner.

## How it works

On each run, the runtime:

1. Executes the model SQL against the configured adapter and pulls the result into Arrow.
2. blake3-hashes the Parquet-encoded bytes to derive each file's name.
3. Uploads files under `storage_prefix` (e.g. `s3://bucket/path/<table>/`).
4. Emits a Delta log commit referencing the new files. Existing Delta protocol features (partitioning, rowTracking) are honored when the underlying table declares them.
5. Calls `sync_iceberg_metadata()` so Iceberg-compatible readers see the new snapshot.

The writer's `discover()` step reads the bootstrap Delta commit to pick up the table's schema, partition spec, and rowTracking configuration. Subsequent writes adapt to schema changes (added columns, type widening) applied to the underlying Delta table between runs. That's Phase 5 schema evolution.

## Configuration

A content-addressed sidecar carries the strategy plus `storage_prefix` and an optional `partition_columns` list:

```toml
# models/fct_events.toml
name = "fct_events"

[strategy]
type = "content_addressed"
storage_prefix = "s3://${ROCKY_BUCKET}/marts/fct_events"
partition_columns = ["event_date"]

[target]
catalog = "analytics"
schema  = "marts"
table   = "fct_events"
```

| Field | Required | Description |
|---|---|---|
| `storage_prefix` | Yes | Object-store key prefix that holds `_delta_log/` + Parquet files for the target table. The runtime requires write access to this prefix. Env-var substitution applies (see [Environment Variables](/reference/configuration/#environment-variables)). |
| `partition_columns` | No | Logical partition column names. Empty for unpartitioned tables. The runtime asserts this matches the table's declared partition columns at materialization time. |

For partitioned tables, `partitionValues` in the Delta log are keyed by physical UUID (column-mapping mode), not the logical column name. The writer handles this internally; you only declare the logical names.

## Constraints + things to know

- **UniForm and Deletion Vectors are mutually exclusive.** The writer surfaces a clear error if the target table has DVs enabled. Use one or the other.
- **rowTracking writers need `baseRowId`.** The Phase 3 writer surface for rowTracking carries `baseRowId` + `rowCommitVersion`; Rocky handles assignment.
- **Replication tables don't accept this strategy.** Content-addressed is a *transformation* strategy. Pointing a replication pipeline target at a content-addressed model returns a "not supported on replication tables" error at validate time.
- **No DuckDB POC yet.** The strategy requires real Delta + object storage; it's exercised via live-verify tests against a sandbox rather than a playground POC. See `engine/crates/rocky-cli/src/commands/run_content_addressed.rs` for the e2e test if you want the reference invocation.

## Related

- [Model Format](/reference/model-format/#content-addressed) — the sidecar field reference, including the full Strategy Examples block.
- [Silver Layer](/concepts/silver-layer/) — where content-addressed models sit in the lakehouse mental model.
- [Adapters](/concepts/adapters/) — adapter contracts for the writer side of the contract.
