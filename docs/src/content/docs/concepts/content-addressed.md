---
title: Content-Addressed Materialization
description: Write Parquet files named by the hash of their own bytes, so several query engines can read the same table.
sidebar:
  order: 16
---

`materialization = "content_addressed"` writes a model's SELECT result to an
object-store prefix you control. It writes **Parquet files named by the hash of
their own bytes, plus a Delta log commit**. That naming is what
[content-addressed](/reference/glossary/#content-addressed) means: the file's name
comes from its contents, not from a timestamp or a counter.

Any engine that reads Iceberg or Delta reads those files directly. DuckDB
`iceberg_scan`, Trino, and Spark do not go through Rocky.

Shipped end to end in engine v1.30.0. That includes partitioned tables,
post-`ALTER` schema evolution, and rowTracking, a Delta feature that gives every
row a stable ID.

## When to use it

Use content-addressed materialization when you want Rocky to own the *writer* and
explicitly **not** own the readers. Three cases fit:

- **Several query engines read one table.** DuckDB analysts, Trino dashboards, and Spark batch jobs all read the marts your pipeline writes. Pointing each engine at object storage avoids routing every read through one warehouse.
- **You commit into a managed Delta or Iceberg catalog.** Unity Catalog managed tables with UniForm exposed, Iceberg REST catalogs, and the like. UniForm is a Delta feature that also publishes Iceberg metadata, so an Iceberg reader can read the Delta table.
- **You want stable, de-duplicatable file names.** The same logical batch hashes to the same file name, which helps replay, audit, and storage de-dup against an external lake.

Stay on `full_refresh`, `incremental`, or `merge` when you have a single
warehouse, or when the runner has no direct object-store access.

## How a write happens

```
  model SQL
      │  execute against the configured adapter
      ▼
  Arrow result set
      │  encode as Parquet
      ▼
  Parquet bytes
      │  blake3 hash of those bytes derives the file name
      ▼
  files uploaded under storage_prefix, e.g. s3://bucket/path/<table>/
      │  one commit referencing the new files
      ▼
  _delta_log commit
      │  sync_iceberg_metadata()
      ▼
  Iceberg-compatible readers see the new snapshot
```

Rocky honors the Delta protocol features the underlying table already declares,
such as partitioning and rowTracking.

The writer's `discover()` step reads the bootstrap Delta commit. That is where it
picks up the table's schema, its partition spec, and its rowTracking
configuration. Later writes adapt to schema changes applied to the underlying
Delta table between runs, such as an added column or a widened type.

## Configuration

A content-addressed sidecar carries the strategy, a `storage_prefix`, and an
optional `partition_columns` list:

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

In a partitioned table, the `partitionValues` in the Delta log are keyed by
physical UUID, not by the logical column name. That is column-mapping mode. The
writer handles it for you. You declare the logical names only.

## Constraints and things to know

- **UniForm and deletion vectors cannot both be on.** A deletion vector is a Delta feature that records deleted rows in a side file rather than rewriting the Parquet. The writer returns a clear error when the target table has them enabled. Use one feature or the other.
- **A rowTracking writer needs `baseRowId`.** Every Delta `add` action on a rowTracking table carries `baseRowId` and `defaultRowCommitVersion`. Rocky assigns both.
- **A replication table cannot use this strategy.** Content-addressed is a *transformation* strategy. Point a replication pipeline target at a content-addressed model and you get a "not supported on replication tables" error when the pipeline runs (`rocky run` or `rocky apply`), not at `rocky validate` time.
- **No DuckDB POC yet.** The strategy needs real Delta plus object storage, so it is exercised by live-verify tests against a sandbox rather than by a playground POC. For the reference invocation, read the end-to-end test in `engine/crates/rocky-cli/src/commands/run_content_addressed.rs`.

## Related

- [Model Format](/reference/model-format/#content-addressed) — the sidecar field reference, including the full Strategy Examples block.
- [Silver Layer](/concepts/silver-layer/) — where content-addressed models sit in the lakehouse mental model.
- [Adapters](/concepts/adapters/) — the adapter contracts on the writer side.
- [The Architecture of Trust](/concepts/architecture-of-trust/) — the recipe-identity hashes stamped on every materialization, and what replay can and cannot verify.
