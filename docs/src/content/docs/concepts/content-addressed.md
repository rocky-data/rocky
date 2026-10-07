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

Stay on `full_refresh` or `merge` when you have a single
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
      │  one replace commit: remove the old files, add the new ones
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

## How each run replaces the table

Each run replaces the table. After a run, readers see that run's rows only.
Rows from earlier runs are not visible.

Rocky makes one Delta commit per run. The commit removes every live file that
is not in the new output. It adds every new file that is not live yet.

```
  run 1 ─▶ v1: add A                live = {A}
  run 2 ─▶ v2: remove A, add B      live = {B}
  run 3 (same output as run 2) ─▶ no commit, live = {B}
```

- **A partitioned model also makes one commit.** All partition groups land in
  that commit, so readers never see half a run.
- **An unchanged file stays.** A file with the same content keeps the same
  name. Rocky neither removes it nor adds it again.
- **An unchanged output writes no commit.** The run records the current table
  version as its output version.

Earlier versions stay in the Delta log. You can still read them with
`VERSION AS OF`. Their files are now eligible for your `VACUUM`. After a
`VACUUM`, the old versions are gone for good. `rocky gc` holds a replaced
file until `delta.deletedFileRetentionDuration` (default 7 days) has passed
since its removal.

A replace is not an append, so it affects streaming readers:

- **A Delta streaming reader fails on the first replace.** A `readStream` of
  the table stops at a commit that removes data. Set `skipChangeCommits` on
  the reader, or read the table as a batch.
- **Each run replays the whole Delta log.** Rocky reads every JSON commit to
  find the live files. The cost of that read grows with the number of commits.

### A Delta checkpoint makes the write refuse

Rocky reads only the JSON commits in `_delta_log`. It does not read Delta
checkpoints. A checkpoint is a Parquet summary of the log that another engine
writes, for example Databricks after its own commits. With a checkpoint, Rocky
cannot see every live file, so a replace could leave old rows visible.

So when `_delta_log` holds `_last_checkpoint` or a `*.checkpoint.parquet` file,
the write refuses. The error names the table and the checkpoint. Tables that
only Rocky writes never get a checkpoint.

To recover:

1. Drop the table.
2. Create it again on an empty `storage_prefix`.
3. Run the model again. Rocky writes the whole output in one commit.

### Other refusals

Rocky also refuses the write, and writes no commit, in these cases:

| Case | Why |
|---|---|
| `delta.appendOnly = true` | A replace must remove files. The error gives the `ALTER TABLE` that turns it off. |
| A Delta feature turned on after the table was created, such as deletion vectors, in-commit timestamps, change data feed, v2 checkpoints, clustering or `CHECK` constraints | The writer does not implement the feature, so a commit could corrupt the table. Rocky checks the latest protocol and table properties, not only the first commit. |
| Coordinated or catalog-managed commits (any file under `_delta_log/_staged_commits/`) | A direct commit would bypass the commit coordinator. |
| A Delta action Rocky does not know | Rocky cannot replay the log with confidence. |
| The schema, partitioning or protocol changed during the write | The prepared files no longer match the table. Run the model again. |

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
