---
title: Content-Addressed Materialization
description: Write Parquet files named by the hash of their own bytes, so several query engines can read the same table.
sidebar:
  order: 16
---

A model with `[strategy] type = "content_addressed"` writes its SELECT result to an object-store prefix you control. It writes **Parquet files named by the hash of their own bytes, plus a Delta log commit**. That is what [content-addressed](/reference/glossary/#content-addressed) means: the name comes from the contents, not from a timestamp or counter. Any engine that reads Iceberg or Delta (DuckDB `iceberg_scan`, Trino, Spark) reads the files directly, not through Rocky. The writer supports partitioned tables, post-`ALTER` schema evolution and rowTracking (a Delta feature that gives every row a stable ID).

## When to use it

Use it when Rocky should own the *writer* and not the readers:

- **Several query engines read one table**, so you avoid routing every read through one warehouse.
- **You commit into a managed Delta or Iceberg catalog**: Unity Catalog managed tables with UniForm (a Delta feature that also publishes Iceberg metadata), Iceberg REST catalogs.
- **You want stable, de-duplicatable file names.** The same logical batch hashes to the same name, which helps replay, audit and storage de-dup.

Stay on `full_refresh` or `merge` with a single warehouse, or when the runner has no direct object-store access.

## How a write happens

```
  model SQL ─▶ Arrow result set ─▶ Parquet bytes ─▶ blake3 hash of the bytes names the file
    ─▶ files uploaded under storage_prefix (e.g. s3://bucket/path/<table>/)
    ─▶ one replace commit in _delta_log: remove the old files, add the new ones
    ─▶ sync_iceberg_metadata(): Iceberg-compatible readers see the new snapshot
```

The writer's `discover()` step reads the bootstrap Delta commit for the table's schema, partition spec and rowTracking configuration. Rocky honors the Delta protocol features the table declares. Later writes adapt to schema changes applied between runs, such as an added column or a widened type.

## How each run replaces the table

Each run replaces the table: readers see that run's rows only. Rocky makes one Delta commit per run. It removes every live file not in the new output and adds every new file not yet live.

```
  run 1 ─▶ v1: add A                live = {A}
  run 2 ─▶ v2: remove A, add B      live = {B}
  run 3 (same output as run 2) ─▶ no commit, live = {B}
```

- A partitioned model also makes one commit, so readers never see half a run.
- A file with the same content keeps its name. Rocky neither removes nor re-adds it.
- An unchanged output writes no commit. The run records the current table version as its output version.

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

Rocky reads only the JSON commits in `_delta_log`, not Delta checkpoints (a Parquet log summary another engine writes, for example Databricks). With a checkpoint, Rocky cannot see every live file, so a replace could leave old rows visible. When `_delta_log` holds `_last_checkpoint` or a `*.checkpoint.parquet` file, the write refuses and the error names the table and the checkpoint. Tables only Rocky writes never get one.

To recover: drop the table, create it again on an empty `storage_prefix`, and run the model again. Rocky writes the whole output in one commit.

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

In a partitioned table the Delta log keys `partitionValues` by physical UUID (column-mapping mode). The writer handles it. You declare logical names only.

## Constraints and things to know

- **UniForm and deletion vectors cannot both be on.** A deletion vector records deleted rows in a side file. The writer errors when the target has them enabled. Use one or the other.
- **A rowTracking writer needs `baseRowId`.** Every Delta `add` action on a rowTracking table carries `baseRowId` and `defaultRowCommitVersion`. Rocky assigns both.
- **A replication table cannot use this strategy.** It is a *transformation* strategy. A replication pipeline target pointed at such a model gets a "not supported on replication tables" error when `rocky run` or `rocky apply` runs, not at `rocky validate`.
- **No DuckDB POC.** The strategy needs real Delta plus object storage. See the end-to-end test in `engine/crates/rocky-cli/src/commands/run_content_addressed.rs`.

## Related

- [Model Format](/reference/model-format/#content-addressed) — the sidecar field reference, including the full Strategy Examples block.
- [Silver Layer](/concepts/silver-layer/) — where content-addressed models sit in the lakehouse mental model.
- [The Architecture of Trust](/concepts/architecture-of-trust/) — the recipe-identity hashes stamped on every materialization, and what replay can and cannot verify.
