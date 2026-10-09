---
title: Apache Spark adapter
description: Connect Rocky to Apache Spark over Spark Connect (Beta) — fields, table formats, which strategies run, and what Rocky refuses
sidebar:
  order: 8
---

The Spark adapter is **Beta**. It talks to a Spark Connect server over gRPC, so Rocky needs no JVM. It is tested live against Spark 4.0.1 with Delta Lake 4.0.0. Rocky logs a warning when you register it.

Use it for an open-source Spark cluster. For Databricks, use the [Databricks adapter](/reference/adapters/databricks/): it adds Unity Catalog governance and Databricks-only SQL.

## Requirements

- A Spark Connect server (`sbin/start-connect-server.sh`). Spark 4.0 is tested. Spark 3.5 is not tested, and its parser has no `* EXCEPT`, which quarantine uses.
- Delta Lake (the default) or Apache Iceberg set up in the session catalog. Rocky's tables need `MERGE`, `DELETE` and `CREATE OR REPLACE TABLE`, which Spark's built-in Parquet tables do not support.

A local server with Delta Lake:

```bash
docker run -d --rm -p 15002:15002 -e SPARK_NO_DAEMONIZE=1 apache/spark:4.0.1 \
  /opt/spark/sbin/start-connect-server.sh \
  --packages io.delta:delta-spark_2.13:4.0.0 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.connect.grpc.binding.address=0.0.0.0
```

## Fields

The adapter reads the shared `[adapter]` fields:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `host` | string | Yes | `host`, `host:port` or `sc://host[:port]`. The default port is `15002`. Connection-string parameters (`sc://host/;token=…`) are refused: use the fields below. |
| `token` | string | No | Bearer token, sent as `authorization: Bearer <token>`. Setting it turns TLS on. |
| `username` | string | No | The Spark Connect user id, shown in the Spark UI. Default `rocky`. |
| `timeout_secs` | integer | No | Per-statement timeout. Default `300`. When it expires, the server may still be running the statement. |

Adapter-specific keys go in `[adapter.NAME.extra]`. Rocky refuses a key it does not know, so a typo fails loudly:

| Key | Default | Description |
|-----|---------|-------------|
| `port` | from `host`, else `15002` | Spark Connect port. Wins over a port in `host`. |
| `use_ssl` | `true` with a `token`, else `false` | Use TLS. Rocky checks the certificate against the Mozilla root set. `use_ssl = false` with a `token` is refused, so a token is never sent in clear text. |
| `table_format` | `"delta"` | `"delta"` or `"iceberg"`: the format of the tables Rocky creates. |

```toml
[adapter.spark]
type = "spark"
host = "sc://${SPARK_CONNECT_HOST}:15002"
token = "${SPARK_CONNECT_TOKEN}"

[adapter.spark.extra]
table_format = "delta"
```

`rocky validate` runs the same parse the adapter runs, so a bad field or an unknown `extra` key shows up there first.

## Names and table format

Spark names a table `catalog.schema.table`. The session catalog is `spark_catalog`; another catalog is whatever the server configures as `spark.sql.catalog.<name>`. Rocky cannot create a catalog, so leave `auto_create_catalogs` off. `auto_create_schemas = true` runs `CREATE SCHEMA IF NOT EXISTS catalog.schema`.

Before its first statement, Rocky runs `SET spark.sql.sources.default = delta` (or `iceberg`) in its session. So every `CREATE TABLE` Rocky renders makes a table of that format. That includes models, snapshots, seeds and branch copies.

Identifiers are quoted with backticks. String literals use Spark's default backslash escapes: Rocky writes `\'` and `\\`.

## Strategies

| Strategy | SQL | Notes |
|----------|-----|-------|
| `full_refresh` | `CREATE OR REPLACE TABLE … AS` | One atomic statement. |
| `view` | `CREATE OR REPLACE VIEW … AS` | A switch between view and table needs `drop_existing_kind`. |
| `incremental` | `INSERT INTO … SELECT` | Watermark filter `WHERE ts > TIMESTAMP '…'`. |
| `merge` | `MERGE INTO … USING (…) ON … WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *` | Explicit `update_columns` render `UPDATE SET t.c = s.c`. |
| `delete_insert` | `MERGE … WHEN MATCHED THEN DELETE`, then `INSERT INTO` | Open-source Delta Lake refuses a subquery in a `DELETE`, so the delete is a `MERGE`. Two statements, not atomic. |
| `time_interval` | Delta: `INSERT INTO … REPLACE WHERE <window>`. Iceberg: `DELETE` then `INSERT`. | Delta is one atomic commit. The Iceberg form is two statements, not atomic. |
| `snapshot` | The generic SCD2 SQL | Runs with `hard_deletes = "ignore"`. `hard_deletes = "invalidate"` or `"new_record"` fails at run time with `DELTA_UNSUPPORTED_SUBQUERY`: its `UPDATE … WHERE NOT EXISTS (…)` is refused by open-source Delta Lake. |

Schema drift never alters a column type in place on Spark. A type change rebuilds the table with a full refresh.

## Not supported

- `materialized_view`: open-source Spark has no materialized views. Rocky refuses it.
- Lakehouse `format = …` DDL and Delta maintenance (`rocky compact`, `rocky archive`): not verified on open-source Spark, so Rocky refuses them.
- User-defined functions (E051), governance (grants, tags, masking policies) and Spark as a discovery source.
- Reattachable executions: a statement runs on one gRPC stream. A dropped connection fails the statement, and Rocky does not retry it, because it may have run.

## Testing

`cargo test -p rocky-spark --features spark-conformance` runs the live harness in `engine/crates/rocky-spark/tests/conformance.rs` against the server above. It proves the string-literal round trip, every strategy's SQL, `DESCRIBE TABLE`, the view/table probe, Arrow fetch and the checksum query.

## See also

- [Databricks adapter](/reference/adapters/databricks/) — Spark SQL on Databricks, with Unity Catalog
- [`[adapter.NAME]`](/reference/configuration/#adaptername) — fields shared by every adapter type
