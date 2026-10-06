---
title: ClickHouse adapter
description: Connect Rocky to ClickHouse (Beta) — fields, TLS, table engines and sort keys, which strategies run, and what Rocky refuses
sidebar:
  order: 7
---

The ClickHouse adapter is **Beta**. It talks to the server's HTTP interface. It is tested live against ClickHouse 26.10, and it needs ClickHouse 23.6 or later. Rocky logs a warning when you register it.

ClickHouse has no transactions and no `MERGE` statement. Rocky runs the strategies it can run safely and refuses the rest at compile time. The table in [Strategies](#strategies) shows which is which.

## Fields

The adapter reads the shared `[adapter]` fields:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `host` | string | Yes | Server host name. Add a port as `host:port`. The default port is `8123`, or `8443` with `secure = true`. |
| `database` | string | No | The session's default database: where an unqualified table name in model SQL resolves. Default `default`. |
| `username` | string | No | User. Default `default`. |
| `password` | string | No | Password. |
| `timeout_secs` | integer | No | Per-statement timeout, also sent to the server as `max_execution_time`. Default `300`. |

Adapter-specific keys go in `[adapter.NAME.extra]`. Rocky refuses a key it does not know, so a typo fails loudly:

| Key | Default | Description |
|-----|---------|-------------|
| `port` | from `host`, else `8123` / `8443` | HTTP interface port. Wins over a port in `host`. |
| `secure` | `false` | Use HTTPS. Rocky always checks the certificate. |
| `ca_cert` | unset | Path to a PEM file of extra root certificates, for a private CA. Needs `secure = true`. |

```toml
[adapter.ch]
type = "clickhouse"
host = "${CLICKHOUSE_HOST}"
username = "${CLICKHOUSE_USER}"
password = "${CLICKHOUSE_PASSWORD}"

[adapter.ch.extra]
secure = true
```

`rocky validate` runs the same parse the adapter runs, so a bad field or an unknown `extra` key shows up there first.

## Authentication and TLS

Rocky sends the user and password in the `X-ClickHouse-User` and `X-ClickHouse-Key` headers. It never puts them in the URL, which proxies and server logs record.

With `secure = true`, Rocky connects over HTTPS and checks the certificate chain and the host name. It trusts the Mozilla root set plus `ca_cert`. There is no mode that skips the check.

## Names: databases, not catalogs

ClickHouse names a table `database.table`. A Rocky **schema** is a ClickHouse **database**, and there is no catalog level. Set the model's `catalog` to the empty string:

```toml
# models/_defaults.toml
[target]
catalog = ""         # ClickHouse has no catalog level
schema = "marts"     # the ClickHouse database
```

A non-empty `catalog` is refused. Rocky does not ignore it, because two models that differ only by catalog would then write the same table. With `auto_create_schemas = true`, Rocky runs `CREATE DATABASE IF NOT EXISTS`.

ClickHouse names are case-sensitive: `orders` and `Orders` are two tables. Rocky writes every name without quotes. A name must start with a letter or `_`.

## Strategies

This table lists every strategy and how Rocky runs it on ClickHouse:

| Strategy | On ClickHouse |
|----------|---------------|
| `full_refresh` | `CREATE OR REPLACE TABLE … AS`. One statement builds the new table and swaps it in atomically. If the `SELECT` fails, the old table stays. |
| `view` | `CREATE OR REPLACE VIEW`. |
| `incremental` (append) | The model with `@incremental_filter` resolved, appended with `INSERT INTO … SELECT`. |
| `delete_insert` | A lightweight `DELETE` of the incoming keys, then an `INSERT`. Not atomic, see below. |
| `time_interval` | The window is written to a staging table, deleted from the target, and copied in. Not atomic, see below. |
| `merge`, `incremental` with `unique_key` | Refused, `E053`. ClickHouse has no `MERGE`. |
| Snapshot models | Refused, `E049`. They need `MERGE`. |
| `materialized_view` | Refused. A ClickHouse materialized view is an insert trigger over new rows. It is not a refreshed query result. |
| User-defined functions | Refused, `E051`. |

Rocky reports `E053` at compile time when a pipeline that loads the model targets ClickHouse. Another adapter in `rocky.toml` that no pipeline targets does not change this. A model that no pipeline loads is judged against every pipeline's target.

### Why Rocky refuses `merge`

ClickHouse cannot update a row by key in one statement. A `ReplacingMergeTree` table removes duplicate keys only during background merges. Until a merge runs, a reader sees both the old and the new row. Rocky does not emulate `merge` with it, because the result would be wrong for a while after every run.

Use one of these instead:

- `delete_insert` with `partition_by = ["<key>"]` to replace rows by key.
- `incremental` without `unique_key` to append.
- `full_refresh` to rebuild.

### What "not atomic" means

ClickHouse commits each statement on its own. `delete_insert` and `time_interval` need two or more statements, so a failure can stop between them. Rocky orders the statements so that the common failure, a broken `SELECT`, stops before anything is deleted:

```
time_interval, one window:

  CREATE TABLE stage AS target          empty copy: same columns, same engine
  INSERT INTO stage <model SELECT>      a failing SELECT stops here; target untouched
  DELETE FROM target WHERE <window>     lightweight DELETE
  INSERT INTO target SELECT * FROM stage
  DROP TABLE stage
```

`delete_insert` runs its `DELETE` with the model's `SELECT` as a subquery, so a broken `SELECT` also fails before the delete.

Each window gets its own staging table, named `<table>__rocky_stage_<hash>`, because `rocky run` writes several windows at once. If a run stops before the last step, the staging table stays until that window runs again.

A failure after the `DELETE` leaves the window, or the keys, empty. Run the model again to repair it. Readers can see the window empty for the moment between the `DELETE` and the `INSERT`.

### Time windows on a `Date` column

Rocky compares the window with `toDateTime('…')`, so the window works on a `Date`, `DateTime` or `DateTime64` column. In your own SQL, `@start_date` and `@end_date` are strings like `'2026-01-01 00:00:00'`. ClickHouse refuses to compare that string with a `Date` column. Wrap the placeholder for a `Date` column:

```sql
SELECT order_id, order_date, amount
FROM raw.orders
WHERE order_date >= toDate(@start_date)
  AND order_date < toDate(@end_date)
```

## Table engine, partitions and sort key

A model sidecar can set the ClickHouse table attributes in a `[clickhouse]` block:

```toml
name = "fct_orders"

[strategy]
type = "full_refresh"

[clickhouse]
engine       = "MergeTree"               # a MergeTree-family name, no parameters
order_by     = ["customer_id", "order_date"]
partition_by = "toYYYYMM(order_date)"    # a column, or fn(column)
```

Rocky renders them between the table name and `AS`:

```sql
CREATE OR REPLACE TABLE marts.fct_orders
  ENGINE = MergeTree PARTITION BY toYYYYMM(order_date) ORDER BY (customer_id, order_date) AS …
```

Without the block, a table is `ENGINE = MergeTree ORDER BY tuple()`: no sort key and no partitions.

The attributes apply when Rocky creates the table: on every `full_refresh`, and on the first run of `incremental`, `delete_insert` and `time_interval`. Changing them later does not change an existing table. Run the model once with `rocky run --full-refresh` to rebuild it.

An `order_by` column must not be `Nullable`. ClickHouse refuses a nullable sort key unless the server allows it. Use `coalesce` or `assumeNotNull` in the model to make the column non-nullable.

`rocky compile` checks the block:

| Code | When |
|------|------|
| `E053` | The `engine` is not a MergeTree-family name, or has parameters. An `order_by` entry is not a column name. `partition_by` is not a column or `fn(column)`. The block is on a `view`, `materialized_view`, `dynamic_table`, `content_addressed` or `ephemeral` model, or next to a lakehouse `format`. |
| `W053` | An `order_by` or `partition_by` column is not in the model's output. Rocky checks this only when it knows the full output. |

Another adapter refuses a model that sets `[clickhouse]`, at SQL generation. It never drops the attributes.

## Session settings

Rocky sends these settings with every statement. They make ClickHouse behave the way the rest of Rocky expects:

| Setting | Why |
|---------|-----|
| `session_timezone = 'UTC'` | Time windows and watermarks convert in UTC, whatever the server's time zone. Needs ClickHouse 23.6 or later. |
| `date_time_output_format = 'iso'` | Timestamps come back as `2026-01-02T03:04:05Z`, the shape Rocky reads a watermark from. |
| `join_use_nulls = 1` | An outer join fills unmatched columns with `NULL`, as standard SQL does and as Rocky's type checker assumes. Without it, ClickHouse fills them with `0` or `''`. |
| `wait_end_of_query = 1` | An error arrives as an HTTP error, never after a partial result. |
| `mutations_sync = 2` | A mutation returns after it is applied. |

`join_use_nulls = 1` changes results for SQL that relied on ClickHouse's default of filling unmatched columns with default values. Wrap such a column in `coalesce(col, 0)` to keep the old result.

## Types

Rocky reads column types from `system.columns`. It peels `Nullable(…)` into the column's nullability and drops `LowCardinality(…)`, which is a storage encoding. It reports each type with a name that is valid ClickHouse DDL and that Rocky's type checker reads:

| ClickHouse | Reported as | Rocky type |
|------------|-------------|------------|
| `Int8`, `Int16`, `Int32`, `Int64` | `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT` | integer |
| `Float32`, `Float64` | `REAL`, `DOUBLE` | float |
| `Bool` | `BOOLEAN` | boolean |
| `String` | `TEXT` | string |
| `Date` | `DATE` | date |
| `DateTime`, `DateTime('tz')` | `TIMESTAMP` | timestamp |
| `DateTime64(p[, 'tz'])` | `DateTime64(p)` | timestamp |
| `Decimal(p, s)` | `DECIMAL(p,s)` | decimal |

Other types keep their ClickHouse name and are unknown to the type checker: unsigned and 128/256-bit integers, `FixedString`, `Date32`, `UUID`, `Enum`, `Array`, `Map`, `Tuple` and `JSON`. An unknown type is never an error. A contract on such a column reports "not checked".

A column added by drift (or by `on_schema_change = "append_new_columns"`) is added as `Nullable(T)`, so rows that predate it read `NULL`, not `''` or `0`. Types ClickHouse cannot wrap in `Nullable` (`Array`, `Map`, `Tuple`, `Variant`, …) are added as they are.

`MAX` over no rows returns the type's default in ClickHouse (`1970-01-01 00:00:00` for a `DateTime`). Freshness checks and watermark reads use `maxOrNull`, so an empty table reads as empty, not as fresh in 1970.

When a source column's type changes, Rocky rebuilds the target table. It does not run `ALTER TABLE … MODIFY COLUMN`, because the reported type has no `Nullable` wrapper and the change could drop a column's nullability.

## Compile-time checks

`rocky compile` has no ClickHouse-specific operand rules for `E042` / `E043`. On a ClickHouse project it uses the conservative default: an operand is refused only when every warehouse Rocky knows refuses it.

## Not supported yet

- `merge`, snapshots and materialized views (see [Strategies](#strategies)).
- Engine parameters, such as `ReplacingMergeTree(version)`, and `PRIMARY KEY`, `SAMPLE BY`, `TTL` or `SETTINGS` clauses.
- Governance (grants, tags), checksum-bisection `rocky compare`, and ClickHouse as a discovery source.
- The native TCP protocol. Rocky uses HTTP only.

## See also

- [`[adapter.NAME]`](/reference/configuration/#adaptername) — fields shared by every adapter type
- [Compiler diagnostics](/concepts/compiler/) — `E053`, `W053`, `E049`, `E051`
