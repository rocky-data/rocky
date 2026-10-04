---
title: Redshift adapter
description: Connect Rocky to Amazon Redshift (Beta) — fields, table distribution and sort keys, late-binding views, and what differs from PostgreSQL
sidebar:
  order: 6
---

The Redshift adapter is **Beta**. Rocky tests its SQL in unit tests only. There is no local Redshift to run it against, so no Redshift SQL runs in CI. Rocky logs a warning when you register it.

It shares a crate and a connection with the [PostgreSQL adapter](/reference/adapters/postgres/). Redshift speaks the PostgreSQL wire protocol. This page lists only what differs.

## Fields

The fields and `extra` keys are the PostgreSQL adapter's, with three differences:

- The default port is `5439`.
- `merge_mode = "on_conflict"` is refused. Redshift has no `INSERT … ON CONFLICT`.
- One extra key: `late_binding_views` (`true` / `false`, default `false`). See [Views](#views).

```toml
[adapter.rs]
type = "redshift"
host = "${REDSHIFT_HOST}"        # cluster or workgroup endpoint
database = "dev"
username = "${REDSHIFT_USER}"
password = "${REDSHIFT_PASSWORD}"

[adapter.rs.extra]
sslmode = "require"
late_binding_views = true
```

## Authentication

Rocky supports password auth. Use a database user, or temporary credentials you fetched yourself.

IAM auth, through `GetClusterCredentials` or the Serverless `GetCredentials` call, is not built in yet. It is planned as a follow-up.

## Table distribution and sort keys

A model sidecar can set the Redshift table attributes in a `[redshift]` block:

```toml
name = "fct_orders"

[strategy]
type = "merge"
unique_key = ["order_id"]

[redshift]
dist_style = "key"                      # auto | even | all | key
dist_key   = "customer_id"
sort_key   = ["order_date", "order_id"]
sort_style = "compound"                 # compound (default) | interleaved | auto
```

Rocky renders them between the table name and `AS` when it creates the table:

```sql
CREATE TABLE marts.fct_orders DISTSTYLE KEY DISTKEY (customer_id)
  COMPOUND SORTKEY (order_date, order_id) AS …
```

The attributes apply when Rocky creates the table: on every `full_refresh`, and on the first run of `merge`, `delete_insert` and `time_interval`.

`rocky compile` checks the block:

| Code | When |
|------|------|
| `E052` | An invalid column name. `dist_style = "key"` with no `dist_key`. `dist_key` with another `dist_style`. `sort_style = "auto"` with columns. More than 8 interleaved sort columns. A `[redshift]` block on a `view`, `materialized_view`, `dynamic_table`, `content_addressed` or `ephemeral` model, or next to a lakehouse `format`. |
| `W052` | `dist_key` or a `sort_key` column is not in the model's output. Rocky checks this only when it knows the full output. |

Another adapter refuses a model that sets `[redshift]`, at SQL generation. It never drops the attributes and builds the table without them.

## Views

A normal Redshift view locks the tables it reads, so a `full_refresh` of such a table fails. A late-binding view does not lock them. Set `late_binding_views = true` to render every view model as one:

```sql
CREATE OR REPLACE VIEW marts.v_orders AS
SELECT … FROM marts.fct_orders
WITH NO SCHEMA BINDING
```

Redshift needs every table in a late-binding view to name its schema: `marts.fct_orders`, not `fct_orders`.

## What differs from PostgreSQL

| Concern | Redshift |
|---------|----------|
| `MERGE` | `MERGE INTO <target> USING (<model SQL>) AS rocky_src ON <table>.<key> = rocky_src.<key>` — no target alias, and both `WHEN` arms are always present |
| Names | Up to 127 bytes. Names fold to lower case. |
| String literals | A backslash is an escape: Rocky writes `\'` and `\\` (from the Redshift lexer's PostgreSQL 8.0 roots — not verified live) |
| Current time | `GETDATE()` |
| Date arithmetic | `DATEADD(day, -n, CURRENT_DATE)` |
| Text casts | `VARCHAR(65535)`. A bare `VARCHAR` is `VARCHAR(256)` on Redshift and cuts longer values. |
| `TABLESAMPLE` | None. Null-rate checks scan the whole table. |
| Introspection | `svv_columns`, which also covers late-binding views and Spectrum tables |
| Type changes in place | A longer `VARCHAR(n)` only. Any other change rebuilds the table. |
| Materialized views | Drop and recreate on each run. Rocky does not set `AUTO REFRESH`. |

## Not supported yet

- IAM authentication.
- Governance, checksum-bisection `rocky compare`, and Redshift as a discovery source.
- `AUTO REFRESH`, `BACKUP` and attributes on materialized views.

## See also

- [PostgreSQL adapter](/reference/adapters/postgres/) — fields, TLS modes, and how each strategy runs
- [`[adapter.NAME]`](/reference/configuration/#adaptername) — fields shared by every adapter type
