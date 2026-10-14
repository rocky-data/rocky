---
title: PostgreSQL adapter
description: Connect Rocky to PostgreSQL — fields, TLS modes, merge rendering, and how each materialization runs
sidebar:
  order: 5
---

The PostgreSQL warehouse adapter runs your SQL over a native PostgreSQL connection. It needs no libpq and no OpenSSL. TLS uses rustls.

The adapter is tested live against PostgreSQL 16. `MERGE` needs PostgreSQL 15 or later. Older servers can use `merge_mode = "on_conflict"` (see [Merge](#merge)).

## Fields

The adapter reads the shared `[adapter]` fields:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `host` | string | Yes | Server host name. Add a port as `host:port`. The default port is `5432`. |
| `database` | string | Yes | The database to connect to. A model's `catalog` must be this database or empty. |
| `username` | string | Yes | Login role. |
| `password` | string | No | Password. Leave it out when the server asks for none, for example under `trust` auth. |
| `timeout_secs` | integer | No | Connect timeout and per-statement `statement_timeout`. Default `300`. |

Adapter-specific keys go in `[adapter.NAME.extra]`. Rocky refuses a key it does not know, so a typo fails loudly:

| Key | Default | Description |
|-----|---------|-------------|
| `port` | from `host`, else `5432` | Server port. Wins over a port in `host`. |
| `sslmode` | `"prefer"` | `disable`, `prefer`, `require` or `verify-full`. See [TLS](#tls). |
| `sslrootcert` | unset | Path to a PEM file of extra root certificates, trusted under `verify-full`. |
| `max_connections` | `8` | Upper bound on open connections (1–256). |
| `merge_mode` | `"merge"` | `merge` or `on_conflict`. See [Merge](#merge). |

```toml
[adapter.pg]
type = "postgres"
host = "${PGHOST}"
database = "analytics"
username = "${PGUSER}"
password = "${PGPASSWORD}"

[adapter.pg.extra]
sslmode = "verify-full"
```

`rocky validate` runs the same parse the adapter runs, so a missing field or an unknown `extra` key shows up there first.

## Authentication

Rocky supports password auth with `username` and `password`. IAM and certificate auth are not built in.

## TLS

`sslmode` uses libpq's names:

| `sslmode` | Encrypts | Checks the certificate |
|-----------|----------|------------------------|
| `disable` | No | — |
| `prefer` | If the server offers TLS | No |
| `require` | Yes | No |
| `verify-full` | Yes | Yes: the chain against the Mozilla root set plus `sslrootcert`, and the host name |

`prefer` and `require` match libpq: they encrypt but do not check the certificate. Use `verify-full` in production. For a private CA, such as Amazon RDS, point `sslrootcert` at its PEM bundle.

## Names and case

Rocky writes every table name bare: `marts.fct_orders`, not `"marts"."fct_orders"`. PostgreSQL folds a bare name to lower case. So a target `Orders` and a model that reads `FROM orders` name the same table.

Rocky refuses a name longer than 63 bytes. PostgreSQL would cut it short without an error, and two long names could become one table.

A name that is also a reserved word, such as `order`, fails at the server.

## How each strategy runs

Rocky sends a write that needs more than one statement as one string. PostgreSQL runs a multi-statement string as one transaction. So the write commits fully or not at all, even on a pooled connection.

| Strategy | SQL | Atomic |
|----------|-----|--------|
| `full_refresh` | `DROP TABLE IF EXISTS t; CREATE TABLE t AS …` | Yes |
| `view` | `CREATE OR REPLACE VIEW t AS …` | Yes |
| `materialized_view` | `DROP MATERIALIZED VIEW IF EXISTS t; CREATE MATERIALIZED VIEW t AS …` | Yes |
| `merge` | `MERGE INTO …` or `INSERT … ON CONFLICT` | Yes (one statement) |
| `time_interval` | `DELETE FROM t WHERE <window>; INSERT INTO t …` | Yes |
| `delete_insert` | `DELETE …; INSERT …` | Yes |
| snapshot (SCD2) | `MERGE` plus `INSERT` / `UPDATE` statements | Each statement; needs PostgreSQL 15+ |
| `dynamic_table` | — | Refused: Snowflake only |

**Materialized views.** PostgreSQL has no `CREATE OR REPLACE MATERIALIZED VIEW`. Each run drops and recreates the view in one transaction. That applies a changed definition and refreshes the data. A bare `REFRESH MATERIALIZED VIEW` would keep a stale definition.

**Dependent views.** PostgreSQL does not let you drop a table that a view reads. A `full_refresh` of such a table fails with the server's "other objects depend on it" error. Rocky does not add `CASCADE`, because that would drop views Rocky does not manage. Use `merge`, `time_interval` or `delete_insert` for a table that views read.

**Grants on a rebuilt table.** A `full_refresh` drops and recreates the table, so grants and comments on it are lost on each run. The adapter does not manage grants yet. Re-grant after the run, or grant on the schema with `ALTER DEFAULT PRIVILEGES`.

**Views and columns.** `CREATE OR REPLACE VIEW` can add columns at the end, but it cannot drop or rename one. A view model that removes a column fails until you drop the view.

## Merge

By default `strategy = "merge"` renders standard `MERGE` (PostgreSQL 15+):

```sql
MERGE INTO marts.customers AS t
USING (<model SQL>) AS s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET name = s.name, email = s.email
WHEN NOT MATCHED THEN INSERT (customer_id, name, email) VALUES (s.customer_id, s.name, s.email)
```

For PostgreSQL 9.5 to 14, set `merge_mode = "on_conflict"`:

```sql
INSERT INTO marts.customers (customer_id, name, email)
SELECT customer_id, name, email FROM (<model SQL>) AS s
ON CONFLICT (customer_id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email
```

`ON CONFLICT` needs a unique index on exactly the `unique_key` columns. Rocky creates one in the same transaction (`CREATE UNIQUE INDEX IF NOT EXISTS <table>__rocky_mk_<hash>`), so a table Rocky built works on the first merge. If the table already holds duplicate keys, the index build fails and the merge stops. That is correct: an upsert by those keys is undefined.

Snapshots use `MERGE`, so `merge_mode = "on_conflict"` refuses snapshot pipelines.

`rocky plan` and `rocky emit-sql` read `merge_mode` from the adapter block, so the preview matches what `rocky run` executes.

## Schema drift

Rocky reads columns from `pg_catalog`, which also covers materialized views. It reports types in a short form, such as `INTEGER`, `VARCHAR(40)`, `NUMERIC(12,2)` and `TIMESTAMPTZ`.

These type changes run as `ALTER TABLE … ALTER COLUMN … TYPE`:

- `SMALLINT` → `INTEGER` → `BIGINT`
- `REAL` → `DOUBLE PRECISION`
- a longer `VARCHAR(n)`, or `VARCHAR(n)` → `TEXT`
- `NUMERIC(p,s)` → `NUMERIC(p2,s)` with `p2 > p`

Any other change rebuilds the table.

## Types in the compiler

These source types give the type checker a concrete type: `SMALLINT`, `INTEGER`, `BIGINT`, `BOOLEAN`, `REAL`, `DOUBLE PRECISION`, `TEXT`, `VARCHAR`, `NUMERIC(p,s)`, `DATE` and `TIMESTAMP`.

`VARCHAR(n)`, `TIMESTAMPTZ`, a bare `NUMERIC`, `JSONB` and arrays stay unknown. A bare `NUMERIC` has no fixed digits in PostgreSQL, so there is nothing to claim. An unknown type never fails a contract. The contract reports the column as not checked (`I003`).

## Not supported yet

- Governance (tags, grants, masking). Governance calls do nothing.
- Checksum-bisection `rocky compare`. Use the sampled comparison.
- Using PostgreSQL as a discovery source.

## Run the live tests

The adapter's integration tests run against a real server when `ROCKY_POSTGRES_TEST_HOST` is set:

```bash
ROCKY_POSTGRES_TEST_HOST=localhost:5432 \
ROCKY_POSTGRES_TEST_PASSWORD=secret \
cargo test -p rocky-postgres --test live_postgres
```

Optional: `ROCKY_POSTGRES_TEST_DB`, `ROCKY_POSTGRES_TEST_USER`, `ROCKY_POSTGRES_TEST_SSLMODE` and `ROCKY_POSTGRES_TEST_SSLROOTCERT`.

## See also

- [Redshift adapter](/reference/adapters/redshift/) — the same crate, with Redshift's dialect
- [`[adapter.NAME]`](/reference/configuration/#adaptername) — fields shared by every adapter type
