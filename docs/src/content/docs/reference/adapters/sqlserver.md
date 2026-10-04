---
title: SQL Server adapter
description: Connect Rocky to Microsoft SQL Server, Azure SQL or Fabric Warehouse — fields, auth, TLS, and how each materialization runs in T-SQL
sidebar:
  order: 7
---

The SQL Server warehouse adapter (`type = "sqlserver"`) runs your SQL over a native TDS connection. It needs no ODBC driver and no OpenSSL. TLS uses rustls.

| Target | Status |
|--------|--------|
| SQL Server 2016 SP1 and later | Supported. Tested live against SQL Server 2022. |
| Azure SQL Database, Azure SQL Managed Instance | Beta. Same T-SQL; not run in CI. |
| Microsoft Fabric Warehouse | Beta. Set `flavor = "fabric"`. Not run in CI. |

`CREATE OR ALTER VIEW` and `DROP TABLE IF EXISTS` need SQL Server 2016 SP1 or later.

## Fields

The adapter reads the shared `[adapter]` fields:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `host` | string | Yes | Server host name. Add a port as `host,port` or `host:port`. The default port is `1433`. A `tcp:` prefix is accepted. |
| `database` | string | Yes | The database to connect to. A model's `catalog` must be this database or empty. |
| `username` | string | For SQL auth | SQL Server login. |
| `password` | string | For SQL auth | Password for `username`. |
| `oauth_token` | string | For token auth | A Microsoft Entra ID access token for `https://database.windows.net/`. |
| `client_id` | string | For service principal | Entra ID application (client) id. |
| `client_secret` | string | For service principal | Entra ID client secret. |
| `timeout_secs` | integer | No | Connect timeout, and how long one server round trip may stall. Default `300`. |

Adapter-specific keys go in `[adapter.NAME.extra]`. Rocky refuses a key it does not know, so a typo fails loudly:

| Key | Default | Description |
|-----|---------|-------------|
| `port` | from `host`, else `1433` | Server port. Wins over a port in `host`. |
| `flavor` | `"sqlserver"` | `sqlserver` (also Azure SQL) or `fabric`. See [Fabric](#fabric-warehouse). |
| `encrypt` | `"mandatory"` | `mandatory`, `strict` or `optional`. See [TLS](#tls). |
| `trust_server_certificate` | `false` | Accept any server certificate. For a local or test server only. |
| `ca_cert` | unset | Path to a PEM or DER CA certificate, trusted on top of the Mozilla root set. |
| `tenant_id` | unset | Entra ID tenant (directory) id or domain. Required for a service principal. |
| `authority_host` | `https://login.microsoftonline.com` | Entra ID authority, for a sovereign cloud such as `https://login.microsoftonline.us`. |
| `max_connections` | `8` | Upper bound on open connections (1–256). |

```toml
[adapter.mssql]
type = "sqlserver"
host = "${MSSQL_HOST}"
database = "analytics"
username = "${MSSQL_USER}"
password = "${MSSQL_PASSWORD}"
```

`rocky validate` runs the same parse the adapter runs. A missing field, an unknown `extra` key, or a wrong auth setup shows up there first.

A named instance (`host\SQLEXPRESS`) is refused: finding its port needs the SQL Browser service. Set `extra.port` to the instance's TCP port instead.

## Authentication

Configure exactly one method. Rocky refuses a block that sets two.

| Method | Fields | Notes |
|--------|--------|-------|
| SQL authentication | `username`, `password` | Not available on Fabric. |
| Entra ID access token | `oauth_token` | Rocky does not refresh it. Get one with `az account get-access-token --resource https://database.windows.net/`. A run that outlives the token fails at its next new connection. |
| Entra ID service principal | `client_id`, `client_secret`, `extra.tenant_id` | Rocky fetches a token with the OAuth 2.0 client-credentials grant and refreshes it 5 minutes before it expires. |

```toml
[adapter.fabric]
type = "sqlserver"
host = "${FABRIC_SQL_ENDPOINT}"
database = "sales_warehouse"
client_id = "${AZURE_CLIENT_ID}"
client_secret = "${AZURE_CLIENT_SECRET}"

[adapter.fabric.extra]
flavor = "fabric"
tenant_id = "${AZURE_TENANT_ID}"
```

Windows (Kerberos / NTLM) and managed-identity auth are not supported yet.

## TLS

| `encrypt` | Encrypts | Checks the certificate |
|-----------|----------|------------------------|
| `mandatory` | Everything; fails if the server cannot | Yes, unless `trust_server_certificate = true` |
| `strict` | Everything, from the first byte (TDS 8.0) | Always. SQL Server 2022 and Azure SQL. |
| `optional` | The login packet only | — |

The certificate chain is checked against the Mozilla root set plus `ca_cert`, and the certificate must name `host`. A local container uses a self-signed certificate: set `trust_server_certificate = true` there, and never in production. `trust_server_certificate` and `ca_cert` cannot be set together.

Azure SQL's `Redirect` connection policy sends the first login to another node. Rocky follows that redirect once.

## Names and case

Rocky brackets every name: `[analytics].[marts].[fct_orders]`. So a name that is also a reserved word, such as `order`, works. Names must match `^[A-Za-z0-9_]+$` and be at most 128 characters.

Whether `Orders` and `orders` name one table depends on the database collation, not on quoting. The default collations (`_CI_`) ignore case; a `_CS_` or `_BIN2` collation does not.

## How each strategy runs

SQL Server does not run a batch as one transaction. So Rocky wraps every write that needs more than one statement in `SET XACT_ABORT ON; BEGIN TRANSACTION; …; COMMIT TRANSACTION;`. A connection that returns an error is closed, not reused. The server then rolls back anything the error left open.

| Strategy | SQL | Atomic |
|----------|-----|--------|
| `full_refresh` | `SELECT * INTO t__rocky_new FROM (…)`, then `DROP TABLE t` + `sp_rename` in one transaction | Yes |
| first create (merge, delete_insert, incremental, time_interval) | `SELECT * INTO t FROM (…)` | Yes; fails if `t` exists |
| `view` | `CREATE OR ALTER VIEW t AS …` | Yes |
| `merge` | `MERGE INTO t WITH (HOLDLOCK) AS rocky_t USING (…) … ;` | Yes (one statement) |
| `incremental` | `INSERT INTO t …` filtered by `@incremental_filter` | Yes (one statement) |
| `time_interval` | `DELETE FROM t WHERE <window>; INSERT INTO t …` in one transaction | Yes |
| `delete_insert` | `DELETE rocky_t FROM t AS rocky_t WHERE EXISTS (…); INSERT …` in one transaction | Yes |
| `materialized_view` | — | Refused. See below. |
| snapshot (SCD2) | — | Refused at compile (`E049`). |
| `dynamic_table` | — | Refused: Snowflake only |

**Full refresh swap.** Rocky builds the new table under `<table>__rocky_new`, then drops the old table and renames the new one in one short transaction. Readers keep the old rows until the swap commits. If the build fails, the old table stays as it was. The swap does not keep grants, indexes or constraints of the old table. A view created `WITH SCHEMABINDING` over the table blocks the drop with the server's own error; Rocky does not drop that view.

**CTEs.** T-SQL accepts `WITH` only at the start of a statement. A model that starts with `WITH` would be refused inside `INSERT INTO t`, a `MERGE` source or a derived table. Rocky lifts every CTE in the model, at any depth, to the start of the statement it generates:

```sql
WITH base AS (SELECT order_id, amount FROM raw.orders
)
MERGE INTO [marts].[orders] WITH (HOLDLOCK) AS rocky_t
USING (
SELECT order_id, amount FROM base
) AS rocky_s
ON rocky_t.[order_id] = rocky_s.[order_id]
WHEN MATCHED THEN UPDATE SET [amount] = rocky_s.[amount]
WHEN NOT MATCHED BY TARGET THEN INSERT ([order_id], [amount]) VALUES (rocky_s.[order_id], rocky_s.[amount]);
```

Rocky leaves the SQL as written, and the server reports its own error, when lifting could change the meaning: two CTEs with the same name, or a nested CTE name that the rest of the statement also uses as a bare name.

**IDENTITY columns.** `SELECT … INTO` copies a source column's `IDENTITY` property, which would make every later insert of that column fail. Rocky adds an empty `UNION ALL SELECT TOP (0) …` branch to each `SELECT … INTO`; a `UNION` is the documented way to drop the property.

**Incremental models.** `@incremental_filter` becomes `(1 = 1)` on a run that loads every row, because T-SQL has no `TRUE`. A `lookback` renders as `DATEADD(hour, -2, MAX(updated_at))`. A watermark literal carries 7 fractional digits, `DATETIME2`'s precision, rounded up. So the row it came from does not pass the next run's `>` filter again, including a `DATETIME` value such as `.003`, which the server compares as 3.333… ms.

**Merge.** `HOLDLOCK` stops two concurrent upserts from both inserting the same new key. Fabric does not accept table hints, so `flavor = "fabric"` leaves it out. A key-only merge has no `WHEN MATCHED` arm and only inserts missing keys.

**Materialized views.** SQL Server's version is an indexed view. It needs `WITH SCHEMABINDING`, two-part names, no outer joins, and a unique clustered index on a key Rocky cannot infer. Use `full_refresh` or `view` instead.

## Previews and samples

`rocky preview`, the MCP sample tools and `rocky ai-contract` limit rows with `SELECT TOP (n)` on SQL Server.

## User-defined functions

Rocky does not create functions on SQL Server. `rocky compile` reports `E051` for every function a model calls. A T-SQL scalar function takes `@`-prefixed parameters and must be called with its schema (`dbo.f(x)`), so a model's bare `f(x)` call would not reach it. Create the function outside Rocky and call it by its schema-qualified name.

## Compile-time operand checks

`rocky compile` judges aggregates and comparisons against SQL Server's documented conversion rules:

| Case | Result |
|------|--------|
| `SUM` / `AVG` over a text column | `E042`: SQL Server refuses a character operand |
| A number compared with a text column | `W043`: SQL Server converts the text per row and fails on a value that is not a number |
| A date compared with a text column | `W043` |
| A number compared with a numeric string literal (`id = '10'`) | Clean |

## Schema drift

Rocky reads columns from `INFORMATION_SCHEMA.COLUMNS`. It reports each type in a form that is valid T-SQL, such as `INT`, `NVARCHAR(40)`, `NVARCHAR(MAX)`, `DECIMAL(12,2)` and `DATETIME2(7)`. A `float` is reported as `DOUBLE PRECISION`.

These type changes run as `ALTER TABLE … ALTER COLUMN … <type> NULL`:

- `TINYINT` → `SMALLINT` → `INT` → `BIGINT`
- `REAL` → `FLOAT`
- a longer `VARCHAR(n)`, `NVARCHAR(n)` or `VARBINARY(n)`, or the `(MAX)` form
- `DECIMAL(p,s)` → `DECIMAL(p2,s)` with `p2 > p`

Any other change rebuilds the table. A new source column is added with `ALTER TABLE … ADD <column> <type>`.

## Types in the compiler

These source types give the type checker a concrete type: `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `REAL`, `DOUBLE PRECISION` (a `float`), `DECIMAL(p,s)`, `DATE` and `DATETIME`.

`BIT`, `(N)VARCHAR(n)`, `DATETIME2(n)`, `DATETIMEOFFSET(n)`, `MONEY` and `UNIQUEIDENTIFIER` stay unknown. An unknown type never fails a contract. The contract reports the column as not checked (`I003`).

## String literals

Rocky doubles a quote inside a string literal and leaves a backslash as it is. One T-SQL rule it cannot express: a backslash followed directly by a line break is a line continuation, and the server drops both characters. A value that holds that exact pair does not round-trip.

Rocky also writes literals without the `N` prefix. On a `VARCHAR` column with a non-Unicode collation, characters outside that code page become `?`.

## Fabric Warehouse

Set `flavor = "fabric"`. It changes these renderings:

- Text casts use `VARCHAR(8000)`, because Fabric has no `NVARCHAR`.
- `MERGE` has no `HOLDLOCK` hint.
- Null-rate checks scan the whole table, because Fabric has no `TABLESAMPLE`.
- SQL authentication is refused. Use an access token or a service principal.

## Known limits

- A model whose SELECT ends in `ORDER BY` (without `TOP` or `OFFSET`), `OPTION (…)` or `FOR JSON` / `FOR XML` fails on every write. T-SQL refuses those inside the derived table Rocky wraps the model in (error 1033). Remove the clause.
- An hour, minute or second `lookback` on a `DATE` watermark column fails: `DATEADD(hour, …)` does not accept a `DATE`. Use a day lookback, or a `DATETIME2` watermark.
- `on_schema_change = "append_new_columns"` builds a probe table named `<table>__rocky_probe_<pid>_<nonce>`. A target name over about 85 characters makes it longer than 128 characters, and the run fails.
- `rows_copied` for a multi-statement write is the last non-zero count the server reported.

## Not supported yet

- Governance (tags, grants, masking). Governance calls do nothing.
- Snapshot (SCD2) pipelines and the `materialized_view` strategy.
- `regex_match` checks, and checksum-bisection `rocky compare`.
- `rocky branch promote`: it refuses with "generated promote statement does not target".
- Using SQL Server as a discovery source.

## Run the live tests

The adapter's integration tests run against a real server when `ROCKY_SQLSERVER_TEST_HOST` is set. A local container works:

```bash
docker run -d -e ACCEPT_EULA=Y -e 'MSSQL_SA_PASSWORD=<password>' -p 1433:1433 \
  mcr.microsoft.com/mssql/server:2022-latest
# then: CREATE DATABASE rocky_test

ROCKY_SQLSERVER_TEST_HOST=localhost \
ROCKY_SQLSERVER_TEST_PASSWORD='<password>' \
cargo test -p rocky-sqlserver --test live_sqlserver
```

The same variables drive the end-to-end `rocky run` test (`cargo test -p rocky --test sqlserver_live_pipeline`). Optional: `ROCKY_SQLSERVER_TEST_DB` (default `rocky_test`), `ROCKY_SQLSERVER_TEST_USER` (default `sa`) and `ROCKY_SQLSERVER_TEST_TRUST_CERT` (default `true`).

## See also

- [PostgreSQL adapter](/reference/adapters/postgres/)
- [`[adapter.NAME]`](/reference/configuration/#adaptername) — fields shared by every adapter type
