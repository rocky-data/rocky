# 08-postgres-child-connector — Reference child connector for Postgres

> **Credentials:** none (Docker required for the live tour; the POC brings its own Postgres)
> **Runtime:** ~20-40s on a warm Docker pull (`rocky validate` only, <1s, without Docker)

## What it shows

`rocky load --connector pg_events` runs `connector/main.py` once. It reads new rows of `public.events` through the `duckdb` CLI's `postgres` extension and writes one Parquet file. Rocky checks it, appends to `poc.main.events` in DuckDB, and saves the cursor.

Support tier `reference`: maintained by the Rocky project. `rocky connector test pg_events` runs on each pull request that touches it (no Docker); `run.sh` runs against Docker weekly.

## Read this first: what this connector fits

| Fact | What it means for you |
|---|---|
| The target only appends. | Use it for insert-only tables, such as events or logs. |
| An updated row is a new row to Rocky only if its cursor value moves forward. | Then it lands again, as a duplicate. An update that keeps the cursor value is never seen. Deletes are never seen. |
| A row that commits late, with a cursor value below the saved cursor, is lost for good. | Use a cursor the database sets at commit order, or leave a safety lag. |
| The newest rows wait until a newer row arrives. | Rows at the source maximum are held back so ties land together. A source that stops receiving rows keeps its last rows unloaded. |
| Rows with a NULL cursor value are skipped for good. | Make the cursor column `NOT NULL`. |
| Delivery is at least once. | A crash between the promote and the state commit loads a batch twice. Deduplicate downstream on a key. |
| Each run scans the whole source table. | The bounds compare `epoch_us(...)` integers, so DuckDB does not push the filter to Postgres. Add an index-friendly filter for large tables. |
| Egress: `extensions.duckdb.org` and your Postgres host. | The manifest declares only `extensions.duckdb.org`. Rocky does not enforce the list. |

## The cursor contract

State is `{"cursor": "<ISO-8601 UTC, microseconds>" | null}`, `state_schema_version = 1`.

1. With `TimeZone = 'UTC'`, the program reads `MAX(cursor_column)` once (M).
2. It extracts rows with `cursor < value < M` (no lower bound on the first run).
3. The new cursor is the maximum value extracted. With no rows, or an empty source, it does not change.
4. With no rows, no Parquet file is written or listed; Rocky skips the load and still commits the state.

## Secrets and settings

`rocky.toml` declares `secrets = ["PGHOST", "PGPORT", "PGUSER", "PGPASSWORD", "PGDATABASE"]`; Rocky hands only those to the program. libpq reads them (`ATTACH '' AS pg (TYPE postgres, READ_ONLY)`), so no connection string is in SQL, argv or output.

Rocky replaces secret values of 8 bytes or more with `${NAME}` in logs; **shorter values are not replaced**. The POC user is `rocky_reader` because a user named `postgres` (8 bytes) would turn every "postgres" in the logs into `${PGUSER}`. `run.sh` greps all outputs, debug logs, scratch dirs and state for the password. `keep_scratch = true` keeps scratch dirs for that; by default Rocky removes a successful run's scratch dir after the state commit.

`[connector.pg_events.options]` holds `schema`, `table`, `cursor_column` and optional `extension_directory`. They reach the program in `options.json`, **on disk in clear**, so no secrets there. The extension cache defaults to `connector/.duckdb-extensions/`, because the program's `HOME` is a new scratch directory on every run.

## Prerequisites

- `rocky` on `PATH`
- `docker` with `docker compose` (live tour)
- `python3` 3.8 or newer (the connector uses only the standard library)
- the standalone `duckdb` CLI on `PATH`
- network access to `extensions.duckdb.org` on the first run

## Run

```bash
./run.sh
```

Without Docker, `run.sh` runs `rocky validate` and exits 0 with a SKIP message. `REQUIRE_DOCKER=1` makes that a failure.

For your own Postgres, export the five `PG*` variables, edit `[connector.pg_events.options]`, and run `rocky load --connector pg_events`. **Do not run `run.sh` against your own database: it inserts rows.**

## Expected output

Three loads of 19, 5 and 0 rows (5 rows are inserted before load 2); `commit_seq` reaches 3. The target has 24 rows with distinct ids; id 25, at the source maximum, waits until a newer row exists. The password grep reports `0 hits`.

```text
POC complete: 3 loads, 24 rows, cursor held back at the source maximum, no password leak.
```

## Related

- [Write a connector](../../../../../docs/src/content/docs/guides/write-a-connector.md): start from `rocky connector init`
- [Child connectors guide](../../../../../docs/src/content/docs/guides/child-connectors.md): protocol, checks, guarantees
