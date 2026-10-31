# 08-postgres-child-connector — Reference child connector for Postgres

> **Category:** 07-adapters
> **Credentials:** none (Docker daemon required for the live tour; the POC brings its own Postgres)
> **Runtime:** ~20-40s on a warm Docker pull (`rocky validate` only, in <1s, when Docker is absent)
> **Rocky features:** `rocky load --connector`, the child connector protocol, a timestamp cursor in connector state, declared secrets and log redaction

## Read this first: what this connector fits

| Fact | What it means for you |
|---|---|
| The target only appends. | Use it for insert-only tables, such as events or logs. |
| An updated row is a new row to Rocky only if its cursor value moves forward. | Then the row lands again, as a duplicate. An update that keeps the cursor value is never seen. Deletes are never seen. |
| A row that commits late, with a cursor value below the saved cursor, is lost for good. | Use a cursor that the database sets at commit order, or leave a safety lag. |
| Delivery is at least once. | A crash between the promote and the state commit loads a batch twice. Deduplicate downstream on a key. |
| Each run scans the whole source table. | The bounds compare `epoch_us(...)` integers, so DuckDB does not push the filter to Postgres. Fine for a demo; add an index-friendly filter for large tables. |
| Python 3.8 or newer, standard library only. | `main.py` imports nothing outside the standard library. That is why the settings file is `source.json`, not TOML (`tomllib` needs 3.11). |
| It needs the standalone `duckdb` CLI on `PATH`. | `main.py` runs all SQL through it, with the DuckDB `postgres` extension. |
| Egress: `extensions.duckdb.org` and your Postgres host. | The manifest declares only `extensions.duckdb.org`, the one fixed host. Rocky does not enforce the list. |

## What it shows

A complete child connector in about 240 lines of Python. `rocky load --connector pg_events` starts `connector/main.py` once. The program reads the new rows of `public.events` from Postgres and writes one Parquet file. Rocky checks the file, appends the rows to `poc.main.events` in DuckDB, and saves the cursor for the next run.

```
  rocky load ──▶ main.py ──▶ duckdb CLI ──(postgres ext, libpq)──▶ Postgres
      ▲             │
      │             └──▶ batch-0.parquet + _rocky_result.json {"cursor": ...}
      └── check file, append to poc.main.events, save state (commit_seq + 1)
```

## The cursor contract

The state is `{"cursor": "<ISO-8601 UTC, microseconds>" | null}`, with `state_schema_version = 1`.

```
  source values:   t1  t2 ... t19  t20  ← t20 is the maximum M
  run 1 extracts:  t1 .. t19           cursor = t19   (t20 waits)
  insert t21..t25; M = t25
  run 2 extracts:  t20 .. t24           cursor = t24   (t25 waits)
  run 3 extracts:  nothing              cursor = t24   (state still commits)
```

1. The program sets `TimeZone = 'UTC'` and reads `MAX(cursor_column)` once. Call it M.
2. It extracts the rows with `cursor < value < M`. The first run has no lower bound. NULL values are skipped.
3. The new cursor is the maximum value it extracted. When it extracts no row, the cursor does not change.
4. Rows at M wait one run. So rows that tie at M are never split across two runs.
5. An empty source table selects nothing and keeps the cursor.
6. With no rows, the program writes no Parquet file and lists none. Rocky skips the load and still commits the state.

## Secrets

`rocky.toml` declares `secrets = ["PGHOST", "PGPORT", "PGUSER", "PGPASSWORD", "PGDATABASE"]`. Rocky hands only those variables to the program. The program runs `ATTACH '' AS pg (TYPE postgres, READ_ONLY)`, so libpq reads them from the environment. No connection string is in the SQL, in argv or in the output. The program prints counts only.

Rocky replaces a secret value of 8 bytes or more with `${NAME}` in its logs. Shorter values are not replaced, so the POC password is longer than 8 bytes. The POC user is `rocky_reader`, not `postgres`. A user named `postgres` (8 bytes) would turn every "postgres" in the logs, such as the extension name, into `${PGUSER}`. `run.sh` greps the password over every output, every debug log, the kept scratch directories and the state file, and expects 0 hits.

## Layout

```
.
├── README.md              this file
├── rocky.toml             DuckDB target + [connector.pg_events] with the PG* secrets
├── docker-compose.yml     postgres:16-alpine, seeded from seed.sql
├── seed.sql               public.events, 20 rows, distinct created_at values
├── run.sh                 validate → compose up → 3 loads with asserts → password grep → compose down
└── connector/
    ├── connector.toml     manifest: command = ["./main.py"], state_schema_version = 1
    ├── main.py            the connector (Python 3.8+, stdlib only)
    ├── source.json        schema, table, cursor_column, optional extension_directory
    └── .gitignore         ignores .duckdb-extensions/, the extension cache
```

`main.py` finds `source.json` next to itself. The DuckDB extension cache defaults to `connector/.duckdb-extensions/`, also next to `main.py`. The program's `HOME` is a new scratch directory on every run, so a cache there would download the extension every time.

## Prerequisites

- `rocky` on `PATH`
- `docker` with `docker compose` (live tour)
- `python3` 3.8 or newer
- the `duckdb` CLI on `PATH`
- network access to `extensions.duckdb.org` on the first run

## Run

```bash
./run.sh
```

When Docker is absent, `run.sh` runs `rocky validate` and exits 0 with a SKIP message. Set `REQUIRE_DOCKER=1` to make that a failure.

To point the connector at your own Postgres, export `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` and `PGDATABASE`, edit `connector/source.json`, and run `rocky load --connector pg_events`. Do not run `run.sh` against your own database: it inserts rows.

## Expected output

```text
=== rocky validate (parse-only, no network) ===
...
=== docker compose up -d --wait (postgres:16-alpine, 20 seeded rows) ===
=== load 1: rocky load --connector pg_events (expect 19 rows) ===
    status=loaded total_rows=19 commit_seq 0 -> 1
=== insert 5 rows with later timestamps (2026-10-01 00:21 to 00:25 UTC) ===
=== load 2: rocky load --connector pg_events (expect 5 rows) ===
    status=loaded total_rows=5 commit_seq 1 -> 2
=== load 3: rocky load --connector pg_events (expect 0 rows) ===
    status=loaded total_rows=0 commit_seq 2 -> 3
=== target table poc.main.events ===
    rows=24 distinct ids=24 (id 25, at the source maximum, waits for a later run)
=== password grep: JSON outputs, debug logs, kept scratch dirs, saved state ===
    0 hits

POC complete: 3 loads, 24 rows, cursor held back at the source maximum, no password leak.
```

## Related

- [Child connectors guide](../../../../../docs/src/content/docs/guides/child-connectors.md): the protocol, the checks and the guarantees
- [`07-trino-docker`](../07-trino-docker/): the same Docker skip pattern
