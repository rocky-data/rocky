# ADR-DUCKLAKE — DuckLake as a working store behind the DuckDB adapter

**Status:** Accepted (advisor-ruled 2026-11-12, Hugo to confirm)

**Scope:** Package L-2 of the open-storage work. A `type = "duckdb"` adapter can attach a DuckLake catalog, and `rocky run` writes models whose target catalog is that attachment. It builds on [`ADR-ICEBERG-DUCKDB.md`](ADR-ICEBERG-DUCKDB.md) (the `[[attach]]` list and the config-carrying dialect) and [`ADR-REPLACE.md`](ADR-REPLACE.md).

DuckLake is an open table format: table data is Parquet files under a data path, and the catalog (snapshots, schemas, file lists) is kept in a SQL database. DuckDB reads and writes it through its `ducklake` extension.

---

## Context

### What is true after L-1

- `[[adapter.<name>.attach]]` attaches Iceberg REST catalogs (`rocky-core/src/config.rs`, `DuckDbAttach`).
- `DuckDbSqlDialect` holds the set of Iceberg aliases. An Iceberg alias returns `TableReplace::InPlaceOverwrite`. `refuse_on_in_place_catalog` refuses the strategies v1 does not write there.
- Seeds, snapshot, quarantine, branch/promote and `rocky load` check `InPlaceOverwrite` to refuse an Iceberg target.

### Spike (2026-11-12, DuckDB 2.0.0, ducklake 417a9117f5, DuckDB-file metadata)

| Operation on an attached DuckLake catalog | Result |
|---|---|
| `ATTACH 'ducklake:m.ducklake' AS dl (DATA_PATH 'd/')` | Works. The metadata file is attached as a hidden catalog `__ducklake_metadata_dl`, which is not listed but can be queried by name. |
| `CREATE OR REPLACE TABLE` | Atomic, may change columns, one snapshot. A failed one keeps the old table. A model that reads its own target sees the old rows. |
| A transaction mixing DDL and DML | Rolls back fully on failure. |
| `MERGE`, `UPDATE`, `DELETE`, `CREATE SCHEMA`, `NOT NULL`, `DEFAULT`, `COMMENT ON`, `RENAME` | Work. |
| `PRIMARY KEY`, `UNIQUE`, `CHECK`, indexes, sequences | Refused. Rocky emits none of them on the supported paths. |
| Types | Almost all round-trip exactly, including `HUGEINT`, `UBIGINT`, `INTERVAL`, `JSON` and nested types. `ENUM`, `UNION`, `BIT` and fixed arrays are refused. |
| `ALTER COLUMN … TYPE` | Widening only. Rocky's drift widenings (`INTEGER` → `BIGINT`, `DECIMAL(10,2)` → `DECIMAL(18,2)`, `FLOAT` → `DOUBLE`) and `ADD COLUMN` work. |
| One transaction writing the lake and the main database | Refused: `Attempting to write to database "dl" in a transaction that has already modified database "…"`. Temp tables are allowed. |
| Two writers | The loser's `COMMIT` fails with `Failed to commit DuckLake transaction. Transaction conflict - …`. Concurrent inserts both commit. |
| A second process, DuckDB-file metadata | Fails at attach: `Could not set lock on file`. |
| A relative `DATA_PATH` | Stored verbatim and resolved against the working directory on every attach. Data lands in the wrong place, with no error. |
| No `DATA_PATH` | Stored as `<metadata>.files/`. A later different `DATA_PATH` is refused. |
| A write of 10 rows or fewer | Kept in the metadata, with no Parquet file. |
| A view in the lake | Binds its two-part names against the lake, not the main database. The same SQL as a view and as a table can read different tables. |
| A profiled scan of a lake table | Reports `rows_scanned: 0`; the scan carries `Total Files Read` and `Inlined Tables Read`. |
| `ducklake:sqlite:…`, `ducklake:postgres:…` | Auto-installs a scanner even offline. A Postgres attach error prints the password. |
| Missing `ducklake` extension, offline | `LOAD` fails. |

Not measured: an S3 data path, a Postgres or SQLite catalog. The live conformance test (D4) runs Rocky's own rendered SQL.

## Decision

### D1 — Configuration: `type = "ducklake"` in the `[[attach]]` list

```toml
[adapter.lake]
type = "duckdb"
path = "warehouse.duckdb"

[[adapter.lake.attach]]
alias = "dl"
type = "ducklake"
metadata_path = "lake/metadata.ducklake"
data_path = "lake/data/"        # optional; a local directory or s3://…

[adapter.lake.attach.s3]        # only with an s3:// data_path
key_id = "${S3_KEY_ID}"
secret = "${S3_SECRET}"
```

- `DuckDbAttach` becomes an enum tagged on `type`: `Iceberg(DuckDbIcebergAttach)` and `DuckLake(DuckDbDuckLakeAttach)`. The Iceberg TOML is unchanged. Each variant refuses unknown keys.
- v1 keeps DuckLake metadata in a local DuckDB file only. `metadata_path` may not hold a `:` (except a Windows drive letter), so `sqlite:`, `postgres:`, `md:` and `:memory:` are refused by `rocky validate` before anything connects. A later `metadata_type` key defaulting to `duckdb` is a non-breaking addition.
- `metadata_path` may not be the adapter's own `path`. `rocky validate` compares the two lexically; the connect step compares them again after resolving symlinks.
- Relative paths are made absolute against the working directory when Rocky connects, as the adapter's own `path` is. `DATA_PATH` is omitted when `data_path` is unset. Rocky never sends `OVERRIDE_DATA_PATH`, so DuckLake refuses a `data_path` that differs from the stored one.
- After the attach, Rocky reads the stored data path from the metadata catalog. A relative stored path (a lake made outside Rocky) is refused and the lake is detached: it would resolve differently per process.
- `data_path` is a local directory or an `s3://` prefix. `s3` needs an `s3://` `data_path`. The secret's scope defaults to `data_path` with a trailing `/`, so `s3://b/lake` does not also cover `s3://b/lake2/`. A defaulted scope counts as a scope for the L-1 rule that two S3 secrets each need one.
- An alias may not start with `__ducklake_metadata_`. No model or replication target may name a catalog with that prefix, on any dialect.
- Extensions are loaded per kind: `ducklake`, plus `httpfs` only for an `s3://` data path. With `offline = true` a missing extension is `OfflineExtensionMissing`, before any `ATTACH`.

### D2 — Replace: DuckLake is `TableReplace::Atomic`

A DuckLake alias returns `TableReplace::Atomic`: a full refresh renders `CREATE OR REPLACE TABLE … AS`, which DuckLake commits as one snapshot. No new variant, no executor, no column reconcile, no recovery. Recovery and the staged swap return early for `Atomic`, as for a native catalog.

A commit conflict is a `TransactionContext Error`, which `classify_duckdb_failure` already treats as transient. The losing transaction wrote nothing, so the existing retry of an idempotent statement is safe; Rocky adds no retry of its own. A transaction that writes the lake and the main database is classified permanent: it fails the same way on every attempt.

### D3 — Scope: a catalog-kind map

`SqlDialect::catalog_kind(catalog)` returns `Native`, `IcebergRest` or `DuckLake`. `DuckDbSqlDialect` holds an alias-to-kind map built from the config on every path, connected or not, so `rocky plan` renders what `rocky run` executes. `refuse_on_catalog_kind` (renamed from `refuse_on_in_place_catalog`) is an exhaustive match on the kind, then on the strategy.

Supported on a DuckLake alias: `full_refresh`, `ephemeral`, `incremental` (append, with a `unique_key`, with a `lookback`), `merge`, `delete_insert`, `time_interval`, and replication (first copy, incremental, rebuild).

Refused before any SQL runs, with an error that names the strategy and the catalog:

- `view`: a lake view binds its names against the lake (spike), so it could read another table than the same model as a table. A later version may allow a view whose every upstream is in the same lake.
- `materialized_view`, `dynamic_table`, `microbatch`, `content_addressed`, `snapshot` and a lakehouse `format`.
- Seeds, `rocky load`, quarantine, and branch/promote. These are a v1 scope choice: their SQL shapes would likely work, but each needs its own conformance case first.

Reads change too, because an attached catalog shares the connection:

- Discovery reads schemas and tables from the database's own catalog only. An attached catalog holds Rocky's targets, never its sources.
- `list_tables` and `create_schema_sql` name the catalog for every attached alias. With any attachment, the native `list_tables` reads the own catalog only.
- A profiled DuckLake scan is counted as unattributed in upstream-read measurement, never as zero rows read.

### D4 — Tests and CI

- Required CI, no network and no `INSTALL ducklake` (a 55 MB download): config parsing and validation, attach rendering, path absolutising, the stored-path check, kind → `Atomic`, the refusal matrix, discovery and profile parsing, the failure classifier on the measured messages, and the offline-missing error.
- A live conformance test, `rocky-duckdb/tests/ducklake_conformance.rs` (feature `ducklake-conformance`, `#[ignore]`), runs `rocky run` on each supported strategy with Rocky's own SQL. It covers a self-read, a failed replace that keeps the old table, a drift widening, the refusals, and a relative path. It needs no Docker. `.github/workflows/ducklake-conformance.yml` runs it, path-filtered and non-required, plus a weekly schedule. Models write more than 10 rows, so Parquet files are written.
- A credential-free POC: `examples/playground/pocs/07-adapters/10-duckdb-ducklake`.

## Consequences

- DuckLake becomes a Rocky target with open Parquet files and no server.
- DuckDB-file metadata is single-writer: a second process cannot attach the lake while one holds it. Concurrent writers need a Postgres catalog, which is a later package.
- DuckLake keeps old data files until snapshots expire. Rocky expires nothing; run DuckLake's `ducklake_expire_snapshots` and `ducklake_cleanup_old_files` yourself.
- A missing metadata file is created on attach, as a missing main DuckDB file is. A typo in `metadata_path` starts an empty lake rather than failing.
- `delete_insert` runs its `DELETE` and `INSERT` as two statements, so on DuckLake it is two snapshots and a reader can see the gap between them. Native DuckDB has the same gap today; it is tracked separately.

## Alternatives considered

- **A separate `ducklake` adapter type.** Rejected: it duplicates the factory, discovery and loader wiring for the same connection (as for Iceberg).
- **Reuse `InPlaceOverwrite`.** Rejected: DuckLake has an atomic `CREATE OR REPLACE`, so the reconcile executor would add risk for nothing.
- **Probe `duckdb_databases()` for the kind.** Rejected: `rocky plan` could not render the same SQL (ADR-ICEBERG-DUCKDB D2).
- **Pass a relative `data_path` through.** Rejected: DuckLake stores it verbatim and data lands in a different place per working directory, without an error.

## Red team (2026-11-12)

Reviewer: Claude Opus 5.5, a fresh read-only agent that did not author the ruling, with DuckDB 2.0.0 and ducklake probes.

| # | Finding | Disposition |
|---|---|---|
| 1 | A lake view binds two-part names against the lake and can read the wrong table. | Fixed: `view` is refused on DuckLake in v1 (D3). |
| 2 | "A conflict fails the model, with no retry" contradicts `classify_duckdb_failure`, which treats `TransactionContext Error` as transient. The cross-catalog error was misclassified too. | Fixed: D2 now states the retry is safe and kept; the cross-catalog error is classified permanent. Both measured messages are unit-tested. |
| 3 | `metadata_path` must refuse every scheme prefix and `:memory:`, and compare against the adapter's path canonically. | Fixed (D1). |
| 4 | Discovery reads schemas from every catalog, so lake schemas become replication sources. | Fixed: discovery reads the own catalog only, with a test that attaches a second catalog. |
| 5 | A DuckLake scan reports `rows_scanned: 0`. | Fixed: counted as unattributed, measured and unit-tested on the real shape. |
| 6 | A lake made elsewhere keeps a relative stored data path. A missing metadata file starts a new lake. | First half fixed: a relative stored path is refused (D1). Second half rebutted: it matches how a missing main file behaves; recorded in Consequences. |
| 7 | The hidden metadata catalog can be named as a target. | Fixed: refused as a target catalog on every dialect. |
| 8 | `delete_insert` is two autocommit statements, so two snapshots. | Recorded in Consequences. Native DuckDB has the same gap; a wrapping transaction needs its own rollback path, so it is a separate change. |
| 9 | The matrix is otherwise sound; `content_addressed` was missing from the ruling's list; some refusals are scope choices. | Fixed: D3 lists `content_addressed` and says which refusals are scope choices. |
| 10 | A defaulted scope needs a trailing `/`, and must count as a scope with two secrets. | Fixed (D1). |
| 11 | The supported list rests on a non-required job. | Accepted: the PR that lands this ADR runs the conformance workflow once and records the run. |
