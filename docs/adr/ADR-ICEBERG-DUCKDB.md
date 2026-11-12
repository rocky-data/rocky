# ADR-ICEBERG-DUCKDB — Iceberg tables through DuckDB and a REST catalog

**Status:** Accepted (advisor-ruled 2026-11-11, Hugo to confirm)

**Scope:** Package L-1 of the open-storage work. A `type = "duckdb"` adapter can attach an Iceberg REST catalog, and `rocky run` writes models whose target catalog is that attachment. It builds on [`ADR-REPLACE.md`](ADR-REPLACE.md), decision D3.

---

## Context

### What is true today

- A DuckDB adapter has three settings: `path`, `extension_directory` and `offline` (`rocky-core/src/config.rs`, `AdapterConfig`, which is `deny_unknown_fields`). It cannot attach another catalog.
- One adapter holds one DuckDB connection (`rocky-duckdb/src/adapter.rs`, `Arc<Mutex<DuckDbConnector>>`). Extensions are loaded when it opens (`rocky-duckdb/src/lib.rs`, `prepare_extensions`). DuckDB is bundled as version 2.0; extensions such as `iceberg` and `httpfs` are fetched at run time by `INSTALL`, unless `offline = true`.
- The dialect, `DuckDbSqlDialect`, carries no configuration. `rocky-cli/src/registry.rs` looks it up by adapter type, also on paths that never connect (`compile`, `preview`, the MCP tools).
- The DuckDB dialect writes a full refresh as `CREATE OR REPLACE TABLE`, and reports `TableReplace::Atomic` for every catalog.

### Spike (2026-11-11, DuckDB 2.0, Lakekeeper REST catalog, SeaweedFS S3)

| Operation on an attached Iceberg catalog | Result |
|---|---|
| `ATTACH 'demo' AS lk (TYPE iceberg, ENDPOINT …, AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')`, with `CREATE SECRET (TYPE s3, …)` | Works |
| `CREATE OR REPLACE TABLE` | Refused: "not supported in DuckDB-Iceberg" |
| `CREATE TABLE … AS`, `INSERT INTO … BY NAME`, `UPDATE`, `MERGE`, `CREATE VIEW` | Work |
| `BEGIN; ALTER … ADD/DROP COLUMN; DELETE; INSERT BY NAME; COMMIT` | One atomic catalog commit |
| `ALTER COLUMN … TYPE` (`INTEGER` → `BIGINT`) inside that transaction | Works |
| A failed `INSERT` inside the transaction, then `COMMIT` | The transaction rolls back; the old rows stay |
| A transaction that mixes `RENAME` or `DROP` with writes, or creates two tables | Refused |
| `information_schema.tables` | Lists the Iceberg tables as `BASE TABLE`; `duckdb_databases()` reports type `iceberg` |
| Type round-trip | `TINYINT`, `SMALLINT`, `UTINYINT` and `USMALLINT` come back `INTEGER`; `UINTEGER` comes back `BIGINT`; `HUGEINT` comes back `DECIMAL(38,0)`. `UBIGINT`, `INTERVAL`, `JSON` and fixed-size arrays are refused. A `LIST` column is in `DESCRIBE` (re-measured during implementation; the first spike reported it missing) |
| `ALTER COLUMN … TYPE` | Allowed: `INTEGER` → `BIGINT`, `FLOAT` → `DOUBLE`, `DECIMAL(p,s)` → `DECIMAL(p',s)`. Refused: `INTEGER` → `DOUBLE`, `DATE` → `TIMESTAMP` (Iceberg v3), every narrowing |
| `DROP COLUMN` of a partition column | Refused by DuckDB-Iceberg inside the transaction ("referenced by the current partition spec") |

## Decision

### D1 — Configuration: an `[[attach]]` list on the DuckDB adapter

```toml
[adapter.lake]
type = "duckdb"

[[adapter.lake.attach]]
alias = "lk"                 # the catalog name models target
type = "iceberg"
warehouse = "demo"
endpoint = "${ICEBERG_ENDPOINT}"
authorization = "none"       # v1 accepts only "none"

[adapter.lake.attach.s3]     # becomes CREATE SECRET (TYPE s3, SCOPE …)
key_id = "${S3_KEY_ID}"
secret = "${S3_SECRET}"
endpoint = "localhost:8333"
url_style = "path"
use_ssl = false
region = "us-east-1"
```

- No new adapter type: it would duplicate the factory, discovery and loader wiring for the same connection.
- `alias` and `type` decide the rendered SQL. `type` is an enum. `alias` is validated as an identifier; a `${VAR}` in it is expanded before parsing like every other value, so it cannot be told apart from a literal. Plan and run read the same expanded config, so they agree.
- `alias` must be a valid identifier. It must not equal the file's own catalog name, `memory`, or another alias. It is never the default catalog: a model with an empty catalog renders a two-part name and is not an Iceberg write.
- The S3 secret is a `TEMPORARY` secret named `rocky_attach_<alias>`. It is scoped by `s3.scope` (for example `s3://warehouse`); Rocky cannot learn the bucket from the catalog without probing, so with no `scope` the secret covers every `s3://` path of the connection. Secret values are `RedactedString`. Rocky never creates a `PERSISTENT` secret, and runs the secret and attach statements on a path that does not log SQL.
- `offline = true` refuses to connect when `httpfs` or `iceberg` is missing from the extension directory, as it does for `icu`.
- `compile`, `plan` and other paths that do not connect never attach.
- OAuth2 and vended credentials are refused in v1. They come later, with a Lakekeeper conformance case.

### D2 — The dialect carries its configuration; Rocky never probes

`DuckDbSqlDialect` holds the set of Iceberg aliases. The registry builds it from the adapter's config on every path, connected or not, so `rocky plan` renders what `rocky run` executes. Rocky never asks the database which catalogs are Iceberg; ADR-REPLACE ruled probing out for the same reason. `create_schema_sql` and the adapter's `list_tables` become catalog-aware.

### D3 — A third replace: `TableReplace::InPlaceOverwrite`

For an Iceberg alias, `table_replace(catalog)` returns `InPlaceOverwrite`. A full refresh of an existing table then runs as one transaction, inside one adapter call that holds the connection for the whole batch:

```
BEGIN
  read the target's columns and the model's output columns
  ALTER TABLE … ADD COLUMN …        (new columns, first)
  ALTER TABLE … DROP COLUMN …       (removed columns)
  ALTER TABLE … ALTER COLUMN … TYPE (a widening the dialect allows)
  DELETE FROM <t>
  INSERT INTO <t> BY NAME <select>
COMMIT                               any error → ROLLBACK; the old table is kept
```

- Column types are compared after normalising Iceberg's round-trip (above). A re-run with no change emits no `ALTER`.
- Refused, with the old table kept: changing a nested type and a type change that is not a widening (Rocky checks both before any write). Dropping a partition or identifier column is refused by DuckDB-Iceberg inside the transaction, which then rolls back; Rocky does not read the partition spec itself.
- A first build is the non-replacing `CREATE TABLE … AS`.
- `rocky plan` shows the fixed skeleton (`BEGIN`, `DELETE`, `INSERT BY NAME`, `COMMIT`) with a note that column changes are decided at run time, the way the staged swap's statements stand in for its probes.
- A commit conflict with another writer retries the whole transaction. That is safe for a full refresh, which reads nothing it wrote.
- `recover_interrupted_swap` does nothing for this variant: there is no backup table.
- Columns added by `ALTER` sit at the end, so the table's column order can differ from the model's.

### D4 — Scope of v1

Supported on an Iceberg alias: `full_refresh` (D3) and append `incremental`.

Refused before any SQL runs, with an error that names the strategy and the catalog: views, `merge` (also `incremental` with a `unique_key`), `incremental` with a `lookback`, `delete_insert`, `time_interval`, `microbatch`, `snapshot`, quarantine (both modes), branch and promote, `rocky load`, seeds, a lakehouse `format`, and `drop_existing_kind`. A replication full refresh of an existing table is refused too (its rebuild SQL is a CTAS, not a bare query); a replication first copy and incremental work. `UPDATE` and `MERGE` work in the spike; each comes later with its own conformance case.

An append and its watermark write are not one transaction. A crash between them repeats the append on the next run, as on every other warehouse.

### D5 — Tests and CI

- The renderer, the reconcile logic and every failure point of the executor are tested in the required `engine-ci`, against a scripted adapter, with no network.
- A live conformance test runs against Lakekeeper and SeaweedFS in Docker (`rocky-duckdb/tests/iceberg_conformance.rs`, feature `iceberg-conformance`, `#[ignore]`). It runs in `.github/workflows/iceberg-conformance.yml`, path-filtered and non-required, plus a weekly schedule. Images are pinned by digest. Extensions are downloaded on each run: no cache action is pinned in this repository, and adding one is a separate supply-chain decision.
- A credential-free POC sits in `examples/playground/pocs/07-adapters/`.

### Amendments from implementation and red team (2026-11-12)

- **Stage first.** The transaction starts with `CREATE OR REPLACE TEMP TABLE temp.main.__rocky_overwrite AS <select>` and inserts from it. Measured: `DELETE` then `INSERT … SELECT … FROM <target>` in one transaction reads the emptied table, so a model that reads its own target lost its old rows. A temporary table in the same transaction is allowed. "A full refresh reads nothing it wrote" is now true by construction.
- **Nested columns.** DuckDB-Iceberg refuses `ADD COLUMN` of a nested type. Rocky refuses it before any write.
- **Uncertain commit.** A `COMMIT` that fails and is not a recognised conflict is reported as possibly committed (`InPlaceOverwriteCommitUncertain`). A conflict is a certain refusal and is retried.
- **Conflict messages.** Two shapes measured against Lakekeeper: DuckDB's own `conflicts with a concurrent commit`, and the catalog's `(Conflict_409) … CatalogCommitConflicts`. Only these, after `Failed to commit Iceberg transaction`, are retried.
- **Secret scope.** With more than one attachment, each `s3` table must set `scope`.
- **Replication.** Every replication strategy passes the same refusal as models. A rebuild of an existing table (drift, missing watermark) is refused with the reason.

## Consequences

- Iceberg becomes a Rocky target with no vendor warehouse.
- A full refresh on Iceberg deletes and rewrites every row. Iceberg keeps the old data files until snapshots expire; Rocky does not expire them.
- Two writers can conflict at commit. A full refresh retries; an append fails and is re-run by the next run.

## Alternatives considered

- **A separate `duckdb_iceberg` adapter type.** Rejected: it duplicates the registry wiring for one connection.
- **Probe `duckdb_databases()` at run time.** Rejected: `rocky plan` could not render the same SQL.
- **Refuse schema changes on Iceberg in v1.** Rejected: drift rebuild would have nowhere to go.
- **Drop and create.** Rejected: DuckDB-Iceberg refuses `DROP` and `CREATE` in one transaction, so a failure would lose the table (RD-009).
