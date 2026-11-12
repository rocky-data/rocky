---
title: DuckDB adapter
description: The local in-process adapter, and when it needs a file on disk instead of memory
sidebar:
  order: 1
---

DuckDB runs inside the Rocky process. Use it as a warehouse, as a source, or as both at once. One adapter instance can serve discovery and execution together, because both sides read the same database.

## Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `path` | string | No | (in-memory) | Path to a persistent DuckDB file. Required when using the same DuckDB adapter for both discovery and execution, so the discovery side sees rows written by the warehouse side. |
| `extension_directory` | string | No | (DuckDB's default, under the home directory) | Directory DuckDB keeps its extensions in. Rocky sets DuckDB's `extension_directories` list to this one directory at connect. Every extension Rocky loads uses it. An empty string counts as unset. A relative path resolves against the working directory of the `rocky` process, not against `rocky.toml`. |
| `offline` | bool | No | `false` | Rocky never runs `INSTALL` and turns DuckDB's auto-install off. See [Offline mode](#offline-mode). |
| `attach` | list of tables | No | (none) | Iceberg REST and DuckLake catalogs to attach on connect. See [Iceberg catalogs](#iceberg-catalogs) and [DuckLake catalogs](#ducklake-catalogs). |

Pick one. Each example is a complete adapter declaration, not a complete `rocky.toml`.

**In-memory.** DuckDB opens an in-memory database instead of a database file.

```toml
[adapter.local]
type = "duckdb"
```

**Persistent file.**

```toml
[adapter.local]
type = "duckdb"
path = "warehouse.duckdb"
```

## Offline mode

By default, Rocky loads DuckDB's `icu` extension at connect. `icu` provides `current_date` and the time-zone functions. If `icu` is not in the extension directory, DuckDB downloads it. On a machine with no network, that download fails, Rocky logs a warning and carries on, and a query that uses a time-zone function then fails.

Set `offline = true` to make that case explicit. DuckDB's auto-install is turned off, and Rocky never runs `INSTALL`. It runs `LOAD icu` from the extension directory only. If `icu` is not there, the connect fails with an error that names the directory and says how to fill it.

```toml
[adapter.local]
type = "duckdb"
extension_directory = "/opt/rocky/duckdb-extensions"
offline = true
```

Fill the directory once, on a machine with network access:

```sql
SET extension_directories = ['/opt/rocky/duckdb-extensions'];
INSTALL icu;
```

Rocky sets that path as DuckDB's `extension_directories` (a list of one path). DuckDB 2.0 deprecates the older `extension_directory` setting. The Rocky config key stays `extension_directory`.

Offline mode applies to every DuckDB connection Rocky opens for the project: the adapter itself, and the local in-memory databases of `rocky test`, `rocky ci`, `rocky compile` (the seed it runs for source schemas), `rocky publish-ir --with-seed` and `rocky replay --execute`. These read the settings of the `type = "duckdb"` adapters in `rocky.toml`. With several DuckDB adapters, the local database is offline when any of them sets `offline = true`, and uses the first `extension_directory` found. For `rocky compile`, a missing extension only logs a warning, and compile goes on without the seed schemas.

Offline mode limits what Rocky does. It does not sandbox a model's own SQL: a model that runs `INSTALL` or `SET autoinstall_known_extensions = true` can still reach the network. `rocky validate` warns with `V059` when `extension_directory` or `offline` is set on an adapter whose `type` is not `duckdb`, because no other adapter reads them.

One path stays online. `rocky playground` seeds its new project database before a `rocky.toml` exists, so it uses DuckDB's defaults.

Then copy the directory to the offline machine. DuckDB stores files under `<version>/<platform>/`, so the directory must come from the same DuckDB build that Rocky runs. The connect error prints the version and platform it expects.

## Iceberg catalogs

A DuckDB adapter can write Apache Iceberg tables through an Iceberg REST catalog, such as Lakekeeper or Polaris. List each catalog under `[[adapter.<name>.attach]]`. A model writes to it by naming its `alias` as the target catalog.

```toml
[adapter.lake]
type = "duckdb"

[[adapter.lake.attach]]
alias = "lk"
type = "iceberg"
warehouse = "demo"
endpoint = "${ICEBERG_ENDPOINT}"
authorization = "none"

[adapter.lake.attach.s3]
key_id = "${S3_KEY_ID}"
secret = "${S3_SECRET}"
endpoint = "localhost:8333"
url_style = "path"
use_ssl = false
region = "us-east-1"
scope = "s3://warehouse"
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `alias` | string | Yes | The catalog name models target. A SQL identifier. Not `memory`, `main`, `temp` or `system`, not the database file's own catalog name, and not another entry's alias. |
| `type` | string | Yes | `"iceberg"`. |
| `warehouse` | string | Yes | The warehouse name the REST catalog knows. |
| `endpoint` | string | Yes | The REST catalog's base URL. |
| `authorization` | string | No | `"none"`, the only value today. |
| `s3` | table | No | `key_id`, `secret`, `endpoint`, `url_style` (`"path"` or `"vhost"`), `use_ssl`, `region`, `scope`. Rocky turns it into a temporary DuckDB secret named `rocky_attach_<alias>`, never a persistent one. Without `scope`, the secret applies to every `s3://` path, including a model's own `read_parquet('s3://…')`. With more than one attachment, each `s3` table needs a `scope`. |

`rocky run` and every other command that connects load DuckDB's `httpfs` and `iceberg` extensions, create the secret and attach the catalog. With `offline = true`, both extensions must already be in the extension directory. `rocky compile`, `rocky plan` and `rocky emit-sql` do not connect, but they know the aliases, so they render the same SQL `rocky run` executes.

What Rocky writes to an attached catalog:

- **`full_refresh`.** A first build is `CREATE TABLE … AS`. Later runs are one transaction:

  ```
  BEGIN TRANSACTION
    CREATE OR REPLACE TEMP TABLE temp.main.__rocky_overwrite AS <model SQL>
    ALTER TABLE … ADD COLUMN / DROP COLUMN / ALTER COLUMN … TYPE   (only when the columns changed)
    DELETE FROM <table>
    INSERT INTO <table> BY NAME SELECT * FROM temp.main.__rocky_overwrite
  COMMIT                              any failure → ROLLBACK; the old table and rows stay
  ```

  The model's rows are held in a temporary table first, so a model that reads its own table sees the old rows, as with `CREATE OR REPLACE` elsewhere. The temporary table uses DuckDB's memory and spills to disk when it is large.

  Types are compared the way Iceberg stores them (`TINYINT`, `SMALLINT`, `UTINYINT` and `USMALLINT` are `INTEGER`; `UINTEGER` is `BIGINT`; `HUGEINT` is `DECIMAL(38,0)`), so a re-run with no change runs no `ALTER`. A type may widen from `INTEGER` to `BIGINT`, from `FLOAT` to `DOUBLE`, or to a wider `DECIMAL` precision. Any other type change, a nested type change, a new nested column (`LIST`, `STRUCT` or `MAP`; DuckDB cannot add one to an Iceberg table), or dropping a partition column fails the model and leaves the table as it was. If a `COMMIT` fails without saying why, the error says the catalog may have committed it; running the model again is safe. New columns go at the end of the table. A commit conflict with another writer runs the transaction again, up to 3 times.
- **`incremental` without `unique_key`.** Appends, like on a plain DuckDB file.

Everything else is refused before any model runs, with an error that names the strategy and the catalog: views, `merge`, `incremental` with a `unique_key` or a `lookback`, `delete_insert`, `time_interval`, `microbatch`, snapshots, quarantine, branch promote, seeds, `rocky load`, a lakehouse `format`, and a replication `full_refresh` of an existing table.

The [`07-adapters/09-duckdb-iceberg-rest`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/07-adapters/09-duckdb-iceberg-rest) POC runs this against a local Lakekeeper and SeaweedFS in Docker, with no credentials.

## DuckLake catalogs

A DuckDB adapter can also write [DuckLake](https://ducklake.select/) tables. DuckLake keeps table data as Parquet files under a data path and its catalog in a database; Rocky supports a local DuckDB file as that database. Add a `type = "ducklake"` entry to the same `[[adapter.<name>.attach]]` list:

```toml
[adapter.lake]
type = "duckdb"
path = "warehouse.duckdb"

[[adapter.lake.attach]]
alias = "dl"
type = "ducklake"
metadata_path = "lake/metadata.ducklake"
data_path = "lake/data/"
```

| Key | Type | Required | Notes |
|---|---|---|---|
| `alias` | string | Yes | The catalog name models target. The same rules as an Iceberg alias, and it must not start with `__ducklake_metadata_`. |
| `type` | string | Yes | `"ducklake"`. |
| `metadata_path` | string | Yes | The DuckDB file that holds the catalog. It is created on the first attach. It must not be the adapter's own `path`, and it must not hold a `:` (`sqlite:`, `postgres:` and `:memory:` are not supported). |
| `data_path` | string | No | A local directory or an `s3://` prefix. When unset, DuckLake uses the path it stored when the catalog was created (`<metadata_path>.files/` for a new one). |
| `s3` | table | No | Only with an `s3://` `data_path`. The same keys as for Iceberg; `scope` defaults to `data_path`. |

Rocky makes a relative `metadata_path` and `data_path` absolute against the working directory before it attaches. DuckLake stores the data path as written, and a relative one would resolve differently in each later process. For the same reason Rocky refuses a lake whose stored data path is relative. Rocky never overrides the stored data path, so a different `data_path` for an existing lake fails at attach.

`rocky run` loads the `ducklake` extension (and `httpfs` for an `s3://` data path). With `offline = true`, they must already be in the extension directory.

What Rocky writes to an attached DuckLake catalog:

- **`full_refresh`** is `CREATE OR REPLACE TABLE … AS`, one atomic snapshot, as on a plain DuckDB file.
- **`ephemeral`, `incremental`, `merge`, `delete_insert`, `time_interval`** and replication work as on a plain DuckDB file.

Refused before any model runs, with an error that names the strategy and the catalog: `view` (a view stored in DuckLake looks up its table names in the lake, not the main database), `materialized_view`, `dynamic_table`, `microbatch`, `content_addressed`, snapshots, a lakehouse `format`, seeds, `rocky load`, quarantine and branch promote.

Things to know:

- A DuckDB-file catalog has one writer at a time. A second process fails at attach with `Could not set lock on file`.
- A write of 10 rows or fewer stays in the metadata file, with no Parquet file.
- DuckLake keeps old Parquet files until snapshots expire. Rocky expires nothing; run DuckLake's `ducklake_expire_snapshots` and `ducklake_cleanup_old_files` yourself.
- Discovery never reads an attached catalog: its schemas are not replication sources.

The [`07-adapters/10-duckdb-ducklake`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/07-adapters/10-duckdb-ducklake) POC runs this with no server, no Docker and no credentials.

## Authentication

None. DuckDB runs in-process. An attached Iceberg catalog uses `authorization = "none"`; OAuth2 and vended credentials are not supported yet.

## See also

- [`[adapter.NAME]`](/reference/configuration/#adaptername) — fields shared by every adapter type, including the retry policy.
