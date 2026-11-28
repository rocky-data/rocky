# 13-duckdb-ducklake — DuckLake tables through DuckDB

> **Credentials:** none (the live run downloads DuckDB's `ducklake` extension once)
> **Runtime:** a few seconds after the first extension download

## What it shows

A DuckDB adapter attaches a [DuckLake](https://ducklake.select/) catalog as `dl`: metadata in a local DuckDB file, data as Parquet under `lake/data/`. No server. Four models cover `full_refresh`, append `incremental`, `merge` and `delete_insert`.

`CREATE OR REPLACE TABLE` is one atomic DuckLake snapshot, so a failed full refresh keeps the old table and a model reading its own target sees the old rows.

Rocky makes relative `metadata_path` and `data_path` absolute before attaching: DuckLake stores the data path as written and resolves it against each later process's working directory.

## Caveats

- Refused on a DuckLake catalog, before any SQL runs, naming the strategy and catalog: `view` (a lake view binds table names against the lake, not the main database), `materialized_view`, `dynamic_table`, `microbatch`, `content_addressed`, `snapshot`, a lakehouse `format`, seeds, `rocky load`, quarantine and branch/promote. See [`docs/adr/ADR-DUCKLAKE.md`](../../../../../docs/adr/ADR-DUCKLAKE.md).
- One process writes the lake at a time (the metadata is a DuckDB file). A second fails at attach with `Could not set lock on file`.
- Old Parquet files stay until snapshots expire. Rocky expires nothing; use `ducklake_expire_snapshots` and `ducklake_cleanup_old_files`.

## Run

```bash
./run.sh
```

## Expected output

`run.sh` validates, compiles and emits SQL, runs the pipeline twice on a fresh lake, checks for Parquet under `lake/data/`, and confirms a view model is refused:

```text
    refused: view is not supported on catalog 'dl'

POC complete: Rocky wrote, replaced and merged DuckLake tables through DuckDB.
```
