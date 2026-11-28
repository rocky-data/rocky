# 09-files-to-duckdb — Load Parquet, CSV, and JSONL into DuckDB

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky load`, load pipeline, format auto-detect, DuckDB adapter

## What it shows

One `type = "load"` pipeline ingests Parquet, CSV and JSONL from `data/` in a single `rocky load`. With no `format = ...` set, each file is dispatched by extension (`read_parquet()`, `read_csv_auto()`, `read_json_auto()`) into a table named after its file stem. `run.sh` first stages `data/` from `seeds/`.

The same pipeline shape works against Snowflake, Databricks and BigQuery by swapping the `[adapter]` block. **Limit:** BigQuery loads local files as CSV only. Parquet and JSONL need a `gs://` path.

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI (`brew install duckdb`)
- Network access on first run: DuckDB autoloads its `parquet` and `json` extensions from `extensions.duckdb.org`.

> **Offline note:** if the extension download is blocked, the `.parquet` and `.jsonl` loads fail (only the `.csv` loads) and `rocky load` exits non-zero, so the run stops before the query step. Run once with network to cache the extensions; after that the demo works offline.

## Run

```bash
./run.sh
```

## Expected output

Shape of a successful run (byte and `duration_ms` values vary; the Parquet size depends on DuckDB's writer):

```text
=== load (Parquet + CSV + JSONL → DuckDB, format auto-detected) ===
{
  "version": "<rocky version>",
  "command": "load",
  "source_dir": "data/",
  "format": "auto",
  "files_loaded": 3,
  "files_failed": 0,
  "total_rows": 21,
  "total_bytes": 2180,
  "files": [
    { "file": "data/customers.csv",  "target": "main.customers", "rows_loaded":  5, "bytes_read":  268, "duration_ms": 15 },
    { "file": "data/events.jsonl",   "target": "main.events",    "rows_loaded":  6, "bytes_read":  693, "duration_ms": 12 },
    { "file": "data/orders.parquet", "target": "main.orders",    "rows_loaded": 10, "bytes_read": 1219, "duration_ms": 18 }
  ],
  "duration_ms": 60
}

=== query loaded tables ===
┌──────────────┬───────────┐
│ source_table │ row_count │
├──────────────┼───────────┤
│ customers    │         5 │
│ events       │         6 │
│ orders       │        10 │
└──────────────┴───────────┘
```

## Related

- [`engine/crates/rocky-cli/src/commands/load.rs`](../../../../../engine/crates/rocky-cli/src/commands/load.rs)
- [`engine/crates/rocky-duckdb/src/loader.rs`](../../../../../engine/crates/rocky-duckdb/src/loader.rs)
