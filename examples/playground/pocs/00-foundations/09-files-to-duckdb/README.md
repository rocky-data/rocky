# 09-files-to-duckdb — Load Parquet, CSV, and JSONL into DuckDB

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky load`, load pipeline, format auto-detect, DuckDB adapter

## What it shows

A single `type = "load"` pipeline ingests three different file formats — Parquet,
CSV, and JSONL — from one `data/` directory in one `rocky load` invocation.
No `format = ...` is set, so each file is dispatched by extension: `.parquet`
goes through DuckDB's `read_parquet()`, `.csv` through `read_csv_auto()`, and
`.jsonl` through `read_json_auto()`. Each file becomes a DuckDB table named
after its file stem.

## Why it's distinctive

- Format-agnostic ingest. Drop heterogeneous files into `data/`; Rocky picks
  them up and routes each through the warehouse's native scanner. No `pip
  install pandas`, no staging buckets, no per-format pipelines.
- The same `[pipeline.<name>]` shape works against Snowflake / Databricks /
  BigQuery: swap the `[adapter]` block and re-run. The `LoaderAdapter` trait
  guarantees each warehouse handles all three formats via its own bulk-load
  primitive (`COPY INTO`, `LOAD DATA`, etc.).

## Layout

```
.
├── README.md             this file
├── rocky.toml            load pipeline (no format set → auto-detect)
├── run.sh                stage data/ → rocky load → query results
├── seeds/
│   ├── orders.csv        10 rows — minted into Parquet at run time
│   ├── customers.csv     5 rows — copied into data/ as-is
│   └── events.jsonl      6 rows — copied into data/ as-is
└── data/                 staged at run time (orders.parquet + customers.csv + events.jsonl)
```

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI (`brew install duckdb`) — mints `orders.parquet` from the seed
  CSV and queries the loaded tables back

## Run

```bash
./run.sh
```

## Expected output

```text
=== load (Parquet + CSV + JSONL → DuckDB, format auto-detected) ===
{
  "command": "load",
  "source_dir": "data/",
  "format": "auto",
  "files_loaded": 3,
  "total_rows": 21,
  "files": [
    { "file": "data/customers.csv",   "target": "main.customers", "rows_loaded":  5 },
    { "file": "data/events.jsonl",    "target": "main.events",    "rows_loaded":  6 },
    { "file": "data/orders.parquet",  "target": "main.orders",    "rows_loaded": 10 }
  ]
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

## What happened

1. `duckdb :memory:` reads `seeds/orders.csv` and writes `data/orders.parquet`;
   `customers.csv` and `events.jsonl` are copied into `data/` unchanged.
2. `rocky validate` confirms the load pipeline parses cleanly.
3. `rocky load` calls `discover_files(data/, None)` which keeps any path whose
   extension is in `{csv, parquet, jsonl}` and dispatches each to the DuckDB
   loader, which issues `CREATE TABLE main.<stem> AS SELECT * FROM
   read_<format>('<path>')`.
4. The DuckDB query proves all three tables are materialized with the correct
   row counts.

## Related

- `rocky load` source: [`engine/crates/rocky-cli/src/commands/load.rs`](../../../../engine/crates/rocky-cli/src/commands/load.rs)
- DuckDB loader: [`engine/crates/rocky-duckdb/src/loader.rs`](../../../../engine/crates/rocky-duckdb/src/loader.rs) (`load_sql` + `create_table_sql`)
- Same shape against Snowflake or Databricks: swap the `[adapter]` block; the
  pipeline definition is unchanged.
