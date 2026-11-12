# 10-duckdb-ducklake — DuckLake tables through DuckDB

> **Category:** 07-adapters
> **Credentials:** none (the live run downloads DuckDB's `ducklake` extension once)
> **Runtime:** a few seconds after the first extension download
> **Rocky features:** `[[adapter.<name>.attach]]` with `type = "ducklake"`, atomic full refresh, append incremental, merge, delete_insert

## What it shows

A `type = "duckdb"` adapter attaches a [DuckLake](https://ducklake.select/)
catalog as `dl`. Its metadata is a local DuckDB file and its data is Parquet
files under `lake/data/`. Every model whose target catalog is `dl` is a
DuckLake table. There is no server and no vendor warehouse.

```
rocky run ──▶ DuckDB (warehouse.duckdb) ──ATTACH 'ducklake:…'──▶ lake/metadata.ducklake
                    │
                    └────────── Parquet data files ──▶ lake/data/
```

## Why it's distinctive

DuckLake's `CREATE OR REPLACE TABLE` is one atomic snapshot. So a full
refresh is the same statement as on plain DuckDB: a failed one keeps the old
table, and a model that reads its own target sees the old rows.

Rocky makes a relative `metadata_path` and `data_path` absolute before it
attaches. DuckLake stores the data path as written and resolves a relative
one against each later process's working directory, which would put the
files in a different place per run.

## Layout

| File | Purpose |
|---|---|
| `rocky.toml` | A DuckDB adapter with one `type = "ducklake"` attachment |
| `models/orders.sql` | `full_refresh`, 100 rows (more than 10, so DuckLake writes Parquet) |
| `models/order_log.sql` | Append `incremental` on `ordered_at` |
| `models/customer_totals.sql` | `merge` on `customer` |
| `models/orders_by_customer.sql` | `delete_insert` by `customer` |
| `run.sh` | Two runs, a Parquet check, and a refused view model |

## What v1 refuses on a DuckLake catalog

`view` (a lake view binds its table names against the lake, not the main
database), `materialized_view`, `dynamic_table`, `microbatch`,
`content_addressed`, `snapshot`, a lakehouse `format`, seeds, `rocky load`,
quarantine and branch/promote. Each is refused before any SQL runs, with an
error that names the strategy and the catalog. See
[`docs/adr/ADR-DUCKLAKE.md`](../../../../../docs/adr/ADR-DUCKLAKE.md).

## Run

```bash
./run.sh
```

## Notes

- The metadata is a DuckDB file, so one process writes the lake at a time.
  A second process fails at attach with `Could not set lock on file`.
- DuckLake keeps old Parquet files until snapshots expire. Rocky expires
  nothing; use DuckLake's `ducklake_expire_snapshots` and
  `ducklake_cleanup_old_files`.
