# 12-catalog-emit — `rocky catalog`: column-level lineage as JSON + Parquet

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky catalog`, `--format json|parquet|both`, `--out`, state-store enrichment, per-column docs (`[columns.<name>]` → `CatalogColumn.description`)

## What it shows

`rocky catalog` walks the compiler's SemanticGraph and writes lineage artefacts under `./.rocky/catalog/`:

- `catalog.json` — project metadata, project-level `last_run_id`, an `assets[]` array (one per model: columns, upstream/downstream models) and an `edges[]` array of column-level edges with transform kind (`direct`, `aggregation: sum`, …) and confidence.
- `edges.parquet` — flat table of column-lineage edges, readable by any Parquet reader without the engine.
- `assets.parquet` — flat table of asset columns; `last_run_id` / `last_materialized_at` are populated when the state store has a matching successful run.

A `[columns.<name>]` table in a model's sidecar documents a *projected* output column; it surfaces as `CatalogColumn.description` in `rocky catalog --output json`. A description keyed to a column the SELECT doesn't produce is silently dropped.

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI (to read back the Parquet file)
- `python3` and `jq`

## Run

```bash
./run.sh
```

## What to expect

The script compiles 3 models, runs `rocky catalog --format both`, prints the first six column-lineage edges and each asset's columns and upstream models, queries `edges.parquet` with DuckDB, then prints the column descriptions from `fct_revenue.toml` (`[columns.total]`, `[columns.orders]`) and asserts `fct_revenue.total.description` is populated.

## Related

- Engine source: `engine/crates/rocky-cli/src/commands/catalog.rs`
- [`01-lineage-column-level/`](../01-lineage-column-level/): interactive single-model queries with `rocky lineage`.
- [`11-lineage-diff/`](../11-lineage-diff/): `rocky lineage-diff` compares two refs at compile time and doesn't persist.
