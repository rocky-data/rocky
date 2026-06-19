# 12-catalog-emit — `rocky catalog`: column-level lineage as JSON + Parquet

![rocky catalog walks the SemanticGraph and writes catalog.json + edges.parquet + assets.parquet under .rocky/catalog/](../../../../../docs/public/demo-rocky-catalog.gif)

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky catalog`, `--format json|parquet|both`, `--out`, state-store enrichment, per-column docs (`[columns.<name>]` → `CatalogColumn.description`)

## What it shows

`rocky catalog` walks the compiler's SemanticGraph and writes a
self-contained, machine-consumable lineage artefact under
`./.rocky/catalog/`:

- `catalog.json` — the front door. Project metadata, project-level
  `last_run_id`, an `assets[]` array (one per model with its columns +
  upstream/downstream models), and an `edges[]` array of column-level
  lineage edges including transform kind (`direct`, `aggregation: sum`,
  …) and confidence. Each asset column carries a `description` field when
  the model's sidecar documents it via a `[columns.<name>]` table.
- `edges.parquet` — flat table of column-lineage edges. Any Parquet
  reader (DuckDB, Polars, Spark, BigQuery external table, Athena…) can
  query it without going through the engine.
- `assets.parquet` — flat table of asset columns with `last_run_id` /
  `last_materialized_at` enrichment populated when the state store has a
  matching successful run.

## Why it's distinctive

- **The catalog is a *queryable artefact*, not a JSON dump.** dbt's
  `target/manifest.json` is a single 100K+ line monster; Rocky emits
  a flat Parquet table you can query with `read_parquet('.../edges.parquet')`.
- **Column-level lineage is compiler-derived, not regex-derived.**
  Aggregations, joins, and DSL-based group-bys all carry through with
  semantic transform kinds (`aggregation: sum`, `direct`, …) instead of
  table-to-table edges only.
- **No engine on the read path.** Once the catalog is written, lineage
  consumers (Looker, Superset, custom dashboards) read Parquet directly.
- **Per-column docs ride along.** A `[columns.<name>]` table in a model's
  sidecar attaches a description to one *projected* output column, and it
  surfaces as `CatalogColumn.description` in `rocky catalog --output json`.
  A description keyed to a column the SELECT doesn't produce is silently
  dropped, so the keys stay honest about the real output schema.

## Layout

```
.
├── README.md           this file
├── rocky.toml          DuckDB transformation pipeline
├── run.sh              compile → catalog → inspect
├── data/seed.sql       small `raw__orders.orders` seed (200 rows)
└── models/
    ├── _defaults.toml      catalog=poc, schema=demo
    ├── raw_orders.sql      SELECT … FROM raw__orders.orders
    ├── stg_orders.sql      filter cancelled
    ├── fct_revenue.sql     group by customer_id → SUM(amount), COUNT(order_id)
    └── fct_revenue.toml    [columns.total] / [columns.orders] docs the SUM/COUNT outputs
```

## Prerequisites

- `rocky` ≥ 1.25.0 on PATH
- `duckdb` CLI (only used in the demo to read back the Parquet file)
- `python3` (used to pretty-print the JSON in `run.sh`)

## Run

```bash
./run.sh
```

## What happened

1. **Compile** — semantic graph for 3 models; column types propagate
   from `raw_orders` → `stg_orders` → `fct_revenue`.
2. **`rocky catalog --format both`** — walks the SemanticGraph and
   writes the three artefacts under `.rocky/catalog/`.
3. **Inspect** — the demo prints the first six column-lineage edges,
   shows each asset's columns + upstream models, then queries
   `edges.parquet` directly via DuckDB to prove the artefact is
   tool-agnostic.
4. **Per-column docs** — `fct_revenue.toml` carries `[columns.total]` and
   `[columns.orders]` descriptions (keyed to the aggregated output columns,
   not the raw inputs). The demo re-emits `catalog.json`, prints every
   column with a description, then asserts `fct_revenue.total.description`
   is populated in `rocky catalog --output json`.

## Related

- Engine source: `engine/crates/rocky-cli/src/commands/catalog.rs`
- Sibling POC:
  [`01-lineage-column-level/`](../01-lineage-column-level/), which runs `rocky
  lineage` for interactive single-model lineage queries (the catalog is
  the bulk-export equivalent).
- Sibling POC:
  [`11-lineage-diff/`](../11-lineage-diff/), which runs `rocky lineage-diff` for
  PR-time blast radius (compares two refs at compile time, doesn't
  persist).
