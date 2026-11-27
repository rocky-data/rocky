# 07-dagster-dag-mode — Full Lineage from Rocky's Unified DAG

> **Category:** 05-orchestration
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** rocky dag, dag_mode, dagster-rocky, lineage, surface_column_lineage

## What it shows

With `dag_mode: true` on `RockyComponent`
(`src/rocky_dag_mode_poc/defs/rocky/defs.yaml`), dagster-rocky renders the
`rocky dag` output (source, load, transformation) as one asset graph with no
hand-written asset definitions.

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI for seeding (`brew install duckdb`)
- `uv` for dependency management

## Run

```bash
# Verify the DAG structure
./run.sh

# Install deps + launch Dagster UI
uv sync && uv run dg dev
```

Open http://localhost:3000 and verify:
1. Asset graph shows `source:ingest` → `load:ingest` → `stg_orders` → `fct_customer_revenue`
2. Click any node to see upstream/downstream dependencies
3. Open `fct_customer_revenue` → the **Column lineage** tab shows per-column upstream edges back to `stg_orders` (surfaced via `surface_column_lineage: true` in `defs.yaml`, which calls `rocky lineage` per model at component load)
4. "Materialize all" executes the full pipeline

## Expected output

```text
3. Rocky DAG output:
   Nodes: 4
   Edges: 3
   Layers: 4

   [source          ] ingest (source)
   [load            ] ingest (load) <- ['source:ingest']
   [transformation  ] fct_customer_revenue <- ['transformation:stg_orders']
   [transformation  ] stg_orders <- ['load:ingest']
...
```

## Related

- dagster-rocky: [`integrations/dagster/`](https://github.com/rocky-data/rocky/tree/main/integrations/dagster)
- Basic Dagster example: [`engine/examples/dagster-integration/`](https://github.com/rocky-data/rocky/tree/main/engine/examples/dagster-integration)
