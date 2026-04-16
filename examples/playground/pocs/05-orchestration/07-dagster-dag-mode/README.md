# 07-dagster-dag-mode — Full Lineage from Rocky's Unified DAG

> **Category:** 05-orchestration
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** rocky dag, dag_mode, dagster-rocky, lineage

## What it shows

Demonstrates `dag_mode=True` on `RockyComponent`: Rocky reports the full unified DAG (source, load, transformation stages) via `rocky dag`, and dagster-rocky renders it as a single connected asset graph in the Dagster UI with zero hand-written asset definitions.

## Why it's distinctive

- `rocky.toml` + model sidecars + SQL files **are** the asset definition. No Python per-asset boilerplate.
- The Dagster asset graph shows the full pipeline lineage: source → load → staging → gold.

## Layout

```
.
├── README.md              this file
├── definitions.py         Dagster code location (< 10 lines)
├── rocky.toml             Pipeline config: ingest (replication) + transform
├── run.sh                 Seeds data, runs pipeline, prints DAG
├── data/
│   └── seed.sql           200-row orders table
└── models/
    ├── _defaults.toml     Shared target defaults
    ├── stg_orders.sql     Staging model (filters cancelled orders)
    ├── stg_orders.toml    Target: staging schema
    ├── fct_customer_revenue.sql   Gold model (customer aggregation)
    └── fct_customer_revenue.toml  Target: gold schema, depends_on stg_orders
```

## Prerequisites

- `rocky` on PATH (>= 1.1.0)
- `duckdb` CLI for seeding (`brew install duckdb`)
- `dagster-rocky` >= 1.2.0 (for `dag_mode`)
- `dagster` >= 1.13.0

## Run

```bash
# Verify the DAG structure
./run.sh

# Launch Dagster UI
uv run dg dev
```

Open http://localhost:3000 and verify:
1. Asset graph shows `source:ingest` → `load:ingest` → `stg_orders` → `fct_customer_revenue`
2. Click any node to see upstream/downstream dependencies
3. "Materialize all" executes the full pipeline

## Expected output

```text
=== Dagster dag_mode — full lineage from Rocky's unified DAG ===

1. Seeding DuckDB with sample data...
   done (200 orders in raw__orders.orders)

2. Running replication pipeline...
   done (source tables replicated)

3. Rocky DAG output:
   Nodes: 4
   Edges: 3
   Layers: 4

   [source          ] ingest (source)
   [load            ] ingest (load) <- ['source:ingest']
   [transformation  ] stg_orders <- ['load:ingest']
   [transformation  ] fct_customer_revenue <- ['transformation:stg_orders']

4. Compiling models...
   Models: 2
   Layers: 2
   - stg_orders (full_refresh)
   - fct_customer_revenue (full_refresh) depends_on=['stg_orders']
```

## What happened

1. `run.sh` seeds a DuckDB file with 200 orders
2. `rocky run` replicates the source table via the `ingest` pipeline
3. `rocky dag` produces the full unified DAG: 4 nodes, 3 edges, 4 execution layers
4. `rocky compile` verifies model dependencies (`fct_customer_revenue` depends on `stg_orders`)
5. `definitions.py` uses `RockyComponent(dag_mode=True)` — Dagster reads the cached DAG and builds a connected asset graph automatically

## Related

- dagster-rocky: [`integrations/dagster/`](https://github.com/rocky-data/rocky/tree/main/integrations/dagster)
- DAG builder: [`integrations/dagster/src/dagster_rocky/dag_assets.py`](https://github.com/rocky-data/rocky/tree/main/integrations/dagster/src/dagster_rocky/dag_assets.py)
- Basic Dagster example: [`engine/examples/dagster-integration/`](https://github.com/rocky-data/rocky/tree/main/engine/examples/dagster-integration)
