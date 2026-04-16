"""Dagster dag_mode smoke test.

Demonstrates Rocky's DAG-driven asset builder: a single `rocky dag` call
produces the full connected asset graph in Dagster — sources through
transformations — with zero hand-written Python asset definitions.

Run:
    cd examples/playground/pocs/05-orchestration/07-dagster-dag-mode
    ./run.sh          # seeds data + caches state
    dagster dev -f definitions.py

Then open http://localhost:3000 and verify:
    1. The asset graph shows: source:ingest → load:ingest → stg_orders → fct_customer_revenue
    2. Clicking any asset shows its upstream/downstream dependencies
    3. "Materialize all" runs the full pipeline successfully
"""

from __future__ import annotations

from dagster_rocky import RockyComponent

# dag_mode=True: builds assets from `rocky dag` instead of the
# discover-only flow. Every pipeline stage (source, load, model)
# becomes a Dagster asset with resolved dependencies.
component = RockyComponent(
    binary_path="rocky",
    config_path="rocky.toml",
    state_path=".rocky-state.redb",
    models_dir="models",
    dag_mode=True,
)

defs = component.build_defs()
