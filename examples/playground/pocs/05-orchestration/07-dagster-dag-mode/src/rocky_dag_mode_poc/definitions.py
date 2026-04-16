"""Dagster dag_mode smoke test.

Demonstrates Rocky's DAG-driven asset builder: a single `rocky dag` call
produces the full connected asset graph in Dagster — sources through
transformations — with zero hand-written Python asset definitions.

The RockyComponent is configured in defs/rocky/defs.yaml with
``dag_mode: true``. This definitions.py just loads the component folder.

Run:
    cd examples/playground/pocs/05-orchestration/07-dagster-dag-mode
    ./run.sh          # seeds data + caches state
    uv sync && uv run dg dev

Then open http://localhost:3000 and verify:
    1. The asset graph shows: source:ingest -> load:ingest -> stg_orders -> fct_customer_revenue
    2. Clicking any asset shows its upstream/downstream dependencies
    3. "Materialize all" runs the full pipeline successfully
"""

from pathlib import Path

from dagster.components import load_from_defs_folder

defs = load_from_defs_folder(path_within_project=Path(__file__).parent)
