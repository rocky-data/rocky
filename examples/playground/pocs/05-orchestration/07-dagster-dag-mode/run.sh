#!/usr/bin/env bash
# Dagster dag_mode — end-to-end demo
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

echo "=== Dagster dag_mode — full lineage from Rocky's unified DAG ==="
echo ""
echo "This POC verifies that rocky dag produces a connected DAG and that"
echo "dagster-rocky's dag_mode can consume it."
echo ""

# --- Cleanup ---
rm -f poc.duckdb .rocky-state.redb
mkdir -p expected

# --- Seed ---
echo "1. Seeding DuckDB with sample data..."
duckdb poc.duckdb < data/seed.sql
echo "   done (200 orders in raw__orders.orders)"

# --- Rocky pipeline (replication) ---
echo ""
echo "2. Running replication pipeline..."
rocky -c rocky.toml run --pipeline ingest --filter source=orders --output json > expected/run.json 2>/dev/null
echo "   done (source tables replicated)"

# --- Rocky DAG ---
echo ""
echo "3. Rocky DAG output:"
rocky -c rocky.toml dag --models models --output json 2>/dev/null | tee expected/dag.json | python3 -c "
import json, sys
dag = json.load(sys.stdin)
print(f'   Nodes: {dag[\"summary\"][\"total_nodes\"]}')
print(f'   Edges: {dag[\"summary\"][\"total_edges\"]}')
print(f'   Layers: {dag[\"summary\"][\"execution_layers\"]}')
print()
for node in dag['nodes']:
    deps = node.get('depends_on', [])
    dep_str = f' <- {deps}' if deps else ''
    print(f'   [{node[\"kind\"]:16s}] {node[\"label\"]}{dep_str}')
"

# --- Rocky compile (verify models) ---
echo ""
echo "4. Compiling models..."
rocky compile --models models --output json 2>/dev/null | tee expected/compile.json | python3 -c "
import json, sys
result = json.load(sys.stdin)
print(f'   Models: {result[\"models\"]}')
print(f'   Layers: {result[\"execution_layers\"]}')
for m in result.get('models_detail', []):
    deps = m.get('depends_on', [])
    dep_str = f' depends_on={deps}' if deps else ''
    print(f'   - {m[\"name\"]} ({m[\"strategy\"][\"type\"]}){dep_str}')
"

# --- Dagster instructions ---
echo ""
echo "=== Dagster smoke test ==="
echo ""
echo "To test dag_mode in the Dagster UI:"
echo ""
echo "  cd $HERE"
echo "  uv sync && uv run dg dev"
echo ""
echo "Then open http://localhost:3000 and verify:"
echo "  1. Asset graph shows: source:ingest -> load:ingest -> stg_orders -> fct_customer_revenue"
echo "  2. Dependencies are connected (click any node to see upstream/downstream)"
echo "  3. 'Materialize all' executes successfully"
echo ""

# NOTE: poc.duckdb and .rocky-state.redb are left in place so
# `uv run dg dev` can load the cached state. Clean up manually
# with: rm -f poc.duckdb .rocky-state.redb
echo "POC complete. Run 'uv sync && uv run dg dev' to open the Dagster UI."
