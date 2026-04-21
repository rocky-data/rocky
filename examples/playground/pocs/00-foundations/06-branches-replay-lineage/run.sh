#!/usr/bin/env bash
# Arc 1 — branches + replay + column-level lineage
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

rm -f .rocky-state.redb poc.duckdb
mkdir -p expected

echo "==> 1. Compile the 3-model DAG (type-check, no warehouse required)"
rocky compile --models models > expected/compile.json

echo "==> 2. Seed raw__orders.orders into DuckDB"
duckdb poc.duckdb < data/seed.sql

echo "==> 3. Run replication on main (writes poc.staging__orders.orders)"
rocky -c rocky.toml -o json run --filter source=orders > expected/run_main.json

echo "==> 4. Create a named branch 'fix_revenue' (state-store entry, no warehouse side effects)"
rocky branch create fix_revenue \
    --description "Experiment with a revised revenue definition" \
    > expected/branch_create.json
rocky branch list > expected/branch_list.json
rocky branch show fix_revenue > expected/branch_show.json

echo "==> 5. Run replication against the branch (writes poc.branch__fix_revenue.orders)"
rocky -c rocky.toml -o json run --filter source=orders --branch fix_revenue \
    > expected/run_branch.json

echo "==> 6. Replay — inspect the last recorded run (SQL hashes, per-model status, cost)"
rocky replay latest > expected/replay_latest.json
rocky history > expected/history.json
rocky cost latest > expected/cost_latest.json

echo "==> 7. Column-level lineage — downstream blast radius of raw_orders.amount"
rocky lineage raw_orders --models models --column amount --downstream \
    > expected/lineage_downstream.json

echo
echo "==> Warehouse tables after the demo:"
duckdb poc.duckdb <<'SQL'
SELECT table_schema, table_name FROM information_schema.tables
WHERE table_catalog = 'poc' ORDER BY table_schema, table_name;
SQL

echo
echo "POC complete: branch + run + branch-run + lineage captured in expected/."
