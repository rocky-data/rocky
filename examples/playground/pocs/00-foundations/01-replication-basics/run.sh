#!/usr/bin/env bash
# 01-replication-basics — source → staging replication via schema patterns.
set -euo pipefail

export ROCKY_SUPPRESS_DEPRECATION=1

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

mkdir -p expected
rm -f .rocky-state.redb playground.duckdb

echo "=== seed ==="
duckdb playground.duckdb < data/seed.sql

echo "=== discover (sources matching raw__<source>) ==="
rocky discover > expected/discover.json

echo "=== plan (preview the replication SQL) ==="
rocky plan --filter source=orders > expected/plan.json

echo "=== run (replicate raw__orders → staging__orders) ==="
rocky run --filter source=orders > expected/run.json
echo "replicated tables:"
duckdb playground.duckdb \
    "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema LIKE 'staging__%' ORDER BY 1,2"

echo "=== state (watermarks) ==="
rocky state > expected/state.json

echo "POC complete: source replicated to staging__orders via the schema pattern."
