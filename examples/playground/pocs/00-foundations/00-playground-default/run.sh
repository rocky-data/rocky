#!/usr/bin/env bash
# 00-playground-default — runnable, inspectable transformation pipeline.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

mkdir -p expected
rm -f .rocky-state.redb .rocky-state.redb.lock playground.duckdb
rm -f models/.rocky-state.redb models/.rocky-state.redb.lock

echo "=== seed ==="
duckdb playground.duckdb < data/seed.sql

echo "=== validate ==="
rocky validate

echo "=== compile ==="
rocky compile --models models --contracts contracts > expected/compile.json

echo "=== run (materialize the model DAG) ==="
rocky run > expected/run.json
echo "materialized in playground.main:"
duckdb playground.duckdb \
    "SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name"

echo "=== test ==="
rocky test --models models --contracts contracts > expected/test.json

echo "=== profile (observed stats from the materialized model) ==="
rocky profile customer_orders > expected/profile.json

echo "=== lineage ==="
rocky lineage revenue_summary --models models > expected/lineage.json

echo "POC complete: 3 models materialize, then test + profile + lineage cleanly."
