#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

duckdb poc.duckdb < data/seed.sql

rocky validate

# Run normally to create the production target
rocky -c rocky.toml -o json run --filter source=orders > expected/run_prod.json
echo "Prod run: ok"

# Run in shadow mode (writes to <table>_rocky_shadow)
rocky -c rocky.toml -o json run --shadow --filter source=orders > expected/run_shadow.json 2>&1 || true
echo "Shadow run: attempted (see expected/run_shadow.json)"

# Compare shadow against prod
rocky -c rocky.toml -o json compare --filter source=orders > expected/compare.json 2>&1 || true

echo
echo "POC complete (spec): shadow + compare flags exercised."
