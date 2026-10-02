#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

duckdb poc.duckdb < data/seed.sql

rocky validate

# Run normally to create the production target
rocky -c rocky.toml -o json run --filter source=orders > expected/run_prod.json
echo "Prod run: ok"

# The first shadow run compares and drops its table.
rocky -c rocky.toml -o json run --shadow --filter source=orders > expected/run_shadow_once.json
echo "One-off shadow run and cleanup: ok"

# Reuse the name, then keep the table for a separate comparison.
rocky -c rocky.toml -o json run --shadow --keep-shadow --filter source=orders > expected/run_shadow.json
echo "Shadow run and in-run comparison: ok"

# Compare shadow against prod
rocky -c rocky.toml -o json compare --filter source=orders > expected/compare.json

echo
echo "POC complete: both comparisons passed."
