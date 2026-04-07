#!/usr/bin/env bash
# <POC name> — end-to-end demo
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Clean state from previous runs
rm -f .rocky-state.redb poc.duckdb
mkdir -p expected

# Optional: load seed data into the persistent DuckDB
# duckdb poc.duckdb < seeds/seed.sql

# Validate the config
rocky validate -c rocky.toml

# Compile / type-check models (if the POC has models/)
# rocky compile --models models/ ${HAS_CONTRACTS:+--contracts contracts/}

# Plan and run the pipeline (uncomment when discovery + run are wired)
# rocky -c rocky.toml plan --filter source=orders --output json > expected/plan.json
# rocky -c rocky.toml run  --filter source=orders --output json > expected/run.json

echo "POC complete."
