#!/usr/bin/env bash
# <POC name> — end-to-end demo
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Clean state from previous runs. `models/.rocky-state.redb` is the canonical
# location since the LSP↔CLI unification (rocky-core::state::resolve_state_path);
# the bare CWD path is the deprecated legacy fallback. Wipe both so re-runs
# of POCs that create named state entries (branches, idempotency keys) don't
# trip "already exists" on the second invocation.
rm -f .rocky-state.redb .rocky-state.redb.lock poc.duckdb
rm -f models/.rocky-state.redb models/.rocky-state.redb.lock
mkdir -p expected

# Optional: load seed data into the persistent DuckDB
# duckdb poc.duckdb < data/seed.sql

# Validate the config
rocky validate -c rocky.toml

# Compile / type-check models (if the POC has models/)
# rocky compile --models models/ ${HAS_CONTRACTS:+--contracts contracts/}

# Plan and run the pipeline (uncomment when discovery + run are wired)
# rocky -c rocky.toml plan --filter source=orders --output json > expected/plan.json
# rocky -c rocky.toml run  --filter source=orders --output json > expected/run.json

echo "POC complete."
