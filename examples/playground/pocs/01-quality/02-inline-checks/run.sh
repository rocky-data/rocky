#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

duckdb poc.duckdb < data/seed.sql

rocky validate
rocky -c rocky.toml -o json plan --filter source=events > expected/plan.json
rocky -c rocky.toml -o json run  --filter source=events > expected/run.json

echo "POC complete: pipeline replicated; check results in expected/run.json"
