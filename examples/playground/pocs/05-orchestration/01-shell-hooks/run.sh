#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
chmod +x scripts/*.sh
rm -f .rocky-state.redb poc.duckdb

duckdb poc.duckdb < data/seed.sql

rocky validate

echo "=== rocky hooks list ==="
rocky -c rocky.toml -o json hooks list > expected/hooks_list.json 2>&1 || true
head -20 expected/hooks_list.json

echo
echo "=== rocky hooks test on_pipeline_start ==="
rocky -c rocky.toml hooks test on_pipeline_start 2>&1 || true

echo
echo "POC complete: hooks listed and tested."
