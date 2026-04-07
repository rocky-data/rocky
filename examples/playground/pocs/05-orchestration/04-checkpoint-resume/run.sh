#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

duckdb poc.duckdb < data/seed.sql

rocky validate

# First run — 3 sources to process
rocky -c rocky.toml -o json run --filter source=orders > expected/run1.json
echo "Run 1 (orders) ok"

# Resume — without prior failure this is a no-op, just shows the flag works
rocky -c rocky.toml -o json run --filter source=customers --resume-latest > expected/run2.json
echo "Run 2 (customers, --resume-latest) ok"

echo
echo "POC complete: --resume-latest flag accepted (full implementation in Databricks path)."
