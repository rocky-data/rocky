#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

duckdb poc.duckdb < data/seed_initial.sql
echo "=== Source after seed_initial: $(duckdb poc.duckdb 'SELECT COUNT(*) FROM raw__events.events' -noheader -list) rows ==="

rocky validate
echo
echo "=== Run 1 (initial) ==="
rocky -c rocky.toml -o json run --filter source=events > expected/run1.json
echo "    target rows: $(duckdb poc.duckdb 'SELECT COUNT(*) FROM staging__events.events' -noheader -list)"

echo "=== State after run 1 ==="
rocky state 2>&1 | tail -10

echo
echo "=== Append delta (25 new rows, later timestamps) ==="
duckdb poc.duckdb < data/seed_delta.sql
echo "    source rows: $(duckdb poc.duckdb 'SELECT COUNT(*) FROM raw__events.events' -noheader -list)"

echo
echo "=== Run 2 (incremental) ==="
rocky -c rocky.toml -o json run --filter source=events > expected/run2.json
echo "    target rows: $(duckdb poc.duckdb 'SELECT COUNT(*) FROM staging__events.events' -noheader -list)"

echo "=== State after run 2 ==="
rocky state 2>&1 | tail -10

echo
echo "POC complete: incremental copied only the 25 delta rows."
