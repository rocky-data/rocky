#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

duckdb poc.duckdb < data/seed.sql

rocky validate
echo
echo "=== Run 1 ==="
rocky -c rocky.toml -o json run --filter source=orders > expected/run1.json
echo "    target rows: $(duckdb poc.duckdb 'SELECT COUNT(*) FROM staging__orders.orders' -noheader -list)"
duckdb poc.duckdb -noheader -list "SELECT 'amount type: ' || data_type FROM information_schema.columns WHERE table_schema='staging__orders' AND table_name='orders' AND column_name='amount'"

echo
echo "=== Drift the source: amount DECIMAL(10,2) -> VARCHAR ==="
duckdb poc.duckdb "ALTER TABLE raw__orders.orders ALTER COLUMN amount TYPE VARCHAR"

echo
echo "=== Run 2 (drift triggers DROP + RECREATE) ==="
rocky -c rocky.toml -o json run --filter source=orders > expected/run2.json
grep -A8 '"drift"' expected/run2.json || true

duckdb poc.duckdb -noheader -list "SELECT 'amount type: ' || data_type FROM information_schema.columns WHERE table_schema='staging__orders' AND table_name='orders' AND column_name='amount'"

echo
echo "POC complete: drift detected and target recreated with new type."
