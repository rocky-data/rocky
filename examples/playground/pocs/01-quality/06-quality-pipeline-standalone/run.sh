#!/usr/bin/env bash
# Quality pipeline — standalone data quality checks
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

duckdb poc.duckdb < data/seed.sql

rocky validate > /dev/null

echo "=== Run quality pipeline (standalone checks + assertions) ==="
rocky -c rocky.toml -o json run --pipeline nightly_dq > expected/run_quality.json

echo
echo "=== Summary ==="
jq '{
  pipeline_type,
  check_results: [
    .check_results[] | {
      table: (.asset_key | join(".")),
      checks: [ .checks[] | { name, severity, passed } ]
    }
  ],
  quarantine: (.quarantine // [])
}' expected/run_quality.json

echo
echo "=== Quarantine split (orders) ==="
echo "orders__valid row count:"
duckdb poc.duckdb -c "SELECT COUNT(*) AS valid_rows FROM staging__orders.orders__valid;"
echo "orders__quarantine row count + _error_* label columns:"
duckdb poc.duckdb -c "SELECT order_id, customer_id, status,
    _error_orders_customer_id_required,
    _error_orders_status_allowed
  FROM staging__orders.orders__quarantine
  ORDER BY order_id;"

echo
echo "POC complete: unified check surface + mixed severity + row quarantine (split)."
echo "Flip fail_on_error=true in rocky.toml to make error-severity failures non-zero."
