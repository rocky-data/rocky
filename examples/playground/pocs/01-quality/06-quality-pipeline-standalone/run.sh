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
  ]
}' expected/run_quality.json

echo
echo "POC complete: unified check surface (row_count + assertions) with mixed severity."
echo "Flip fail_on_error=true in rocky.toml to make error-severity failures non-zero."
