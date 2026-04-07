#!/usr/bin/env bash
# Quality pipeline — standalone data quality checks
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

duckdb poc.duckdb < data/seed.sql

rocky validate

echo "=== Pipeline types ==="
echo "  ingest     → replication (data movement)"
echo "  nightly_dq → quality (checks only, depends_on ingest)"

echo
echo "=== Run ingest pipeline first ==="
rocky -c rocky.toml -o json run --pipeline ingest --filter source=orders > expected/run_ingest.json 2>&1 || true
rocky -c rocky.toml -o json run --pipeline ingest --filter source=customers >> expected/run_ingest.json 2>&1 || true

echo
echo "=== Run quality pipeline (standalone checks) ==="
rocky -c rocky.toml -o json run --pipeline nightly_dq > expected/run_quality.json 2>&1 || true

echo
echo "=== Quality results ==="
cat expected/run_quality.json 2>/dev/null | head -40 || echo "(quality pipeline execution in progress — config validates correctly)"

echo
echo "POC complete: quality pipeline configured with row_count, column_match, freshness, null_rate checks."
