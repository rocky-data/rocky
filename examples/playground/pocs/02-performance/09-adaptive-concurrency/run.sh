#!/usr/bin/env bash
# Concurrency config demo — fixed concurrency = 16 over 20 tables
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

duckdb poc.duckdb < data/seed.sql
echo "=== Seeded 20 tables in raw__orders ==="

rocky validate

echo
echo "=== Fixed concurrency (no AIMD throttle in this POC) ==="
echo "  concurrency = 16       (fixed limit; an integer turns the throttle off)"
echo "  error_rate_abort_pct = 50  (abort if >50% tables fail)"
echo "  table_retries = 2      (retry failed tables twice)"
echo
echo "  The AIMD throttle only runs with concurrency = \"adaptive\" (the default)."
echo "  See the README for how it halves on rate limits and ramps back up."
echo
echo
echo "=== Running 20 tables with concurrency=16 ==="
rocky -c rocky.toml -o json run --filter source=orders > expected/run.json 2>&1

PROCESSED=$(duckdb poc.duckdb "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'staging__orders'" -noheader -list)
echo "    Tables processed: $PROCESSED"
test "$PROCESSED" -eq 20

echo
echo "POC complete: fixed concurrency config validated; 20 tables processed in parallel."
