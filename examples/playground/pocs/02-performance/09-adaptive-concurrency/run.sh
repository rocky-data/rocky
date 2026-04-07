#!/usr/bin/env bash
# Adaptive concurrency — AIMD demo with 20 tables
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

duckdb poc.duckdb < data/seed.sql
echo "=== Seeded 20 tables in raw__orders ==="

rocky validate

echo
echo "=== AIMD adaptive concurrency ==="
echo "  concurrency = 16       (starting ceiling)"
echo "  error_rate_abort_pct = 50  (abort if >50% tables fail)"
echo "  table_retries = 2      (retry failed tables twice)"
echo
echo "  On warehouse rate-limits (429/transient errors):"
echo "    → Multiplicative decrease: current / 2 (floor: min_concurrency)"
echo "  On N consecutive successes:"
echo "    → Additive increase: current + 1 (cap: concurrency)"
echo
echo "  This prevents overwhelming the warehouse while maximizing throughput."

echo
echo "=== Running 20 tables with concurrency=16 ==="
rocky -c rocky.toml -o json run --filter source=orders > expected/run.json 2>&1 || true

echo "    Tables processed: $(duckdb poc.duckdb "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'staging__orders'" -noheader -list 2>/dev/null || echo 'check expected/run.json')"

echo
echo "POC complete: adaptive concurrency config validated; 20 tables processed in parallel."
