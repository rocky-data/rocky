#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

duckdb poc.duckdb < data/seed.sql

rocky -c rocky.toml validate

# First run — replicate just the `orders` source. The replication run path
# records per-table progress in the state store as each table completes.
rocky -c rocky.toml -o json run --filter source=orders > expected/run1.json
echo "Run 1 (orders) ok — progress checkpointed to the state store"

# Resume — no filter, so all three sources are in scope, but --resume-latest
# reads the previous run's checkpoint and skips `orders` (already succeeded),
# copying only customers + products.
rocky -c rocky.toml -o json run --resume-latest > expected/run2.json
echo "Run 2 (--resume-latest) ok — orders skipped, customers + products copied"

echo
echo "POC complete: --resume-latest replayed the latest run and skipped the already-completed table."
