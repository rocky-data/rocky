#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

# Build a baseline of 3 runs against the normal seed.
for i in 1 2 3; do
    duckdb poc.duckdb < data/seed.sql
    rocky -c rocky.toml run --filter source=events --output json > "expected/run-${i}.json"
    echo "Baseline run $i: ok"
done

# Simulate an incident: source is suddenly truncated to 5 rows.
duckdb poc.duckdb < data/seed_truncated.sql
rocky -c rocky.toml run --filter source=events --output json \
    > expected/run-incident.json 2> expected/run-incident.log
echo "Incident run: ok"

# Assert the anomaly actually fired — the whole point of the POC.
if grep -q "row count anomaly detected" expected/run-incident.log; then
    echo "Anomaly detected as expected:"
    grep -o '"reason":"[^"]*"' expected/run-incident.log | head -1
else
    echo "ERROR: expected a row count anomaly warning but none fired" >&2
    exit 1
fi

echo
echo "=== Run history ==="
rocky -c rocky.toml history 2>&1 || true

echo
echo "POC complete. Inspect expected/run-incident.json for the divergence."
