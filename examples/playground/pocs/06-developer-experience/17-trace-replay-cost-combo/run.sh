#!/usr/bin/env bash
# One RunRecord, three views: trace + cost + replay
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

rm -f .rocky-state.redb .rocky-state.redb.lock poc.duckdb
rm -f models/.rocky-state.redb models/.rocky-state.redb.lock
mkdir -p expected

echo "==> 1. Seed three sources of varying size (10k orders, 5k events, 200 customers)"
duckdb poc.duckdb < data/seed.sql

echo "==> 2. Run the full pipeline — one invocation, one RunRecord, three stages"
rocky -c rocky.toml -o json run > expected/run.json

echo "==> 3. 'rocky trace latest' — causality + concurrency lanes over the RunRecord"
rocky -c rocky.toml trace latest > expected/trace.json

echo "==> 4. 'rocky cost latest' — per-model warehouse spend over the SAME RunRecord"
rocky -c rocky.toml cost latest --output table

echo "==> 5. 'rocky replay latest' — reproducibility artefact over the SAME RunRecord"
rocky -c rocky.toml replay latest > expected/replay.json

echo
echo "All three commands above target the same run_id. That's the property:"
echo "  - one trace        (causality + concurrency lanes)"
echo "  - one cost graph   (per-model warehouse spend)"
echo "  - one replay handle (reproducibility artefact)"
echo
echo "All three hang off the single RunRecord that 'rocky run' produced."
echo "A bundled-vendor stack reconstructs this from N separate telemetry"
echo "domains — Rocky emits it as one tree."
echo
echo "POC complete: JSON dumped to expected/."
