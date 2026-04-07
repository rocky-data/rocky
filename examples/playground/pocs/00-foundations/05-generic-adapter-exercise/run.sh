#!/usr/bin/env bash
# 05-generic-adapter-exercise — end-to-end DuckDB generic adapter flow
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

echo "=== validate ==="
rocky validate

echo ""
echo "=== list pipelines ==="
rocky -o table list pipelines

echo ""
echo "=== list adapters ==="
rocky -o table list adapters

echo ""
echo "=== list models ==="
rocky -o table list models --models models/

echo ""
echo "=== doctor --check auth ==="
rocky doctor --check auth

echo ""
echo "=== seed ==="
rocky seed --seeds seeds/

echo ""
echo "=== discover ==="
rocky -o table discover

echo ""
echo "=== plan (orders only) ==="
rocky -o json plan --filter source=orders > expected/plan.json
rocky -o table plan --filter source=orders

echo ""
echo "=== compile ==="
rocky compile --models models > expected/compile.json

echo ""
echo "=== test ==="
rocky test --models models > expected/test.json

echo ""
echo "POC complete: generic adapter exercise passed."
echo "  - 3 seeds loaded (customers, orders, products)"
echo "  - 3 source schemas discovered"
echo "  - plan generated for orders pipeline"
echo "  - stg_orders compiled and tested (unique, not_null, accepted_values)"
echo "  - Artifacts saved to expected/"
