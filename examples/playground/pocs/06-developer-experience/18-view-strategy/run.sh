#!/usr/bin/env bash
# 18-view-strategy — DuckDB smoke test for the `view` materialization strategy.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

echo "=== validate ==="
rocky validate

echo "=== compile (verifies type = \"view\" deserializes + lowers to IR) ==="
rocky compile --models models > expected/compile.json
head -30 expected/compile.json

echo "POC complete: a view-strategy model compiles end-to-end on DuckDB."
