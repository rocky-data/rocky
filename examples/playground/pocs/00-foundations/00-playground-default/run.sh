#!/usr/bin/env bash
# 00-playground-default — model-level smoke test of the stock scaffold
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

mkdir -p expected
rm -f .rocky-state.redb poc.duckdb playground.duckdb

echo "=== validate ==="
rocky validate

echo "=== compile ==="
rocky compile --models models --contracts contracts > expected/compile.json

echo "=== test ==="
rocky test --models models --contracts contracts > expected/test.json

echo "=== lineage ==="
rocky lineage revenue_summary --models models > expected/lineage.json

echo "POC complete: 3 models compile + test cleanly, lineage traced."
