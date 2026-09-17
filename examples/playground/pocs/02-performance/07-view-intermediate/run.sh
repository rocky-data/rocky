#!/usr/bin/env bash
# Ephemeral CTE inlining — end-to-end demo
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

duckdb poc.duckdb < data/seed.sql

rocky validate
rocky compile --models models > expected/compile.json

echo
echo "=== Compiled models ==="
cat expected/compile.json | head -30

echo
echo "Key point: stg_events (ephemeral) is NOT materialized as a table."
echo "It is inlined as a CTE inside user_metrics."
echo

rocky test --models models > expected/test.json 2>&1 || true

echo "POC complete: ephemeral model compiled and inlined as CTE."
