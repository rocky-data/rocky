#!/usr/bin/env bash
# Delete+Insert partitioned strategy — end-to-end demo
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

duckdb poc.duckdb < data/seed.sql

rocky validate
rocky compile --models models > expected/compile.json

echo "=== Compiled model ==="
cat expected/compile.json | head -20

echo
echo "Delete+Insert strategy:"
echo "  1. DELETE FROM target WHERE region IN (affected partitions)"
echo "  2. INSERT INTO target SELECT ... FROM source WHERE region IN (...)"
echo "  This avoids MERGE overhead and prevents duplicates from late-arriving data."

rocky test --models models > expected/test.json 2>&1 || true

echo
echo "POC complete: delete_insert strategy parsed and compiled."
