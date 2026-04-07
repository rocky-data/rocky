#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

duckdb poc.duckdb < data/seed.sql

rocky validate
rocky -c rocky.toml -o json run --filter source=events > expected/run.json

echo "=== rocky optimize ==="
rocky -c rocky.toml -o json optimize > expected/optimize.json 2>&1 || true
head -20 expected/optimize.json

echo "=== rocky profile-storage ==="
rocky -c rocky.toml -o json profile-storage events > expected/profile.json 2>&1 || true
head -20 expected/profile.json

echo "=== rocky compact events ==="
rocky -c rocky.toml -o json compact events > expected/compact.json 2>&1 || true
head -10 expected/compact.json

echo
echo "POC complete: optimize / profile-storage / compact emitted JSON."
