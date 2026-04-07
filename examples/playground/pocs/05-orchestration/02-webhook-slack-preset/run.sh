#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

duckdb poc.duckdb < data/seed.sql

rocky validate

echo "=== rocky hooks list ==="
rocky -c rocky.toml -o json hooks list > expected/hooks_list.json 2>&1 || true
head -20 expected/hooks_list.json

echo
echo "POC complete: webhook hook configured with slack preset (URL is a webhook.site placeholder)."
