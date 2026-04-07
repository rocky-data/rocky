#!/usr/bin/env bash
# hybrid-dbt-packages — Rocky consuming dbt package tables as external sources
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

echo "=== Seeding dbt package tables (simulated) ==="
duckdb poc.duckdb < data/seed.sql

echo "=== Validate pipeline config ==="
rocky validate

echo "=== Compile Rocky models (type-check against external sources) ==="
rocky compile --models models > expected/compile.json

echo "=== Lineage: combined_marketing_revenue ==="
rocky lineage combined_marketing_revenue --models models > expected/lineage.json

echo "=== Lineage: DOT graph ==="
rocky lineage combined_marketing_revenue --models models --format dot > expected/lineage.dot

echo
echo "POC complete: Rocky models compiled and lineage traced through dbt package external sources."
