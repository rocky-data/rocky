#!/usr/bin/env bash
# 10-route-by-tenant — fan one Parquet file out to per-tenant DuckDB schemas
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Clean state from previous runs
rm -rf .rocky_state poc.duckdb data/
mkdir -p data expected

echo "=== mint data/events.parquet from seeds/events.csv (3 tenants mixed) ==="
duckdb :memory: <<SQL
COPY (SELECT * FROM read_csv_auto('seeds/events.csv', header=true))
TO 'data/events.parquet' (FORMAT PARQUET);
SQL
ls -lh data/events.parquet

echo ""
echo "=== validate ==="
rocky validate

echo ""
echo "=== 1. ingest pipeline — Parquet -> main.events ==="
rocky load --pipeline ingest --output json | tee expected/load.json

echo ""
echo "=== pre-create per-tenant schemas (rocky transformation runs don't auto-create yet) ==="
duckdb poc.duckdb <<SQL
CREATE SCHEMA IF NOT EXISTS account_acme;
CREATE SCHEMA IF NOT EXISTS account_beta;
CREATE SCHEMA IF NOT EXISTS account_ceres;
SQL

echo ""
echo "=== 2. compile route models ==="
rocky compile --models models/ --output json > expected/compile.json
python3 - <<'PY'
import json, pathlib
c = json.loads(pathlib.Path("expected/compile.json").read_text())
for m in c.get("models_detail", []):
    tgt = m.get("target", {})
    qualified = ".".join(p for p in (tgt.get("catalog"), tgt.get("schema"), tgt.get("table")) if p)
    print(f"   {m['name']:14s} -> {qualified}")
PY

echo ""
echo "=== 3. route pipeline — main.events fanned out per account ==="
rocky run --pipeline route --output json | tee expected/run.json

echo ""
echo "=== query per-tenant schemas ==="
duckdb poc.duckdb <<SQL
SELECT 'main.events'           AS table_name, COUNT(*) AS row_count FROM main.events
UNION ALL SELECT 'account_acme.events',  COUNT(*) FROM account_acme.events
UNION ALL SELECT 'account_beta.events',  COUNT(*) FROM account_beta.events
UNION ALL SELECT 'account_ceres.events', COUNT(*) FROM account_ceres.events
ORDER BY table_name;
SQL

echo ""
echo "POC complete: 1 Parquet file -> 1 staging table -> 3 per-tenant schemas."
