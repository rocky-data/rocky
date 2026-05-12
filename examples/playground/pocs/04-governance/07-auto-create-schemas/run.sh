#!/usr/bin/env bash
# 07-auto-create-schemas — transformation pipeline auto-creates a fresh target schema
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Clean state from previous runs.
rm -f .rocky-state.redb .rocky-state.redb.lock poc.duckdb
rm -f models/.rocky-state.redb models/.rocky-state.redb.lock
rm -rf expected
mkdir -p expected

echo "=== seed (raw__orders.orders, 8 rows; target schema 'poc.mart' is NOT seeded) ==="
duckdb poc.duckdb < seeds/seed.sql
duckdb poc.duckdb -c "SELECT COUNT(*) AS source_rows FROM raw__orders.orders;"

echo ""
echo "=== precondition — 'mart' schema does NOT exist in DuckDB ==="
duckdb poc.duckdb <<'SQL'
SELECT schema_name
FROM information_schema.schemata
WHERE catalog_name = 'poc'
ORDER BY schema_name;
SQL
echo "  (expected: empty result — no 'poc.mart' schema yet)"

echo ""
echo "=== validate ==="
rocky -c rocky.toml validate > /dev/null && echo "  rocky.toml valid"

echo ""
echo "=== compile ==="
rocky compile --models models/ --output json > expected/compile.json 2>/dev/null
python3 - <<'PY'
import json, pathlib
r = json.loads(pathlib.Path("expected/compile.json").read_text())
models = r.get("models")
count = len(models) if isinstance(models, list) else (models if isinstance(models, int) else "?")
print(f"  {count} models compiled")
PY

echo ""
echo "=== run — pipeline creates 'poc.mart' before materializing the two models ==="
echo "    (pre-v1.29.0 this run failed at execute time with:"
echo "     'Catalog Error: Schema with name mart does not exist')"
rocky -c rocky.toml run --output json > expected/run.json 2>/dev/null

python3 - <<'PY'
import json, pathlib
r = json.loads(pathlib.Path("expected/run.json").read_text())
mats = r.get("materializations", [])
print(f"  {len(mats)} model(s) materialized:")
for m in mats:
    fqn = ".".join(m.get("asset_key", [])) or m.get("metadata", {}).get("target_table_full_name", "?")
    strategy = m.get("metadata", {}).get("strategy", "?")
    rows = m.get("rows_copied")
    rows_str = f"{rows} rows" if rows is not None else "ok"
    print(f"    - {fqn} ({strategy}): {rows_str}")
PY

echo ""
echo "=== postcondition — 'mart' schema now exists, and both models live in it ==="
duckdb poc.duckdb <<'SQL'
SELECT schema_name
FROM information_schema.schemata
WHERE catalog_name = 'poc'
ORDER BY schema_name;

.print
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_catalog = 'poc' AND table_schema = 'mart'
ORDER BY table_name;

.print
SELECT customer_id, order_count, total_revenue
FROM poc.mart.order_revenue_by_customer
ORDER BY customer_id;
SQL

echo ""
echo "POC complete. auto_create_schemas honoured on the transformation path (engine v1.29.0)."
