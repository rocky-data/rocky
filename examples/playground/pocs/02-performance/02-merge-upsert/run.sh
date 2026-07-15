#!/usr/bin/env bash
# 02-merge-upsert — model-level MERGE strategy, executed end-to-end against DuckDB
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Clean state from previous runs
rm -rf .rocky-state.redb .rocky-state.redb.lock models/.rocky-state.redb models/.rocky-state.redb.lock poc.duckdb expected
mkdir -p expected

echo "=== validate ==="
rocky validate

echo ""
echo "=== seed (50 customers in seeds.customers) ==="
duckdb poc.duckdb < data/seed.sql
duckdb poc.duckdb -c "SELECT COUNT(*) AS seed_rows FROM seeds.customers;"

echo ""
echo "=== first rocky run — initial materialization ==="
rocky run --pipeline transform --output json > expected/run1.json
python3 - <<'PY'
import json, pathlib
r = json.loads(pathlib.Path("expected/run1.json").read_text())
for m in r.get("materializations", []):
    print(f"  - {'.'.join(m['asset_key'])} ({m.get('metadata',{}).get('strategy','?')})")
PY

echo ""
echo "  dim_customers row count after run #1:"
duckdb poc.duckdb -c "SELECT COUNT(*) AS rows FROM poc.demo.dim_customers;"

echo ""
echo "=== apply delta (UPDATE customer 5 tier, rename customer 7, INSERT customer 51) ==="
duckdb poc.duckdb < data/delta.sql

echo ""
echo "=== second rocky run — MERGE upsert ==="
rocky run --pipeline transform --output json > expected/run2.json
python3 - <<'PY'
import json, pathlib
r = json.loads(pathlib.Path("expected/run2.json").read_text())
for m in r.get("materializations", []):
    print(f"  - {'.'.join(m['asset_key'])} ({m.get('metadata',{}).get('strategy','?')})")
PY

echo ""
echo "=== verify MERGE semantics ==="
duckdb poc.duckdb <<'SQL'
.print
.print -- row count after MERGE (expect 51 = 50 original + 1 inserted) --
SELECT COUNT(*) AS rows FROM poc.demo.dim_customers;

.print
.print -- updated rows (customer 5 should be 'gold'; customer 7 should be renamed) --
SELECT customer_id, name, email, tier
FROM poc.demo.dim_customers
WHERE customer_id IN (5, 7, 51)
ORDER BY customer_id;
SQL

echo ""
echo "=== compiled MERGE SQL (for reference) ==="
rocky compile --models models/ --output json > expected/compile.json
python3 - <<'PY'
import json, pathlib
c = json.loads(pathlib.Path("expected/compile.json").read_text())
for m in c.get("models_detail", []):
    if m.get("strategy", {}).get("type") == "merge":
        print(f"  model: {m['name']}")
        print(f"  unique_key: {m['strategy'].get('unique_key')}")
        print(f"  update_columns: {m['strategy'].get('update_columns')}")
PY

echo ""
echo "POC complete: 50 rows on first run, MERGE upserts to 51 rows on second."
