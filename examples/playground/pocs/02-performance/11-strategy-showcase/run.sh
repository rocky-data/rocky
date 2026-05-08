#!/usr/bin/env bash
# 11-strategy-showcase — three materialization strategies side-by-side
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Clean state from previous runs
rm -rf .rocky_state poc.duckdb expected
mkdir -p expected

echo "=== seed (100 orders in raw__orders.orders) ==="
duckdb poc.duckdb < data/seed.sql
duckdb poc.duckdb -c "SELECT COUNT(*) AS source_rows FROM raw__orders.orders;"

echo ""
echo "=== validate ==="
rocky validate > /dev/null && echo "  rocky.toml valid (3 pipelines)"

echo ""
echo "=== run #1 — first materialization for all three strategies ==="
rocky run --pipeline fr    --filter source=orders --output json > expected/run1-fr.json
rocky run --pipeline inc   --filter source=orders --output json > expected/run1-inc.json
rocky run --pipeline merge                        --output json > expected/run1-merge.json

duckdb poc.duckdb <<'SQL'
SELECT 'poc.fr.orders'    AS table_name, COUNT(*) AS rows FROM poc.fr.orders
UNION ALL SELECT 'poc.inc.orders',   COUNT(*) FROM poc.inc.orders
UNION ALL SELECT 'poc.merge.orders', COUNT(*) FROM poc.merge.orders
ORDER BY table_name;
SQL

echo ""
echo "=== mutate source (UPDATE order 5 status, UPDATE order 2 amount, INSERT 20 new) ==="
duckdb poc.duckdb < data/delta.sql
duckdb poc.duckdb -c "SELECT COUNT(*) AS source_rows FROM raw__orders.orders;"

echo ""
echo "=== run #2 — same three pipelines, see how each treats the mutation ==="
rocky run --pipeline fr    --filter source=orders --output json > expected/run2-fr.json
rocky run --pipeline inc   --filter source=orders --output json > expected/run2-inc.json
rocky run --pipeline merge                        --output json > expected/run2-merge.json

duckdb poc.duckdb <<'SQL'
SELECT 'poc.fr.orders'    AS table_name, COUNT(*) AS rows FROM poc.fr.orders
UNION ALL SELECT 'poc.inc.orders',   COUNT(*) FROM poc.inc.orders
UNION ALL SELECT 'poc.merge.orders', COUNT(*) FROM poc.merge.orders
ORDER BY table_name;
SQL

echo ""
echo "=== differential behavior on the two updated rows (order_id 2 + 5) ==="
echo "    full_refresh and merge should reflect the new values; incremental should NOT"
echo "    (the watermark filter only catches rows with ordered_at > MAX(target.ordered_at))"
duckdb poc.duckdb <<'SQL'
.print
.print -- full_refresh table (rebuilt entirely; sees all updates) --
SELECT order_id, status, amount FROM poc.fr.orders WHERE order_id IN (2, 5) ORDER BY order_id;

.print
.print -- incremental table (no UPDATE; existing rows untouched) --
SELECT order_id, status, amount FROM poc.inc.orders WHERE order_id IN (2, 5) ORDER BY order_id;

.print
.print -- merge table (UPDATE on match; both rows reflect the new values) --
SELECT order_id, status, amount FROM poc.merge.orders WHERE order_id IN (2, 5) ORDER BY order_id;
SQL

echo ""
echo "=== strategy cheat sheet ==="
cat <<'EOF'

  | Strategy     | First run | Second run with delta              | Best for                                  |
  |--------------|-----------|------------------------------------|-------------------------------------------|
  | full_refresh | 100 rows  | rebuild -> 120 rows; updates seen  | small tables, source-of-truth dimensions  |
  | incremental  | 100 rows  | append-only -> 120 rows; no update | large append-mostly facts (logs, events)  |
  | merge        | 100 rows  | upsert -> 120 rows; updates seen   | facts with late updates (orders, status)  |

EOF
echo "POC complete."
