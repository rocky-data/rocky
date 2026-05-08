#!/usr/bin/env bash
# 09-files-to-duckdb — Parquet + CSV + JSONL → DuckDB via a single `rocky load`
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Clean state from previous runs
rm -rf .rocky_state poc.duckdb data/
mkdir -p data expected

echo "=== stage data/ with one of each format ==="
# Mint orders.parquet from the seed CSV (DuckDB writes Parquet natively).
duckdb :memory: <<SQL
COPY (SELECT * FROM read_csv_auto('seeds/orders.csv', header=true))
TO 'data/orders.parquet' (FORMAT PARQUET);
SQL
# CSV and JSONL are shipped as-is — Rocky will read them in place.
cp seeds/customers.csv data/customers.csv
cp seeds/events.jsonl  data/events.jsonl
ls -lh data/

echo ""
echo "=== validate ==="
rocky validate

echo ""
echo "=== load (Parquet + CSV + JSONL → DuckDB, format auto-detected) ==="
rocky load --output json | tee expected/load.json

echo ""
echo "=== query loaded tables ==="
duckdb poc.duckdb <<SQL
SELECT 'orders'    AS source_table, COUNT(*) AS row_count FROM main.orders
UNION ALL SELECT 'customers',          COUNT(*)              FROM main.customers
UNION ALL SELECT 'events',             COUNT(*)              FROM main.events
ORDER BY source_table;

.print
.print -- sample row from each --

SELECT * FROM main.orders    ORDER BY order_id    LIMIT 1;
SELECT * FROM main.customers ORDER BY customer_id LIMIT 1;
SELECT * FROM main.events    ORDER BY event_id    LIMIT 1;
SQL

echo ""
echo "POC complete: 3 formats → 3 DuckDB tables in main."
