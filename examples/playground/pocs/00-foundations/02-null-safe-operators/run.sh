#!/usr/bin/env bash
# 02-null-safe-operators — show DSL != vs SQL != on NULL rows
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

mkdir -p expected
rm -f .rocky-state.redb playground.duckdb

rocky validate
rocky compile --models models > expected/compile.json
rocky test    --models models > expected/test.json

# Persist to a file so we can query the row counts after.
duckdb playground.duckdb < data/seed.sql
duckdb playground.duckdb "
    CREATE OR REPLACE TABLE raw_orders AS SELECT * FROM seeds.orders;
    CREATE OR REPLACE TABLE filter_dsl  AS SELECT order_id, status, amount FROM raw_orders WHERE status IS DISTINCT FROM 'cancelled';
    CREATE OR REPLACE TABLE filter_sql AS SELECT order_id, status, amount FROM raw_orders WHERE status != 'cancelled';
    SELECT 'filter_dsl (NULL-safe)' AS query, COUNT(*) AS rows FROM filter_dsl
    UNION ALL
    SELECT 'filter_sql (NULL-blind)' AS query, COUNT(*) AS rows FROM filter_sql;
"

echo
echo "POC complete: DSL filter keeps NULL-status rows, SQL filter drops them."
