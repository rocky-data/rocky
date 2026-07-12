#!/usr/bin/env bash
# 02-null-safe-operators — show DSL != vs SQL != on NULL rows
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

mkdir -p expected
rm -f .rocky-state.redb playground.duckdb

rocky validate
rocky compile --models models --expand-macros > expected/compile.json
rocky test    --models models > expected/test.json

# Pull the SQL Rocky ACTUALLY lowered each model to — the DSL side is what
# proves the != -> IS DISTINCT FROM lowering. The row-count delta below is
# built from these strings, not hand-written SQL, so the demo regresses if the
# lowering ever does.
DSL_SQL="$(python3 -c 'import json; print(json.load(open("expected/compile.json"))["expanded_sql"]["filter_dsl"])')"
SQL_SQL="$(python3 -c 'import json; print(json.load(open("expected/compile.json"))["expanded_sql"]["filter_sql"])')"

echo
echo "Rocky lowered the DSL filter (filter_dsl) to:"
echo "    ${DSL_SQL//$'\n'/ }"

# Guard the headline claim: fail loudly if != stops lowering to IS DISTINCT FROM.
case "$DSL_SQL" in
    *"IS DISTINCT FROM"*) ;;
    *) echo "REGRESSION: filter_dsl did not lower != to IS DISTINCT FROM" >&2; exit 1 ;;
esac

# Persist to a file so we can query the row counts after, using Rocky's own SQL.
duckdb playground.duckdb < data/seed.sql
duckdb playground.duckdb "
    CREATE OR REPLACE TABLE raw_orders AS SELECT * FROM seeds.orders;
    CREATE OR REPLACE TABLE filter_dsl AS $DSL_SQL;
    CREATE OR REPLACE TABLE filter_sql AS $SQL_SQL;
    SELECT 'filter_dsl (NULL-safe)' AS query, COUNT(*) AS rows FROM filter_dsl
    UNION ALL
    SELECT 'filter_sql (NULL-blind)' AS query, COUNT(*) AS rows FROM filter_sql;
"

echo
echo "POC complete: DSL filter keeps NULL-status rows, SQL filter drops them."
