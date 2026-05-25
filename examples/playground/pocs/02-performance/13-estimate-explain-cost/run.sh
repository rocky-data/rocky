#!/usr/bin/env bash
# 13-estimate-explain-cost — dry-run cost estimation via warehouse EXPLAIN.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

rm -f .rocky-state.redb .rocky-state.redb.lock poc.duckdb
rm -f models/.rocky-state.redb models/.rocky-state.redb.lock
mkdir -p expected

echo "==> 0. Seed raw__sales (5000 orders, 200 customers) — read by EXPLAIN, never written by estimate"
duckdb poc.duckdb < data/seed.sql

echo
echo "==> 1. Compile the two models (type-check, no warehouse writes)"
rocky compile --models models/ > expected/compile.json 2>/dev/null
echo "    models compiled : $(jq -r '.models | length' expected/compile.json)"

echo
echo "==> 2. rocky estimate — run each model's SELECT through DuckDB EXPLAIN"
rocky -c rocky.toml -o json estimate --models models/ > expected/estimate.json 2>/dev/null
echo "    total_models    : $(jq -r '.total_models' expected/estimate.json)"
for m in $(jq -r '.estimates[].model_name' expected/estimate.json); do
  echo "    - $m"
done

echo
echo "==> 3. Estimated rows per scan from the join model's plan (revenue_by_tier):"
jq -r '.estimates[] | select(.model_name == "revenue_by_tier") | .raw_explain' expected/estimate.json \
  | grep -E "rows|Table:|HASH_JOIN|status=" | sed 's/[│└┌─┐┘├┤ ]\+/ /g' | sed 's/^ //' | grep -v '^$' | head -12

echo
echo "==> 4. Nothing was materialized — estimate is a dry-run"
TABLES=$(duckdb poc.duckdb -noheader -csv \
  -c "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'demo';")
echo "    tables in target schema 'demo' : $TABLES  (expected 0)"

echo
# --- Success gates -----------------------------------------------------------
if [[ "$(jq -r '.total_models' expected/estimate.json)" != "2" ]]; then
  echo "FAIL: expected 2 model estimates"; exit 1
fi
if ! jq -e '.estimates[] | select(.model_name == "revenue_by_tier") | .raw_explain | test("HASH_JOIN")' \
     expected/estimate.json > /dev/null; then
  echo "FAIL: join model's EXPLAIN plan missing the expected HASH_JOIN node"; exit 1
fi
if [[ "$TABLES" != "0" ]]; then
  echo "FAIL: estimate materialized a table — it must be a dry-run"; exit 1
fi

echo "POC complete: rocky estimate emitted real DuckDB EXPLAIN plans for both models"
echo "              (row estimates, join strategy, filter pushdown) without writing any table."
