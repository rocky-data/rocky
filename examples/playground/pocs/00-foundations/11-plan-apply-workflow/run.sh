#!/usr/bin/env bash
# 11-plan-apply-workflow — plan once, apply by id, re-plan is idempotent.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Clean state from previous runs. The persisted plans live under .rocky/plans/;
# wipe them so re-runs of this POC always start from a clean plan store.
rm -f .rocky-state.redb .rocky-state.redb.lock poc.duckdb
rm -rf .rocky/plans
mkdir -p expected

echo "==> 0. Seed raw__orders.orders (200 rows)"
duckdb poc.duckdb < data/seed.sql

echo
echo "==> 1. rocky plan — generate a plan, persist it, DO NOT execute"
rocky -c rocky.toml -o json plan --filter source=orders > expected/plan.json
PLAN_ID=$(jq -r '.plan_id' expected/plan.json)
echo "    plan_id    : $PLAN_ID"
echo "    plan_kind  : $(jq -r '.plan_kind' expected/plan.json)"
echo "    statements : $(jq -r '.statements | length' expected/plan.json)"
echo "    persisted  : $([[ -f ".rocky/plans/$PLAN_ID.json" ]] && echo yes || echo no) (.rocky/plans/$PLAN_ID.json)"

echo
echo "==> 2. Nothing materialized yet — plan is a dry-run"
PRE=$(duckdb poc.duckdb -noheader -csv \
  -c "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'staging__orders';")
echo "    target tables before apply : $PRE  (expected 0)"

echo
echo "==> 3. rocky apply <plan_id> — execute the persisted plan"
rocky -c rocky.toml -o json apply "$PLAN_ID" > expected/apply.json
echo "    status        : $(jq -r '.status' expected/apply.json)"
echo "    tables_copied : $(jq -r '.tables_copied' expected/apply.json)"
ROWS=$(duckdb poc.duckdb -noheader -csv -c "SELECT count(*) FROM staging__orders.orders;")
echo "    rows in staging__orders.orders : $ROWS  (expected 200)"

echo
echo "==> 4. Idempotency — re-plan the same intent yields the SAME plan_id"
rocky -c rocky.toml -o json plan --filter source=orders > expected/plan_again.json
PLAN_ID2=$(jq -r '.plan_id' expected/plan_again.json)
echo "    first plan_id  : $PLAN_ID"
echo "    second plan_id : $PLAN_ID2"

echo
# --- Success gates -----------------------------------------------------------
if [[ "$PRE" != "0" ]]; then
  echo "FAIL: target table existed before apply — plan should be a dry-run"; exit 1
fi
if [[ "$(jq -r '.status' expected/apply.json)" != "Success" ]]; then
  echo "FAIL: apply did not report Success"; exit 1
fi
if [[ "$ROWS" != "200" ]]; then
  echo "FAIL: expected 200 rows materialized, got $ROWS"; exit 1
fi
if [[ "$PLAN_ID" != "$PLAN_ID2" ]]; then
  echo "FAIL: re-plan produced a different plan_id — content hash is not stable"; exit 1
fi

echo "POC complete: rocky plan persisted plan $PLAN_ID (dry-run),"
echo "              rocky apply executed it (200 rows materialized),"
echo "              and re-planning the same intent reproduced the same plan_id."
