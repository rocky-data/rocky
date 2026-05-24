#!/usr/bin/env bash
# 06-mcp-grounding — schema-only authoring reconciles WRONG; grounding in the
# real data fixes it. Deterministic, credential-free: no LLM, no Claude Code.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Prefer an explicitly-set binary, then the local feature-branch build, then
# `rocky` on PATH.
ROCKY_BIN="${ROCKY_BIN:-}"
if [[ -z "$ROCKY_BIN" ]]; then
    REPO_ROOT="$(cd "$HERE/../../../../.." && pwd)"
    if [[ -x "$REPO_ROOT/engine/target/release/rocky" ]]; then
        ROCKY_BIN="$REPO_ROOT/engine/target/release/rocky"
    elif [[ -x "$REPO_ROOT/engine/target/debug/rocky" ]]; then
        ROCKY_BIN="$REPO_ROOT/engine/target/debug/rocky"
    else
        ROCKY_BIN="rocky"
    fi
fi
echo "==> Using rocky binary: $ROCKY_BIN ($("$ROCKY_BIN" --version))"

rm -f .rocky-state.redb .rocky-state.redb.lock poc.duckdb
rm -f models/.rocky-state.redb models/.rocky-state.redb.lock
mkdir -p expected

# --- 1. Both models compile clean -------------------------------------------
#
# This is the trap. Schema-only verification is satisfied by BOTH models — the
# wrong one and the right one. "It compiled" tells you nothing about whether
# the result reconciles.
echo
echo "==> 1. rocky compile — both models type-check (the naive model included)"
"$ROCKY_BIN" compile --models models -o json > expected/compile.json 2>/dev/null
python3 -c "import json; d=json.load(open('expected/compile.json')); print('    has_errors:', d['has_errors'], '| models:', d['models'])"

# --- 2. Look at the real data -----------------------------------------------
#
# The two surprises a schema can't show you: status is uppercase 'COMPLETE'
# (not 'completed'), and amount is in CENTS (not dollars).
echo
echo "==> 2. Sample the source — the traps a schema cannot reveal"
duckdb poc.duckdb < data/seed.sql
echo "    distinct status values:"
duckdb -noheader -list poc.duckdb \
    "SELECT '      ' || string_agg(DISTINCT status, ', ') FROM seeds.orders;"
echo "    a few rows (note amount_cents is in cents):"
duckdb -box poc.duckdb \
    "SELECT order_id, status, amount_cents FROM seeds.orders ORDER BY order_id LIMIT 5;"

# --- 3. Materialize both models ---------------------------------------------
#
# `rocky test --declarative` runs the sidecar [[tests]] against the
# materialized target table, so the tables must exist in poc.duckdb first.
echo
echo "==> 3. Materialize both models into poc.demo.*"
duckdb poc.duckdb "CREATE SCHEMA IF NOT EXISTS demo;"
duckdb poc.duckdb "CREATE OR REPLACE TABLE demo.revenue_naive   AS $(cat models/revenue_naive.sql);"
duckdb poc.duckdb "CREATE OR REPLACE TABLE demo.revenue_correct AS $(cat models/revenue_correct.sql);"

# --- 4. The reconcile gap ---------------------------------------------------
NAIVE=$(duckdb -noheader -list poc.duckdb \
    "SELECT COALESCE(CAST(revenue_usd AS VARCHAR), 'NULL') FROM demo.revenue_naive;")
CORRECT=$(duckdb -noheader -list poc.duckdb \
    "SELECT printf('%.2f', revenue_usd) FROM demo.revenue_correct;")

echo
echo "==> 4. Reconcile naive vs correct"
echo "    revenue_naive   (WHERE status='completed', cents-as-dollars) -> revenue_usd = $NAIVE"
echo "    revenue_correct (WHERE status='COMPLETE',  cents/100.0)      -> revenue_usd = $CORRECT"

# Enforce the gap at the shell level so the story can't silently rot: the naive
# model must NOT reconcile to the true total.
if [[ "$NAIVE" == "1000.00" || "$NAIVE" == "1000.0" || "$NAIVE" == "1000" ]]; then
    echo "    ERROR: naive model unexpectedly reconciled to the true total." >&2
    exit 1
fi
if [[ "$CORRECT" != "1000.00" ]]; then
    echo "    ERROR: correct model did not reconcile to the expected \$1000.00." >&2
    exit 1
fi
echo "    Confirmed: the naive guess is wrong; only the grounded model is right."

# --- 5. The typed assertion is green for the grounded model -----------------
#
# revenue_correct carries a [[tests]] aggregate assertion (SUM(revenue_usd) =
# 1000). It passes. The naive model carries no such assertion — its author
# never looked at the data, so there was nothing to encode.
echo
echo "==> 5. rocky test --declarative — the grounded model's assertion passes"
"$ROCKY_BIN" -c rocky.toml test --models models --declarative -o json > expected/test.json
python3 -c "
import json
d = json.load(open('expected/test.json'))['declarative']
print('    aggregate assertion on revenue_correct:',
      f\"{d['passed']} passed, {d['failed']} failed, {d['errored']} errored\")
"
"$ROCKY_BIN" -c rocky.toml test --models models --declarative -o table

echo
echo "POC complete: schema-only authoring compiled but reconciled WRONG"
echo "(revenue_usd = $NAIVE); the data-grounded model reconciled to the true"
echo "\$$CORRECT and its typed assertion is green. The interactive MCP demo"
echo "(README.md + .mcp.json) shows an agent reaching the grounded model on"
echo "its own via the Rocky MCP tools."
