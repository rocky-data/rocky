#!/usr/bin/env bash
# 09-named-tests — reusable named tests applied via [[use_test]]
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb models/.rocky-state.redb models/.rocky-state.redb.lock

# Seed + materialize the two marts so the declarative assertions have real
# target tables to run against.
duckdb poc.duckdb < data/seed.sql
rocky validate > /dev/null
rocky -c rocky.toml -o json run --pipeline poc > expected/run.json

echo "=== rocky test --models models (compile + execute every model) ==="
# The literal local-test path: compiles and executes each model against
# in-memory DuckDB. Must pass.
rocky test --models models > expected/test.json
TOTAL=$(jq '.total'  expected/test.json)
PASSED=$(jq '.passed' expected/test.json)
FAILED=$(jq '.failed' expected/test.json)
echo "models tested: total=$TOTAL passed=$PASSED failed=$FAILED"
[ "$FAILED" -eq 0 ] || { echo "FAIL: rocky test reported $FAILED model failures"; exit 1; }
[ "$TOTAL"  -eq 2 ] || { echo "FAIL: expected 2 models, got $TOTAL"; exit 1; }

echo
echo "=== rocky test --declarative (run + name the resolved named tests) ==="
# The declarative surface runs the [[tests]] and [[use_test]] assertions
# against the materialized tables and names each one it applied. This is the
# surface that proves the named definitions resolved.
rocky test --models models --declarative --pipeline poc > expected/test_declarative.json

D_TOTAL=$(jq '.declarative.total'  expected/test_declarative.json)
D_PASSED=$(jq '.declarative.passed' expected/test_declarative.json)
D_FAILED=$(jq '.declarative.failed' expected/test_declarative.json)
echo "assertions: total=$D_TOTAL passed=$D_PASSED failed=$D_FAILED"
[ "$D_FAILED" -eq 0 ] || { echo "FAIL: $D_FAILED declarative assertions failed"; exit 1; }
[ "$D_TOTAL"  -eq 4 ] || { echo "FAIL: expected 4 resolved assertions, got $D_TOTAL"; exit 1; }

echo
echo "Applied assertions (model / type / column / severity):"
jq -r '.declarative.results[]
       | "  - \(.model): \(.test_type)(\(.column // "—")) [\(.severity)]"' \
  expected/test_declarative.json

# Assert each named test resolved with the right binding. We key on the
# resolved (type, column, severity) tuple + inlined SQL, because that is what
# proves the named definition was looked up and applied — the definition name
# itself is consumed at load time and does not appear in the output.

# 1. inline not_null coexists on orders_mart
jq -e '.declarative.results
       | any(.model=="orders_mart" and .test_type=="not_null" and .column=="order_id")' \
  expected/test_declarative.json > /dev/null \
  || { echo "FAIL: inline not_null(order_id) not applied to orders_mart"; exit 1; }

# 2. positive_amount resolved with the severity override (warning, not error)
#    and inlined its expression SQL
jq -e '.declarative.results
       | any(.model=="orders_mart" and .test_type=="expression"
             and .severity=="warning" and (.sql | contains("amount > 0")))' \
  expected/test_declarative.json > /dev/null \
  || { echo "FAIL: positive_amount severity override (warning) not resolved"; exit 1; }

# 3. known_status applied to its default column (status) on orders_mart
jq -e '.declarative.results
       | any(.model=="orders_mart" and .test_type=="accepted_values" and .column=="status")' \
  expected/test_declarative.json > /dev/null \
  || { echo "FAIL: known_status default-column (status) not applied to orders_mart"; exit 1; }

# 4. known_status re-bound to fulfilment_state on shipments_mart (column override)
jq -e '.declarative.results
       | any(.model=="shipments_mart" and .test_type=="accepted_values"
             and .column=="fulfilment_state")' \
  expected/test_declarative.json > /dev/null \
  || { echo "FAIL: known_status column-bind override (fulfilment_state) not applied"; exit 1; }

echo
echo "POC complete: 2 models, 1 inline test + 3 [[use_test]] references resolved"
echo "(default column, column-bind override, severity override) — all 4 assertions pass."
