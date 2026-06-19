#!/usr/bin/env bash
# Fixture-driven unit tests — singular [[test]] blocks (dbt 1.8 unit-test parity)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

rocky validate > /dev/null

echo "=== Run fixture-driven unit tests ([[test]] blocks) ==="
# `rocky test` runs BOTH local model tests and the fixture-driven [[test]]
# blocks. The unit-test outcome lands in the JSON `unit_tests` field.
rocky test --models models --output json > expected/test.json

echo
echo "=== Unit-test summary ==="
jq '.unit_tests | { total, passed, failed,
  cases: [ .results[] | { model, test, passed } ] }' expected/test.json

# Assert the unit_tests field reports every case passing.
TOTAL=$(jq '.unit_tests.total'  expected/test.json)
PASSED=$(jq '.unit_tests.passed' expected/test.json)
FAILED=$(jq '.unit_tests.failed' expected/test.json)

if [ "$TOTAL" -lt 2 ]; then
  echo "FAIL: expected at least 2 unit tests (multiset + ordered), got $TOTAL" >&2
  exit 1
fi
if [ "$FAILED" -ne 0 ] || [ "$PASSED" -ne "$TOTAL" ]; then
  echo "FAIL: unit_tests not all passing — passed=$PASSED failed=$FAILED total=$TOTAL" >&2
  exit 1
fi

echo
echo "POC complete: $PASSED/$TOTAL fixture-driven unit tests passed"
echo "  (default multiset comparison + ordered=true positional comparison)."
