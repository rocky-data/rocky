#!/usr/bin/env bash
# 15-run-variables — per-run `@var()` variables substituted into model SQL.
#
# The model `regional_sales` filters on three run variables:
#   @var(region)         REQUIRED  — no inline default, inside a string literal
#   @var(channel, web)   OPTIONAL  — inline default of `web`, inside a literal
#   @var(min_amount, 0)  OPTIONAL  — inline default of `0`, BARE (numeric, no
#                                    surrounding quotes)
#
# This script proves:
#   1. `rocky run --var region=us` materializes ONLY the matching rows
#      (the optional channel defaults to `web`, min_amount to `0`)
#   2. a repeated `--var` (region + channel) changes which rows land
#   3. a BARE `@var(min_amount, 0)` in numeric position works on the run path:
#      `--var min_amount=200` filters to amounts >= 200
#   4. `rocky emit-sql --var region=us` shows the RESOLVED values, not the
#      literal `@var(...)` markers — both quoted and bare
#   5. `@var(channel, web)` resolves to its inline DEFAULT when --var is omitted
#   6. a REQUIRED `@var(region)` with no --var makes `rocky compile` fail with
#      a clear E028 error naming the variable
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

mkdir -p expected
rm -f .rocky-state.redb .rocky-state.redb.lock poc.duckdb
rm -f models/.rocky-state.redb models/.rocky-state.redb.lock

echo "==== 1. seed three regions across two channels ===="
duckdb poc.duckdb < data/seed.sql
echo "raw__sales.sales by region/channel:"
duckdb poc.duckdb -c "SELECT region, channel, count(*) AS rows FROM raw__sales.sales GROUP BY region, channel ORDER BY region, channel"

echo
echo "==== 2. rocky run --var region=us — materialize ONLY us rows (channel defaults to web) ===="
rocky -c rocky.toml -o json run --models models/ --var region=us \
    > expected/run-us.json 2> expected/run-us.log
echo "materialized poc.main.regional_sales:"
duckdb poc.duckdb -c "SELECT region, channel, customer, amount FROM poc.main.regional_sales ORDER BY customer"

REGIONS="$(duckdb -noheader -list poc.duckdb 'SELECT DISTINCT region FROM poc.main.regional_sales')"
CHANNELS="$(duckdb -noheader -list poc.duckdb 'SELECT DISTINCT channel FROM poc.main.regional_sales')"
US_WEB_ROWS="$(duckdb -noheader -list poc.duckdb 'SELECT count(*) FROM poc.main.regional_sales')"
if [ "$REGIONS" != "us" ]; then
    echo "FAIL: target holds regions other than 'us': [$REGIONS]" >&2
    exit 1
fi
if [ "$CHANNELS" != "web" ]; then
    echo "FAIL: channel did not default to 'web': [$CHANNELS]" >&2
    exit 1
fi
if [ "$US_WEB_ROWS" -ne 2 ]; then
    echo "FAIL: expected 2 us/web rows, got $US_WEB_ROWS" >&2
    exit 1
fi
echo "OK: only us rows landed, channel defaulted to web ($US_WEB_ROWS rows)"

echo
echo "==== 3. repeated --var: region=us AND channel=mobile selects a different slice ===="
rocky -c rocky.toml -o json run --models models/ --var region=us --var channel=mobile \
    > expected/run-us-mobile.json 2> expected/run-us-mobile.log
MOBILE_ROWS="$(duckdb -noheader -list poc.duckdb 'SELECT count(*) FROM poc.main.regional_sales')"
MOBILE_CUST="$(duckdb -noheader -list poc.duckdb 'SELECT customer FROM poc.main.regional_sales')"
echo "rows with region=us AND channel=mobile: $MOBILE_ROWS ($MOBILE_CUST)"
if [ "$MOBILE_ROWS" -ne 1 ] || [ "$MOBILE_CUST" != "carol" ]; then
    echo "FAIL: expected the single us/mobile row (carol), got $MOBILE_ROWS [$MOBILE_CUST]" >&2
    exit 1
fi
echo "OK: overriding the optional channel var changed the materialized slice"

echo
echo "==== 4. a BARE @var(min_amount, 0) in numeric position works on the run path ===="
# min_amount is bare (no surrounding quotes): `amount >= @var(min_amount, 0)`.
# Supplying --var min_amount=200 keeps only us/web rows with amount >= 200.
rocky -c rocky.toml -o json run --models models/ --var region=us --var min_amount=200 \
    > expected/run-us-min200.json 2> expected/run-us-min200.log
MIN_ROWS="$(duckdb -noheader -list poc.duckdb 'SELECT count(*) FROM poc.main.regional_sales')"
MIN_CUST="$(duckdb -noheader -list poc.duckdb 'SELECT customer FROM poc.main.regional_sales')"
echo "rows with region=us AND amount >= 200: $MIN_ROWS ($MIN_CUST)"
if [ "$MIN_ROWS" -ne 1 ] || [ "$MIN_CUST" != "bob" ]; then
    echo "FAIL: expected the single us/web row with amount >= 200 (bob), got $MIN_ROWS [$MIN_CUST]" >&2
    exit 1
fi
echo "OK: a bare numeric @var filtered the run — no string-literal wrapper needed"

echo
echo "==== 5. emit-sql --var region=us — markers resolve to literal values ===="
rocky emit-sql --models models/ --var region=us --out-dir build > /dev/null 2>&1
# Strip the model's preserved `--` comment lines (which legitimately mention
# the @var() markers) so the assertions look only at the executable SQL.
EXEC_SQL="$(grep -v '^[[:space:]]*--' build/regional_sales.sql)"
echo "emitted WHERE clause (comments stripped):"
echo "$EXEC_SQL" | grep -i "where\|region\|channel\|amount"
if ! echo "$EXEC_SQL" | grep -q "region  = 'us'"; then
    echo "FAIL: emitted SQL did not resolve @var(region) to 'us'" >&2
    exit 1
fi
# The bare @var(min_amount, 0) renders into numeric position — no quotes.
if ! echo "$EXEC_SQL" | grep -q "amount >= 0"; then
    echo "FAIL: bare @var(min_amount, 0) did not resolve to numeric 'amount >= 0'" >&2
    exit 1
fi
if echo "$EXEC_SQL" | grep -q "@var("; then
    echo "FAIL: emitted SQL still contains a literal @var() marker" >&2
    exit 1
fi
echo "OK: quoted @var(region) -> 'us' and bare @var(min_amount, 0) -> amount >= 0; no marker remains"

echo
echo "==== 6. the optional @var(channel, web) resolved to its inline DEFAULT ===="
# channel was NOT passed to emit-sql above, so it falls back to web.
if ! echo "$EXEC_SQL" | grep -q "channel = 'web'"; then
    echo "FAIL: @var(channel, web) did not fall back to its inline default 'web'" >&2
    exit 1
fi
echo "OK: @var(channel, web) resolved to 'web' with no --var supplied"

echo
echo "==== 7. a REQUIRED @var with no --var is a compile error (E028) ===="
set +e
rocky -o json compile --models models/ > expected/compile-missing.json 2> expected/compile-missing.log
COMPILE_EXIT=$?
set -e
echo "rocky compile exit code (no --var region): $COMPILE_EXIT"
if [ "$COMPILE_EXIT" -eq 0 ]; then
    echo "FAIL: compile succeeded despite an unsupplied required @var(region)" >&2
    exit 1
fi
echo "diagnostic:"
jq -r '.diagnostics[]? | select(.code == "E028") | "  [\(.code)] \(.model): \(.message)\n  suggestion: \(.suggestion)"' \
    expected/compile-missing.json
if ! jq -e '.diagnostics[]? | select(.code == "E028") | .message | test("region")' \
        expected/compile-missing.json > /dev/null; then
    echo "FAIL: expected an E028 diagnostic naming the missing variable 'region'" >&2
    exit 1
fi
echo "OK: missing required @var(region) failed compile with E028 naming the variable"

echo
echo "PASS: @var() run variables resolve at run time — a required var filters the"
echo "      materialized rows and renders into emitted SQL, an optional var falls"
echo "      back to its inline default, and an unsupplied required var fails E028."
exit 0
