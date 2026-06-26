#!/usr/bin/env bash
# 11-fail-loud-on-compile-error — Rocky's "fail loud, never silently drop data".
#
# A transformation pipeline with two independent models:
#   - stg_orders     — clean full_refresh, materializes
#   - daily_revenue  — time_interval model with a real compile error (E020):
#                      its declared `time_column = order_date` is absent from
#                      the SELECT output.
#
# This script PROVES that `rocky run` over that project fails loudly instead of
# reporting a green run:
#   1. the run exits NON-ZERO
#   2. `--output json` reports status `partial_failure`
#   3. the broken model is recorded with failure_kind `compile-error` (E020)
#   4. the broken model's target table is ABSENT (it never reached the warehouse)
#   5. the clean model's table EXISTS and is fully populated — good data lands
# Then it applies the one-line fix and shows the same pipeline runs green.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

mkdir -p expected
rm -f .rocky-state.redb .rocky-state.redb.lock poc.duckdb
rm -f models/.rocky-state.redb models/.rocky-state.redb.lock

# The final step rewrites models/daily_revenue.sql with the fixed SELECT. Keep
# a pristine copy and restore it on exit so the committed POC stays "broken"
# (that is the demo subject) no matter how this script ends.
BROKEN_BACKUP="$(mktemp)"
cp models/daily_revenue.sql "$BROKEN_BACKUP"
restore_broken() { cp "$BROKEN_BACKUP" models/daily_revenue.sql; rm -f "$BROKEN_BACKUP"; }
trap restore_broken EXIT
# On Ctrl-C / timeout, exit so the EXIT trap fires and restores the file rather
# than leaving models/daily_revenue.sql in its temporarily-fixed state.
trap 'exit 130' INT TERM

echo "==== 1. seed a raw source table both models read ===="
duckdb poc.duckdb < data/seed.sql
echo "raw__sales.orders rows: $(duckdb -noheader -list poc.duckdb 'SELECT count(*) FROM raw__sales.orders')"

echo
echo "==== 2. rocky run — one clean model, one model that fails to compile ===="
# The run is EXPECTED to fail. Capture the exit code without tripping `set -e`.
set +e
rocky -c rocky.toml -o json run --models models/ --partition 2026-04-07 \
    > expected/run-broken.json 2> expected/run-broken.log
RUN_EXIT=$?
set -e
echo "rocky run exit code: $RUN_EXIT"

echo
echo "==== 3. assert: the run exited NON-ZERO ===="
if [ "$RUN_EXIT" -eq 0 ]; then
    echo "FAIL: rocky run exited 0 over a project with a compile error" >&2
    exit 1
fi
echo "OK: run exited $RUN_EXIT (non-zero) instead of silently succeeding"

echo
echo "==== 4. assert: status is PartialFailure with a compile-error on daily_revenue ===="
STATUS="$(jq -r '.status' expected/run-broken.json)"
echo "status: $STATUS"
if [ "$STATUS" != "PartialFailure" ]; then
    echo "FAIL: expected status PartialFailure, got '$STATUS'" >&2
    exit 1
fi

echo "errors recorded by the run:"
jq -r '.errors[] | "  - \(.asset_key | join(".")): failure_kind=\(.failure_kind) :: \(.error)"' \
    expected/run-broken.json

KIND="$(jq -r '.errors[] | select(.asset_key | index("daily_revenue")) | .failure_kind' expected/run-broken.json)"
if [ "$KIND" != "compile-error" ]; then
    echo "FAIL: expected failure_kind compile-error for daily_revenue, got '$KIND'" >&2
    exit 1
fi
if ! jq -e '.errors[] | select(.asset_key | index("daily_revenue")) | .error | test("E020")' \
        expected/run-broken.json > /dev/null; then
    echo "FAIL: the daily_revenue error did not name the E020 compile diagnostic" >&2
    exit 1
fi
echo "OK: daily_revenue failed loud with failure_kind=compile-error (E020)"

echo
echo "==== 5. assert: the broken model's table is ABSENT (never built) ===="
if duckdb -noheader -list poc.duckdb "SELECT 1 FROM poc.main.daily_revenue LIMIT 1" 2>/dev/null; then
    echo "FAIL: poc.main.daily_revenue exists — a broken model should not materialize" >&2
    exit 1
fi
echo "OK: poc.main.daily_revenue does not exist"

echo
echo "==== 6. assert: the clean model's data DID land ===="
GOOD_ROWS="$(duckdb -noheader -list poc.duckdb 'SELECT count(*) FROM poc.main.stg_orders')"
echo "poc.main.stg_orders rows: $GOOD_ROWS"
if [ "$GOOD_ROWS" -lt 1 ]; then
    echo "FAIL: stg_orders is empty — the clean model should still materialize" >&2
    exit 1
fi
echo "OK: the clean model materialized $GOOD_ROWS rows despite the sibling failure"

echo
echo "==== 7. apply the one-line fix and re-run — the same pipeline goes green ===="
cp daily_revenue.fixed.sql models/daily_revenue.sql
rm -f poc.duckdb
duckdb poc.duckdb < data/seed.sql
rocky -c rocky.toml -o json run --models models/ --partition 2026-04-07 \
    > expected/run-fixed.json 2> expected/run-fixed.log
FIXED_STATUS="$(jq -r '.status' expected/run-fixed.json)"
FIXED_FAILED="$(jq -r '.tables_failed' expected/run-fixed.json)"
echo "fixed run status: $FIXED_STATUS (tables_failed: $FIXED_FAILED)"
if [ "$FIXED_STATUS" != "Success" ] || [ "$FIXED_FAILED" -ne 0 ]; then
    echo "FAIL: expected a clean success after the fix, got status=$FIXED_STATUS failed=$FIXED_FAILED" >&2
    exit 1
fi
FIXED_ROWS="$(duckdb -noheader -list poc.duckdb 'SELECT count(*) FROM poc.main.daily_revenue')"
echo "poc.main.daily_revenue rows after fix: $FIXED_ROWS"
if [ "$FIXED_ROWS" -lt 1 ]; then
    echo "FAIL: daily_revenue did not materialize after the fix" >&2
    exit 1
fi
echo "OK: with the compile error fixed, the same pipeline runs green and builds daily_revenue"

echo
echo "PASS: rocky run failed loud on the compile error (non-zero exit, partial_failure,"
echo "      compile-error on daily_revenue), the broken table was never built, the clean"
echo "      model's data still landed, and the fixed pipeline then ran green."
exit 0
