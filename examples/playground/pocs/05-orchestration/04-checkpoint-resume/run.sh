#!/usr/bin/env bash
# 04-checkpoint-resume — `rocky run --resume-latest` after a partial failure.
#
# A replication pipeline with three sources. One target is occupied by a VIEW
# that Rocky did not create, so the first run copies two tables and fails the
# third. This script PROVES the resume contract:
#   1. the failed run exits NON-ZERO and checkpoints the tables that succeeded
#   2. `--resume-latest` from a DIFFERENT scope (here, with a `--filter`) is
#      refused — a checkpoint belongs to the scope that wrote it
#   3. `--resume-latest` from the SAME scope skips the two copied tables and
#      retries only the failed one, recording `resumed_from`
#   4. once that run succeeds, `--resume-latest` is refused again — the
#      latest run succeeded, so it has nothing to resume
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb .rocky-state.redb.lock poc.duckdb

# Read one top-level field out of a `rocky -o json run` result.
field() { python3 -c 'import json,sys; v=json.load(open(sys.argv[1]))[sys.argv[2]]; print("null" if v is None else v)' "$1" "$2"; }
expect() { # expect <file> <field> <value>
    local got; got="$(field "$1" "$2")"
    if [ "$got" != "$3" ]; then echo "FAIL: $1 $2 = $got, expected $3" >&2; exit 1; fi
    echo "OK: $2 = $got"
}

duckdb poc.duckdb < data/seed.sql
# Occupy the `products` target with a view whose column matches the source, so
# drift detection has nothing to alter. `full_refresh` then replaces the target
# table, and DuckDB refuses to replace a view with a table, so this one table
# fails while the other two copy.
duckdb poc.duckdb "CREATE SCHEMA staging__products; CREATE VIEW staging__products.products AS SELECT 0::BIGINT AS id"

rocky -c rocky.toml validate

echo
echo "==== 1. run — two tables copy, one fails ===="
# The run is EXPECTED to fail. Capture the exit code without tripping `set -e`.
set +e
rocky -c rocky.toml -o json run > expected/run1.json 2> expected/run1.log
RUN1_EXIT=$?
set -e
# Exit 2 is the partial-failure contract: some tables copied, at least one did not.
if [ "$RUN1_EXIT" -ne 2 ]; then echo "FAIL: run 1 exited $RUN1_EXIT, expected 2 (partial failure)" >&2; exit 1; fi
echo "OK: run 1 exited 2 (partial failure)"
expect expected/run1.json status PartialFailure
expect expected/run1.json tables_copied 2
expect expected/run1.json tables_failed 1
grep -q "is of type View" expected/run1.json \
    || { echo "FAIL: run 1 did not fail on the view collision (see expected/run1.json errors[])" >&2; exit 1; }
echo "OK: the failure is the occupied target"
# The run output does not carry its own id; the run record does.
RUN1_ID="$(rocky -c rocky.toml -o json history | python3 -c '
import json,sys
runs=[r for r in json.load(sys.stdin)["runs"] if "partial" in r["status"].lower()]
assert len(runs)==1, runs
print(runs[0]["run_id"])')"
echo "run 1 id: $RUN1_ID"

echo
echo "==== 2. a resume from a different scope is refused ===="
# `--filter source=orders` is a different scope from the run that wrote the
# checkpoint (no filter). The checkpoint is not visible from here, so the
# resume refuses instead of skipping tables another scope copied.
set +e
rocky -c rocky.toml -o json run --resume-latest --filter source=orders \
    > expected/run-other-scope.json 2> expected/run-other-scope.log
OTHER_EXIT=$?
set -e
if [ "$OTHER_EXIT" -eq 0 ]; then echo "FAIL: a resume from another scope was accepted" >&2; exit 1; fi
grep -q "no progress found" expected/run-other-scope.log \
    || { echo "FAIL: expected 'no progress found' in expected/run-other-scope.log" >&2; exit 1; }
echo "OK: refused — $(grep -m1 -o 'cannot resume.*' expected/run-other-scope.log)"

echo
echo "==== 3. repair the target, then resume from the same scope ===="
duckdb poc.duckdb "DROP VIEW staging__products.products"
rocky -c rocky.toml -o json run --resume-latest > expected/run2.json
expect expected/run2.json status Success
expect expected/run2.json resumed_from "$RUN1_ID"
expect expected/run2.json tables_skipped 2
expect expected/run2.json tables_copied 1
expect expected/run2.json tables_failed 0
PRODUCTS_ROWS="$(duckdb -noheader -list poc.duckdb 'SELECT count(*) FROM staging__products.products')"
if [ "$PRODUCTS_ROWS" != "50" ]; then echo "FAIL: staging__products.products has ${PRODUCTS_ROWS:-no} rows, expected 50" >&2; exit 1; fi
echo "OK: products rows = $PRODUCTS_ROWS (the failed table was copied on resume)"

echo
echo "==== 4. the latest run succeeded, so --resume-latest has nothing to resume ===="
set +e
rocky -c rocky.toml -o json run --resume-latest > expected/run3.json 2> expected/run3.log
RUN3_EXIT=$?
set -e
if [ "$RUN3_EXIT" -eq 0 ]; then echo "FAIL: a succeeded run was resumed" >&2; exit 1; fi
grep -q "already succeeded" expected/run3.log \
    || { echo "FAIL: expected 'already succeeded' in expected/run3.log" >&2; exit 1; }
echo "OK: refused — $(grep -m1 -Eo '(nothing to resume|cannot resume).*' expected/run3.log)"

echo
echo "POC complete: the failed table was retried, the copied tables were skipped, a cross-scope resume was refused, and a resume after success was refused."
