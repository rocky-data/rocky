#!/usr/bin/env bash
# 10-pr-preview-and-data-diff — PR preview workflow on a 5-model DAG.
#
# Today (Phase 0): the engine handlers for `rocky preview {create,diff,cost}`
# bail with a "Phase N implementation" message — that's expected. This POC is
# the integration-test baseline that Phase 1+ will populate.
#
# Each preview step below is wrapped so the stub error is captured and the
# script keeps going; non-stub steps (compile, test, run on main) must
# succeed, otherwise we exit non-zero.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Prefer an explicitly-set binary, then the local feature-branch build
# (which has `preview` wired), then `rocky` on PATH.
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

CHANGED_VARIANT="models/fct_revenue.sql.changed"
LIVE_FILE="models/fct_revenue.sql"
BACKUP_FILE="models/fct_revenue.sql.orig"

# Always restore the original fct_revenue on exit, even if the script fails
# halfway through. Idempotent: re-running the POC must leave the working tree
# byte-identical to the starting state.
revert_change() {
    if [[ -f "$BACKUP_FILE" ]]; then
        mv -f "$BACKUP_FILE" "$LIVE_FILE"
        echo "==> Reverted synthetic change in $LIVE_FILE"
    fi
}
trap revert_change EXIT

rm -f .rocky-state.redb poc.duckdb
mkdir -p expected

echo "==> 1. Compile the 5-model DAG (type-check, no warehouse)"
"$ROCKY_BIN" compile --models models > expected/compile.json

echo "==> 2. Seed raw tables into DuckDB"
duckdb poc.duckdb < data/seed.sql

echo "==> 3. Run the pipeline on 'main' state — populates poc.demo.*"
"$ROCKY_BIN" -c rocky.toml -o json run > expected/run_main.json
echo "    main run: ok"

# Capture a 'before' marker. Prefer git HEAD; fall back to a sentinel string
# when the POC is run outside of a git checkout (rare, but possible in CI
# tarballs).
if BASE_REF=$(git rev-parse HEAD 2>/dev/null); then
    echo "==> 4. Captured base ref: $BASE_REF"
else
    BASE_REF="poc-base"
    echo "==> 4. Not in a git checkout; using sentinel base ref: $BASE_REF"
fi

echo "==> 5. Apply synthetic change to fct_revenue.sql (adds 'WHERE s.amount > 25')"
cp "$LIVE_FILE" "$BACKUP_FILE"
cp "$CHANGED_VARIANT" "$LIVE_FILE"

# Helper: run a preview subcommand that is either a known Phase-N stub or
# not yet wired in the binary on PATH. Capture stderr, surface a clear note,
# and keep the script going. A real non-stub failure still aborts.
run_stub() {
    local label="$1"
    local outfile="$2"
    shift 2
    echo "==> $label"
    if "$@" > "$outfile" 2> "${outfile}.err"; then
        echo "    $label: succeeded (Phase 1+ implementation has landed)"
        return 0
    fi
    local exit_code=$?
    local err_text
    err_text="$(cat "${outfile}.err" 2>/dev/null || true)"
    if grep -q "implementation lands in Phase" "${outfile}.err" 2>/dev/null; then
        echo "    $label: stub (exit $exit_code) — Phase 1+ will populate this"
        head -n 4 "${outfile}.err" | sed 's/^/      /'
        return 0
    fi
    if echo "$err_text" | grep -qiE "unrecognized subcommand|invalid subcommand|unexpected argument|error:.*'preview'"; then
        echo "    $label: 'preview' not wired in this rocky binary yet — Phase 0a engine PR not on PATH"
        head -n 4 "${outfile}.err" | sed 's/^/      /'
        return 0
    fi
    echo "    $label: FAILED with non-stub error (exit $exit_code)"
    echo "$err_text" | sed 's/^/      /'
    return $exit_code
}

PREVIEW_BRANCH="pr-preview-poc-10"

run_stub "6. rocky preview create --base $BASE_REF" \
    expected/preview_create.json \
    "$ROCKY_BIN" -c rocky.toml -o json preview create \
        --base "$BASE_REF" \
        --name "$PREVIEW_BRANCH" \
        --models models

run_stub "7. rocky preview diff --name $PREVIEW_BRANCH" \
    expected/preview_diff.json \
    "$ROCKY_BIN" -c rocky.toml -o json preview diff \
        --name "$PREVIEW_BRANCH" \
        --base "$BASE_REF"

run_stub "8. rocky preview cost --name $PREVIEW_BRANCH" \
    expected/preview_cost.json \
    "$ROCKY_BIN" -c rocky.toml -o json preview cost \
        --name "$PREVIEW_BRANCH"

# revert_change runs via trap.

echo
echo "POC complete: stubs exercised; expected/ holds Phase 1+ target shapes."
echo "When Phase 1 lands, the .err sidecars disappear and the JSON files"
echo "match expected/preview_*.example.json (the aspirational fixtures)."
