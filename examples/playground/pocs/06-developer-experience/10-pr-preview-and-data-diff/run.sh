#!/usr/bin/env bash
# 10-pr-preview-and-data-diff — `rocky preview create / diff / cost` on a
# 5-model DAG. Production path as of engine-v1.18.0. Earlier revisions of
# this script wrapped each preview call in a stub-tolerating helper; that
# scaffolding is gone.
#
# Flow:
#   1. Compile + seed DuckDB.
#   2. Run the pipeline on the base ref (populates `poc.demo.*`).
#   3. Capture HEAD as the preview's `--base`.
#   4. Apply a synthetic (uncommitted) edit to `fct_revenue.sql`.
#   5. `rocky preview create` registers a per-PR branch schema and copies
#      base tables via DuckDB CTAS. NOTE: the prune set is derived from a
#      committed `git diff <base>...HEAD`, so this working-tree edit is
#      not seen — the prune set is empty and all 5 models are copied.
#   6. `rocky run --branch <name>` runs the pipeline, with the edit, into
#      the branch schema and records the branch run. This is the run
#      `preview diff` / `preview cost` pair against the base run (#2162).
#   7. `rocky preview diff` produces a row-count diff between the branch
#      run and the base run.
#   8. `rocky preview cost` produces a per-model bytes/duration/USD
#      delta versus the base run.
#   9. Both are checked for a real pairing, then the synthetic change is
#      reverted via `trap`, idempotent on re-run.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Prefer an explicitly-set binary, then the local feature-branch build,
# then `rocky` on PATH.
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

# Always restore the original fct_revenue on exit, even if the script
# fails halfway through. Idempotent on re-run.
revert_change() {
    if [[ -f "$BACKUP_FILE" ]]; then
        mv -f "$BACKUP_FILE" "$LIVE_FILE"
        echo "==> Reverted synthetic change in $LIVE_FILE"
    fi
}
trap revert_change EXIT

rm -f .rocky-state.redb .rocky-state.redb.lock .rocky_state.redb.lock poc.duckdb
rm -f models/.rocky-state.redb models/.rocky-state.redb.lock
mkdir -p expected

echo "==> 1. Compile the 5-model DAG (type-check, no warehouse)"
"$ROCKY_BIN" compile --models models > expected/compile.json

echo "==> 2. Seed raw tables into DuckDB"
duckdb poc.duckdb < data/seed.sql

echo "==> 3. Run the pipeline on 'main' state — populates poc.demo.*"
"$ROCKY_BIN" -c rocky.toml -o json run > expected/run_main.json

# Capture a 'before' marker. Prefer git HEAD; fall back to a sentinel
# string when the POC is run outside a git checkout.
if BASE_REF=$(git rev-parse HEAD 2>/dev/null); then
    echo "==> 4. Captured base ref: $BASE_REF"
else
    BASE_REF="poc-base"
    echo "==> 4. Not in a git checkout; using sentinel base ref: $BASE_REF"
fi

echo "==> 5. Apply synthetic change to fct_revenue.sql (adds 'WHERE s.amount > 25')"
cp "$LIVE_FILE" "$BACKUP_FILE"
cp "$CHANGED_VARIANT" "$LIVE_FILE"

# Branch name must be a bare SQL identifier ([A-Za-z0-9_]+) because it
# becomes the `branch__<name>` schema prefix for the copy-from-base CTAS;
# hyphens make the copy fail. Underscores keep the schema name valid.
PREVIEW_BRANCH="pr_preview_poc_10"

echo "==> 6. rocky preview create --base $BASE_REF"
"$ROCKY_BIN" -c rocky.toml -o json preview create \
    --base "$BASE_REF" \
    --name "$PREVIEW_BRANCH" \
    --models models \
    > expected/preview_create.json

echo "==> 7. rocky run --branch $PREVIEW_BRANCH — record the branch run diff/cost pair against"
# `preview create` records no run. Without this one, `preview diff` and
# `preview cost` have nothing to pair with the base run from step 3.
"$ROCKY_BIN" -c rocky.toml -o json run \
    --branch "$PREVIEW_BRANCH" \
    > expected/run_branch.json

echo "==> 8a. rocky preview diff --name $PREVIEW_BRANCH (sampled — default)"
"$ROCKY_BIN" -c rocky.toml -o json preview diff \
    --name "$PREVIEW_BRANCH" \
    --base "$BASE_REF" \
    > expected/preview_diff.json

echo "==> 8b. rocky preview diff --algorithm bisection --name $PREVIEW_BRANCH"
# Bisection requires a model declaring a single-column integer / numeric
# `unique_key` on a `Merge` strategy; the POC's models are
# `full_refresh`, so bisection skips them (the tracing log records it)
# and the sampled diff is what applies here.
"$ROCKY_BIN" -c rocky.toml -o json preview diff \
    --name "$PREVIEW_BRANCH" \
    --base "$BASE_REF" \
    --algorithm bisection \
    > expected/preview_diff_bisection.json

echo "==> 9. rocky preview cost --name $PREVIEW_BRANCH"
"$ROCKY_BIN" -c rocky.toml -o json preview cost \
    --name "$PREVIEW_BRANCH" \
    > expected/preview_cost.json

# Quick non-empty sanity check; we don't pin the JSON shape here because
# the example shapes already live in `expected/preview_*.example.json`.
for f in expected/preview_create.json expected/run_branch.json expected/preview_diff.json expected/preview_diff_bisection.json expected/preview_cost.json; do
    if [[ ! -s "$f" ]]; then
        echo "FAIL: $f is empty" >&2
        exit 1
    fi
done

# The paired path, proven: diff and cost both found the branch run. Before
# step 7 existed both were empty ("No paired runs in the state store").
DIFFED_MODELS="$(jq '.models | length' expected/preview_diff.json)"
if [[ "$DIFFED_MODELS" -eq 0 ]]; then
    echo "FAIL: preview diff paired no models — the branch run was not found" >&2
    exit 1
fi
BRANCH_RUN_ID="$(jq -r '.branch_run_id' expected/preview_cost.json)"
if [[ -z "$BRANCH_RUN_ID" ]]; then
    echo "FAIL: preview cost found no branch run" >&2
    exit 1
fi
echo "==> Paired: preview diff covered $DIFFED_MODELS model(s); preview cost used branch run $BRANCH_RUN_ID"

# revert_change runs via trap.

echo
echo "POC complete: rocky preview create, run --branch, diff and cost exercised end-to-end."
echo "JSON output captured under expected/ (gitignored, regenerated each run)."
echo "See the README for what a local run produces vs. a real committed-diff PR."
