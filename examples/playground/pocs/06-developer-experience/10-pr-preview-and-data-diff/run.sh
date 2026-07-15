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
#      not seen — the prune set is empty and all 5 models are copied,
#      none re-run. The full prune/re-run/diff path fires only on a real
#      PR where the change is a committed diff (see README).
#   6. `rocky preview diff` produces a structural + sampled-row diff
#      between branch and base (empty here: no branch run to diff).
#   7. `rocky preview cost` produces a per-model bytes/duration/USD
#      delta versus the latest base-schema run.
#   8. The synthetic change is reverted via `trap`, idempotent on re-run.

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

echo "==> 7a. rocky preview diff --name $PREVIEW_BRANCH (sampled — default)"
"$ROCKY_BIN" -c rocky.toml -o json preview diff \
    --name "$PREVIEW_BRANCH" \
    --base "$BASE_REF" \
    > expected/preview_diff.json

echo "==> 7b. rocky preview diff --algorithm bisection --name $PREVIEW_BRANCH"
# In this local run there is no branch run in the state store (empty
# prune set → nothing re-run), so bisection has nothing to diff and the
# tracing log records the skip. On a real PR, bisection additionally
# requires a model declaring a single-column integer / numeric
# `unique_key` on a `Merge` strategy; the POC's fct_revenue is
# `full_refresh`, so the sampled diff is what applies there.
"$ROCKY_BIN" -c rocky.toml -o json preview diff \
    --name "$PREVIEW_BRANCH" \
    --base "$BASE_REF" \
    --algorithm bisection \
    > expected/preview_diff_bisection.json

echo "==> 8. rocky preview cost --name $PREVIEW_BRANCH"
"$ROCKY_BIN" -c rocky.toml -o json preview cost \
    --name "$PREVIEW_BRANCH" \
    > expected/preview_cost.json

# Quick non-empty sanity check; we don't pin the JSON shape here because
# the example shapes already live in `expected/preview_*.example.json`.
for f in expected/preview_create.json expected/preview_diff.json expected/preview_diff_bisection.json expected/preview_cost.json; do
    if [[ ! -s "$f" ]]; then
        echo "FAIL: $f is empty" >&2
        exit 1
    fi
done

# revert_change runs via trap.

echo
echo "POC complete: rocky preview create/diff/cost exercised end-to-end."
echo "JSON output captured under expected/ (gitignored, regenerated each run)."
echo "See the README for what a local run produces vs. a real committed-diff PR."
