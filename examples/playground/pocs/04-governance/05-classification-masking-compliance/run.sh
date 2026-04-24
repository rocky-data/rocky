#!/usr/bin/env bash
# 05-classification-masking-compliance — end-to-end demo
#
# Walks the operator through the Wave A + Wave B governance surface:
#   1. `rocky run`        — materialize models on local DuckDB.
#   2. `rocky compliance` — static resolver over the [classification]
#                           sidecars + [mask] / [mask.<env>] policy.
#   3. `--exceptions-only` — narrow per_column to the unmasked rows.
#   4. `--fail-on exception` — CI gate (exit 1 when any exception exists).
#
# DuckDB does NOT enforce masking at apply time (no Unity Catalog column
# tags, no CREATE MASK). The POC's value here is showing the config flow
# + static compliance check + CI gate. See README for the Databricks
# "apply for real" path.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

# Load seed tables so `rocky run` has something to replicate.
duckdb poc.duckdb < data/seed.sql

rocky validate

# -------------------------------------------------------------------------
# Step 1 — run the pipeline.
#
# Replication pipeline is the simplest shape that lets us exercise the
# governance rollup against real `[classification]` sidecars. Like POC
# 09, the DuckDB path may exit non-zero on edges unrelated to the demo —
# we tolerate that with `|| true`. The per-column classification is read
# from the model sidecars, not from the warehouse, so the compliance
# rollup works regardless.
echo
echo "== Step 1: rocky run (materialize models) =="
rocky -c rocky.toml -o json run \
  --filter source=users \
  > expected/run_users.json || true
STATUS=$(jq -r '.status // "error"' expected/run_users.json)
echo "run (users)     status = $STATUS"

# -------------------------------------------------------------------------
# Step 2 — compliance rollup scoped to `dev`.
#
# No `[mask.dev]` override exists, so `--env dev` falls back to the
# workspace [mask] defaults (pii=hash, pii_high=redact, internal=none).
# `owner_email` resolves (pii); `audit_note` does NOT (audit_only has no
# strategy) — that's the one exception we expect.
echo
echo "== Step 2: rocky compliance --env dev =="
rocky -c rocky.toml compliance --env dev --models models

# -------------------------------------------------------------------------
# Step 3 — same report scoped to `prod`, filtered to exceptions only.
#
# `[mask.prod]` tightens pii from "hash" -> "redact" (still resolves) but
# does NOT add a strategy for `audit_only`, so the prod exception set is
# the same single row. `--exceptions-only` trims per_column to just the
# offending row.
echo
echo "== Step 3: rocky compliance --env prod --exceptions-only =="
rocky -c rocky.toml compliance --env prod --exceptions-only --models models

# -------------------------------------------------------------------------
# Step 4 — CI gate. `--fail-on exception` exits 1 when any exception
# fires. We capture the exit code instead of letting it kill the demo,
# because the whole point of this step is to SHOW that it returns 1.
#
# Note: the `set +e` / `set -e` dance is load-bearing. `|| true` would
# swallow the exit code and defeat the demonstration; `set -euo pipefail`
# at the top of the script would abort the demo on a non-zero exit.
echo
echo "== Step 4: rocky compliance --env prod --fail-on exception =="
set +e
rocky -c rocky.toml compliance \
    --env prod \
    --fail-on exception \
    --models models \
    -o json \
    > expected/compliance_prod_fail_on.json
CI_EXIT=$?
set -e
echo "CI gate exit code = $CI_EXIT  (1 = exceptions present -> block the merge)"

# -------------------------------------------------------------------------
# Demo invariants
# -------------------------------------------------------------------------
EXCEPTIONS=$(jq -r '.summary.total_exceptions' expected/compliance_prod_fail_on.json)
CLASSIFIED=$(jq -r '.summary.total_classified' expected/compliance_prod_fail_on.json)
if [[ "$CI_EXIT" -ne 1 ]]; then
  echo "FAIL: expected --fail-on exception to exit 1, got $CI_EXIT" >&2
  exit 1
fi
if [[ "$EXCEPTIONS" -lt 1 ]]; then
  echo "FAIL: expected >= 1 exception in compliance report, got $EXCEPTIONS" >&2
  exit 1
fi

echo
echo "POC complete:"
echo "  - classified columns scanned : $CLASSIFIED"
echo "  - exceptions in prod         : $EXCEPTIONS"
echo "  - CI gate exit code          : $CI_EXIT"
echo
echo "Next: uncomment [classifications.allow_unmasked] in rocky.toml to"
echo "      suppress the audit_only exception without adding a mask."
