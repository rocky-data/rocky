#!/usr/bin/env bash
# 15-semantic-breaking-change-gate — `rocky ci-diff --semantic` (informational)
# + `rocky branch promote` pre-promote gate (hard veto).
#
# The POC stands up a throw-away git repo INSIDE the POC dir so the
# breaking-change gate has real `main` ↔ `HEAD` refs to diff. The repo is
# self-contained — nothing leaks out via `..` or the parent repo.
#
# Flow:
#   1. Initialize the throw-away git repo with the baseline models.
#   2. Seed raw__orders, run the replication pipeline on `main`, create
#      the `fix_orders` rocky branch and run on it (so promote has a
#      shadow target to copy from).
#   3. Edit `orders_summary.sql` to drop `total_amount`, commit on a
#      feature branch — a clear Breaking-severity column drop.
#   4. `rocky ci-diff --semantic` surfaces the BREAKING classification
#      (informational — exit 0 even on Breaking findings).
#   5. `rocky branch promote fix_orders` — the pre-promote gate VETOES
#      with exit 1 and an explanatory error.
#   6. `rocky branch promote fix_orders --allow-breaking` — gate
#      overridden; promote completes and emits a
#      `BreakingChangesAllowed` audit event for the paper trail.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Restore the committed baseline of orders_summary.sql on ANY exit
# (success, failure, SIGINT) so the source tree stays clean — the smoke
# runner re-runs every POC and we don't want the feature-branch variant
# leaking back into the repo. Idempotent on re-run.
restore_baseline() {
    cat > models/orders_summary.sql <<'SQL'
-- Baseline shape: per-customer totals.
-- The feature branch will drop the `total_amount` column — a Breaking-
-- severity change the pre-promote gate must reject.
SELECT
    customer_id,
    SUM(amount) AS total_amount
FROM poc.demo.raw_orders
GROUP BY customer_id
SQL
}
trap restore_baseline EXIT

# Wipe anything the previous run left behind. Rocky stores state at
# `<models>/.rocky-state.redb` since the LSP↔CLI state-path unification;
# the bare-CWD `.rocky-state.redb` is the legacy fallback. Wipe both.
rm -rf .rocky-state.redb .rocky-state.redb.lock poc.duckdb .rocky
rm -f models/.rocky-state.redb models/.rocky-state.redb.lock
rm -rf .git
# Restore baseline up-front so the throwaway git repo has the right
# initial state to commit.
restore_baseline
mkdir -p expected

echo "==> 1. Initialize throw-away git repo (scoped to this POC dir)"
# `--initial-branch=main` so the base-ref default works on machines whose
# git defaults to `master`. Repo lives in this dir only.
git -c init.defaultBranch=main init -q
git config user.email "poc@rocky.local"
git config user.name "POC"
git config commit.gpgsign false
# Excludes go to .git/info/exclude so we don't leave a .gitignore on
# disk after the trap restores baseline — the parent playground
# .gitignore already filters these patterns from the outer Rocky repo.
cat > .git/info/exclude <<'EOF'
.rocky-state.redb
.rocky-state.redb.lock
.rocky/
poc.duckdb
expected/
EOF
git add -A
git commit -q -m "baseline: orders_summary with total_amount column"

echo "==> 2. Seed raw__orders + run replication on 'main'"
duckdb poc.duckdb < data/seed.sql
rocky -c rocky.toml -o json run --filter source=orders \
    > expected/run_main.json

echo "==> 3. Create rocky branch 'fix_orders' and run on it"
rocky branch create fix_orders \
    --description "drop total_amount from orders_summary" \
    > expected/branch_create.json
rocky -c rocky.toml -o json run --filter source=orders --branch fix_orders \
    > expected/run_branch.json

echo "==> 4. Switch to feature branch and drop the 'total_amount' column"
git checkout -q -b feat/drop-total-amount
cat > models/orders_summary.sql <<'SQL'
-- Feature branch: total_amount column dropped (a Breaking change).
SELECT
    customer_id
FROM poc.demo.raw_orders
GROUP BY customer_id
SQL
git add models/orders_summary.sql
git commit -q -m "drop total_amount column"

echo
echo "==> 5. rocky ci-diff --semantic (informational — exit 0 even on Breaking)"
rocky ci-diff --semantic --models models main \
    > expected/ci_diff_semantic.json
python3 - <<'PY'
import json
d = json.load(open("expected/ci_diff_semantic.json"))
findings = d.get("breaking_findings", [])
print(f"    breaking_findings: {len(findings)} entry/entries")
for f in findings:
    print(f"      - severity={f['severity']:9s} kind={f['change']['kind']}")
breaking = [f for f in findings if f["severity"] == "breaking"]
assert len(breaking) >= 1, f"expected at least one Breaking finding, got: {findings}"
PY

echo
echo "==> 6. rocky branch promote fix_orders  (gate VETOES — exit 1 expected)"
set +e
rocky -c rocky.toml -o json branch promote fix_orders \
    --base-ref main \
    --models models \
    > expected/promote_blocked.json 2>expected/promote_blocked.stderr
PROMOTE_RC=$?
set -e
echo "    exit $PROMOTE_RC  (nonzero = gate fired as expected)"
if [[ "$PROMOTE_RC" -eq 0 ]]; then
    echo "    FAIL: gate did not block the promote" >&2
    exit 1
fi
python3 - <<'PY'
import json
d = json.load(open("expected/promote_blocked.json"))
print(f"    success={d['success']}  audit kinds: "
      f"{[a['kind'] for a in d['audit']]}")
breaking = d.get("breaking_changes") or []
print(f"    breaking_changes carried: {len(breaking)} entry/entries")
assert any(a["kind"] == "breaking_changes_blocked" for a in d["audit"]), \
    "expected BreakingChangesBlocked in audit log"
PY

echo
echo "==> 7. rocky branch promote fix_orders --allow-breaking  (override)"
rocky -c rocky.toml -o json branch promote fix_orders \
    --base-ref main \
    --models models \
    --allow-breaking \
    > expected/promote_allowed.json
python3 - <<'PY'
import json
d = json.load(open("expected/promote_allowed.json"))
print(f"    success={d['success']}  audit kinds: "
      f"{[a['kind'] for a in d['audit']]}")
assert d["success"] is True, "expected promote to succeed under --allow-breaking"
assert any(a["kind"] == "breaking_changes_allowed" for a in d["audit"]), \
    "expected BreakingChangesAllowed in audit log"
PY

echo
echo "POC complete: gate VETOED the column drop; --allow-breaking overrode with a paper trail."
