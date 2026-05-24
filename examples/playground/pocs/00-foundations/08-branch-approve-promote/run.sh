#!/usr/bin/env bash
# 08-branch-approve-promote — gated promotion of a branch into prod, plus the
# soundness property that the signed approval is bound to your *model bytes*.
set -euo pipefail
export ROCKY_SUPPRESS_DEPRECATION=1

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Step 7 tampers with a model file; restore the committed baseline on ANY exit
# so the source tree stays clean and re-runs start fresh. Idempotent.
restore_model() {
    cat > models/orders_clean.sql <<'SQL'
-- A transformation model on top of the replicated orders. Its file bytes are
-- folded into the branch_state_hash, so editing this SQL after an approval is
-- signed drifts the hash and invalidates the approval (the soundness property
-- this POC's step 8 demonstrates).
SELECT
    order_id,
    customer_id,
    amount,
    ordered_at
FROM poc.prod__orders.orders
WHERE status <> 'cancelled'
SQL
}
trap restore_model EXIT
restore_model

rm -rf .rocky-state.redb .rocky-state.redb.lock poc.duckdb .rocky/approvals
rm -f models/.rocky-state.redb models/.rocky-state.redb.lock
mkdir -p expected

echo "==> 1. Seed raw__orders.orders into DuckDB"
duckdb poc.duckdb < data/seed.sql

echo "==> 2. Run replication on main (writes poc.prod__orders.orders)"
rocky -c rocky.toml -o json run --filter source=orders > expected/run_main.json

echo "==> 3. Create a named branch 'fix_orders'"
rocky branch create fix_orders \
    --description "Clean cancelled rows out of the orders feed" \
    > expected/branch_create.json

echo "==> 4. Run replication against the branch (writes poc.branch__fix_orders.orders)"
rocky -c rocky.toml -o json run --filter source=orders --branch fix_orders \
    > expected/run_branch.json

echo "==> 5. Try to promote without an approval — gate refuses (exit 1)"
set +e
rocky -c rocky.toml branch promote fix_orders > expected/promote_blocked.json 2>&1
echo "    (exit $?)"
set -e

echo "==> 6. Approve — signs an artifact bound to branch_state_hash"
echo "       (the hash covers the config AND the models/ source bytes)"
rocky -c rocky.toml branch approve fix_orders \
    --message "verified row counts + orders_clean SQL; safe to ship" \
    > expected/branch_approve.json
ls -1 .rocky/approvals/fix_orders/

echo "==> 7. Tamper: edit models/orders_clean.sql AFTER the sign-off"
printf '  AND amount > 0   -- sneaked in after approval\n' >> models/orders_clean.sql

echo "==> 8. Promote — the approval is now STALE (model bytes changed -> hash drift)"
set +e
rocky -c rocky.toml branch promote fix_orders > expected/promote_stale.json 2>&1
echo "    (exit $?)  reason: $(grep -o 'state_hash_mismatch' expected/promote_stale.json | head -1)"
set -e

echo "==> 9. Re-approve the current state, then promote — gate satisfied"
rocky -c rocky.toml branch approve fix_orders \
    --message "re-verified after the orders_clean edit" \
    > expected/branch_reapprove.json
rocky -c rocky.toml branch promote fix_orders > expected/branch_promote.json

echo
echo "==> Warehouse tables after promotion:"
duckdb poc.duckdb <<'SQL'
SELECT table_schema, table_name FROM information_schema.tables
WHERE table_catalog = 'poc' ORDER BY table_schema, table_name;
SQL

echo
echo "POC complete: the gate refused promote without a signed approval (step 5),"
echo "rejected the approval once a model was edited under it (step 8), and shipped"
echo "only after a re-approval matched the new branch_state_hash (step 9)."
