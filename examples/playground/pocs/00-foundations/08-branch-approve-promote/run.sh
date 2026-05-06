#!/usr/bin/env bash
# 08-branch-approve-promote — gated promotion of a branch into prod
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

rm -rf .rocky-state.redb poc.duckdb .rocky/approvals
mkdir -p expected

echo "==> 1. Seed raw__orders.orders into DuckDB"
duckdb poc.duckdb < data/seed.sql

echo "==> 2. Run replication on main (writes poc.prod__orders.orders)"
rocky -c rocky.toml -o json run --filter source=orders > expected/run_main.json

echo "==> 3. Create a named branch 'fix_orders'"
rocky branch create fix_orders \
    --description "Drop cancelled rows from the orders feed" \
    > expected/branch_create.json

echo "==> 4. Run replication against the branch (writes poc.branch__fix_orders.orders)"
rocky -c rocky.toml -o json run --filter source=orders --branch fix_orders \
    > expected/run_branch.json

echo "==> 5. Try to promote without an approval — gate refuses"
set +e
rocky -c rocky.toml branch promote fix_orders > expected/promote_blocked.json 2>&1
echo "    (exit $?)"
set -e

echo "==> 6. Sign the branch — git identity binds the artifact"
rocky -c rocky.toml branch approve fix_orders \
    --message "verified row counts; safe to ship" \
    > expected/branch_approve.json
ls -1 .rocky/approvals/fix_orders/

echo "==> 7. Promote — gate now satisfied; copy branch tables onto prod targets"
rocky -c rocky.toml branch promote fix_orders > expected/branch_promote.json

echo
echo "==> Warehouse tables after promotion:"
duckdb poc.duckdb <<'SQL'
SELECT table_schema, table_name FROM information_schema.tables
WHERE table_catalog = 'poc' ORDER BY table_schema, table_name;
SQL

echo
echo "POC complete: approval gate refused promote until a signed artifact landed; audit JSON in expected/."
