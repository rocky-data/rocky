#!/usr/bin/env bash
# 11-lineage-diff — `rocky lineage-diff` PR-time blast-radius
#
# `rocky lineage-diff` compares two git refs and lists, per changed model,
# the column-level diff plus downstream consumers on HEAD's compile. The
# command is git-driven, so this POC sets up its own scratch repo at
# /tmp/rocky-poc-lineage-diff with two commits: a baseline on `main` and
# a feature branch that renames a column upstream and adds two derived
# columns. The resulting `lineage-diff` output names every changed
# column across two models in one shot — the message that lands in a PR
# comment.
#
# Self-contained: no warehouse credentials, no network. The scratch repo
# is fully torn down at the start of each run.

set -euo pipefail

export RUST_LOG="${RUST_LOG:-error}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRATCH="${ROCKY_LINEAGE_SCRATCH:-/tmp/rocky-poc-lineage-diff}"

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

mkdir -p "$HERE/expected"
rm -rf "$SCRATCH"
mkdir -p "$SCRATCH"
cp -r "$HERE/models" "$HERE/rocky.toml" "$SCRATCH/"
cd "$SCRATCH"

GIT_COMMIT="git -c user.email=demo@rocky.dev -c user.name=Demo -c commit.gpgsign=false"

echo "==> 1. Initialise scratch repo + commit baseline 5-model DAG"
git init -q -b main
$GIT_COMMIT add .
$GIT_COMMIT commit -q -m "baseline: 5-model DAG"

echo "==> 2. Branch + apply revenue-rework changes"
$GIT_COMMIT checkout -q -b feature/revenue-rework

cat > models/stg_orders.sql <<'EOF'
SELECT
    order_id,
    customer_id,
    amount AS amount_usd,
    amount * 0.20 AS tax_amount_usd,
    status,
    order_date
FROM poc.demo.raw_orders
WHERE status != 'cancelled'
EOF

cat > models/fct_revenue.sql <<'EOF'
SELECT
    s.customer_id,
    c.segment,
    c.region,
    SUM(s.amount_usd) AS total_revenue,
    SUM(s.tax_amount_usd) AS total_tax
FROM poc.demo.stg_orders s
JOIN poc.demo.dim_customers c USING (customer_id)
GROUP BY s.customer_id, c.segment, c.region
EOF

$GIT_COMMIT add -A
$GIT_COMMIT commit -q -m "rename amount->amount_usd; add tax columns"

echo
echo "==> 3. rocky lineage-diff main --output table"
"$ROCKY_BIN" lineage-diff main --output table | tee "$HERE/expected/lineage_diff.txt"

echo
echo "==> 4. rocky lineage-diff main -o json (captured for shape contract)"
"$ROCKY_BIN" lineage-diff main -o json > "$HERE/expected/lineage_diff.json"

echo
echo "POC complete: lineage-diff output captured under expected/."
