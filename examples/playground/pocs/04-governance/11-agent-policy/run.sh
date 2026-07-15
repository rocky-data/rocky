#!/usr/bin/env bash
# 11-agent-policy — the agent-policy plane ENFORCED end-to-end.
#
# Drives the full propose -> embed -> apply -> deny/allow -> record path with
# no credentials (DuckDB). An agent-authored plan touching a contracted model
# is DENIED at apply time with the rule named; an additive bronze change is
# ALLOWED and materializes; `rocky audit` shows both decisions in the ledger.
#
# The change-classification is computed against a git baseline, so the demo
# runs inside a throwaway git repo in a temp dir (never the surrounding repo).
#
# The apply-side principal comes from ROCKY_PRINCIPAL: `plan --principal agent`
# labels the plan-time evaluation, but `rocky apply` resolves who is applying
# from its own environment — without ROCKY_PRINCIPAL=agent the apply runs as a
# human, humans are never gated, and the deny below would not fire.
set -euo pipefail

export ROCKY_SUPPRESS_DEPRECATION=1

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "$HERE/expected"

# Throwaway git repo so the base<->head classification diff has a baseline.
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
cp "$HERE/rocky.toml" "$WORK/"
cp -r "$HERE/models" "$WORK/"
cd "$WORK"

git init -q -b main
git config user.email "poc@rocky.dev"
git config user.name "rocky-poc"
git add -A
git commit -q -m "baseline: three models under a [policy] block"

# Extract the 64-hex plan_id from a `rocky plan --output json` payload.
plan_id() { grep -oE '[0-9a-f]{64}' | head -1; }

echo "=== 1. agent proposes a change to a CONTRACTED model (fct_orders) ==="
# Additive edit (adds a column) to the contracted, gold-layer model.
printf 'SELECT 1 AS order_id, 100 AS amount, 0 AS discount\n' > models/fct_orders.sql
DENY_PLAN=$(rocky -c rocky.toml -o json plan \
    --principal agent --base HEAD --model fct_orders \
    | tee "$HERE/expected/plan-deny.json" | grep '"plan_id"' | plan_id)
echo "plan_id (contracted change): $DENY_PLAN"

echo "=== 2. rocky apply — expected DENIAL (contracted boundary) ==="
if ROCKY_PRINCIPAL=agent rocky -c rocky.toml apply "$DENY_PLAN" > "$HERE/expected/apply-deny.txt" 2>&1; then
    echo "FAIL: apply of a contracted-model change should have been DENIED"
    cat "$HERE/expected/apply-deny.txt"
    exit 1
fi
echo "correctly denied:"
grep -i "DENIES\|deny" "$HERE/expected/apply-deny.txt" || cat "$HERE/expected/apply-deny.txt"

# Restore the contracted model to baseline before the allow scenario.
git checkout -q -- models/fct_orders.sql

echo "=== 3. agent proposes a NEW bronze model (net-new, provably additive) ==="
# A net-new model is a `ModelAdded` — provably additive with no body-change
# ambiguity. (Editing an existing model's SQL to add a column also changes the
# body, which the classifier cannot prove is value-safe, so that fails closed
# to a review — autonomy is reserved for net-new / upstream schema evolution.)
cat > models/bronze_metrics.toml <<'TOML'
name = "bronze_metrics"

# Materializes into the DuckDB default schema (poc.main); the policy scope keys
# off the `layer` tag, not the target schema, so this is still a bronze model.
[target]
catalog = "poc"
schema = "main"
table = "bronze_metrics"

[tags]
layer = "bronze"
TOML
printf "SELECT 1 AS metric_id, 'ok' AS status\n" > models/bronze_metrics.sql
ALLOW_PLAN=$(rocky -c rocky.toml -o json plan \
    --principal agent --base HEAD --model bronze_metrics \
    | tee "$HERE/expected/plan-allow.json" | grep '"plan_id"' | plan_id)
echo "plan_id (additive bronze): $ALLOW_PLAN"

echo "=== 4. rocky apply — expected ALLOW (additive bronze, no review) ==="
ROCKY_PRINCIPAL=agent rocky -c rocky.toml apply "$ALLOW_PLAN" > "$HERE/expected/apply-allow.txt" 2>&1
echo "allowed and materialized:"
cat "$HERE/expected/apply-allow.txt"

echo "=== 5. rocky audit — the decision ledger records both ==="
rocky -c rocky.toml -o json audit > "$HERE/expected/audit.json"
rocky -c rocky.toml audit

echo
echo "POC complete: contracted change DENIED, additive bronze change ALLOWED, both recorded."
