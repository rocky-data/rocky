#!/usr/bin/env bash
# 11-agent-policy — the agent-policy plane ENFORCED as an incident guardrail.
#
# The visceral case: an AI agent tries to DROP a PII-classified column that sits
# under a cross-team contract on a gold-layer model — a breaking change to
# governed data. `rocky apply` DENIES it at the policy seam, names the deciding
# rule, and does no warehouse work. The decision ledger prints who tried, what
# they tried, on which target, and why it was refused.
#
# Then the contrast that makes "who decided" concrete:
#   - `rocky policy check` resolves the SAME change to deny for an agent and
#     allow for a human — humans own the boundary.
#   - a human applies the SAME plan and it materializes; the ledger records the
#     agent deny and the human allow side by side.
#   - an agent's net-new bronze model is ALLOWED and materializes — the dial
#     grants autonomy where the change is provably safe.
#
# The change-classification is computed against a git baseline, so the demo runs
# inside a throwaway git repo in a temp dir (never the surrounding repo).
#
# The apply-side principal comes from ROCKY_PRINCIPAL: `plan --principal agent`
# labels the plan-time evaluation, but `rocky apply` resolves who is applying
# from its own environment — without ROCKY_PRINCIPAL=agent the apply runs as a
# human, humans are never gated, and the deny below would not fire.
set -euo pipefail

export ROCKY_SUPPRESS_DEPRECATION=1

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "$HERE/expected"

# Prefer the engine binary built from this tree over a stale global install.
# Resolve to an ABSOLUTE path now, before we cd into the throwaway repo.
find_rocky() {
    local d="$HERE"
    while [ "$d" != "/" ]; do
        if [ -f "$d/engine/Cargo.toml" ]; then
            for cand in "$d/engine/target/release/rocky" "$d/engine/target/debug/rocky"; do
                [ -x "$cand" ] && { echo "$cand"; return 0; }
            done
        fi
        d="$(dirname "$d")"
    done
    command -v rocky
}
ROCKY="$(find_rocky)"
echo "==> using rocky: $ROCKY"

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
git commit -q -m "baseline: gold fct_orders (contracted, email=pii) under a [policy] block"

# Extract the 64-hex plan_id from a `rocky plan --output json` payload.
plan_id() { grep -oE '[0-9a-f]{64}' | head -1; }

echo
echo "=== 1. agent drops the PII 'email' column from the contracted gold model ==="
# fct_orders is contracted (sibling .contract.toml), gold, and classifies email
# as pii. Dropping email is a BREAKING change to governed data. The contract
# still lists email, so the compile warns (W010) that the column left — a signal,
# not a hard error: the plan builds and the policy plane is what refuses the agent.
printf 'SELECT 1 AS order_id, 100 AS amount\n' > models/fct_orders.sql
DROP_PLAN=$("$ROCKY" -c rocky.toml -o json plan \
    --principal agent --base HEAD --model fct_orders 2>/dev/null \
    | tee "$HERE/expected/plan-drop.json" | grep '"plan_id"' | plan_id)
echo "plan_id (breaking drop of a contracted PII column): $DROP_PLAN"

echo
echo "=== 2. rocky apply as an AGENT — expected DENIAL at the policy seam ==="
if ROCKY_PRINCIPAL=agent "$ROCKY" -c rocky.toml apply "$DROP_PLAN" > "$HERE/expected/apply-deny.txt" 2>&1; then
    echo "FAIL: an agent apply of a contracted-PII breaking change must be DENIED"
    cat "$HERE/expected/apply-deny.txt"
    exit 1
fi
# A non-zero exit alone is not proof — an earlier version of this POC passed on
# an unrelated runtime error. Assert the policy denial itself, and that the
# incident rule (rule 0: contracted + pii + breaking) is the one that fired.
if ! grep -q "policy DENIES" "$HERE/expected/apply-deny.txt"; then
    echo "FAIL: apply failed, but not with a policy denial:"
    cat "$HERE/expected/apply-deny.txt"
    exit 1
fi
if ! grep -q "rule 0" "$HERE/expected/apply-deny.txt"; then
    echo "FAIL: denied, but not by the incident rule (rule 0):"
    cat "$HERE/expected/apply-deny.txt"
    exit 1
fi
echo "correctly denied — the deciding rule and remediation hint are in the error:"
grep "policy DENIES" "$HERE/expected/apply-deny.txt"

echo
echo "=== 3. rocky audit — the custody chain of the refused attempt ==="
# principal / capability / target / decision / deciding rule, straight from the
# decision ledger. Nothing touched the warehouse; the attempt is still recorded.
"$ROCKY" -c rocky.toml audit --output table
"$ROCKY" -c rocky.toml -o json audit > "$HERE/expected/audit-after-deny.json"
echo
echo "--- full custody chain for the denied plan (rocky audit --for) ---"
"$ROCKY" -c rocky.toml audit --for "$DROP_PLAN" --output table 2>/dev/null \
    | tee "$HERE/expected/audit-for.txt"

echo
echo "=== 4a. who decided? the SAME change, agent vs human (rocky policy check) ==="
# Static and warehouse-free: the policy plane resolves the identical
# (capability, model) to deny for an agent and allow for a human.
echo "--- agent ---"
"$ROCKY" -c rocky.toml policy check \
    --principal agent --capability schema_change.breaking --model fct_orders --output table 2>/dev/null
echo "--- human ---"
"$ROCKY" -c rocky.toml policy check \
    --principal human --capability schema_change.breaking --model fct_orders --output table 2>/dev/null

echo
echo "=== 4b. a human applies the SAME plan — ungated, materializes ==="
# No ROCKY_PRINCIPAL: the apply runs as a human, who is never gated by the
# policy. The identical plan the agent could not apply now lands. (The same W010
# contract warning is present for both — proof the agent's refusal was the policy
# plane, not a broken compile.)
"$ROCKY" -c rocky.toml apply "$DROP_PLAN" > "$HERE/expected/apply-human.txt" 2>&1
echo "human apply succeeded — the boundary is human-owned."

# Restore fct_orders to its contracted baseline before the allow scenario.
git checkout -q -- models/fct_orders.sql

echo
echo "=== 5. an agent's net-new bronze model is ALLOWED and materializes ==="
# A net-new model is a provably-additive ModelAdded on a bronze, non-PII surface
# — rule 2 grants it without a review.
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
ALLOW_PLAN=$("$ROCKY" -c rocky.toml -o json plan \
    --principal agent --base HEAD --model bronze_metrics 2>/dev/null \
    | tee "$HERE/expected/plan-allow.json" | grep '"plan_id"' | plan_id)
echo "plan_id (additive bronze): $ALLOW_PLAN"
ROCKY_PRINCIPAL=agent "$ROCKY" -c rocky.toml apply "$ALLOW_PLAN" > "$HERE/expected/apply-allow.txt" 2>&1
echo "allowed and materialized (rule 2)."

echo
echo "=== 6. rocky audit — the full ledger: agent deny, human allow, agent allow ==="
"$ROCKY" -c rocky.toml audit --output table
"$ROCKY" -c rocky.toml -o json audit > "$HERE/expected/audit.json"
echo
echo "--- trust scorecard by principal ---"
"$ROCKY" -c rocky.toml audit --scorecard --by principal --output table \
    | tee "$HERE/expected/scorecard.txt"

# Assert the ledger recorded the three decisions we drove (principal is three
# lines above effect in each decision object).
if ! grep -A3 '"principal": "agent"' "$HERE/expected/audit.json" | grep -q '"effect": "deny"'; then
    echo "FAIL: audit ledger is missing the agent deny decision"
    exit 1
fi
if ! grep -A3 '"principal": "human"' "$HERE/expected/audit.json" | grep -q '"effect": "allow"'; then
    echo "FAIL: audit ledger is missing the human allow decision"
    exit 1
fi
if ! grep -A3 '"principal": "agent"' "$HERE/expected/audit.json" | grep -q '"effect": "allow"'; then
    echo "FAIL: audit ledger is missing the agent allow decision"
    exit 1
fi

echo
echo "POC complete: an agent's breaking drop of a contracted PII column was DENIED"
echo "and recorded; a human applied the same plan; a net-new bronze model was ALLOWED."
