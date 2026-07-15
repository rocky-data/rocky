#!/usr/bin/env bash
# 07-policy — pin agent-policy rules with `rocky policy test`.
#
# Two runs:
#   1. rocky.toml       — the policy plus a scenario per rule → all pass, exit 0.
#   2. rocky-hole.toml  — the same scenarios after a careless edit loosened the
#      contract-boundary `deny` to `allow` → the pinned scenario catches it and
#      the command exits non-zero. We assert that non-zero exit so this run.sh
#      still ends green: the guardrail firing is the success condition.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected

# Prefer a locally-built engine binary so the POC exercises the tree you're
# working in, not a stale global install. Falls back to `rocky` on PATH (the
# smoke runner puts a fresh one there).
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

echo
echo "==> 1. Green: the policy matches every pinned scenario"
"$ROCKY" -c rocky.toml policy test --output table
"$ROCKY" -c rocky.toml policy test --output json > expected/policy-test.json

echo
echo "==> 2. Red: a careless edit loosened the contract-boundary deny to allow"
echo "    (rocky-hole.toml — the scenarios are unchanged; the policy is not)"
hole_exit=0
"$ROCKY" -c rocky-hole.toml policy test --output table > expected/policy-test-hole.log 2>&1 || hole_exit=$?
if [ "$hole_exit" -eq 0 ]; then
    echo "    UNEXPECTED: the hole was not caught — failing the POC"
    cat expected/policy-test-hole.log
    exit 1
fi
echo "    caught it — rocky policy test exited $hole_exit (non-zero). The pinned"
echo "    scenario turns a silent hole into a red CI check:"
sed 's/^/      /' expected/policy-test-hole.log
"$ROCKY" -c rocky-hole.toml policy test --output json > expected/policy-test-hole.json 2>&1 || true

echo
echo "POC complete: policy scenarios pass on the good config and catch the hole on the bad one."
