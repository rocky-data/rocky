#!/usr/bin/env bash
# Run every credential-free POC in sequence with an optional per-POC timeout.
# Used in CI to catch regressions in the catalog.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

passed=0
failed=0
skipped=0

# Use gtimeout if available (brew install coreutils) else fall back to no limit.
run_with_timeout() {
    if command -v gtimeout >/dev/null; then
        gtimeout 60s "$@"
    elif command -v timeout >/dev/null; then
        timeout 60s "$@"
    else
        "$@"
    fi
}

for run in pocs/*/*/run.sh; do
    poc_dir=$(dirname "$run")
    readme="$poc_dir/README.md"

    # Skip POCs whose README explicitly lists credentials.
    if grep -qiE '\*\*credentials:\*\*[^n]*(anthropic|databricks|snowflake|fivetran)' "$readme" 2>/dev/null; then
        echo "--- SKIP $poc_dir (credentials required)"
        skipped=$((skipped + 1))
        continue
    fi

    # Skip POCs that need docker.
    if grep -qiE 'docker[ -]compose' "$readme" 2>/dev/null; then
        echo "--- SKIP $poc_dir (docker required)"
        skipped=$((skipped + 1))
        continue
    fi

    echo "=== RUN $poc_dir"
    if (cd "$poc_dir" && run_with_timeout bash run.sh > /tmp/poc-output.log 2>&1); then
        echo "    PASS"
        passed=$((passed + 1))
    else
        echo "    FAIL — last 10 lines:"
        tail -10 /tmp/poc-output.log | sed 's/^/    /'
        failed=$((failed + 1))
    fi
done

echo
echo "Summary: $passed passed, $failed failed, $skipped skipped"
exit $failed
