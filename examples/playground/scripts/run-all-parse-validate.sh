#!/usr/bin/env bash
# Parse-validate every credential-gated POC.
#
# Companion to `run-all-duckdb.sh`. That script runs credential-free POCs
# end-to-end against DuckDB; credential-gated POCs (`: "${VAR:?...}"` guards
# in `run.sh`) skip there and never reach `rocky validate`. Parse-time
# breakage — invalid strategy variants in model frontmatter, V032/V033
# adapter-kind regressions, malformed [pipeline.*] sections — therefore
# stays silent until someone runs the POC with real creds.
#
# This script closes that gap: for each credential-gated POC, it stubs
# every `${VAR}` referenced in `rocky.toml` (with the literal string
# `stub-for-parse-validation`) and runs `rocky validate`. Authentication
# isn't exercised — only config parsing + lint diagnostics.
#
# Compatible with macOS Bash 3.2 (no `mapfile`, no `${var,,}`, no
# `[[ =~ ]]` capture groups).
#
# Skip rules (in order):
#   1. POC's run.sh has no `: "${VAR:?...}"` guard — covered by
#      run-all-duckdb.sh.
#   2. POC is Docker-gated — needs different infra.
#   3. POC is Rust-toolchain-gated — needs `cargo`.
#
# Exit status mirrors `run-all-duckdb.sh`: zero when every parsed POC
# passes, non-zero (count of FAILs) otherwise.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

passed=0
failed=0
skipped=0
fails=""

for cfg in pocs/*/*/rocky.toml; do
    poc_dir=$(dirname "$cfg")
    run="$poc_dir/run.sh"
    readme="$poc_dir/README.md"

    if [ ! -f "$run" ]; then
        # POC config without a runner — rare, but skip cleanly.
        echo "--- SKIP $poc_dir (no run.sh)"
        skipped=$((skipped + 1))
        continue
    fi

    # Only parse-validate POCs that gate on credentials. Same fail-fast
    # guard pattern (`: "${VAR:?...}"`) that run-all-duckdb.sh keys off
    # for the inverse decision.
    if ! grep -qE ': *"\$\{[A-Z_][A-Z0-9_]*:\?' "$run" 2>/dev/null; then
        skipped=$((skipped + 1))
        continue
    fi

    # Docker-gated POCs need a compose stack — outside this script's
    # remit.
    if grep -qiE 'docker[ -]compose' "$readme" 2>/dev/null; then
        echo "--- SKIP $poc_dir (docker required)"
        skipped=$((skipped + 1))
        continue
    fi

    # Rust-toolchain-gated POCs (cargo invocations in run.sh) skipped
    # for the same reason run-all-duckdb.sh skips them — parse-validate
    # has no business pulling crates.io.
    if grep -qE '^[[:space:]]*cargo[[:space:]]+(check|build|test|run)\b' "$run" 2>/dev/null; then
        echo "--- SKIP $poc_dir (Rust toolchain)"
        skipped=$((skipped + 1))
        continue
    fi

    # Collect every `${VAR}` referenced in rocky.toml. Capture matches
    # all three substitution forms (`${VAR}`, `${VAR:-default}`,
    # `${VAR:?msg}`) and dedupes. Empty result is fine — some POCs
    # (e.g. the AI ones) gate run.sh on ANTHROPIC_API_KEY but use a
    # vanilla DuckDB config.
    vars=$(grep -oE '\$\{[A-Z_][A-Z0-9_]*' "$cfg" 2>/dev/null \
        | sed 's/^\${//' \
        | sort -u)

    # Build the env-var injection inline. `env VAR1=stub VAR2=stub ...
    # rocky validate` keeps the stubs scoped to the single invocation
    # without polluting the loop's environment.
    env_args=""
    for var in $vars; do
        env_args="$env_args $var=stub-for-parse-validation"
    done

    echo "=== PARSE-VALIDATE $poc_dir"
    # shellcheck disable=SC2086 # env_args is intentionally word-split
    if env $env_args rocky -c "$cfg" validate > /tmp/parse-validate-output.log 2>&1; then
        echo "    PASS"
        passed=$((passed + 1))
    else
        echo "    FAIL — last 20 lines:"
        tail -20 /tmp/parse-validate-output.log | sed 's/^/    /'
        failed=$((failed + 1))
        fails="$fails $poc_dir"
    fi
done

echo
echo "Summary: $passed passed, $failed failed, $skipped skipped"
if [ -n "$fails" ]; then
    echo "Failed POCs:"
    for f in $fails; do
        echo "  - $f"
    done
fi
exit $failed
