#!/usr/bin/env bash
# Run every credential-free POC in sequence with an optional per-POC timeout.
# Used in CI to catch regressions in the catalog.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Name the binary under test ONCE, and export it.
#
# Ten POCs do not call bare `rocky`: they resolve a path themselves, and every
# one of them prefers `engine/target/release/rocky` over PATH. A caller that
# puts a DEBUG build on PATH — which is what a PR-time job would do (#1676) —
# would silently exercise a stale release binary in those ten, or hard-fail
# where a POC has no fallback. The suite would report on a binary nobody asked
# it to test.
#
# `ROCKY_BIN` is the override those POCs consult first, so exporting it here
# makes PATH and the path-resolving POCs agree by construction.
#
# Fail fast rather than let 101 POCs each discover the same missing binary:
# one message that says what is wrong beats 101 that say a command was not
# found.
ROCKY_BIN="${ROCKY_BIN:-$(command -v rocky || true)}"
if [ -z "$ROCKY_BIN" ]; then
    echo "error: no rocky binary. Put one on PATH or set ROCKY_BIN." >&2
    echo "  (cd engine && cargo build -p rocky) then add engine/target/debug to PATH" >&2
    exit 1
fi
export ROCKY_BIN
echo "Binary under test: $ROCKY_BIN"

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

    # Skip credential-gated POCs by detecting the canonical fail-fast guard
    # `: "${VAR:?...}"` (mandated by examples/playground/CLAUDE.md). Keying
    # off the guard rather than a platform-name allowlist means new adapters
    # don't have to update this script — and avoids the brittleness of the
    # previous `[^n]*` README regex.
    if grep -qE ': *"\$\{[A-Z_][A-Z0-9_]*:\?' "$run" 2>/dev/null; then
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

    # Skip POCs that require a Rust toolchain — a cold cargo build of a
    # standalone POC crate (resolving deps from crates.io and compiling
    # tokio/serde/etc.) consistently exceeds the 60s smoke timeout. These
    # POCs are still verified by their own `cargo test` invocation.
    # Anchor at line start so `echo "=== cargo check ..."` doesn't match.
    if grep -qE '^[[:space:]]*cargo[[:space:]]+(check|build|test|run)\b' "$run" 2>/dev/null; then
        echo "--- SKIP $poc_dir (Rust toolchain — cold cargo build exceeds smoke timeout)"
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
