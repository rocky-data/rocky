#!/usr/bin/env bash
# Run every credential-free POC in sequence with an optional per-POC timeout.
# Used in CI to catch regressions in the catalog.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Where the caller stood: a relative `ROCKY_BIN` is theirs, not this script's.
caller_dir="$PWD"
cd "$REPO_ROOT"

# Name the binary under test ONCE, make both spellings of it resolve to that
# file, and check the claim instead of announcing it.
#
# Ten POCs do not call bare `rocky`: they resolve a path themselves, and every
# one of them prefers `engine/target/release/rocky` over PATH. A caller that
# puts a DEBUG build on PATH — which is what a PR-time job would do (#1676) —
# would silently exercise a stale release binary in those ten, or hard-fail
# where a POC has no fallback. The suite would report on a binary nobody asked
# it to test.
#
# `ROCKY_BIN` is the override those POCs consult first. The other ninety call
# bare `rocky`, and until #1951 the data flow was one-way: PATH could default
# `ROCKY_BIN`, but `ROCKY_BIN` never redirected PATH. A caller who set
# `ROCKY_BIN` to a fresh build while PATH still held an install ran most of
# the suite on the install and read a header naming the build. A POC that
# verified a new refusal passed because the old binary refused nothing.
#
# So the binary's directory is put in front of PATH through a shim named
# `rocky`, which makes bare `rocky` and `$ROCKY_BIN` the same file whatever
# `ROCKY_BIN` is called. Then the header is earned: both spellings must report
# the same version, and every artifact a POC writes that carries an engine
# version is compared against it after the run.
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
command -v jq >/dev/null 2>&1 || {
    echo "error: jq is required (the version check reads each POC's JSON artifacts)." >&2
    exit 1
}
# A bare name is looked up on PATH; a relative path is relative to where the
# caller stood, since this script has already moved to the playground.
case "$ROCKY_BIN" in
    /*) ;;
    */*) ROCKY_BIN="$caller_dir/$ROCKY_BIN" ;;
    *)
        resolved="$(command -v "$ROCKY_BIN" || true)"
        if [ -z "$resolved" ]; then
            echo "error: ROCKY_BIN=$ROCKY_BIN is not on PATH." >&2
            exit 1
        fi
        ROCKY_BIN="$resolved"
        ;;
esac
if [ ! -f "$ROCKY_BIN" ] || [ ! -x "$ROCKY_BIN" ]; then
    echo "error: ROCKY_BIN=$ROCKY_BIN is not an executable file." >&2
    exit 1
fi
# Absolute, so the path survives every `cd` below and the shim can point at it.
ROCKY_BIN="$(cd "$(dirname "$ROCKY_BIN")" && pwd)/$(basename "$ROCKY_BIN")"
export ROCKY_BIN

# Both must succeed, or an empty PATH entry (search the current directory,
# which is every POC directory in turn) or a missing shim would leave the two
# spellings pointing at different files while the version check still passes.
shim_dir="$(mktemp -d)" || { echo "error: mktemp -d failed." >&2; exit 1; }
trap 'rm -rf "$shim_dir"' EXIT
ln -s "$ROCKY_BIN" "$shim_dir/rocky" || { echo "error: could not create the rocky shim in $shim_dir." >&2; exit 1; }
PATH="$shim_dir:$PATH"
export PATH

ENGINE_VERSION="$("$ROCKY_BIN" --version | awk '{print $2}')"
if [ -z "$ENGINE_VERSION" ]; then
    echo "error: '$ROCKY_BIN --version' printed nothing usable." >&2
    exit 1
fi
if [ "$(rocky --version)" != "$("$ROCKY_BIN" --version)" ]; then
    echo "error: bare 'rocky' ($(command -v rocky)) and ROCKY_BIN ($ROCKY_BIN) report different versions." >&2
    exit 1
fi
echo "Binary under test: $ROCKY_BIN (rocky $ENGINE_VERSION, also what bare 'rocky' resolves to)"

# Artifacts under `<poc>/expected/` written since `$2` whose top-level
# `version` is a release string other than the binary's. A POC can only write
# such a file by running some other rocky, so one line here is a POC that did
# not test the binary the header names. Only release-shaped strings count: a
# plan file's `version: 1` is a schema version, not an engine.
version_mismatches() {
    local dir="$1" marker="$2" f v
    [ -d "$dir/expected" ] || return 0
    find "$dir/expected" -maxdepth 1 -name '*.json' -newer "$marker" 2>/dev/null |
        while IFS= read -r f; do
            v="$(jq -r 'if type == "object" then (.version // empty | tostring) else empty end' "$f" 2>/dev/null || true)"
            case "$v" in
                *.*.*) [ "$v" = "$ENGINE_VERSION" ] || echo "${f#"$dir/"} reports rocky $v";;
            esac
        done
}

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
    marker="$(mktemp)"
    # Empty the log first: if the `cd` below fails, nothing writes it, and the
    # "last 10 lines" would be the PREVIOUS POC's output presented as this one's.
    : > /tmp/poc-output.log
    if (cd "$poc_dir" && run_with_timeout bash run.sh > /tmp/poc-output.log 2>&1); then
        mismatches="$(version_mismatches "$poc_dir" "$marker")"
        if [ -z "$mismatches" ]; then
            echo "    PASS"
            passed=$((passed + 1))
        else
            echo "    FAIL — ran a different rocky than $ROCKY_BIN (rocky $ENGINE_VERSION):"
            printf '%s\n' "$mismatches" | sed 's/^/      /'
            failed=$((failed + 1))
        fi
    else
        echo "    FAIL — last 10 lines:"
        tail -10 /tmp/poc-output.log | sed 's/^/    /'
        failed=$((failed + 1))
    fi
    rm -f "$marker"
done

echo
echo "Summary: $passed passed, $failed failed, $skipped skipped"
exit $failed
