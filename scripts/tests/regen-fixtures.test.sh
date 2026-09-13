#!/usr/bin/env bash
#
# Pin the two guards that stop `regen_fixtures.sh` regenerating the dagster
# fixtures from a binary that does not match the tree (#1933).
#
# A stale binary rewrites the fixtures to ITS shape, and the diff reads as
# ordinary codegen churn — a docs-only PR once rewrote five fixtures from
# `schema_version 30` to `29` that way, inside a release PR.
#
# Uses stub `rocky` binaries, so no Rust toolchain and no duckdb are needed.
# Each case asserts the script refuses BEFORE it reaches any prerequisite it
# cannot satisfy here: the guards run in the pre-flight, above every capture.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
readonly SCRIPT_DIR
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
readonly REPO_ROOT
readonly SCRIPT="$REPO_ROOT/scripts/regen_fixtures.sh"
TREE_VERSION="$(sed -n 's/^version = "\(.*\)"/\1/p' "$REPO_ROOT/engine/rocky/Cargo.toml" | head -1)"
readonly TREE_VERSION

fail() {
    echo "FAIL: $1" >&2
    exit 1
}

if [[ -z "$TREE_VERSION" ]]; then
    fail "could not read the tree version; the guard under test reads the same file"
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# Build the sandbox tree ONCE, and run ONLY this copy.
#
# Never invoke the repo's own script: a case that gets past the guards proceeds
# into the capture phase, and the script resolves its destination from its own
# location. Under a MUTATION that removes a guard, ANY case can get that far —
# which is how the first draft rewrote
# integrations/dagster/tests/fixtures_generated/discover.json twice, once while
# mutation-checking the very guard that was removed. `mutation-check.sh`
# reported the dirty worktree both times.
readonly SANDBOX="$WORK/tree"
mkdir -p "$SANDBOX/engine/rocky" "$SANDBOX/scripts"
cp "$REPO_ROOT/engine/rocky/Cargo.toml" "$SANDBOX/engine/rocky/Cargo.toml"
cp "$SCRIPT" "$SANDBOX/scripts/regen_fixtures.sh"
readonly UNDER_TEST="$SANDBOX/scripts/regen_fixtures.sh"

# A stub that answers `--version` with whatever it was built to claim.
make_stub() {
    local path="$1" version="$2"
    mkdir -p "$(dirname "$path")"
    cat >"$path" <<EOF
#!/usr/bin/env bash
echo "rocky $version"
EOF
    chmod +x "$path"
}

# --- 1. a binary from another release is refused, naming both versions ------
make_stub "$WORK/stale/rocky" "0.0.1-stale"
out=0
result="$(ROCKY_BIN="$WORK/stale/rocky" bash "$UNDER_TEST" 2>&1)" || out=$?
[[ "$out" -ne 0 ]] || fail "a mismatched binary must be refused (exit was 0)"
grep -q "does not match this tree" <<<"$result" \
    || fail "the refusal must say the binary does not match the tree: $result"
grep -q "0.0.1-stale" <<<"$result" \
    || fail "the refusal must name the BINARY's version: $result"
grep -q "$TREE_VERSION" <<<"$result" \
    || fail "the refusal must name the TREE's expected version: $result"
echo "ok   a binary from another release is refused, naming both versions"

# --- 2. the fallback is refused when CARGO_TARGET_DIR is set ---------------
# Same version as the tree, so the version guard CANNOT catch this one — that
# is the whole reason the second guard exists.
make_stub "$SANDBOX/engine/target/debug/rocky" "$TREE_VERSION"
out=0
result="$(CARGO_TARGET_DIR="$WORK/elsewhere" bash "$UNDER_TEST" 2>&1)" || out=$?
[[ "$out" -ne 0 ]] || fail "the fallback must be refused when CARGO_TARGET_DIR is set (exit was 0)"
grep -q "cannot be the build you just made" <<<"$result" \
    || fail "the refusal must explain why the fallback is untrustworthy: $result"
grep -q "ROCKY_BIN" <<<"$result" \
    || fail "the refusal must name the override that fixes it: $result"
echo "ok   the fallback is refused when CARGO_TARGET_DIR is set"

# --- 3. an explicit ROCKY_BIN at the tree version passes BOTH guards -------
# It must get past the pre-flight. It will fail later on a prerequisite this
# test does not provide (duckdb, the POC tree) — asserting it fails for a
# DIFFERENT reason is what proves the guards are not refusing everything.
make_stub "$WORK/matching/rocky" "$TREE_VERSION"
out=0
result="$(CARGO_TARGET_DIR="$WORK/elsewhere" ROCKY_BIN="$WORK/matching/rocky" bash "$UNDER_TEST" 2>&1)" || out=$?
if grep -q "does not match this tree\|cannot be the build you just made" <<<"$result"; then
    fail "a matching binary named by ROCKY_BIN must pass both guards: $result"
fi
echo "ok   a matching ROCKY_BIN passes both guards"

echo "regen-fixtures guards: 3 cases ok"
