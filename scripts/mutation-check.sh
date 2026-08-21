#!/usr/bin/env bash
# Mutation-check a test: prove it FAILS when the fix it guards is reverted.
#
# AGENT_REVIEW.md requires evidence that a test is fix-sensitive. The manual
# form of that — edit the source, run the test, undo the edit — has one sharp
# edge: the "undo" step (`git restore` / `git checkout --`) destroys ANY
# uncommitted work in that file, not just the mutation. That has cost real
# work in this repo more than once.
#
# This script removes the edge by construction: it refuses to run unless the
# worktree is clean, so the undo can only ever discard the mutation it made.
#
# Usage:
#   scripts/mutation-check.sh <file> <python-expr-file> <test-command...>
#
# The mutation is a Python script that reads the file on stdin-style (see
# below) — it must print nothing and write the mutated content back. The
# simplest form is a search/replace:
#
#   cat > /tmp/mut.py <<'EOF'
#   import sys
#   p = sys.argv[1]
#   s = open(p).read()
#   old = "if entry.has_prev && prev.exists() {"
#   new = "if prev.exists() {"
#   assert s.count(old) == 1, "mutation target not found exactly once"
#   open(p, "w").write(s.replace(old, new))
#   EOF
#
#   scripts/mutation-check.sh \
#       engine/crates/rocky-core/src/product/commit.rs \
#       /tmp/mut.py \
#       cargo test -p rocky-core --lib product::commit
#
# Exit status:
#   0  the test FAILED under mutation — the test is fix-sensitive (good)
#   1  the test PASSED under mutation — the test proves nothing (bad)
#   2  refused to run (dirty worktree, bad arguments, mutation not applied)
set -uo pipefail

die() {
    echo "mutation-check: $*" >&2
    exit 2
}

[ $# -ge 3 ] || die "usage: $0 <file> <mutation.py> <test-command...>"

target="$1"
mutation="$2"
shift 2

repo_root="$(git rev-parse --show-toplevel 2>/dev/null)" || die "not a git repository"
cd "$repo_root" || die "cannot enter $repo_root"

[ -f "$target" ] || die "no such file: $target"
[ -f "$mutation" ] || die "no such mutation script: $mutation"

# THE GUARD. A dirty worktree means the restore below could discard work that
# was never committed. Commit (or stash with a tag) first — then the restore
# can only take back the mutation.
dirty="$(git status --porcelain)"
if [ -n "$dirty" ]; then
    echo "mutation-check: REFUSING — the worktree is not clean." >&2
    echo "" >&2
    echo "$dirty" >&2
    echo "" >&2
    echo "The restore step would discard these changes along with the mutation." >&2
    echo "Commit them first (the fix AND its test), then re-run. That is also" >&2
    echo "the order the evidence needs: the committed state is what you are" >&2
    echo "proving sensitive." >&2
    exit 2
fi

before="$(git rev-parse HEAD)"

restore() {
    git checkout HEAD -- "$target" 2>/dev/null
    local left
    left="$(git status --porcelain)"
    if [ -n "$left" ]; then
        echo "mutation-check: WARNING — worktree not clean after restore:" >&2
        echo "$left" >&2
    fi
}
trap restore EXIT

python3 "$mutation" "$target" || die "mutation script failed"

if git diff --quiet -- "$target"; then
    die "mutation did not change $target — the target text was not found"
fi

echo "mutation-check: applied mutation to $target"
echo "mutation-check: running: $*"
echo "---"
if "$@"; then
    echo "---"
    echo "mutation-check: FAIL — the test PASSED with the fix reverted."
    echo "mutation-check: that test does not guard this change. Fix the test."
    exit 1
fi

echo "---"
echo "mutation-check: OK — the test failed under mutation, so it is fix-sensitive."
[ "$(git rev-parse HEAD)" = "$before" ] || die "HEAD moved during the check"
exit 0
