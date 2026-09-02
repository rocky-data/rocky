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
#   2  refused to run (dirty worktree, bad arguments, mutation not applied,
#      or the test command does not pass on the clean tree)
#   3  inconclusive — the mutation broke the build, so the test never ran
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
dirty="$(git status --porcelain)" || die "git status failed — refusing to touch the worktree"
# `git status` printing nothing is only reassuring if it SUCCEEDED. With a
# corrupt index it exits nonzero and prints nothing, which would read as a
# clean tree and let the mutation through — the guard failing open in exactly
# the situation it exists for.
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

# `set -u` does not catch these: a failed `mktemp` leaves the variable SET and
# empty, so `> "$var"` becomes `> ""` — an ambiguous redirect that writes
# nothing. Downstream that reads as "no build errors found", which is the
# fail-open direction. Refuse instead.
baseline_log="$(mktemp)" || die "cannot create a temp file for the baseline log"
[ -n "$baseline_log" ] || die "mktemp returned an empty path for the baseline log"
mutated_log="$(mktemp)" || die "cannot create a temp file for the mutated log"
[ -n "$mutated_log" ] || die "mktemp returned an empty path for the mutated log"
filtered_log="$(mktemp)" || die "cannot create a temp file for the filtered log"
[ -n "$filtered_log" ] || die "mktemp returned an empty path for the filtered log"
before="$(git rev-parse HEAD)" || die "cannot read HEAD"

# BASELINE. Run the test command against the clean tree first and require it
# to pass. Without this, "the command exited nonzero" is not evidence about
# the mutation at all: a typo'd crate name, a missing binary or a filter that
# matches nothing all exit nonzero (or zero) for reasons that have nothing to
# do with the code under test.
echo "mutation-check: baseline — running the test command against the CLEAN tree"
if ! "$@" > "$baseline_log" 2>&1; then
    echo "mutation-check: REFUSING — the test command does not pass before any mutation." >&2
    echo "" >&2
    tail -20 "$baseline_log" >&2
    echo "" >&2
    echo "Fix the command (or the test) first. A run that already fails proves" >&2
    echo "nothing about the mutation." >&2
    exit 2
fi
echo "mutation-check: baseline passes"

# ONE exit trap: setting a second `trap ... EXIT` later would silently replace
# the first, and the one that must never be lost is the restore.
restore() {
    rm -f "$baseline_log" "$mutated_log" "$filtered_log"
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
if "$@" > "$mutated_log" 2>&1; then
    cat "$mutated_log"
    echo "---"
    echo "mutation-check: FAIL — the test PASSED with the fix reverted."
    echo "mutation-check: that test does not guard this change. Fix the test."
    exit 1
fi
cat "$mutated_log"
echo "---"

# A nonzero exit is not automatically evidence. If the mutation broke the
# BUILD, the command failed without running the test — the commonest outcome
# when a mutation deletes a condition and leaves an unused binding. Reporting
# that as "fix-sensitive" would be the same false green this script exists to
# prevent, inside the script.
# The runner prints its own failure summary at the start of a line, and it
# begins with `error:` — `cargo test` says "error: test failed, to rerun pass
# ...", nextest says "error: test run failed". Those are what a KILLED mutant
# looks like, which is the verdict this script exists to report. Matching them
# as a broken build inverts the signal (#1535, #1547), so strip them first and
# look for compiler diagnostics in what is left.
#
# Filtered into a file rather than piped: `set -o pipefail` is on, and a
# `grep -v` that filters out every line exits 1, so a piped conditional would
# turn "the log was nothing but the summary" into a status that reads the
# wrong way.
grep -vE '^error: (test|bench|doctest) (failed|run failed)' "$mutated_log" > "$filtered_log"
filter_rc=$?
# grep: 0 = lines kept, 1 = every line was filtered out (a log that was nothing
# but the summary — fine), 2+ = a real failure. A bare `|| true` here would
# swallow that third case and leave an EMPTY filtered log, which the search
# below reads as "no build errors" — reporting a killed mutant for a build that
# may well have broken. Fail closed instead: if the log cannot be filtered, we
# do not know what happened, and INCONCLUSIVE is the honest verdict.
if [ "$filter_rc" -gt 1 ]; then
    echo "mutation-check: INCONCLUSIVE — could not filter the mutated log" >&2
    echo "(grep exited $filter_rc). Refusing to report a verdict from a log" >&2
    echo "that was not searched." >&2
    exit 3
fi
if grep -qE '^error(\[E[0-9]+\])?:|error: could not compile' "$filtered_log"; then
    echo "mutation-check: INCONCLUSIVE — the mutation broke the build, so the test" >&2
    echo "never ran. Choose a mutation that still compiles (flip a condition to" >&2
    echo "\`false &&\`, change a constant) rather than one that deletes code." >&2
    exit 3
fi

echo "mutation-check: OK — the test failed under mutation, so it is fix-sensitive."
[ "$(git rev-parse HEAD)" = "$before" ] || die "HEAD moved during the check"
exit 0
