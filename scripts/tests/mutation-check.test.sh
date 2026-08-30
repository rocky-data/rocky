#!/usr/bin/env bash
# Pin the verdicts of scripts/mutation-check.sh.
#
# The script's whole job is to tell three outcomes apart: the test caught the
# mutation (0), the test did not (1), and the mutation broke the build so the
# test never ran (3). #1535 and #1547 both reported the same inversion — a
# KILLED mutant reported as a broken build, because `cargo test` prints
# `error: test failed, to rerun pass ...` and the detector matched `^error:`.
#
# Every case here uses a stub test command in a scratch repo, so the suite is
# fast and needs no Rust toolchain.
#
#   scripts/tests/mutation-check.test.sh
# The runner banners below are quoted with single quotes on purpose: they are
# verbatim `cargo` and `nextest` output, backticks and all, and must reach the
# stub unexpanded. SC2016 reads every one of them as a missed expansion. The
# directive is file-scoped, so it has to precede the first command.
# shellcheck disable=SC2016
set -uo pipefail

# Overridable so fix-sensitivity can be demonstrated against a modified COPY
# of the script, without making the worktree dirty.
SCRIPT="${MUTATION_CHECK_SCRIPT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/mutation-check.sh}"
[ -x "$SCRIPT" ] || { echo "not executable: $SCRIPT" >&2; exit 2; }

pass=0
fail=0

# Build a scratch repo whose stub command passes on the clean tree and, once
# the mutation lands, prints $2 and exits with $3.
#
#   $1  case name       $2  stub output when mutated
#   $3  stub exit when mutated       $4  expected mutation-check exit status
run_case() {
    local name="$1" banner="$2" stub_exit="$3" want="$4"
    local repo
    repo="$(mktemp -d)"

    (
        cd "$repo" || exit 2
        git init -q .
        git config user.email t@example.invalid
        git config user.name  t
        git config commit.gpgsign false

        echo "CLEAN" > probe.txt

        # The stub stands in for `cargo test`: it passes while the target still
        # reads CLEAN, and fails the way a real runner fails once it does not.
        # The stub stands in for `cargo test`: it passes while the target still
        # reads CLEAN, and once it does not, prints the runner output it is
        # given and exits with the status it is given. Quoted heredoc — nothing
        # here expands at write time, so a banner containing backticks stays
        # literal instead of running as a command.
        cat > stub.sh <<'STUB'
#!/usr/bin/env bash
# $1 target file   $2 runner output when mutated (may be empty)   $3 exit status
if grep -q MUTATED "$1"; then
    echo "running 1 test"
    echo "test tests::probe ... FAILED"
    [ -n "$2" ] && printf '%s\n' "$2"
    exit "$3"
fi
echo "test result: ok. 1 passed; 0 failed"
exit 0
STUB
        chmod +x stub.sh

        cat > mut.py <<'MUT'
import sys
p = sys.argv[1]
s = open(p).read()
assert s.count("CLEAN") == 1, "mutation target not found exactly once"
open(p, "w").write(s.replace("CLEAN", "MUTATED"))
MUT

        git add -A && git commit -qm init
        "$SCRIPT" probe.txt mut.py ./stub.sh probe.txt "$banner" "$stub_exit" > /dev/null 2>&1
        exit $?
    )
    local got=$?

    if [ "$got" = "$want" ]; then
        echo "  ok    $name (exit $got)"
        pass=$((pass + 1))
    else
        echo "  FAIL  $name — wanted exit $want, got $got"
        fail=$((fail + 1))
    fi
    rm -rf "$repo"
}

echo "mutation-check verdicts:"

# The regressions. A runner summary means the test RAN and FAILED: verdict 0.
run_case "cargo test failure is a killed mutant" \
    'error: test failed, to rerun pass `-p rocky --bin rocky`' 1 0
run_case "nextest failure is a killed mutant" \
    'error: test run failed' 1 0
run_case "doctest failure is a killed mutant" \
    'error: doctest failed, to rerun pass `--doc`' 1 0

# The guard those regressions must not weaken. A real compiler diagnostic
# means the test never ran: verdict 3.
run_case "coded compiler error is inconclusive" \
    'error[E0308]: mismatched types' 1 3
run_case "could not compile is inconclusive" \
    'error: could not compile `rocky-core` (lib) due to 1 previous error' 1 3
run_case "uncoded compiler error is inconclusive" \
    'error: linking with `cc` failed: exit status: 1' 1 3

# A test that still passes under mutation proves nothing: verdict 1.
run_case "surviving mutant fails the check" "" 0 1

echo
if [ "$fail" -gt 0 ]; then
    echo "$pass passed, $fail FAILED"
    exit 1
fi
echo "$pass passed"
