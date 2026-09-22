#!/usr/bin/env bash
#
# Pin the decision logic behind .github/workflows/changelog-gate.yml (#1938):
# an engine/-touching PR must add an engine/CHANGELOG.md entry, or carry a
# `Changelog: none` marker with a real reason.
#
# A PR-triggered workflow cannot be verified locally, so this exercises the
# script the workflow calls (scripts/changelog_gate.sh) directly, against
# fixture PR bodies and fixture `git diff` text -- no git repo, no network,
# no GitHub Actions runner. The workflow's own live-fire check (does the
# `changes` job skip correctly, does `HEAD^1 HEAD` resolve on a real PR ref,
# does the `edited` trigger re-run it) is a throwaway branch, described in
# the PR, not this script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
readonly SCRIPT_DIR
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
readonly REPO_ROOT
readonly UNDER_TEST="$REPO_ROOT/scripts/changelog_gate.sh"

fail() {
    echo "FAIL: $1" >&2
    exit 1
}

readonly DIFF_WITH_ENTRY='diff --git a/engine/CHANGELOG.md b/engine/CHANGELOG.md
index abc123..def456 100644
--- a/engine/CHANGELOG.md
+++ b/engine/CHANGELOG.md
@@ -7,6 +7,8 @@
 ## [Unreleased]

+### Fixed
+
+- **Something changed.** Details about the change. (#1938)
'

readonly DIFF_HEADER_ONLY='diff --git a/engine/CHANGELOG.md b/engine/CHANGELOG.md
index abc123..def456 100644
--- a/engine/CHANGELOG.md
+++ b/engine/CHANGELOG.md
@@ -4,6 +4,7 @@
 and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

+## [Unreleased]
 ## [1.74.0] - 2026-09-01
'

readonly DIFF_EMPTY=''

readonly BODY_NO_MARKER='## Summary

Just a normal PR body with no marker anywhere in it.
'

readonly BODY_BARE_MARKER='## Summary

Changelog: none

## Test Plan
- ran it locally
'

readonly BODY_VALID_MARKER='## Summary

Changelog: none - test-only refactor, no behaviour change

## Test Plan
- ran it locally
'

run_gate() {
    local body="$1" diff="$2"
    PR_BODY="$body" CHANGELOG_DIFF="$diff" bash "$UNDER_TEST"
}

# --- 1. no marker, no CHANGELOG addition: FAILS, names both remedies -------
out=0
result="$(run_gate "$BODY_NO_MARKER" "$DIFF_EMPTY" 2>&1)" || out=$?
[[ "$out" -ne 0 ]] || fail "no marker and no entry must fail (exit was 0): $result"
grep -q "no engine/CHANGELOG.md entry" <<<"$result" \
    || fail "must say no entry was found: $result"
grep -q "no marker" <<<"$result" \
    || fail "must say no marker was found: $result"
grep -q 'Changelog: none' <<<"$result" \
    || fail "must show the accepted marker shape: $result"
echo "ok   1. no marker, no entry: fails, names both remedies"

# --- 2. no marker, CHANGELOG addition present: PASSES ----------------------
out=0
result="$(run_gate "$BODY_NO_MARKER" "$DIFF_WITH_ENTRY" 2>&1)" || out=$?
[[ "$out" -eq 0 ]] || fail "an added CHANGELOG line must pass on its own: $result"
echo "ok   2. no marker, entry present: passes"

# --- 3. marker without a reason, no CHANGELOG addition: FAILS, says so -----
out=0
result="$(run_gate "$BODY_BARE_MARKER" "$DIFF_EMPTY" 2>&1)" || out=$?
[[ "$out" -ne 0 ]] || fail "a bare marker with no entry must fail (exit was 0): $result"
grep -q "no reason" <<<"$result" \
    || fail "must distinguish 'no reason' from 'no marker at all': $result"
grep -q 'Changelog: none' <<<"$result" \
    || fail "must show the accepted marker shape: $result"
echo "ok   3. marker without a reason, no entry: fails, names the shape"

# --- 4. marker without a reason, CHANGELOG addition present: PASSES -------
# The entry wins regardless of the marker's own validity -- the marker is
# only consulted when no entry was found.
out=0
result="$(run_gate "$BODY_BARE_MARKER" "$DIFF_WITH_ENTRY" 2>&1)" || out=$?
[[ "$out" -eq 0 ]] || fail "an added CHANGELOG line must pass even beside a bad marker: $result"
echo "ok   4. marker without a reason, entry present: passes"

# --- 5. marker with a reason, no CHANGELOG addition: PASSES ----------------
out=0
result="$(run_gate "$BODY_VALID_MARKER" "$DIFF_EMPTY" 2>&1)" || out=$?
[[ "$out" -eq 0 ]] || fail "a valid marker must pass without an entry: $result"
echo "ok   5. marker with a reason, no entry: passes"

# --- 6. the CHANGELOG.md diff header re-appearing is not an entry ----------
# A PR that re-adds the `## [Unreleased]` heading itself (no real line under
# it) must not be mistaken for an entry.
out=0
result="$(run_gate "$BODY_NO_MARKER" "$DIFF_HEADER_ONLY" 2>&1)" || out=$?
[[ "$out" -ne 0 ]] || fail "the bare [Unreleased] header must not count as an entry: $result"
echo "ok   6. header-only diff does not count as an entry"

# --- 7. 'because' is accepted, not only a hyphen --------------------------
out=0
result="$(run_gate 'Changelog: none because this only touches tests, no behaviour change' "$DIFF_EMPTY" 2>&1)" || out=$?
[[ "$out" -eq 0 ]] || fail "'because' must be accepted like '-': $result"
echo "ok   7. 'because' separator is accepted"

# --- 8. a reason under 10 characters is rejected, even though non-empty ---
out=0
result="$(run_gate 'Changelog: none - short' "$DIFF_EMPTY" 2>&1)" || out=$?
[[ "$out" -ne 0 ]] || fail "a reason under 10 characters must fail: $result"
grep -q "no reason" <<<"$result" \
    || fail "a too-short reason must read as 'no reason': $result"
echo "ok   8. a reason under 10 characters is rejected"

# --- 9. case-insensitive on the keyword ------------------------------------
out=0
result="$(run_gate 'CHANGELOG: NONE - this reason is definitely long enough' "$DIFF_EMPTY" 2>&1)" || out=$?
[[ "$out" -eq 0 ]] || fail "the marker keyword must be case-insensitive: $result"
echo "ok   9. marker keyword is case-insensitive"

# --- 10. a bullet-prefixed marker does not count: the line must START with
#         the marker, not merely contain it -------------------------------
out=0
result="$(run_gate '- Changelog: none - this reason is definitely long enough' "$DIFF_EMPTY" 2>&1)" || out=$?
[[ "$out" -ne 0 ]] || fail "a bullet-prefixed marker must not count (exit was 0): $result"
grep -q "no marker" <<<"$result" \
    || fail "a bullet-prefixed marker must read as no marker at all, not a malformed one: $result"
echo "ok   10. a bullet-prefixed marker does not count as a marker"

# --- 11. CRLF line endings (a body edited in the GitHub web UI) still parse
out=0
crlf_body=$'## Summary\r\nChangelog: none - test-only refactor, no behaviour change\r\n'
result="$(run_gate "$crlf_body" "$DIFF_EMPTY" 2>&1)" || out=$?
[[ "$out" -eq 0 ]] || fail "a CRLF body with a valid marker must pass: $result"
echo "ok   11. CRLF line endings still parse"

# --- 12. the unfilled PR template text never accidentally satisfies the
#         gate -- the new checklist line mentions the marker in prose but
#         must not itself BE a marker line -------------------------------
template_body="$(cat "$REPO_ROOT/.github/PULL_REQUEST_TEMPLATE.md")"
out=0
result="$(run_gate "$template_body" "$DIFF_EMPTY" 2>&1)" || out=$?
[[ "$out" -ne 0 ]] || fail "the unfilled PR template body must not pass the gate on its own (exit was 0): $result"
echo "ok   12. the unfilled PR template body does not satisfy the gate"

echo "changelog-gate decision logic: 12 cases ok"
