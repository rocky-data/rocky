#!/usr/bin/env bash
#
# Pin the decision logic behind .github/workflows/changelog-gate.yml (#1938):
# an engine/-touching PR must add an engine/CHANGELOG.md line UNDER
# [Unreleased] specifically (by HEAD line position, not text content -- see
# scripts/changelog_gate.sh for why text-content matching was replaced), or
# carry a `Changelog: none` marker with a real reason.
#
# A PR-triggered workflow cannot be verified locally, so this exercises the
# script the workflow calls (scripts/changelog_gate.sh) directly. Diff
# fixtures are generated with a real `diff -u` between a base and a head
# fixture file, not hand-typed: a hand-typed diff's `@@ ... @@` line numbers
# can silently drift out of sync with a hand-typed "head file" fixture, and
# the position-based check below depends on them being exactly right. The
# workflow's own live-fire check (does the `changes` job skip correctly,
# does `HEAD^1 HEAD` resolve on a real PR ref, does the `edited` trigger
# re-run it) is a throwaway branch, described in the PR, not this script.

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

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# Real unified diff between two fixture files -- see the file header for why
# this beats hand-typing `@@ ... @@` line numbers.
gen_diff() {
    local base="$1" head="$2"
    diff -u "$base" "$head" || true
}

write() {
    local path="$1"
    shift
    printf '%s\n' "$@" >"$path"
}

# --- fixture files -----------------------------------------------------

readonly EMPTY_UNRELEASED="$WORK/empty_unreleased.md"
write "$EMPTY_UNRELEASED" \
    '# Changelog' '' '## [Unreleased]' '' '## [1.74.0] - 2026-09-01' '' '- An old, already-released entry.'

readonly WITH_ENTRY_BASE="$WORK/with_entry_base.md"
write "$WITH_ENTRY_BASE" \
    '# Changelog' '' '## [Unreleased]' '' '## [1.74.0] - 2026-09-01' '' '- An old, already-released entry.'
readonly WITH_ENTRY_HEAD="$WORK/with_entry_head.md"
write "$WITH_ENTRY_HEAD" \
    '# Changelog' '' '## [Unreleased]' '' '### Fixed' '' '- **Something changed.** Details about the change. (#1938)' '' \
    '## [1.74.0] - 2026-09-01' '' '- An old, already-released entry.'

readonly NO_UNRELEASED_BASE="$WORK/no_unreleased_base.md"
write "$NO_UNRELEASED_BASE" \
    '# Changelog' '' '## [1.74.0] - 2026-09-01' '' '- An old, already-released entry.'
readonly HEADER_ONLY_HEAD="$WORK/header_only_head.md"
write "$HEADER_ONLY_HEAD" \
    '# Changelog' '' '## [Unreleased]' '## [1.74.0] - 2026-09-01' '' '- An old, already-released entry.'

readonly OLD_RELEASE_ONLY_HEAD="$WORK/old_release_only_head.md"
write "$OLD_RELEASE_ONLY_HEAD" \
    '# Changelog' '' '## [Unreleased]' '' '## [1.74.0] - 2026-09-01' '' \
    '- Added a note to an old release retroactively. (#1938)' '- An old, already-released entry.'

# Case 14 (Codex finding, high): an ordinary bullet added under an EXISTING
# Unreleased heading, no new heading of its own. This is the common real
# case -- most entries join an existing "### Fixed"/"### Added" section.
readonly BULLET_ONLY_BASE="$WORK/bullet_only_base.md"
write "$BULLET_ONLY_BASE" \
    '# Changelog' '' '## [Unreleased]' '' '### Fixed' '' '- An existing entry, already there.' '' \
    '## [1.74.0] - 2026-09-01' '' '- An old, already-released entry.'
readonly BULLET_ONLY_HEAD="$WORK/bullet_only_head.md"
write "$BULLET_ONLY_HEAD" \
    '# Changelog' '' '## [Unreleased]' '' '### Fixed' '' '- An existing entry, already there.' \
    '- A brand new bullet, no new heading of its own. (#1938)' '' \
    '## [1.74.0] - 2026-09-01' '' '- An old, already-released entry.'

# Case 15 (Codex finding, medium): Unreleased already has a "### Fixed"
# heading from other in-flight work; THIS diff adds "### Fixed" again, but
# under the old release, not Unreleased. A text-membership check would
# false-pass on the duplicate heading text; position must not.
readonly DUPLICATE_HEADING_BASE="$WORK/duplicate_heading_base.md"
write "$DUPLICATE_HEADING_BASE" \
    '# Changelog' '' '## [Unreleased]' '' '### Fixed' '' '- Someone else'"'"'s in-flight entry.' '' \
    '## [1.74.0] - 2026-09-01' '' '- An old, already-released entry.'
readonly DUPLICATE_HEADING_HEAD="$WORK/duplicate_heading_head.md"
write "$DUPLICATE_HEADING_HEAD" \
    '# Changelog' '' '## [Unreleased]' '' '### Fixed' '' '- Someone else'"'"'s in-flight entry.' '' \
    '## [1.74.0] - 2026-09-01' '' '### Fixed' '' '- An old, already-released entry.'

# Case 16 (Codex finding, medium): the next heading uses a non-bracket
# shape ("## 1.74.0", no "[ ]"). The Unreleased boundary must still close
# there, not run to end of file.
readonly NON_BRACKET_BASE="$WORK/non_bracket_base.md"
write "$NON_BRACKET_BASE" \
    '# Changelog' '' '## [Unreleased]' '' '## 1.74.0 - 2026-09-01' '' '- An old, already-released entry.'
readonly NON_BRACKET_HEAD="$WORK/non_bracket_head.md"
write "$NON_BRACKET_HEAD" \
    '# Changelog' '' '## [Unreleased]' '' '## 1.74.0 - 2026-09-01' '' \
    '- Added a note to an old release retroactively. (#1938)' '- An old, already-released entry.'

readonly DIFF_WITH_ENTRY="$(gen_diff "$WITH_ENTRY_BASE" "$WITH_ENTRY_HEAD")"
readonly DIFF_HEADER_ONLY="$(gen_diff "$NO_UNRELEASED_BASE" "$HEADER_ONLY_HEAD")"
readonly DIFF_OLD_RELEASE_ONLY="$(gen_diff "$EMPTY_UNRELEASED" "$OLD_RELEASE_ONLY_HEAD")"
readonly DIFF_BULLET_ONLY="$(gen_diff "$BULLET_ONLY_BASE" "$BULLET_ONLY_HEAD")"
readonly DIFF_DUPLICATE_HEADING="$(gen_diff "$DUPLICATE_HEADING_BASE" "$DUPLICATE_HEADING_HEAD")"
readonly DIFF_NON_BRACKET="$(gen_diff "$NON_BRACKET_BASE" "$NON_BRACKET_HEAD")"
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
    local body="$1" diff="$2" changelog_path="$3"
    PR_BODY="$body" CHANGELOG_DIFF="$diff" CHANGELOG_PATH="$changelog_path" bash "$UNDER_TEST"
}

# --- 1. no marker, no CHANGELOG addition: FAILS, names both remedies -------
out=0
result="$(run_gate "$BODY_NO_MARKER" "$DIFF_EMPTY" "$EMPTY_UNRELEASED" 2>&1)" || out=$?
[[ "$out" -ne 0 ]] || fail "no marker and no entry must fail (exit was 0): $result"
grep -q "no engine/CHANGELOG.md entry" <<<"$result" \
    || fail "must say no entry was found: $result"
grep -q "no marker" <<<"$result" \
    || fail "must say no marker was found: $result"
grep -q 'Changelog: none' <<<"$result" \
    || fail "must show the accepted marker shape: $result"
echo "ok   1. no marker, no entry: fails, names both remedies"

# --- 2. no marker, CHANGELOG addition present under Unreleased: PASSES -----
out=0
result="$(run_gate "$BODY_NO_MARKER" "$DIFF_WITH_ENTRY" "$WITH_ENTRY_HEAD" 2>&1)" || out=$?
[[ "$out" -eq 0 ]] || fail "an added CHANGELOG line under Unreleased must pass on its own: $result"
echo "ok   2. no marker, entry present under Unreleased: passes"

# --- 3. marker without a reason, no CHANGELOG addition: FAILS, says so -----
out=0
result="$(run_gate "$BODY_BARE_MARKER" "$DIFF_EMPTY" "$EMPTY_UNRELEASED" 2>&1)" || out=$?
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
result="$(run_gate "$BODY_BARE_MARKER" "$DIFF_WITH_ENTRY" "$WITH_ENTRY_HEAD" 2>&1)" || out=$?
[[ "$out" -eq 0 ]] || fail "an added CHANGELOG line must pass even beside a bad marker: $result"
echo "ok   4. marker without a reason, entry present: passes"

# --- 5. marker with a reason, no CHANGELOG addition: PASSES ----------------
out=0
result="$(run_gate "$BODY_VALID_MARKER" "$DIFF_EMPTY" "$EMPTY_UNRELEASED" 2>&1)" || out=$?
[[ "$out" -eq 0 ]] || fail "a valid marker must pass without an entry: $result"
echo "ok   5. marker with a reason, no entry: passes"

# --- 6. the CHANGELOG.md diff header re-appearing is not an entry ----------
# A PR that re-adds the `## [Unreleased]` heading itself (no real line under
# it) must not be mistaken for an entry.
out=0
result="$(run_gate "$BODY_NO_MARKER" "$DIFF_HEADER_ONLY" "$HEADER_ONLY_HEAD" 2>&1)" || out=$?
[[ "$out" -ne 0 ]] || fail "the bare [Unreleased] header must not count as an entry: $result"
echo "ok   6. header-only diff does not count as an entry"

# --- 7. 'because' is accepted, not only a hyphen --------------------------
out=0
result="$(run_gate 'Changelog: none because this only touches tests, no behaviour change' "$DIFF_EMPTY" "$EMPTY_UNRELEASED" 2>&1)" || out=$?
[[ "$out" -eq 0 ]] || fail "'because' must be accepted like '-': $result"
echo "ok   7. 'because' separator is accepted"

# --- 8. a reason under 10 characters is rejected, even though non-empty ---
out=0
result="$(run_gate 'Changelog: none - short' "$DIFF_EMPTY" "$EMPTY_UNRELEASED" 2>&1)" || out=$?
[[ "$out" -ne 0 ]] || fail "a reason under 10 characters must fail: $result"
grep -q "no reason" <<<"$result" \
    || fail "a too-short reason must read as 'no reason': $result"
echo "ok   8. a reason under 10 characters is rejected"

# --- 9. case-insensitive on the keyword ------------------------------------
out=0
result="$(run_gate 'CHANGELOG: NONE - this reason is definitely long enough' "$DIFF_EMPTY" "$EMPTY_UNRELEASED" 2>&1)" || out=$?
[[ "$out" -eq 0 ]] || fail "the marker keyword must be case-insensitive: $result"
echo "ok   9. marker keyword is case-insensitive"

# --- 10. a bullet-prefixed marker does not count: the line must START with
#         the marker, not merely contain it -------------------------------
out=0
result="$(run_gate '- Changelog: none - this reason is definitely long enough' "$DIFF_EMPTY" "$EMPTY_UNRELEASED" 2>&1)" || out=$?
[[ "$out" -ne 0 ]] || fail "a bullet-prefixed marker must not count (exit was 0): $result"
grep -q "no marker" <<<"$result" \
    || fail "a bullet-prefixed marker must read as no marker at all, not a malformed one: $result"
echo "ok   10. a bullet-prefixed marker does not count as a marker"

# --- 11. CRLF line endings (a body edited in the GitHub web UI) still parse
out=0
crlf_body=$'## Summary\r\nChangelog: none - test-only refactor, no behaviour change\r\n'
result="$(run_gate "$crlf_body" "$DIFF_EMPTY" "$EMPTY_UNRELEASED" 2>&1)" || out=$?
[[ "$out" -eq 0 ]] || fail "a CRLF body with a valid marker must pass: $result"
echo "ok   11. CRLF line endings still parse"

# --- 12. the unfilled PR template text never accidentally satisfies the
#         gate -- the new checklist line mentions the marker in prose but
#         must not itself BE a marker line -------------------------------
template_body="$(cat "$REPO_ROOT/.github/PULL_REQUEST_TEMPLATE.md")"
out=0
result="$(run_gate "$template_body" "$DIFF_EMPTY" "$EMPTY_UNRELEASED" 2>&1)" || out=$?
[[ "$out" -ne 0 ]] || fail "the unfilled PR template body must not pass the gate on its own (exit was 0): $result"
echo "ok   12. the unfilled PR template body does not satisfy the gate"

# --- 13. an added line under an already-released heading is not an entry --
# Caught in review (#1938): an earlier version accepted any non-blank added
# line anywhere in engine/CHANGELOG.md. A PR that edits only a past
# release's notes, leaving Unreleased untouched, must still fail.
out=0
result="$(run_gate "$BODY_NO_MARKER" "$DIFF_OLD_RELEASE_ONLY" "$OLD_RELEASE_ONLY_HEAD" 2>&1)" || out=$?
[[ "$out" -ne 0 ]] || fail "a line added under an old release heading must not count as an entry (exit was 0): $result"
grep -q "no engine/CHANGELOG.md entry" <<<"$result" \
    || fail "an old-release-only edit must read as no entry: $result"
echo "ok   13. a line added under an old release heading does not count as an entry"

# --- 14. an ordinary bullet joining an EXISTING Unreleased heading passes -
# Caught in a second review round: grep -F treated a "-"-prefixed bullet as
# an option string and silently rejected it. The two earlier positive cases
# (2, 4) never exercised this because their diffs also add a brand new
# "### Fixed" heading, which was matched before the loop ever reached the
# bullet line.
out=0
result="$(run_gate "$BODY_NO_MARKER" "$DIFF_BULLET_ONLY" "$BULLET_ONLY_HEAD" 2>&1)" || out=$?
[[ "$out" -eq 0 ]] || fail "an ordinary bullet joining an existing Unreleased heading must pass: $result"
echo "ok   14. an ordinary bullet under an existing heading passes"

# --- 15. a duplicated heading text under an old release still fails -------
# Caught in a second review round: a text-membership check false-passed
# when the SAME heading text (e.g. "### Fixed") already existed in
# Unreleased from other in-flight work, even though the actual addition
# sat under an old release. Position, not text, must decide this.
out=0
result="$(run_gate "$BODY_NO_MARKER" "$DIFF_DUPLICATE_HEADING" "$DUPLICATE_HEADING_HEAD" 2>&1)" || out=$?
[[ "$out" -ne 0 ]] || fail "a duplicated heading added under an old release must not count as an entry (exit was 0): $result"
echo "ok   15. a duplicated heading under an old release still fails"

# --- 16. a non-bracket release heading still closes the Unreleased section
# Caught in a second review round: the boundary regex only matched "## [",
# so a release heading spelled without brackets left the section open to
# end of file and any addition anywhere below it counted as an entry.
out=0
result="$(run_gate "$BODY_NO_MARKER" "$DIFF_NON_BRACKET" "$NON_BRACKET_HEAD" 2>&1)" || out=$?
[[ "$out" -ne 0 ]] || fail "a non-bracket release heading must still close the Unreleased section (exit was 0): $result"
echo "ok   16. a non-bracket release heading still closes the section"

echo "changelog-gate decision logic: 16 cases ok"
