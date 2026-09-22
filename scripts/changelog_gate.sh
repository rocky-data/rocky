#!/usr/bin/env bash
#
# Decide whether an engine/-touching pull request may merge without a fresh
# engine/CHANGELOG.md entry (#1938). Called by
# .github/workflows/changelog-gate.yml, only from its `gate` job, which runs
# only when the workflow's own `changes` job found the PR touches
# `engine/**`. Also called directly by scripts/tests/changelog-gate.test.sh,
# which feeds it fixture bodies and diffs so the decision is testable without
# a live GitHub Actions run.
#
# This script decides ONLY "does the PR carry evidence": either an added
# CHANGELOG.md line, or a valid marker. It does not decide whether the PR
# touches engine/, and it does not special-case dependabot — both of those
# are the caller's job (see the workflow).
#
# Inputs, read from the environment rather than argv or a heredoc, so the
# caller never has to interpolate an attacker-controlled PR body into a
# shell command line:
#
#   PR_BODY         required. The raw pull request body
#                   (github.event.pull_request.body).
#   CHANGELOG_DIFF  required. The output of `git diff --no-renames <base>
#                   <head> -- engine/CHANGELOG.md` (see the workflow for why
#                   HEAD^1 HEAD on the checked-out merge ref stands in for
#                   `base...head`).
#   CHANGELOG_PATH  optional, defaults to engine/CHANGELOG.md. The path to
#                   the file's HEAD (post-PR) content on disk -- used to
#                   find the extent of the `## [Unreleased]` section, so an
#                   added line is only accepted when it actually lands
#                   there (see has_changelog_entry below for why this is
#                   not optional to check).
#
# Exit 0: engine/CHANGELOG.md gained a line under [Unreleased], or the body
#         carries a valid `Changelog: none` marker with a real reason.
# Exit 1: neither. The message names both remedies, and says specifically
#         when a marker was attempted but is missing its reason.

set -euo pipefail

: "${PR_BODY:=}"
: "${CHANGELOG_DIFF:=}"
: "${CHANGELOG_PATH:=engine/CHANGELOG.md}"

readonly MARKER_SHAPE='a line starting "Changelog: none" (case-insensitive), followed by " - " or " because ", followed by a reason of at least 10 characters -- e.g. Changelog: none - test-only refactor, no behaviour change'

# The trimmed, non-blank lines currently inside the `## [Unreleased]`
# section of the HEAD (post-PR) file: after the `## [Unreleased]` heading,
# up to the next `## [` heading or end of file. Reads $CHANGELOG_PATH
# directly rather than the diff, because the diff alone cannot tell which
# section an added line landed in without also tracking hunk line numbers.
unreleased_section_lines() {
    [[ -f "$CHANGELOG_PATH" ]] || return 0
    awk '
        /^## \[Unreleased\]/ { inside = 1; next }
        /^## \[/ { if (inside) exit }
        inside { print }
    ' "$CHANGELOG_PATH"
}

# True when the diff added at least one line to engine/CHANGELOG.md whose
# trimmed content is found inside the Unreleased section of the HEAD file.
# Both conditions matter: added-by-this-diff (a `+` line) rules out a PR
# that adds nothing and merely benefits from entries already in Unreleased
# from other in-flight work; inside-Unreleased rules out a PR that edits
# only an already-released section's notes, which an earlier version of
# this check accepted (any non-blank `+` line, anywhere in the file, passed
# -- caught in review on #1938: a released-section-only edit reproduced a
# false "gained a line under [Unreleased]" pass).
#
# This does not check that the entry is a GOOD one -- see the "optimistic
# count" discussion on #1938. A line whose exact trimmed text happens to
# match a pre-existing Unreleased line elsewhere in the section could, in
# principle, false-positive; accepted as a narrow residual versus the
# complexity of tracking exact hunk line numbers.
has_changelog_entry() {
    local unreleased_lines line trimmed
    unreleased_lines="$(unreleased_section_lines | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' | grep -v '^$' || true)"
    while IFS= read -r line; do
        line="${line%$'\r'}"
        case "$line" in
            '+++'*)
                continue
                ;;
            '+'*)
                trimmed="$(printf '%s' "${line#+}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
                if [[ -n "$trimmed" && "$trimmed" != "## [Unreleased]" ]] \
                    && grep -qxF "$trimmed" <<<"$unreleased_lines"; then
                    return 0
                fi
                ;;
        esac
    done <<<"$CHANGELOG_DIFF"
    return 1
}

# Scans PR_BODY for the marker, line by line. Sets the two globals below
# rather than returning a single verdict: the caller needs both "was a
# marker attempted at all" and "was it valid" to choose the right failure
# message ("no entry and no marker" vs "marker without a reason").
marker_present=false
marker_valid=false

scan_marker() {
    local line reason trimmed_reason
    shopt -s nocasematch
    while IFS= read -r line; do
        line="${line%$'\r'}"
        if [[ "$line" =~ ^changelog:[[:space:]]*none[[:space:]]+(-|because)[[:space:]]+(.*)$ ]]; then
            marker_present=true
            reason="${BASH_REMATCH[2]}"
            trimmed_reason="$(printf '%s' "$reason" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
            if [[ ${#trimmed_reason} -ge 10 ]]; then
                marker_valid=true
            fi
        elif [[ "$line" =~ ^changelog:[[:space:]]*none([[:space:]].*)?$ ]]; then
            # Attempted, but not "none" followed by " - " or " because ": a
            # bare marker, or "none" trailing into unrelated prose. Either
            # way, no reason was supplied in the accepted shape.
            marker_present=true
        fi
    done <<<"$PR_BODY"
    shopt -u nocasematch
}

if has_changelog_entry; then
    echo "engine/CHANGELOG.md gained a line under [Unreleased]. changelog-gate: pass."
    exit 0
fi

scan_marker

if [[ "$marker_valid" == true ]]; then
    echo "PR body carries a valid 'Changelog: none' marker with a reason. changelog-gate: pass."
    exit 0
fi

if [[ "$marker_present" == true ]]; then
    echo "::error::PR touches engine/ but the 'Changelog: none' marker has no reason (or the reason is under 10 characters)." >&2
    echo "::error::Accepted shape: $MARKER_SHAPE" >&2
    exit 1
fi

echo "::error::PR touches engine/ with no engine/CHANGELOG.md entry under [Unreleased] and no marker in the PR body." >&2
echo "::error::Add an entry under '## [Unreleased]' in engine/CHANGELOG.md, OR add a marker line to the PR body: $MARKER_SHAPE" >&2
exit 1
