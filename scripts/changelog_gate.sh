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
# Inputs, both required, read from the environment rather than argv or a
# heredoc, so the caller never has to interpolate an attacker-controlled PR
# body into a shell command line:
#
#   PR_BODY         the raw pull request body (github.event.pull_request.body)
#   CHANGELOG_DIFF  the output of `git diff --no-renames <base> <head> --
#                   engine/CHANGELOG.md` (see the workflow for why HEAD^1 HEAD
#                   on the checked-out merge ref stands in for `base...head`)
#
# Exit 0: engine/CHANGELOG.md gained a line, or the body carries a valid
#         `Changelog: none` marker with a real reason.
# Exit 1: neither. The message names both remedies, and says specifically
#         when a marker was attempted but is missing its reason.

set -euo pipefail

: "${PR_BODY:=}"
: "${CHANGELOG_DIFF:=}"

readonly MARKER_SHAPE='a line starting "Changelog: none" (case-insensitive), followed by " - " or " because ", followed by a reason of at least 10 characters -- e.g. Changelog: none - test-only refactor, no behaviour change'

# True when the diff added at least one non-blank line to engine/CHANGELOG.md
# that is not the `## [Unreleased]` header re-appearing. This does not check
# that the added line sits UNDER the Unreleased section specifically, or that
# it is a good entry -- see the "optimistic count" discussion on #1938. A
# commit that touches the file for an unrelated reason, or adds a line
# elsewhere in it, still counts; that is the same limitation the issue's own
# count method has, accepted for the same reason: it never produces a false
# "no entry" for a PR that genuinely added one.
has_changelog_entry() {
    local line trimmed
    while IFS= read -r line; do
        line="${line%$'\r'}"
        case "$line" in
            '+++'*)
                continue
                ;;
            '+'*)
                trimmed="$(printf '%s' "${line#+}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
                if [[ -n "$trimmed" && "$trimmed" != "## [Unreleased]" ]]; then
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
