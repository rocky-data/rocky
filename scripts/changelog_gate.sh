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

# Prints "start end" (1-indexed; end is the exclusive upper bound) marking
# the line range strictly BETWEEN the `## [Unreleased]` heading and the
# next level-2 heading (any CommonMark ATX `##` heading -- 0-3 leading
# spaces, a space or tab separator, not only this repo's current
# unindented `## [x.y.z] - date` shape -- a PR cannot dodge the boundary
# with a differently-formatted or merely reindented release heading; see
# has_changelog_entry for why this was tightened) in $CHANGELOG_PATH's HEAD
# content. Prints nothing if the file is missing or has no Unreleased
# heading.
unreleased_line_range() {
    [[ -f "$CHANGELOG_PATH" ]] || return 0
    awk '
        BEGIN { start = 0; end = 0 }
        /^ {0,3}##[ \t]+\[Unreleased\]/ && start == 0 { start = NR; next }
        start != 0 && end == 0 && /^ {0,3}##([ \t]|$)/ { end = NR; exit }
        END {
            if (start != 0) {
                if (end == 0) { end = NR + 1 }
                print start, end
            }
        }
    ' "$CHANGELOG_PATH"
}

# Prints the HEAD (new-file) line number of every non-blank content line the
# diff added, one per line. Walks $CHANGELOG_DIFF's hunk headers
# (`@@ -o,oc +n,nc @@`) to seed the new-file line counter, then advances it
# by one for every context or added line (a removed `-` line does not exist
# in the new file, so it does not advance the counter). Two things are
# deliberately NOT counted as content: a `\ No newline at end of file`
# marker (diff metadata, not a line in either file -- counting it shifted
# every following line number by one) and anything before the first `@@`
# (the `---`/`+++` file-header lines -- note this is NOT the same as
# matching literal `+++`: a real added line that happens to start with `++`
# is still content once a hunk is open, and an earlier version's blanket
# `/^\+\+\+/` rule wrongly skipped it too).
added_line_numbers() {
    awk '
        BEGIN { started = 0; new_line = 0 }
        /^@@/ {
            match($0, /\+[0-9]+/)
            new_line = substr($0, RSTART + 1, RLENGTH - 1) + 0
            started = 1
            next
        }
        !started { next }
        /^\\ / { next }
        /^\+/ {
            content = substr($0, 2)
            sub(/\r$/, "", content)
            gsub(/^[ \t]+|[ \t]+$/, "", content)
            if (content != "") { print new_line }
            new_line++
            next
        }
        /^-/ { next }
        { new_line++ }
    ' <<<"$CHANGELOG_DIFF"
}

# True when at least one line the diff added lands, by HEAD line position,
# strictly inside the Unreleased section. Position, not text content: an
# earlier version matched by trimmed text, which (a) let grep's `-F` pattern
# choke on an ordinary bullet starting with "-" (a real bullet under an
# existing "### Fixed" was silently treated as absent -- grep read the
# leading "-" as an option) and (b) false-passed when the SAME heading text
# (e.g. "### Fixed") already existed in Unreleased from other in-flight
# work while the actual addition sat under an old release. Both were caught
# in review on #1938, alongside the non-bracket heading gap `## \[` above
# now closes. This does not check that the entry is a GOOD one -- see the
# "optimistic count" discussion on #1938.
has_changelog_entry() {
    local range start end lineno
    range="$(unreleased_line_range)"
    [[ -n "$range" ]] || return 1
    read -r start end <<<"$range"
    while IFS= read -r lineno; do
        [[ -z "$lineno" ]] && continue
        if (( lineno > start && lineno < end )); then
            return 0
        fi
    done < <(added_line_numbers)
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
