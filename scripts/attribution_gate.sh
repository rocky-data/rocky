#!/usr/bin/env bash
# Fail a pull request whose title, body or commit messages carry agent
# attribution: a "Generated with/by" agent line, a session trailer, an
# agent co-author trailer, or a link to an agent session site.
#
# The repository is public and squash merges copy commit messages onto
# main, where a leaked line cannot be removed without rewriting history.
# Some agent tools append these lines on their own, so a written rule is
# not enough; this gate is the backstop. See AGENTS.md "Git conventions".
#
# Inputs (environment):
#   PR_TITLE      the pull request title
#   PR_BODY       the pull request body (may be empty)
#   COMMITS_FILE  path to a file holding every commit message in the PR
#
# Exit 0 when clean, 1 when a pattern matches, 2 on a usage error.
set -uo pipefail

if [[ -z "${COMMITS_FILE:-}" || ! -r "${COMMITS_FILE}" ]]; then
    echo "attribution-gate: COMMITS_FILE is unset or unreadable" >&2
    exit 2
fi

# Extended regexes, matched case-insensitively. Built from pieces so this
# file does not itself look like an attribution line to a naive scan.
readonly CLAUDE="claude"
# Any mention of the agent's session site counts, link or bare host: the
# rule is strict on purpose, so prose that names the host fails too.
readonly PATTERNS=(
    "${CLAUDE}\\.ai([^[:alnum:]-]|$)"
    "${CLAUDE}\\.com/${CLAUDE}-code"
    "generated[[:space:]]+(with|by)[[:space:]]+\\[?${CLAUDE}"
    "^${CLAUDE}-session:"
    "^co-authored-by:.*(${CLAUDE}|anthropic)"
)

found=0
check() {
    local label="$1" text="$2" pattern hits
    for pattern in "${PATTERNS[@]}"; do
        hits="$(printf '%s\n' "$text" | grep -inE -- "$pattern" || true)"
        if [[ -n "$hits" ]]; then
            echo "::error::${label} matches /${pattern}/:"
            printf '%s\n' "$hits" | sed 's/^/    /'
            found=1
        fi
    done
}

check "PR title" "${PR_TITLE:-}"
check "PR body" "${PR_BODY:-}"
check "a commit message" "$(cat "$COMMITS_FILE")"

if [[ "$found" -ne 0 ]]; then
    echo ""
    echo "Remove agent attribution lines and agent session links from the PR"
    echo "title, body and commit messages (AGENTS.md, Git conventions)."
    echo "The rule is strict: naming the session host in prose fails too."
    echo "Edit the PR body, or reword the commits, then push."
    exit 1
fi
echo "attribution-gate: clean"
