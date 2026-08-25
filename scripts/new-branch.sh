#!/usr/bin/env bash
# Start a branch from a known base, with a clean tree.
#
# Two failure modes this removes, both of which have cost real rework here:
#
#   1. Branching from wherever HEAD happens to be. `git checkout -b fix/x`
#      inherits the CURRENT branch's commits. A branch cut while sitting on
#      another PR's branch carries that PR's payload: it conflicts the moment
#      the parent merges, and a squash-merge of the child can silently land
#      the parent's work (and fire its `Closes #NNN` trailers) under the
#      child's number.
#
#   2. Carrying uncommitted work across the switch. `git checkout` does not
#      stop for a dirty tree — it brings non-conflicting changes along, and
#      they end up committed on the new branch.
#
# Usage:
#   scripts/new-branch.sh <branch-name> [base]
#
# `base` defaults to origin/main and is fetched first, so the branch starts
# from the real remote head rather than a stale local ref.
#
# Deliberate stacking is still possible — pass the base explicitly:
#   scripts/new-branch.sh fix/child feat/parent-pr-branch
# It prints a warning in that case, because a stacked branch must be declared
# on its PR or the merge order will surprise someone.
#
# Exit status:
#   0  branch created and checked out
#   2  refused (dirty tree, bad arguments, unknown base, branch exists)
set -uo pipefail

die() {
    echo "new-branch: $*" >&2
    exit 2
}

[ $# -ge 1 ] || die "usage: $0 <branch-name> [base]"

branch="$1"
base="${2:-origin/main}"

repo_root="$(git rev-parse --show-toplevel 2>/dev/null)" || die "not a git repository"
cd "$repo_root" || die "cannot enter $repo_root"

dirty="$(git status --porcelain)" || die "git status failed — refusing to switch"
if [ -n "$dirty" ]; then
    echo "new-branch: REFUSING — the worktree is not clean." >&2
    echo "" >&2
    echo "$dirty" >&2
    echo "" >&2
    echo "git would carry these onto '$branch', where a later commit would put" >&2
    echo "them on the wrong branch. Commit them, or stash them with a tag:" >&2
    echo "" >&2
    echo "    git stash push -u -m \"<what this is>\"" >&2
    exit 2
fi

git show-ref --verify --quiet "refs/heads/$branch" && die "branch '$branch' already exists"

# Fetch so `origin/main` is the real remote head, not a stale local ref. A
# branch cut from a stale base looks fine and then conflicts on push.
if [ "$base" = "origin/main" ]; then
    git fetch origin main --quiet || die "could not fetch origin/main"
fi

git rev-parse --verify --quiet "$base" >/dev/null || die "unknown base: $base"

if [ "$base" != "origin/main" ]; then
    echo "new-branch: NOTE — basing '$branch' on '$base', not origin/main." >&2
    echo "            That is a STACKED branch. Say so on its pull request, and" >&2
    echo "            merge the parent first — a squash-merge of this branch will" >&2
    echo "            otherwise carry the parent's commits under this branch's" >&2
    echo "            number, including any 'Closes #NNN' trailers they contain." >&2
fi

git checkout -b "$branch" "$base" --quiet || die "could not create '$branch'"

echo "new-branch: created '$branch' from $base ($(git rev-parse --short HEAD))"
