#!/usr/bin/env bash
# Pin the change-detection blocks in engine-ci.yml and codegen-drift.yml.
#
# Both workflows run their required jobs unfiltered on `pull_request`, and a
# `changes` job decides whether each job does real work (#1563). A wrong "no
# match" there skips the required jobs while still satisfying the merge gate,
# so the PR merges having run no CI at all. Three fail-open holes have shipped
# in that one block:
#
#   * a rename folded into its destination path, so a file moved OUT of
#     engine/ never matched;
#   * `printf | grep -q` past the pipe buffer — grep quit on the first match,
#     printf died of SIGPIPE, and pipefail read a real match as no-match;
#   * `git diff` QUOTING a pathname that holds a newline, so the listing line
#     starts with a double quote and the `^`-anchored regex misses it.
#
# Each case below replays the workflow's own `run:` block, extracted verbatim,
# against a scratch git repo. Nothing is reimplemented: a block that stops
# matching the workflow stops being tested.
#
#   scripts/tests/ci-change-detector.test.sh
#
# Set DETECTOR_TEST_ROOT to a copy of the repo to replay modified workflows —
# that is how fix-sensitivity is shown without dirtying the worktree.
set -uo pipefail

ROOT="${DETECTOR_TEST_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"

pass=0
fail=0

# --- extraction -------------------------------------------------------------

# The regex the detector matches changed paths against, e.g.
# `ENGINE_PATHS_RE: ^(engine/|...)` -> everything after the colon.
extract_regex() {
    # $1 workflow file   $2 env var name
    awk -v pat="^ *$2: " 'sub(pat, "") { print; exit }' "$1"
}

# The `run:` block of the step with `id: $2`, dedented to column 0. The block
# scalar ends at the first line indented less than its own body.
extract_run_block() {
    # $1 workflow file   $2 step id
    # shellcheck disable=SC2016  # $0/$1/$2 below are awk fields, not shell args
    awk -v want="$2" '
        $1 == "id:" { step = $2 }
        step == want && $0 ~ /^ *run: \|$/ { collect = 1; next }
        collect {
            if ($0 ~ /^[[:space:]]*$/) { print ""; next }
            here = match($0, /[^ ]/)
            if (!indent) indent = here
            if (here < indent) exit
            print substr($0, indent)
        }
    ' "$1"
}

# --- scratch repos ----------------------------------------------------------

# A repo whose base commit already holds an engine file, so a case can rename
# it away, plus a `docs/` tree and an `engine/` directory that survives every
# case (engine-ci's block runs under `working-directory: engine`).
new_repo() {
    git init -q "$CASE_REPO"
    git -C "$CASE_REPO" config user.email test@example.invalid
    git -C "$CASE_REPO" config user.name detector-test
    git -C "$CASE_REPO" config commit.gpgsign false
    # Pinned, not inherited: with `diff.renames=false` the rename case is
    # vacuous, and with `diff.relative=true` engine-ci's block would see paths
    # already stripped of their `engine/` prefix.
    git -C "$CASE_REPO" config diff.renames true
    git -C "$CASE_REPO" config diff.relative false
    git -C "$CASE_REPO" config core.quotePath true

    mkdir -p "$CASE_REPO/docs" "$CASE_REPO/engine/crates/rocky-core/src"
    : > "$CASE_REPO/engine/.keep"
    : > "$CASE_REPO/docs/.keep"
    # Distinct lines, so rename detection is confident about the move.
    seq 1 40 > "$CASE_REPO/engine/crates/rocky-core/src/lib.rs"

    git -C "$CASE_REPO" add -A
    git -C "$CASE_REPO" commit -qm base
}

# Every builder ends by committing. A silent failure here would leave the repo
# with no second commit, the block would take its diff-failed branch, and a
# case expecting "true" would pass for the wrong reason — so callers must stop
# on a non-zero return.
commit_change() {
    git add -A || return 1
    git commit -qm change || return 1
}

# --- the changes each case commits ------------------------------------------

build_engine_path() {
    : > engine/crates/rocky-core/src/added.rs
    commit_change
}

build_non_engine_path() {
    : > docs/guide.md
    commit_change
}

build_renamed_out() {
    git mv engine/crates/rocky-core/src/lib.rs docs/lib.rs
    commit_change || return 1
    # Exhibit the condition: with rename detection on, plain `git diff` folds
    # the move into its destination and the engine path disappears.
    git diff --name-only HEAD^1 HEAD > "$CASE_SCRATCH/list" || return 1
    if grep -q '^engine/' "$CASE_SCRATCH/list"; then
        echo "setup: the rename was not folded into its destination" >&2
        return 1
    fi
}

# A listing past the 64 KiB pipe buffer with the match near its FRONT — the
# shape that let grep quit early and SIGPIPE the writer. `git diff` sorts by
# path, so the padding has to sort after `engine/`; `zz/` matches neither
# workflow's regex.
build_long_list() {
    local wide i
    wide="$(printf 'x%.0s' {1..90})"
    mkdir -p zz/pad
    : > engine/crates/rocky-core/src/added.rs
    for i in {1..800}; do
        : > "zz/pad/${wide}$(printf '%04d' "$i").md"
    done
    commit_change
}

# The match is NOT the first record. If an implementation matched `^` against
# the whole listing instead of per record, only this case would fail.
build_non_match_first() {
    : > docs/guide.md
    : > engine/crates/rocky-core/src/added.rs
    commit_change
}

# A path that only a `$`-anchored alternative can match. `^` and `$` have to
# anchor to the same NUL record; if `$` anchored to the end of the whole
# listing instead, every one of these single-file watches would go dark —
# including a PR that touches only the workflow file itself.
build_dollar_anchored() {
    mkdir -p "$(dirname "$DOLLAR_PATH")"
    : > "$DOLLAR_PATH"
    : > docs/guide.md
    commit_change
}

build_grep_error() {
    : > docs/guide.md
    commit_change || return 1
    cat > "$CASE_BIN/grep" <<'STUB'
#!/usr/bin/env bash
echo "grep: stubbed failure" >&2
exit 2
STUB
    chmod +x "$CASE_BIN/grep"
}

build_newline_name() {
    local target
    target="$(printf 'engine/crates/rocky-core/tests/bad\nname.rs')"
    mkdir -p engine/crates/rocky-core/tests
    : > "$target"
    commit_change || return 1
    # Exhibit the condition: git quotes the pathname, so the line starts with
    # a double quote rather than `engine/`.
    git diff --no-renames --name-only HEAD^1 HEAD > "$CASE_SCRATCH/list" || return 1
    if ! grep -q '^"engine/' "$CASE_SCRATCH/list"; then
        echo "setup: git did not quote the newline pathname" >&2
        return 1
    fi
}

# No second commit at all: `HEAD^1` does not resolve, so the diff fails.
build_no_parent() {
    :
}

# --- the runner -------------------------------------------------------------

# Echo a failing block's own output, indented under the verdict line.
indent_output() {
    local line
    while IFS= read -r line; do
        printf '          %s\n' "$line"
    done <<< "$1"
}

# $1 label   $2 builder function   $3 expected output value
# $4 "warn" when the block must also print a ::warning::
run_case() {
    local label="$1" builder="$2" want="$3" warn="${4:-}"
    local out status got

    CASE_SCRATCH="$(mktemp -d)" || {
        echo "  FAIL  $label — mktemp failed"
        fail=$((fail + 1))
        return
    }
    CASE_REPO="$CASE_SCRATCH/repo"
    CASE_BIN="$CASE_SCRATCH/bin"
    mkdir -p "$CASE_BIN"

    new_repo > /dev/null 2>&1
    if ! ( cd "$CASE_REPO" && "$builder" ) > /dev/null; then
        echo "  FAIL  $label — the scratch repo did not reach the state under test"
        fail=$((fail + 1))
        rm -rf "$CASE_SCRATCH"
        return
    fi

    : > "$CASE_SCRATCH/github_output"
    out="$( cd "$CASE_REPO/$RUNDIR" \
        && PATH="$CASE_BIN:$PATH" \
           GITHUB_OUTPUT="$CASE_SCRATCH/github_output" \
           env "$REGEX_VAR=$REGEX" bash "$BLOCK" 2>&1 )"
    status=$?
    got="$(awk -v key="$OUT_KEY=" 'index($0, key) == 1 { v = substr($0, length(key) + 1) } END { print v }' \
        "$CASE_SCRATCH/github_output")"

    if [ "$status" != 0 ]; then
        echo "  FAIL  $label — the detector block exited $status"
        indent_output "$out"
        fail=$((fail + 1))
    elif [ "$got" != "$want" ]; then
        echo "  FAIL  $label — wanted $OUT_KEY=$want, got $OUT_KEY=${got:-<empty>}"
        indent_output "$out"
        fail=$((fail + 1))
    elif [ "$warn" = warn ] && ! printf '%s' "$out" | grep -q '::warning::'; then
        echo "  FAIL  $label — no ::warning:: was emitted"
        indent_output "$out"
        fail=$((fail + 1))
    else
        echo "  ok    $label ($OUT_KEY=$got)"
        pass=$((pass + 1))
    fi

    rm -rf "$CASE_SCRATCH"
}

# $1 workflow file   $2 regex env var   $3 output key   $4 dir the step runs in
# $5 a watched path this regex matches only through a `$`-anchored alternative
run_suite() {
    WORKFLOW="$ROOT/.github/workflows/$1"
    REGEX_VAR="$2"
    OUT_KEY="$3"
    RUNDIR="$4"
    DOLLAR_PATH="$5"

    echo "$1 (${REGEX_VAR}, working directory: $RUNDIR):"

    if [ ! -f "$WORKFLOW" ]; then
        echo "  FAIL  no such workflow: $WORKFLOW"
        fail=$((fail + 1))
        return
    fi

    REGEX="$(extract_regex "$WORKFLOW" "$REGEX_VAR")"
    if [ -z "$REGEX" ]; then
        echo "  FAIL  $1 no longer defines $REGEX_VAR — the detector moved, and this suite must move with it"
        fail=$((fail + 1))
        return
    fi

    BLOCK="$(mktemp)"
    extract_run_block "$WORKFLOW" diff > "$BLOCK"
    # Sanity-check the extraction, never the fix: asserting on the fix here
    # would make a reverted fix fail as a bad extraction instead of as the
    # wrong verdict.
    if ! grep -q 'git diff' "$BLOCK" || ! grep -q 'GITHUB_OUTPUT' "$BLOCK"; then
        echo "  FAIL  could not extract the \`id: diff\` run block from $1"
        fail=$((fail + 1))
        rm -f "$BLOCK"
        return
    fi

    run_case "an engine path runs the jobs"            build_engine_path     true
    run_case "an unwatched path skips them"            build_non_engine_path false
    run_case "a path renamed out of the tree runs"     build_renamed_out     true
    run_case "a match early in a huge listing runs"    build_long_list       true
    run_case "a match after a non-match runs"          build_non_match_first true
    run_case "$DOLLAR_PATH runs (\$-anchored)"         build_dollar_anchored true
    run_case "a newline in a pathname runs"            build_newline_name    true
    run_case "a grep error runs and warns"             build_grep_error      true warn
    run_case "a failed diff runs and warns"            build_no_parent       true warn

    rm -f "$BLOCK"
    echo
}

run_suite engine-ci.yml      ENGINE_PATHS_RE  engine  engine \
    .claude/skills/rocky-ai-workflow/SKILL.md
run_suite codegen-drift.yml  CODEGEN_PATHS_RE codegen . \
    justfile

if [ "$fail" -gt 0 ]; then
    echo "$pass passed, $fail FAILED"
    exit 1
fi
echo "$pass passed"
