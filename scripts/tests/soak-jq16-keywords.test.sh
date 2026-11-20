#!/usr/bin/env bash
#
# The soak harness runs inside a debian:bookworm-slim image (scripts/soak-docker.sh),
# whose jq is 1.6. jq 1.6 rejects a jq keyword as a variable name (`$end`); jq
# 1.7 accepts it. (Both accept a keyword as a bare object key.) CI runners ship jq 1.7,
# so the soak self-check workflow cannot catch this. A `$end` in write_done made
# a clean 24 h soak on engine 1.93.0 report INCOMPLETE: no DONE file was written.
#
# This test fails on any non-comment line in the soak scripts that uses a jq
# keyword as a `$variable`. Rename the variable ($end_epoch) instead.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# The names jq 1.6 rejects after `$`, each checked with
# `jq -n --argjson NAME 1 '$NAME'` in the soak image. `not` and `__loc__` pass.
KW='module|def|as|if|then|elif|else|end|and|or|reduce|foreach|try|catch|label|import|include'
PATTERN="\\\$($KW)\\b"

scan() { # FILE... -> prints the offending lines
  grep -nHE "$PATTERN" "$@" | grep -vE '^[^:]+:[0-9]+:[[:space:]]*#' || true
}

# Self-test: the pattern must catch the line that broke the 1.93.0 soak.
bad=$(mktemp)
trap 'rm -f "$bad"' EXIT
printf '%s\n' "    '{end:\$end, complete:\$complete, reason:\$reason}')\"" >"$bad"
n=$(scan "$bad" | grep -c . || true)
[ "$n" = 1 ] || { echo "FAIL: self-test expected 1 hit on the known-bad line, got $n" >&2; exit 1; }

files=("$REPO_ROOT/scripts/soak-scheduler.sh" "$REPO_ROOT/scripts/soak-docker.sh")
for f in "${files[@]}"; do
  [ -s "$f" ] || { echo "FAIL: $f is missing or empty; this test would pass on no input" >&2; exit 1; }
done
hits=$(scan "${files[@]}")
n=$(printf '%s' "$hits" | grep -c . || true)
echo "jq 1.6 keyword uses in the soak scripts: $n"
if [ "$n" != 0 ]; then
  printf '%s\n' "$hits" >&2
  echo "FAIL: jq 1.6 (the soak image's jq) rejects these variable names; rename them" >&2
  exit 1
fi
echo "PASS"
