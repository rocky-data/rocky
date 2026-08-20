#!/usr/bin/env bash
# Per-assert mutation pass: prove each of run.sh's 10 asserts is NON-VACUOUS.
#
# For each assert n, run.sh carries a guarded `MUTATE=n` hook that breaks THAT
# assert's engine gate (a lying digest, a dropped contract column, an un-stripped
# policy, a corrupted plan binding, a skipped review, a right-instead-of-wrong
# digest, a deleted grain test, un-aged data, ...). A correct assert must then
# print exactly `FAIL: n`. This script runs all ten in a throwaway COPY of the
# POC (the mutations never touch the committed tree) and prints the ledger.
#
# Run AFTER `./run.sh` is green. Requires `rocky` on PATH (same as run.sh).
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

command -v rocky >/dev/null 2>&1 || { echo "rocky not on PATH (see README)"; exit 1; }

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
# Copy only the committed inputs (no generated state) into the sandbox.
mkdir -p "$WORK/poc"
cp -R "$HERE"/. "$WORK/poc/"
rm -rf "$WORK/poc/products" "$WORK/poc/.rocky" "$WORK/poc/expected" "$WORK/poc/wh.duckdb" \
       "$WORK/poc/models/.rocky-state.redb" "$WORK/poc/models/revenue_daily."* 2>/dev/null

declare -a WHAT=(
  "1:lie about the candidate hand-off digest (runner refuses the write)"
  "2:drafted SQL omits the revenue_eur contract column (verify goes red)"
  "3:swap a broken spec for the valid one (no reject fires)"
  "4:keep [policy] instead of stripping it (posture passes)"
  "5:strip product_id from the persisted plan payload"
  "6:skip the candidate edit so re-approval supersedes nothing"
  "7:skip rocky review --approve (no sign-off marker)"
  "8:pass the CORRECT digest where a wrong one is expected"
  "9:delete the composite-unique grain test from the sidecar"
  "10:skip backdating so the data never goes stale"
)

echo "=================================================================="
echo " Mutation pass — one broken gate per assert must produce FAIL: <n>"
echo "=================================================================="
pass=0; total=0
LEDGER=""
for entry in "${WHAT[@]}"; do
  n="${entry%%:*}"; desc="${entry#*:}"
  total=$((total + 1))
  # Fresh sandbox per mutation (each run mutates its own copy).
  RUN="$(mktemp -d)"
  cp -R "$WORK/poc/." "$RUN/"
  ( cd "$RUN" && MUTATE="$n" bash run.sh ) > "$RUN/out.log" 2>&1
  rc=$?
  got="$(grep -oE "FAIL: [0-9]+" "$RUN/out.log" | head -1)"
  if [ "$rc" != "0" ] && [ "$got" = "FAIL: $n" ]; then
    echo "  [assert $n] PASS — mutation caught (exit $rc, '$got')"
    echo "             gate broken: $desc"
    LEDGER="${LEDGER}| ${n} | ${desc} | ${got} (exit ${rc}) | caught |"$'\n'
    pass=$((pass + 1))
  else
    echo "  [assert $n] LEAK — expected 'FAIL: $n', got '${got:-<none>}' (exit $rc)"
    echo "             gate broken: $desc"
    echo "             --- last 6 lines ---"
    tail -6 "$RUN/out.log" | sed 's/^/             /'
    LEDGER="${LEDGER}| ${n} | ${desc} | ${got:-none} (exit ${rc}) | LEAK |"$'\n'
  fi
  rm -rf "$RUN"
done

echo
echo "Ledger (assert | broken gate | observed | verdict):"
printf '%s' "$LEDGER"
echo
echo "Mutation pass: $pass / $total asserts are non-vacuous."
[ "$pass" = "$total" ] || { echo "MUTATION PASS INCOMPLETE — $((total - pass)) assert(s) did not catch their mutation."; exit 1; }
echo "All asserts catch a broken gate. The replay lane is a real gate exerciser."
