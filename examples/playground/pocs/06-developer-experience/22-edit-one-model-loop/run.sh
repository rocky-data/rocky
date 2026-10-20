#!/usr/bin/env bash
# 22-edit-one-model-loop — edit one model, rebuild only it and its downstream.
# Every claim is checked against the JSON output. A claim that fails exits 1.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Binary resolution: $ROCKY_BIN, then the engine build in this checkout, then PATH.
if [ -z "${ROCKY_BIN:-}" ]; then
  if repo_root="$(git rev-parse --show-toplevel 2>/dev/null)" && [ -x "$repo_root/engine/target/release/rocky" ]; then
    ROCKY_BIN="$repo_root/engine/target/release/rocky"
  else
    ROCKY_BIN="$(command -v rocky || true)"
  fi
fi
if [ -z "$ROCKY_BIN" ]; then
  echo "error: no rocky binary found. Set ROCKY_BIN, build the engine (cd engine && cargo build --release), or install rocky on PATH." >&2
  exit 1
fi

mkdir -p expected
STATE="$HERE/expected/state.redb"
rm -f "$STATE" "$STATE.lock" expected/prod-state.redb expected/prod-state.redb.lock poc.duckdb
rm -f models/.rocky-state.redb models/.rocky-state.redb.lock

# Scratch files (backups, stderr, the "production" copy of the project) live outside the tree.
SCRATCH="$(mktemp -d)"
# The script edits two model files. Restore both on exit, even on failure.
cp models/stg_orders.sql "$SCRATCH/stg_orders.sql"
cp models/daily_revenue.sql "$SCRATCH/daily_revenue.sql"
WATCH_PID=""
cleanup() {
  [ -n "$WATCH_PID" ] && kill -INT "$WATCH_PID" 2>/dev/null || true
  cp "$SCRATCH/stg_orders.sql" models/stg_orders.sql
  cp "$SCRATCH/daily_revenue.sql" models/daily_revenue.sql
  rm -rf "$SCRATCH"
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

run() { "$ROCKY_BIN" --state-path "$STATE" -c rocky.toml -o json run "$@"; }
# runj <out.json> <args>: run, keep the JSON, and print rocky's stderr if it fails.
runj() {
  local out=$1; shift
  run "$@" > "$out" 2> "$SCRATCH/err" || { cat "$SCRATCH/err" >&2; fail "rocky run $* failed"; }
}
# runfail <args>: run and expect a non-zero exit. Show the error rocky gave.
runfail() {
  if run "$@" > /dev/null 2> "$SCRATCH/err"; then
    fail "rocky run $* should have failed"
  fi
  echo "    failed as expected: $(grep -v '^{' "$SCRATCH/err" | head -1 | cut -c1-110)"
}

# Print "<built models, sorted> | <skipped models, sorted>" for a run JSON file.
# A watch log holds one JSON object per run; pass the 1-based record number.
decisions() {
  python3 - "$1" "${2:-1}" <<'EOF'
import json, sys
text, want = open(sys.argv[1]).read(), int(sys.argv[2])
dec, i, n = json.JSONDecoder(), 0, 0
while True:
    while i < len(text) and text[i].isspace():
        i += 1
    if i >= len(text):
        sys.exit("no record %d in %s" % (want, sys.argv[1]))
    obj, i = dec.raw_decode(text, i)
    n += 1
    if n == want:
        break
d = obj["model_decisions"]
b = sorted(m["model"] for m in d if m["decision"] == "build")
s = sorted(m["model"] for m in d if m["decision"] == "skip")
print(",".join(b) + "|" + ",".join(s))
EOF
}

ALL_MODELS="customer_orders,daily_revenue,region_summary,stg_customers,stg_orders"
now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }

echo "=== seed raw data (50 customers, 500 orders) ==="
duckdb poc.duckdb < data/seed.sql

echo
echo "=== 1. first run: nothing is built yet, so all 5 models build ==="
t0=$(now_ms); runj expected/run1.json; t1=$(now_ms)
got=$(decisions expected/run1.json)
echo "    built|skipped = $got   ($((t1 - t0)) ms)"
[ "$got" = "$ALL_MODELS|" ] || fail "run 1 should build all 5 models, got '$got'"
FULL_MS=$((t1 - t0))

echo
echo "=== 2. run again with no edit: all 5 models skip ==="
runj expected/run2.json
got=$(decisions expected/run2.json)
echo "    built|skipped = $got"
[ "$got" = "|$ALL_MODELS" ] || fail "run 2 should skip all 5 models, got '$got'"

echo
echo "=== 3. edit ONE model (stg_orders: double the amount), run again ==="
sed -i.bak 's/amount, order_date/amount * 2 AS amount, order_date/' models/stg_orders.sql
rm -f models/stg_orders.sql.bak
t0=$(now_ms); runj expected/run3.json; t1=$(now_ms)
got=$(decisions expected/run3.json)
echo "    built|skipped = $got   ($((t1 - t0)) ms)"
EDIT_MS=$((t1 - t0))
[ "$got" = "customer_orders,daily_revenue,stg_orders|region_summary,stg_customers" ] \
  || fail "run 3 should build stg_orders + its 2 downstream models and skip the other 2, got '$got'"
# The rebuilt tables carry the edit. The skipped table is untouched.
total=$(duckdb poc.duckdb "SELECT SUM(revenue) FROM main.daily_revenue" -noheader -list)
[ "$total" = "24970" ] || fail "daily_revenue should sum to 24970 after the edit, got $total"
echo "    daily_revenue total = $total (was 12485 before the edit)"

echo
echo "=== 4. watch mode: save a model file, Rocky re-runs by itself ==="
# --watch cannot be combined with --model. Pass --models so the watcher sees the models directory.
LOG="$HERE/expected/watch.json"; ERR="$SCRATCH/watch.err"
: > "$LOG"; : > "$ERR"
"$ROCKY_BIN" --state-path "$STATE" -c rocky.toml -o json run --watch --models models > "$LOG" 2> "$ERR" &
WATCH_PID=$!
waitfor() { # wait up to 10 s until the watch stderr shows N finished runs
  for _ in $(seq 1 40); do
    [ "$(grep -c '^\[watch\] run completed' "$ERR" || true)" -ge "$1" ] && return 0
    sleep 0.25
  done
  fail "watch did not finish run $1 in time"
}
waitfor 1
got=$(decisions "$LOG" 1)
echo "    start-up run:  built|skipped = $got"
[ "$got" = "|$ALL_MODELS" ] || fail "the start-up watch run should skip all 5 models, got '$got'"
sed -i.bak 's/SUM(amount) AS revenue/SUM(amount) AS revenue, COUNT(*) AS orders/' models/daily_revenue.sql
rm -f models/daily_revenue.sql.bak
waitfor 2
got=$(decisions "$LOG" 2)
echo "    after the save: built|skipped = $got"
[ "$got" = "daily_revenue|customer_orders,region_summary,stg_customers,stg_orders" ] \
  || fail "the watch re-run should build only daily_revenue, got '$got'"
kill -INT "$WATCH_PID"; wait "$WATCH_PID" || fail "watch did not exit cleanly on SIGINT"
WATCH_PID=""

echo
echo "=== 5. --defer --defer-to: build one model, read its upstream from another schema ==="
# Stand in for production: copy the upstream into schema 'prod', then drop the local copy.
# This is a fresh checkout that has no local stg_orders.
duckdb poc.duckdb "CREATE SCHEMA prod; CREATE TABLE prod.stg_orders AS SELECT * FROM main.stg_orders; DROP TABLE main.stg_orders;"
sed -i.bak 's/COUNT(\*) AS orders/COUNT(*) AS orders, MAX(amount) AS max_amount/' models/daily_revenue.sql
rm -f models/daily_revenue.sql.bak
runfail --model daily_revenue
runj expected/defer.json --model daily_revenue --defer --defer-to prod
built() { python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(",".join(m["asset_key"][-1] for m in d["materializations"]))' "$1"; }
mats=$(built expected/defer.json)
echo "    with --defer --defer-to prod: built = $mats"
[ "$mats" = "daily_revenue" ] || fail "--defer should build only daily_revenue, got '$mats'"
if duckdb poc.duckdb "SELECT 1 FROM main.stg_orders" > /dev/null 2>&1; then
  fail "--defer must not rebuild main.stg_orders"
fi
cols=$(duckdb poc.duckdb "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'daily_revenue' AND column_name = 'max_amount'" -noheader -list)
[ "$cols" = "1" ] || fail "daily_revenue should carry the new max_amount column"
echo "    main.stg_orders is still absent; daily_revenue has the new column"

echo
echo "=== 6. --defer-to-state: find the upstream from a saved production state ==="
# Production writes to a different schema (prod), so its state records poc.prod.<model>.
# Build a copy of the project that targets 'prod' and keep its state file.
PROD="$SCRATCH/prod"; mkdir -p "$PROD/models"
cp models/*.sql models/*.toml "$PROD/models/"
perl -pi -e 's/schema = "main"/schema = "prod"/' "$PROD"/models/*.toml
perl -pi -e 's/\b(FROM|JOIN) stg_/$1 prod.stg_/' "$PROD"/models/*.sql
perl -pe 's#models/\*\*#'"$PROD"'/models/**#' rocky.toml > "$PROD/rocky.toml"
"$ROCKY_BIN" --state-path expected/prod-state.redb -c "$PROD/rocky.toml" -o json run > "$PROD/out.json" 2> "$SCRATCH/err" \
  || { cat "$SCRATCH/err" >&2; fail "the production build failed"; }
# main.stg_orders is still absent here, so only the saved state can say where to read it.
runfail --model daily_revenue --force-rebuild
runj expected/defer_state.json --model daily_revenue --defer --defer-to-state expected/prod-state.redb --force-rebuild
mats=$(built expected/defer_state.json)
echo "    with --defer --defer-to-state: built = $mats"
[ "$mats" = "daily_revenue" ] || fail "--defer-to-state should build only daily_revenue, got '$mats'"
if duckdb poc.duckdb "SELECT 1 FROM main.stg_orders" > /dev/null 2>&1; then
  fail "--defer-to-state must not rebuild main.stg_orders"
fi
echo "    main.stg_orders is still absent; the upstream was read from poc.prod.stg_orders"

echo
echo "=== timing (this run, one sample each) ==="
echo "    first run, all 5 models build:        ${FULL_MS} ms"
echo "    after one edit, 3 of 5 models build:  ${EDIT_MS} ms"
echo
echo "POC complete: one edit rebuilt only that model and its downstream; the rest were skipped."
