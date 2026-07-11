#!/usr/bin/env bash
# 20-defer-against-prod — build only the changed model locally; resolve its
# unbuilt upstream ref to a production schema with `rocky run --defer`.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Binary resolution: honor $ROCKY_BIN; inside the monorepo prefer the
# freshly-built engine binary (the smoke runner's bare `rocky` on PATH is
# often a stale install that predates --defer); outside a git checkout fall
# back to `rocky` on PATH.
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
# A single pinned state file so this POC's run history is self-contained and
# deterministic across re-runs (and independent of any global state file).
STATE="$HERE/expected/state.redb"
rm -f "$STATE" "$STATE.lock" poc.duckdb
rm -f models/.rocky-state.redb models/.rocky-state.redb.lock

run() { "$ROCKY_BIN" --state-path "$STATE" -c rocky.toml -o json "$@"; }
tables() { duckdb poc.duckdb "SELECT table_schema || '.' || table_name FROM information_schema.tables WHERE table_schema IN ('main','prod') ORDER BY 1" -noheader -list; }

echo "=== seed raw source ==="
duckdb poc.duckdb < data/seed.sql

echo "=== validate ==="
run validate >/dev/null && echo "config OK"

echo
echo "=== 1. production build: materialize the whole DAG into poc.main ==="
run run > expected/run_prod.json
echo "materialized:"; tables | sed 's/^/    /'

echo
echo "=== 2. publish the upstream's prod home (snapshot stg_events into poc.prod) ==="
# Stand in for "stg_events already lives in production". A real project's prod
# schema is populated by the scheduled prod run; here we copy it so the demo is
# credential-free and self-contained.
duckdb poc.duckdb "CREATE SCHEMA IF NOT EXISTS prod; CREATE OR REPLACE TABLE prod.stg_events AS SELECT * FROM main.stg_events;"
prod_rows=$(duckdb poc.duckdb "SELECT COUNT(*) FROM prod.stg_events" -noheader -list)
echo "    prod.stg_events: $prod_rows rows"

echo
echo "=== 3. simulate a dev checkout WITHOUT a local upstream (drop main.stg_events) ==="
duckdb poc.duckdb "DROP TABLE IF EXISTS main.stg_events;"

echo
echo "=== 4. control: build only user_counts WITHOUT --defer (must fail — no local upstream) ==="
if run run --model user_counts > expected/run_nodefer.json 2>expected/run_nodefer.err; then
    echo "FAIL: a downstream-only build with no local stg_events should not succeed" >&2
    exit 1
fi
echo "    failed as expected — the local upstream is missing"

echo
echo "=== 5. build only user_counts WITH --defer --defer-to prod ==="
run run --model user_counts --defer --defer-to prod > expected/run_defer.json
mats=$(python3 -c 'import json; d=json.load(open("expected/run_defer.json")); print(",".join(".".join(m["asset_key"]) for m in d.get("materializations",[])))')
echo "    materialized: ${mats:-<none>}"

# Exactly one model materialized — user_counts — and nothing in the prod schema
# was rebuilt. The deferred stg_events ref resolved to poc.prod.stg_events.
[ "$mats" = "poc.main.user_counts" ] || {
    echo "FAIL: expected only poc.main.user_counts to materialize, got '${mats}'" >&2; exit 1; }

# Proof the ref resolved to prod: user_counts built from the 100 prod rows into
# 5 user groups, even though no local stg_events exists.
groups=$(duckdb poc.duckdb "SELECT COUNT(*) FROM main.user_counts" -noheader -list)
[ "$groups" = "5" ] || { echo "FAIL: expected 5 user groups from prod data, got $groups" >&2; exit 1; }
echo "    user_counts = $groups groups, built from poc.prod.stg_events ($prod_rows rows)"

# main.stg_events is still absent — --defer did NOT rebuild the deferred upstream.
if duckdb poc.duckdb "SELECT 1 FROM main.stg_events LIMIT 1" >/dev/null 2>&1; then
    echo "FAIL: --defer should not have built main.stg_events" >&2; exit 1
fi
echo "    main.stg_events still absent — the upstream was deferred, not rebuilt"

echo
echo "POC complete: only the selected model built locally; its ref resolved to prod."
