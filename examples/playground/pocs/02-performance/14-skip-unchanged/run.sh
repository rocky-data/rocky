#!/usr/bin/env bash
# 14-skip-unchanged — logic-AND-data-unchanged ⇒ skip; data-changed ⇒ build.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Binary resolution: honor $ROCKY_BIN; inside the monorepo prefer the
# freshly-built engine binary (the smoke runner's bare `rocky` on PATH is
# often a stale install that predates the skip gate); outside a git checkout
# fall back to `rocky` on PATH.
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
# Pin one state file so the baseline that run #1 records is the exact file the
# gate reads on runs #2 and #3. The gate's clause "a prior successful build"
# is satisfied only when run history is persisted to a stable location.
STATE="$HERE/expected/state.redb"
rm -f "$STATE" "$STATE.lock" poc.duckdb
rm -f models/.rocky-state.redb models/.rocky-state.redb.lock

# A full-DAG `rocky run` over the transformation pipeline records a run-history
# entry carrying the skip baseline (logic hash + upstream signature). The skip
# gate reads that entry on the next run.
run() { "$ROCKY_BIN" --state-path "$STATE" -c rocky.toml -o json run "$@"; }
skipped() { python3 -c "import json,sys; print(json.load(open('$1')).get('tables_skipped', 0))"; }
built() { python3 -c "import json,sys; print(len(json.load(open('$1')).get('materializations', [])))"; }

echo "=== seed (byte-stable: identical bytes on runs #1 and #2) ==="
duckdb poc.duckdb < data/seed.sql

# ---------------------------------------------------------------------------
# Run #1 — clean state. No prior build exists, so the gate BUILDS and records
# the baseline (logic hash + upstream rowcount). --skip-unchanged is on so the
# build stamps the skip metadata.
# ---------------------------------------------------------------------------
echo
echo "=== run #1 (clean state) — expect BUILD, records baseline ==="
run --skip-unchanged > expected/run1.json
echo "    tables_skipped=$(skipped expected/run1.json)  materializations=$(built expected/run1.json)"
[ "$(built expected/run1.json)" = "1" ] || { echo "FAIL: run #1 should build the model" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Run #2 — same SQL, BYTE-IDENTICAL upstream data. Logic hash matches and the
# upstream rowcount is unchanged ⇒ the gate SKIPS (no re-materialization).
# We assert on the positive skip signal in the run JSON, not on a rebuild.
# ---------------------------------------------------------------------------
echo
echo "=== run #2 (identical data, --skip-unchanged) — expect SKIP ==="
run --skip-unchanged > expected/run2.json
echo "    tables_skipped=$(skipped expected/run2.json)  materializations=$(built expected/run2.json)"
[ "$(skipped expected/run2.json)" = "1" ] || { echo "FAIL: run #2 should SKIP (logic + data unchanged), got tables_skipped=$(skipped expected/run2.json)" >&2; exit 1; }
[ "$(built expected/run2.json)" = "0" ] || { echo "FAIL: run #2 should not materialize anything" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Run #3 — MUTATE the upstream (append 20 rows). The rowcount moves, so the
# upstream is provably changed ⇒ the gate BUILDS.
# ---------------------------------------------------------------------------
echo
echo "=== mutate upstream: append 20 rows ==="
duckdb poc.duckdb "INSERT INTO raw__events.events
    SELECT 200 + i, 1 + (i % 10), 'click', TIMESTAMP '2026-05-01' + INTERVAL (i * 60) SECOND
    FROM generate_series(1, 20) AS t(i);"
echo "    source rows: $(duckdb poc.duckdb 'SELECT COUNT(*) FROM raw__events.events' -noheader -list)"

echo
echo "=== run #3 (data changed, --skip-unchanged) — expect BUILD ==="
run --skip-unchanged > expected/run3.json
echo "    tables_skipped=$(skipped expected/run3.json)  materializations=$(built expected/run3.json)"
[ "$(built expected/run3.json)" = "1" ] || { echo "FAIL: run #3 should rebuild (upstream data changed)" >&2; exit 1; }
[ "$(skipped expected/run3.json)" = "0" ] || { echo "FAIL: run #3 should not skip" >&2; exit 1; }

echo
echo "POC complete: unchanged ⇒ skipped (#2), data changed ⇒ rebuilt (#3)."
