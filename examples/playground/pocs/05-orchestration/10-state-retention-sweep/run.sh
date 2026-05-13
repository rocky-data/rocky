#!/usr/bin/env bash
# 10-state-retention-sweep — `[state.retention]` + `rocky state retention sweep`.
#
# Walks the two ways Rocky prunes its own history tables:
#
#   1. Manual:   `rocky state retention sweep`        — explicit, scriptable.
#   2. Automatic: end-of-run auto-sweep on `rocky run` — gated by
#      `sweep_interval_seconds` so high-frequency projects don't pay the
#      sweep cost on every invocation.
#
# Operational tables (schema cache, watermarks, partition records,
# branches, idempotency keys) are NEVER swept — only the three history
# domains (`history`, `lineage`, `audit`).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

rm -f .rocky-state.redb .rocky-state.redb.lock poc.duckdb
mkdir -p expected

# Tell run.sh which sweep mode each section is demonstrating. The first
# section disables auto-sweep via env-override-by-removal so the manual
# sweep has something to delete; the second section relies on
# `[state.retention] sweep_interval_seconds = 0` from rocky.toml to
# trigger auto-sweeps end-of-run.

count_runs() {
    rocky -c rocky.toml -o json history 2>/dev/null \
        | python3 -c 'import json,sys;print(len(json.load(sys.stdin)["runs"]))'
}

echo "==> Seed raw__orders.orders"
duckdb poc.duckdb < data/seed.sql

# ---- Part 1: manual sweep ---------------------------------------------------
# Disable auto-sweep with a config override so the run loop leaves the
# history alone; we want a fat history table to feed the manual command.
cp rocky.toml rocky.no-auto.toml
python3 - <<'PY'
import re
p = "rocky.no-auto.toml"
src = open(p).read()
# Park auto-sweep by raising the interval; manual sweep is unaffected.
src = re.sub(r"sweep_interval_seconds\s*=\s*0", "sweep_interval_seconds = 3600", src)
open(p, "w").write(src)
PY

echo "==> Three runs with auto-sweep parked (sweep_interval_seconds = 3600)"
for i in 1 2 3; do
    rocky -c rocky.no-auto.toml -o json run --filter source=orders \
        > "expected/run_${i}.json"
done
echo "    history rows after 3 runs: $(count_runs)"

echo
echo "==> 'rocky state retention sweep --dry-run'  (plan only — no deletes)"
rocky -c rocky.toml -o json state retention sweep --dry-run \
    > expected/sweep_dry_run.json
python3 - <<'PY'
import json
d = json.load(open("expected/sweep_dry_run.json"))
print(f"    plan: runs_deleted={d['runs_deleted']}  runs_kept={d['runs_kept']}  "
      f"domains={d['domains']}  duration_ms={d['duration_ms']}")
PY
echo "    history rows after dry-run: $(count_runs)  (unchanged — dry-run is a no-op)"

echo
echo "==> 'rocky state retention sweep'  (apply)"
rocky -c rocky.toml -o json state retention sweep \
    > expected/sweep_apply.json
python3 - <<'PY'
import json
d = json.load(open("expected/sweep_apply.json"))
print(f"    apply: runs_deleted={d['runs_deleted']}  runs_kept={d['runs_kept']}  "
      f"lineage_deleted={d['lineage_deleted']}  audit_deleted={d['audit_deleted']}")
PY
echo "    history rows after apply: $(count_runs)  (down to min_runs_kept = 1)"

# ---- Part 2: end-of-run auto-sweep -----------------------------------------
echo
echo "==> Three more runs WITH auto-sweep (sweep_interval_seconds = 0)"
echo "    Each run prunes history at end-of-run, so the table never grows."
for i in 4 5 6; do
    rocky -c rocky.toml -o json run --filter source=orders \
        > "expected/run_${i}.json"
    echo "    after run ${i}: history rows = $(count_runs)"
done

rm -f rocky.no-auto.toml

echo
echo "POC complete: manual sweep pruned 3→1, then auto-sweep held the table flat."
