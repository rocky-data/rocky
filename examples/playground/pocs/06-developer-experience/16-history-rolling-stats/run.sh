#!/usr/bin/env bash
# 16-history-rolling-stats — `rocky history --model <name> --rolling-stats`
#
# Five back-to-back replication runs vary the source row count to drive
# real duration variance, then `rocky history --model orders` surfaces the
# rolling statistics block (mean / std-dev / z-score / health-score).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

rm -f .rocky-state.redb .rocky-state.redb.lock poc.duckdb
mkdir -p expected

reseed() {
    local rows="$1"
    duckdb poc.duckdb <<SQL >/dev/null
CREATE SCHEMA IF NOT EXISTS raw__orders;
CREATE OR REPLACE TABLE raw__orders.orders AS
SELECT
    i AS order_id,
    100 + (i % 5) AS customer_id,
    'completed' AS status,
    10.0 * i AS amount
FROM range(1, ${rows}) AS t(i);
SQL
}

echo "==> Seed raw__orders.orders (5 rows)"
reseed 6
rocky -c rocky.toml -o json run --filter source=orders > expected/run_1.json

echo "==> Reseed with 50 rows; second run"
reseed 51
rocky -c rocky.toml -o json run --filter source=orders > expected/run_2.json

echo "==> Reseed with 200 rows; third run"
reseed 201
rocky -c rocky.toml -o json run --filter source=orders > expected/run_3.json

echo "==> Reseed with 25 rows; fourth run"
reseed 26
rocky -c rocky.toml -o json run --filter source=orders > expected/run_4.json

echo "==> Reseed with 12 rows; fifth run"
reseed 13
rocky -c rocky.toml -o json run --filter source=orders > expected/run_5.json

echo
echo "==> rocky history --model orders --rolling-stats --window 5"
rocky -c rocky.toml -o json history --model orders --rolling-stats --window 5 \
    > expected/history_rolling.json

# Quick sanity check — the rolling_stats block must surface and carry the
# window we asked for. Anything else means the flag silently dropped.
python3 - <<'PY'
import json, sys
with open("expected/history_rolling.json") as f:
    h = json.load(f)
rs = h.get("rolling_stats")
if rs is None:
    print("FAIL: rolling_stats block missing from history JSON", file=sys.stderr)
    sys.exit(1)
assert rs["window"] == 5, f"unexpected window: {rs['window']}"
assert rs["samples"] >= 1, f"unexpected samples: {rs['samples']}"
assert 0.0 <= rs["health_score"] <= 1.0, f"health_score out of range: {rs['health_score']}"
print(f"    samples={rs['samples']}  window={rs['window']}  health_score={rs['health_score']}")
lz = rs['duration_ms'].get('latest_z_score')
lz_str = f"{lz:.2f}" if isinstance(lz, (int, float)) else "n/a"
print(f"    duration_ms.mean={rs['duration_ms']['mean']:.2f}  "
      f"std_dev={rs['duration_ms']['std_dev']:.2f}  "
      f"latest_z={lz_str}")
PY

echo
echo "==> Compare bare history vs rolling-stats (block only appears with --rolling-stats)"
rocky -c rocky.toml -o json history --model orders > expected/history_bare.json
python3 - <<'PY'
import json
bare = json.load(open("expected/history_bare.json"))
roll = json.load(open("expected/history_rolling.json"))
assert "rolling_stats" not in bare, "rolling_stats leaked into bare --model output"
assert "rolling_stats" in roll, "rolling_stats missing from --rolling-stats output"
print("    bare output: rolling_stats absent (correct)")
print("    --rolling-stats: rolling_stats present (correct)")
PY

echo
echo "POC complete: five runs feed the window; rolling_stats surfaces health_score + per-dimension z-scores."
