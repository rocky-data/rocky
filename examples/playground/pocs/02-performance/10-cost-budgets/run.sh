#!/usr/bin/env bash
# Arc 2 — per-run cost summary + [budget] breach
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

rm -f .rocky-state.redb poc.duckdb
mkdir -p expected

echo "==> 1. Validate the config (exercises the [budget] parser)"
rocky validate

echo "==> 2. Seed raw__orders.orders"
duckdb poc.duckdb < data/seed.sql

echo "==> 3. Run the pipeline — cost_summary appears in the JSON output"
rocky -c rocky.toml -o json run --filter source=orders > expected/run.json

echo "==> 4. Extract cost_summary + budget_breach event from the run output"
python3 - <<'PY'
import json
run = json.load(open("expected/run.json"))
cs = run.get("cost_summary") or {}
print("cost_summary.adapter_type     :", cs.get("adapter_type"))
print("cost_summary.total_duration_ms:", cs.get("total_duration_ms"))
print("cost_summary.total_cost_usd   :", cs.get("total_cost_usd"))
print("per-model entries             :", len(cs.get("per_model") or []))
breaches = run.get("budget_breaches") or []
if breaches:
    for b in breaches:
        print(f"budget_breach                 : {b['limit_type']}={b['limit']} actual={b['actual']}")
else:
    print("budget_breach                 : (none — lower max_duration_ms or max_usd to trigger)")
PY

echo
echo "POC complete: cost_summary + budget_breach captured in expected/run.json."
