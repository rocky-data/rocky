#!/usr/bin/env bash
# 07-freshness-sla — model-level freshness SLAs as first-class declarative
# metadata, plus a project-wide default. Compile-time only, no warehouse.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
rm -f .rocky-state.redb models/.rocky-state.redb
mkdir -p expected

echo "=== 1. Config is valid (parses the project-wide [freshness] default) ==="
rocky validate
echo

echo "=== 2. Each model's declared freshness SLA surfaces in compile output ==="
echo "    (models_detail[].freshness — the structured metadata downstream tools read)"
rocky compile --with-seed --models models -o json > expected/compile.json 2>/dev/null
python3 - <<'PY'
import json
d = json.load(open("expected/compile.json"))
for m in sorted(d["models_detail"], key=lambda x: x["name"]):
    f = m.get("freshness")
    if f:
        print(f"    {m['name']:14} SLA: expected_lag_seconds={f['max_lag_seconds']:>6}  "
              f"time_column={f.get('time_column')!r}  severity={f.get('severity')!r}")
    else:
        print(f"    {m['name']:14} (no freshness SLA declared)")
PY
echo

echo "POC complete:"
echo "  - stg_shipments declares a per-model [freshness] SLA -> carried into compile output"
echo "  - expected_lag_seconds (public name) deserializes to canonical max_lag_seconds"
echo "  - stg_orders has a temporal column but no SLA -> the W005 coverage diagnostic"
echo "    flags it in the editor (LSP), with an AI 'add [freshness] block' fix. See README."
