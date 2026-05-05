#!/usr/bin/env bash
# 07-config-layering — three-layer config + env-var substitution at every layer
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

mkdir -p expected
rm -f .rocky-state.redb poc.duckdb run2.duckdb

extract_targets() {
    # Pull per-model target out of compile JSON. python3 only, no jq.
    python3 -c "
import json, sys
data = json.load(sys.stdin)
for m in data.get('models_detail', []):
    t = m.get('target', {})
    print(f\"  {m['name']:18s} -> {t.get('catalog', '?')}.{t.get('schema', '?')}.{t.get('table', '?')}\")
"
}

run_compile() {
    local out=$1
    rocky -c rocky.toml validate >/dev/null 2>&1
    rocky compile --models models/ --output json > "$out" 2>/dev/null
}

echo "=== Run 1: NO env vars set — every layer falls back to its default ==="
unset ROCKY_DUCKDB_PATH ROCKY_CATALOG ROCKY_DEFAULT_SCHEMA \
      ROCKY_CUSTOMER_CATALOG ROCKY_CUSTOMER_SCHEMA ROCKY_TABLE_OVERRIDE
run_compile expected/compile-defaults.json
echo "Resolved model targets (defaults):"
extract_targets < expected/compile-defaults.json

echo
echo "=== Run 2: env vars set — orchestrator-style overrides flow into every layer ==="
export ROCKY_DUCKDB_PATH="run2.duckdb"
export ROCKY_CATALOG="prod_warehouse"
export ROCKY_DEFAULT_SCHEMA="silver"
export ROCKY_CUSTOMER_CATALOG="customer_prod"
export ROCKY_CUSTOMER_SCHEMA="gold"
export ROCKY_TABLE_OVERRIDE="customer_facts_v2"
run_compile expected/compile-overrides.json
echo "Resolved model targets (overrides):"
extract_targets < expected/compile-overrides.json

echo
echo "POC complete: same source files, two different resolutions."
echo
echo "Layer 1 (rocky.toml) — adapter.path"
echo "  default  : poc.duckdb"
echo "  override : run2.duckdb              (\$ROCKY_DUCKDB_PATH)"
echo
echo "Layer 2 (models/_defaults.toml) — inherited by orders_summary"
echo "  default  : poc_warehouse.public.orders_summary"
echo "  override : prod_warehouse.silver.orders_summary"
echo "             (\$ROCKY_CATALOG, \$ROCKY_DEFAULT_SCHEMA)"
echo
echo "Layer 3 (per-model sidecar) — customer_facts (FR-001 Option A)"
echo "  default  : customer_warehouse.marts.customer_facts"
echo "  override : customer_prod.gold.customer_facts_v2"
