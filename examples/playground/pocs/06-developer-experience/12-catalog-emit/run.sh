#!/usr/bin/env bash
# 12-catalog-emit — emit a column-level catalog from the SemanticGraph
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

rm -rf .rocky-state.redb models/.rocky-state.redb poc.duckdb .rocky/catalog
mkdir -p expected

echo "==> 1. Compile the 3-model DAG (the catalog walks the SemanticGraph; no warehouse needed)"
rocky compile --models models > expected/compile.json

echo "==> 2. Emit the catalog (catalog.json + edges.parquet + assets.parquet)"
rocky catalog --models models --format both > expected/catalog_summary.json
ls -1 .rocky/catalog/

echo
echo "==> First 6 column-lineage edges:"
python3 -c "
import json
with open('.rocky/catalog/catalog.json') as fh:
    cat = json.load(fh)
for e in cat['edges'][:6]:
    print(f\"  {e['source_model']}.{e['source_column']:12s} -> {e['target_model']}.{e['target_column']}\")
print(f\"  ... ({cat['stats']['edge_count']} edges across {cat['stats']['asset_count']} assets, {cat['stats']['column_count']} columns)\")
"

echo
echo "==> Assets and their upstream models:"
python3 -c "
import json
with open('.rocky/catalog/catalog.json') as fh:
    cat = json.load(fh)
for a in cat['assets']:
    cols = ', '.join(c['name'] for c in a['columns'])
    print(f\"  {a['fqn']:30s} cols=[{cols}]  upstream={a['upstream_models']}\")
"

echo
echo "==> Inspecting Parquet companions with DuckDB (any tool that reads Parquet works):"
duckdb -csv :memory: <<'SQL'
SELECT source_model, source_column, target_model, target_column, transform
FROM read_parquet('.rocky/catalog/edges.parquet')
ORDER BY target_model, target_column
LIMIT 6;
SQL

echo
echo "==> 3. Per-column docs: [columns.<name>] sidecar descriptions surface as CatalogColumn.description"
echo "    (fct_revenue.toml documents the projected output columns 'total' and 'orders')"
rocky catalog --models models --format json --output json > expected/catalog_columns.json
jq -r '
  .assets[] | select(.model_name=="fct_revenue") | .columns[]
  | select(.description != null)
  | "  fct_revenue.\(.name): \(.description)"
' expected/catalog_columns.json

echo
echo "==> Assert the description landed on a projected column (a description on a"
echo "    non-projected column is silently dropped, so this proves the key is in sync):"
desc="$(jq -r '
  .assets[] | select(.model_name=="fct_revenue") | .columns[]
  | select(.name=="total") | .description
' expected/catalog_columns.json)"
if [ -n "$desc" ] && [ "$desc" != "null" ]; then
  echo "  OK: fct_revenue.total.description = \"$desc\""
else
  echo "  FAIL: fct_revenue.total.description is missing or null" >&2
  exit 1
fi

echo
echo "POC complete: catalog.json + edges.parquet + assets.parquet under .rocky/catalog/."
echo "(Run \`rocky run\` against a transformation pipeline first to populate state-store"
echo " enrichment — \`last_run_id\` and \`last_materialized_at\` per asset.)"
