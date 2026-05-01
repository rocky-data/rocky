#!/usr/bin/env bash
# Live BigQuery discovery smoke test.
#
# Runs `rocky discover --output json` against a real GCP project,
# proving the BigQueryDiscoveryAdapter wires up correctly through the
# CLI registry — the same path replication-from-BQ pipelines use.
#
# Required env:
#   GCP_PROJECT_ID                  — GCP project to run against
#   GOOGLE_APPLICATION_CREDENTIALS  — path to SA JSON key (or BIGQUERY_TOKEN)
# Optional:
#   BQ_LOCATION                     — dataset location (default: EU)

set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID before running this live smoke test}"
: "${GOOGLE_APPLICATION_CREDENTIALS:?Set GOOGLE_APPLICATION_CREDENTIALS or BIGQUERY_TOKEN}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected

LOCATION="${BQ_LOCATION:-EU}"
PROJ="$GCP_PROJECT_ID"
DS_ALPHA="hc_phase14_disc_alpha"
DS_BETA="hc_phase14_disc_beta"

drop_datasets() {
    bq --location="$LOCATION" --project_id="$PROJ" \
       rm -r -f -d "$DS_ALPHA" 2>/dev/null || true
    bq --location="$LOCATION" --project_id="$PROJ" \
       rm -r -f -d "$DS_BETA" 2>/dev/null || true
}

drop_datasets
trap drop_datasets EXIT

echo "==> seeding two datasets matching prefix 'hc_phase14_disc_'"
bq --location="$LOCATION" --project_id="$PROJ" mk --dataset "$DS_ALPHA" > /dev/null
bq --location="$LOCATION" --project_id="$PROJ" mk --dataset "$DS_BETA" > /dev/null
bq --project_id="$PROJ" query --use_legacy_sql=false --quiet \
   "CREATE TABLE \`${PROJ}\`.\`${DS_ALPHA}\`.\`orders\` AS SELECT 1 AS id" > /dev/null
bq --project_id="$PROJ" query --use_legacy_sql=false --quiet \
   "CREATE TABLE \`${PROJ}\`.\`${DS_BETA}\`.\`customers\` AS SELECT 1 AS id" > /dev/null

echo "==> rocky discover --output json"
rocky -c live.rocky.toml discover --output json > expected/discover.json

echo "==> verifying both datasets surfaced"
python3 - "$HERE/expected/discover.json" "$DS_ALPHA" "$DS_BETA" <<'PY'
import json, sys
out = json.load(open(sys.argv[1]))
expected_schemas = set(sys.argv[2:])
sources = out.get("sources", [])
schemas = {s.get("components", {}).get("source") for s in sources if s.get("components")}
# `source` component value is whatever the schema_pattern parser
# extracted — it'll be the suffix after the prefix (e.g. "alpha").
expected_components = {s.removeprefix("hc_phase14_disc_") for s in expected_schemas}
if not expected_components.issubset(schemas):
    print(f"FAIL: expected components {expected_components} not all in {schemas}")
    sys.exit(1)
print(f"    sources discovered: {sorted(schemas)}")
PY

echo
echo "POC complete: BigQuery discovery verified live."
