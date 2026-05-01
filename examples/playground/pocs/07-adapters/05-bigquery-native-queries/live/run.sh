#!/usr/bin/env bash
# Live BigQuery smoke test — full-refresh materialization end-to-end.
#
# Runs `rocky run` against a real GCP project, asserts the resulting
# table has the expected row count, then drops the target dataset
# regardless of outcome.
#
# Required env:
#   GCP_PROJECT_ID                  — GCP project to run against
#   GOOGLE_APPLICATION_CREDENTIALS  — path to SA JSON key (or set BIGQUERY_TOKEN)
# Optional:
#   BQ_LOCATION                     — dataset location (default: EU)

set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID before running this live smoke test}"
: "${GOOGLE_APPLICATION_CREDENTIALS:?Set GOOGLE_APPLICATION_CREDENTIALS or BIGQUERY_TOKEN}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected

LOCATION="${BQ_LOCATION:-EU}"
DATASET="hc_phase1_live"

drop_dataset() {
    bq --location="$LOCATION" --project_id="$GCP_PROJECT_ID" \
       rm -r -f -d "$DATASET" 2>/dev/null || true
}

# Drop any stale dataset from a previous failed run before starting.
drop_dataset
trap drop_dataset EXIT

echo "==> dataset: $GCP_PROJECT_ID.$DATASET (location: $LOCATION)"

# Pre-create the target dataset. `run_transformation` in the engine
# does not honor `auto_create_schemas` (only the replication path
# does), so the dataset must exist before `CREATE TABLE` runs.
bq --location="$LOCATION" --project_id="$GCP_PROJECT_ID" \
   mk --dataset "$DATASET"

echo "==> rocky validate"
rocky -c live.rocky.toml validate

echo "==> rocky run"
rocky -c live.rocky.toml run --output json > expected/run.json
echo "    exit ok"

echo "==> verifying row count"
ACTUAL_ROWS="$(bq --project_id="$GCP_PROJECT_ID" \
    query --use_legacy_sql=false --format=csv --quiet \
    "SELECT COUNT(*) FROM \`${GCP_PROJECT_ID}\`.\`${DATASET}\`.\`full_refresh_demo\`" \
    | tail -n 1)"

if [[ "$ACTUAL_ROWS" != "1" ]]; then
    echo "FAIL: expected 1 row, got '$ACTUAL_ROWS'"
    exit 1
fi
echo "    rows = 1 (matches model SELECT literal)"

echo
echo "POC complete: BigQuery full-refresh materialization verified live."
