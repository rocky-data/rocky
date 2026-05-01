#!/usr/bin/env bash
# Live BigQuery MERGE smoke test.
#
# Verifies the MERGE strategy end-to-end against a real GCP project,
# including:
#   - Bootstrap path on first run (target created from model schema).
#   - `WHEN NOT MATCHED THEN INSERT ROW` (BQ-unique syntax).
#   - Upsert semantics: existing rows updated, new rows inserted,
#     untouched rows unchanged across consecutive runs.
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
DATASET="hc_phase1_live_merge"
PROJ="$GCP_PROJECT_ID"

drop_dataset() {
    bq --location="$LOCATION" --project_id="$PROJ" \
       rm -r -f -d "$DATASET" 2>/dev/null || true
}

drop_dataset
trap drop_dataset EXIT

echo "==> dataset: $PROJ.$DATASET (location: $LOCATION)"
bq --location="$LOCATION" --project_id="$PROJ" mk --dataset "$DATASET"

echo "==> seeding source: customers_src (3 baseline rows)"
bq --project_id="$PROJ" query --use_legacy_sql=false --quiet \
"CREATE TABLE \`${PROJ}\`.\`${DATASET}\`.\`customers_src\` AS
 SELECT id, name, amount FROM UNNEST([
   STRUCT(1 AS id, 'alice'   AS name, 100 AS amount),
   STRUCT(2,        'bob',              200),
   STRUCT(3,        'carol',            300)
 ])" > /dev/null

echo "==> rocky validate"
rocky -c live.rocky.toml validate > /dev/null

echo "==> rocky run (initial — bootstrap + MERGE inserts all 3 rows)"
rocky -c live.rocky.toml run --output json > expected/run-initial.json

EXPECTED_INITIAL='id,name,amount
1,alice,100
2,bob,200
3,carol,300'
ACTUAL="$(bq --project_id="$PROJ" query --use_legacy_sql=false --format=csv --quiet \
    "SELECT id, name, amount FROM \`${PROJ}\`.\`${DATASET}\`.\`customers\` ORDER BY id")"
if [[ "$ACTUAL" != "$EXPECTED_INITIAL" ]]; then
    echo "FAIL: initial materialization did not match"
    echo "expected:"; echo "$EXPECTED_INITIAL"
    echo "actual:";   echo "$ACTUAL"
    exit 1
fi
echo "    initial: 3 rows (alice/bob/carol) — bootstrap + MERGE INSERT path"

echo "==> mutating source: update bob, add dave"
bq --project_id="$PROJ" query --use_legacy_sql=false --quiet \
"CREATE OR REPLACE TABLE \`${PROJ}\`.\`${DATASET}\`.\`customers_src\` AS
 SELECT id, name, amount FROM UNNEST([
   STRUCT(1 AS id, 'alice'   AS name, 100 AS amount),
   STRUCT(2,        'bob',              250),
   STRUCT(3,        'carol',            300),
   STRUCT(4,        'dave',             400)
 ])" > /dev/null

echo "==> rocky run (delta — MERGE UPDATE + INSERT)"
rocky -c live.rocky.toml run --output json > expected/run-delta.json

EXPECTED_DELTA='id,name,amount
1,alice,100
2,bob,250
3,carol,300
4,dave,400'
ACTUAL="$(bq --project_id="$PROJ" query --use_legacy_sql=false --format=csv --quiet \
    "SELECT id, name, amount FROM \`${PROJ}\`.\`${DATASET}\`.\`customers\` ORDER BY id")"
if [[ "$ACTUAL" != "$EXPECTED_DELTA" ]]; then
    echo "FAIL: delta materialization did not match"
    echo "expected:"; echo "$EXPECTED_DELTA"
    echo "actual:";   echo "$ACTUAL"
    exit 1
fi
echo "    delta: bob updated to 250, dave inserted, alice/carol unchanged"

echo
echo "POC complete: BigQuery MERGE strategy verified live."
