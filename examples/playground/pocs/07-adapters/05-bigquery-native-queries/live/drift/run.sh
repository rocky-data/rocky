#!/usr/bin/env bash
# Live BigQuery drift smoke test — three-stage replication-from-BQ.
#
# Runs `rocky run` three times against the same source/target pair:
#   1. Initial: replicates a 3-column source to the target. No drift.
#   2. After ALTER source ADD COLUMN: re-runs and verifies the
#      `add_columns` drift action fired and the target gained the new
#      column (no full refresh — historical rows stay intact).
#   3. After DROP+CREATE source with INT64→STRING column type change:
#      re-runs and verifies the `drop_and_recreate` action fired and
#      the target's column type updated.
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
SRC_DS="hc_phase13_drift_src_orders"
TGT_DS="hc_phase13_drift_tgt_orders"

drop_datasets() {
    bq --location="$LOCATION" --project_id="$PROJ" \
       rm -r -f -d "$SRC_DS" 2>/dev/null || true
    bq --location="$LOCATION" --project_id="$PROJ" \
       rm -r -f -d "$TGT_DS" 2>/dev/null || true
}

drop_datasets
trap drop_datasets EXIT

echo "==> seeding source: $SRC_DS.customers (3-column schema with watermark)"
bq --location="$LOCATION" --project_id="$PROJ" mk --dataset "$SRC_DS" > /dev/null
bq --project_id="$PROJ" query --use_legacy_sql=false --quiet \
   "CREATE TABLE \`${PROJ}\`.\`${SRC_DS}\`.\`customers\` AS
    SELECT id, name, _updated_at FROM UNNEST([
      STRUCT(1 AS id, 'alice' AS name, TIMESTAMP '2026-04-01 00:00:00' AS _updated_at),
      STRUCT(2,        'bob',           TIMESTAMP '2026-04-01 00:00:00'),
      STRUCT(3,        'carol',         TIMESTAMP '2026-04-01 00:00:00')
    ])" > /dev/null

echo "==> stage 1: rocky run (initial — replicates 3-column source to target)"
rocky -c live.rocky.toml run --output json > expected/run-initial.json

ROW_COUNT="$(bq --project_id="$PROJ" query --use_legacy_sql=false --format=csv --quiet \
    "SELECT COUNT(*) FROM \`${PROJ}\`.\`${TGT_DS}\`.\`customers\`" | tail -n 1)"
if [[ "$ROW_COUNT" != "3" ]]; then
    echo "FAIL: expected 3 rows in target after initial replication, got $ROW_COUNT"
    exit 1
fi
echo "    target replicated, 3 rows"

echo "==> stage 2: ALTER source ADD COLUMN region STRING; backfill"
bq --project_id="$PROJ" query --use_legacy_sql=false --quiet \
   "ALTER TABLE \`${PROJ}\`.\`${SRC_DS}\`.\`customers\` ADD COLUMN region STRING" > /dev/null
bq --project_id="$PROJ" query --use_legacy_sql=false --quiet \
   "UPDATE \`${PROJ}\`.\`${SRC_DS}\`.\`customers\`
    SET region = CASE id WHEN 1 THEN 'EU' WHEN 2 THEN 'US' ELSE 'APAC' END,
        _updated_at = TIMESTAMP '2026-04-02 00:00:00'
    WHERE id > 0" > /dev/null

echo "==> rocky run (post-add — should detect added column, ALTER target)"
rocky -c live.rocky.toml run --output json > expected/run-add-col.json

echo "==> stage 3: DROP+CREATE source with INT64→STRING id type change"
bq --project_id="$PROJ" query --use_legacy_sql=false --quiet \
   "DROP TABLE \`${PROJ}\`.\`${SRC_DS}\`.\`customers\`" > /dev/null
bq --project_id="$PROJ" query --use_legacy_sql=false --quiet \
   "CREATE TABLE \`${PROJ}\`.\`${SRC_DS}\`.\`customers\` AS
    SELECT id, name, _updated_at, region FROM UNNEST([
      STRUCT('1' AS id, 'alice' AS name,
             TIMESTAMP '2026-04-03 00:00:00' AS _updated_at, 'EU' AS region),
      STRUCT('2',         'bob',           TIMESTAMP '2026-04-03 00:00:00', 'US'),
      STRUCT('3',         'carol',         TIMESTAMP '2026-04-03 00:00:00', 'APAC'),
      STRUCT('4',         'dave',          TIMESTAMP '2026-04-03 00:00:00', 'EU')
    ])" > /dev/null

echo "==> rocky run (post-type-change — should detect drop_and_recreate)"
rocky -c live.rocky.toml run --output json > expected/run-type-change.json

echo "==> verifying drift JSON output across all three runs"
python3 - "$HERE/expected/run-initial.json" \
        "$HERE/expected/run-add-col.json" \
        "$HERE/expected/run-type-change.json" <<'PY'
import json, sys

initial      = json.load(open(sys.argv[1]))
add_col      = json.load(open(sys.argv[2]))
type_change  = json.load(open(sys.argv[3]))

# Stage 1: target was just created from source, no drift.
d = initial.get("drift", {})
if d.get("tables_drifted", -1) != 0:
    print(f"FAIL: initial run reported drift unexpectedly: {d}")
    sys.exit(1)
print(f"    stage 1 drift: tables_checked={d.get('tables_checked')}, tables_drifted=0 OK")

# Stage 2: source added `region`, drift must report `add_columns`.
d = add_col.get("drift", {})
actions = d.get("actions_taken", [])
if d.get("tables_drifted", 0) < 1 or not any(a.get("action") == "add_columns" for a in actions):
    print(f"FAIL: expected add_columns action after ALTER ADD, got {d}")
    sys.exit(1)
print(f"    stage 2 drift: action=add_columns OK ({[a.get('reason') for a in actions]})")

# Stage 3: id type INT64→STRING — drop_and_recreate.
d = type_change.get("drift", {})
actions = d.get("actions_taken", [])
if d.get("tables_drifted", 0) < 1 or not any(a.get("action") == "drop_and_recreate" for a in actions):
    print(f"FAIL: expected drop_and_recreate action after type change, got {d}")
    sys.exit(1)
print(f"    stage 3 drift: action=drop_and_recreate OK ({[a.get('reason') for a in actions]})")
PY

echo "==> verifying target schema reflects all mutations"
COLS="$(bq --project_id="$PROJ" query --use_legacy_sql=false --format=csv --quiet \
    "SELECT column_name FROM \`${PROJ}\`.\`${TGT_DS}\`.INFORMATION_SCHEMA.COLUMNS \
     WHERE table_name = 'customers' ORDER BY ordinal_position" | tail -n +2 | tr '\n' ',' | sed 's/,$//')"
if [[ "$COLS" != *"region"* ]]; then
    echo "FAIL: target column list missing 'region': $COLS"
    exit 1
fi
ID_TYPE="$(bq --project_id="$PROJ" query --use_legacy_sql=false --format=csv --quiet \
    "SELECT data_type FROM \`${PROJ}\`.\`${TGT_DS}\`.INFORMATION_SCHEMA.COLUMNS \
     WHERE table_name = 'customers' AND column_name = 'id'" | tail -n 1)"
if [[ "$ID_TYPE" != "STRING" ]]; then
    echo "FAIL: expected target id type STRING after drop_and_recreate, got '$ID_TYPE'"
    exit 1
fi
echo "    target columns: $COLS"
echo "    target customers.id is now STRING (drop_and_recreate took effect)"

echo
echo "POC complete: BigQuery drift detection (add + type change) verified live."
