#!/usr/bin/env bash
# Live BigQuery drift smoke test — first end-to-end replication-from-BQ.
#
# Runs `rocky run` twice:
#   1. Initial: replicates a 2-column source table to the target.
#   2. After ALTER source ADD COLUMN: re-runs and verifies drift was
#      detected + the target reflects the new column.
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

echo "==> rocky run (initial — replicates 2-column source to target)"
rocky -c live.rocky.toml run --output json > expected/run-initial.json

ROW_COUNT="$(bq --project_id="$PROJ" query --use_legacy_sql=false --format=csv --quiet \
    "SELECT COUNT(*) FROM \`${PROJ}\`.\`${TGT_DS}\`.\`customers\`" | tail -n 1)"
if [[ "$ROW_COUNT" != "3" ]]; then
    echo "FAIL: expected 3 rows in target after initial replication, got $ROW_COUNT"
    exit 1
fi
echo "    target replicated, 3 rows"

echo "==> mutating source: change id from INT64 to STRING (unsafe widening)"
# BigQuery doesn't support ALTER COLUMN TYPE for type swaps, so we DROP +
# CREATE the source. The new schema differs from the target by the `id`
# column type — which Rocky's drift detection should classify as
# DropAndRecreate (no safe widening from STRING to INT64 either way).
bq --project_id="$PROJ" query --use_legacy_sql=false --quiet \
   "DROP TABLE \`${PROJ}\`.\`${SRC_DS}\`.\`customers\`" > /dev/null
bq --project_id="$PROJ" query --use_legacy_sql=false --quiet \
   "CREATE TABLE \`${PROJ}\`.\`${SRC_DS}\`.\`customers\` AS
    SELECT id, name, _updated_at FROM UNNEST([
      STRUCT('1' AS id, 'alice' AS name, TIMESTAMP '2026-04-02 00:00:00' AS _updated_at),
      STRUCT('2',         'bob',           TIMESTAMP '2026-04-02 00:00:00'),
      STRUCT('3',         'carol',         TIMESTAMP '2026-04-02 00:00:00'),
      STRUCT('4',         'dave',          TIMESTAMP '2026-04-02 00:00:00')
    ])" > /dev/null

echo "==> rocky run (post-drift — should detect type change + drop_and_recreate)"
rocky -c live.rocky.toml run --output json > expected/run-drift.json

echo "==> verifying drift detected in JSON output"
python3 - "$HERE/expected/run-initial.json" "$HERE/expected/run-drift.json" <<'PY'
import json, sys

initial = json.load(open(sys.argv[1]))
drift   = json.load(open(sys.argv[2]))

# Initial run: target was just created from source, so no drift yet.
init_drift = initial.get("drift", {})
if init_drift.get("tables_drifted", -1) != 0:
    print(f"FAIL: initial run reported drift unexpectedly: {init_drift}")
    sys.exit(1)
print(f"    initial drift summary: tables_checked={init_drift.get('tables_checked')}, tables_drifted=0")

# Second run: id type changed INT64 → STRING; drift must classify
# as drop_and_recreate (not a safe widening either way).
d = drift.get("drift", {})
if d.get("tables_drifted", 0) < 1:
    print(f"FAIL: post-mutation drift not detected: {d}")
    sys.exit(1)
actions = d.get("actions_taken", [])
if not any(a.get("action") == "drop_and_recreate" for a in actions):
    print(f"FAIL: expected drop_and_recreate action, got {actions}")
    sys.exit(1)
print(f"    drift run summary: tables_checked={d.get('tables_checked')}, tables_drifted={d.get('tables_drifted')}")
for a in actions:
    print(f"    action: table={a.get('table')} action={a.get('action')} reason={a.get('reason')}")
PY

echo "==> verifying target reflects new id type (STRING)"
ID_TYPE="$(bq --project_id="$PROJ" query --use_legacy_sql=false --format=csv --quiet \
    "SELECT data_type FROM \`${PROJ}\`.\`${TGT_DS}\`.INFORMATION_SCHEMA.COLUMNS \
     WHERE table_name = 'customers' AND column_name = 'id'" | tail -n 1)"
if [[ "$ID_TYPE" != "STRING" ]]; then
    echo "FAIL: expected target id type STRING after drop_and_recreate, got '$ID_TYPE'"
    exit 1
fi
echo "    target customers.id is now STRING (drop_and_recreate took effect)"

echo
echo "POC complete: BigQuery drift detection verified live."
