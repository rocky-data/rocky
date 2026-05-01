#!/usr/bin/env bash
# Live BigQuery cost cross-check.
#
# Runs a single-statement transformation that scans a real source
# table, then verifies rocky's reported `bytes_scanned` matches the
# `totalBytesBilled` BigQuery itself reports for the same job ID via
# `bq show -j`. This is the receipt that the cost-attribution chain
# (`stats_from_response` → `populate_cost_summary` → `bq show -j`)
# round-trips exact billed bytes, not just an approximation.
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
DATASET="hc_phase21_cost_check"

drop_dataset() {
    bq --location="$LOCATION" --project_id="$PROJ" \
       rm -r -f -d "$DATASET" 2>/dev/null || true
}

drop_dataset
trap drop_dataset EXIT

echo "==> dataset: $PROJ.$DATASET (location: $LOCATION)"
bq --location="$LOCATION" --project_id="$PROJ" mk --dataset "$DATASET"

echo "==> seeding source: orders_src (10 rows)"
bq --project_id="$PROJ" query --use_legacy_sql=false --quiet \
"CREATE TABLE \`${PROJ}\`.\`${DATASET}\`.\`orders_src\` AS
 SELECT customer_id, amount FROM UNNEST([
   STRUCT( 1 AS customer_id, 100 AS amount),
   STRUCT( 1,                 50),
   STRUCT( 2,                200),
   STRUCT( 2,                150),
   STRUCT( 3,                300),
   STRUCT( 3,                400),
   STRUCT( 4,                250),
   STRUCT( 5,                100),
   STRUCT( 5,                 75),
   STRUCT( 6,                500)
 ])" > /dev/null

# Stage live.rocky.toml + model files into a temp dir with
# `__GCP_PROJECT__` substituted (see ../run.sh for rationale).
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"; drop_dataset' EXIT
cp live.rocky.toml "$STAGE/"
cp -R models "$STAGE/"
find "$STAGE" -type f \( -name "*.toml" -o -name "*.sql" \) \
    -exec sed -i.bak "s|__GCP_PROJECT__|${PROJ}|g" {} +
find "$STAGE" -name "*.bak" -delete

echo "==> rocky validate"
rocky -c "$STAGE/live.rocky.toml" validate > /dev/null

echo "==> rocky run --output json"
rocky -c "$STAGE/live.rocky.toml" run --output json > expected/run.json

echo "==> verifying job_ids captured + bytes match BQ console"
python3 - "$HERE/expected/run.json" "$PROJ" <<'PY'
import json, subprocess, sys
out = json.load(open(sys.argv[1]))
project = sys.argv[2]
mat = out["materializations"][0]
job_ids = mat.get("job_ids", [])
rocky_bytes = mat.get("bytes_scanned")
if not job_ids:
    print(f"FAIL: materializations[0].job_ids empty (got {mat!r})")
    sys.exit(1)
if not isinstance(rocky_bytes, int) or rocky_bytes <= 0:
    print(f"FAIL: bytes_scanned not populated (got {rocky_bytes!r})")
    sys.exit(1)
print(f"    rocky bytes_scanned = {rocky_bytes} (across {len(job_ids)} job(s))")

# `bq show -j <id>` reports the same `totalBytesBilled` rocky should be
# reading via `jobs.get`. Sum across all captured jobs and compare.
total_bq_billed = 0
for jid in job_ids:
    res = subprocess.run(
        ["bq", "show", "--format=prettyjson", "--project_id", project, "-j", jid],
        capture_output=True, text=True, check=True,
    )
    data = json.loads(res.stdout)
    billed = int(data.get("statistics", {}).get("query", {}).get("totalBytesBilled", "0"))
    print(f"    bq show -j {jid}: totalBytesBilled = {billed}")
    total_bq_billed += billed

if rocky_bytes != total_bq_billed:
    print(f"FAIL: rocky's bytes_scanned ({rocky_bytes}) != sum of BQ totalBytesBilled ({total_bq_billed})")
    sys.exit(1)
print(f"    rocky bytes_scanned = bq totalBytesBilled = {total_bq_billed}")
PY

echo
echo "POC complete: BigQuery cost cross-check verified live."
