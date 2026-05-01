#!/usr/bin/env bash
# Live BigQuery time-interval smoke test.
#
# Runs `rocky run --partition KEY` for two consecutive day partitions
# against a real GCP project, exercising the 4-statement DML
# transaction (BEGIN TRANSACTION; DELETE; INSERT; COMMIT TRANSACTION)
# the BigQuery dialect emits for time-interval models. Verifies that
# both partitions materialize the expected aggregated row counts.
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
DATASET="hc_phase1_live_ti"

drop_dataset() {
    bq --location="$LOCATION" --project_id="$GCP_PROJECT_ID" \
       rm -r -f -d "$DATASET" 2>/dev/null || true
}

# Drop any stale dataset from a previous failed run before starting.
drop_dataset
trap drop_dataset EXIT

echo "==> dataset: $GCP_PROJECT_ID.$DATASET (location: $LOCATION)"

# Pre-create the target dataset. `run_transformation` doesn't honor
# `auto_create_schemas` today (see ../README.md), so the dataset must
# exist before CREATE TABLE / DML transactions run.
bq --location="$LOCATION" --project_id="$GCP_PROJECT_ID" \
   mk --dataset "$DATASET"

echo "==> seeding source: orders_src (5 rows across 2 days)"
bq --project_id="$GCP_PROJECT_ID" query --use_legacy_sql=false --quiet \
"CREATE TABLE \`${GCP_PROJECT_ID}\`.\`${DATASET}\`.\`orders_src\` AS
 SELECT order_at, customer_id, amount FROM UNNEST([
   STRUCT(TIMESTAMP '2026-04-01 09:00:00' AS order_at, 1 AS customer_id, 100 AS amount),
   STRUCT(TIMESTAMP '2026-04-01 14:00:00',             1,                 50),
   STRUCT(TIMESTAMP '2026-04-01 19:00:00',             2,                 75),
   STRUCT(TIMESTAMP '2026-04-02 08:00:00',             1,                200),
   STRUCT(TIMESTAMP '2026-04-02 16:00:00',             3,                300)
 ])" > /dev/null

# Stage live.rocky.toml + model files into a temp dir with
# `__GCP_PROJECT__` substituted (see ../run.sh for rationale).
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"; drop_dataset' EXIT
cp live.rocky.toml "$STAGE/"
cp -R models "$STAGE/"
find "$STAGE" -type f \( -name "*.toml" -o -name "*.sql" \) \
    -exec sed -i.bak "s|__GCP_PROJECT__|${GCP_PROJECT_ID}|g" {} +
find "$STAGE" -name "*.bak" -delete

echo "==> rocky validate"
rocky -c "$STAGE/live.rocky.toml" validate > /dev/null

echo "==> rocky run --partition 2026-04-01"
rocky -c "$STAGE/live.rocky.toml" run --partition 2026-04-01 --output json \
    > expected/run-2026-04-01.json

echo "==> rocky run --partition 2026-04-02"
rocky -c "$STAGE/live.rocky.toml" run --partition 2026-04-02 --output json \
    > expected/run-2026-04-02.json

echo "==> verifying materialized output"
RESULT="$(bq --project_id="$GCP_PROJECT_ID" \
    query --use_legacy_sql=false --format=csv --quiet \
    "SELECT order_day, SUM(order_count) AS o, SUM(revenue) AS r
     FROM \`${GCP_PROJECT_ID}\`.\`${DATASET}\`.\`orders_daily\`
     GROUP BY order_day
     ORDER BY order_day")"

EXPECTED='order_day,o,r
2026-04-01 00:00:00,3,225
2026-04-02 00:00:00,2,500'

if [[ "$RESULT" != "$EXPECTED" ]]; then
    echo "FAIL: materialized output did not match"
    echo "expected:"
    echo "$EXPECTED"
    echo "actual:"
    echo "$RESULT"
    exit 1
fi
echo "    2026-04-01: 3 orders, revenue=225"
echo "    2026-04-02: 2 orders, revenue=500"

echo "==> verifying cost attribution reports billed bytes (with 10MB floor)"
python3 - "$HERE/expected/run-2026-04-02.json" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    out = json.load(f)
mat = out["materializations"][0]
bs = mat.get("bytes_scanned")
cu = mat.get("cost_usd")
# The time-interval source seeds 5 rows; well under 10MB. After the
# jobs.get enrichment lands, `bytes_scanned` reflects
# `totalBytesBilled` — which BigQuery floors at 10 MB per query.
# A figure smaller than that means the connector fell back to the
# bare `totalBytesProcessed` from the sync `jobs.query` response.
MIN_BILL = 10 * 1024 * 1024  # 10 MiB
if not isinstance(bs, int) or bs < MIN_BILL:
    print(f"FAIL: bytes_scanned ({bs!r}) below 10MB minimum bill — "
          f"connector likely fell back to totalBytesProcessed")
    sys.exit(1)
if not isinstance(cu, (int, float)) or cu <= 0:
    print(f"FAIL: cost_usd not populated (got {cu!r})")
    sys.exit(1)
print(f"    bytes_scanned = {bs} (>= 10MB floor), cost_usd = {cu}")
PY

echo
echo "POC complete: BigQuery time-interval DML transaction verified live."
