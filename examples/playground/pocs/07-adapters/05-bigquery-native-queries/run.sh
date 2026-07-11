#!/usr/bin/env bash
# BigQuery adapter — POC entrypoint.
#
# Two-mode script:
#
#   1. Compile smoke (always runs, no credentials needed). Verifies the
#      BigQuery model frontmatter type-checks against the current rocky
#      binary. Exits 0. Picked up by the credential-free POC sweep
#      (scripts/run-all-duckdb.sh).
#
#   2. Live demo (runs only when BigQuery credentials are present).
#      Materializes a full-refresh transformation against the sandbox,
#      writes the rocky run receipt to expected/run.json, queries the
#      target table back via `bq query` to prove the row landed, and
#      drops the dataset on exit. Prints pointers to the remaining
#      live drivers under live/ so the full tour is one command away.
#
# The live drivers under live/<scenario>/ remain independently runnable
# and continue to fail-fast on missing env vars. This top-level script
# is the orchestration layer that makes the POC smoke-safe.
#
# Env vars (live mode):
#   GCP_PROJECT_ID                  — GCP project to run against
#                                     (BIGQUERY_TEST_PROJECT also accepted)
#   GOOGLE_APPLICATION_CREDENTIALS  — path to SA JSON key (or BIGQUERY_TOKEN)
# Optional:
#   BQ_LOCATION                     — dataset location (default: EU)

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# ---------------------------------------------------------------------------
# Mode 1: compile smoke. Always-on.
# ---------------------------------------------------------------------------
#
# `rocky compile --models live/models/` reads the model SQL + sidecar TOMLs
# directly without going through a rocky.toml, so the BigQuery `${VAR}`
# references in live/live.rocky.toml don't need substitution. Verifies the
# `full_refresh` strategy frontmatter type-checks against the current binary.

echo "==> compile smoke (no credentials required)"
mkdir -p expected
rocky compile --models live/models/ --output json 2>/dev/null > expected/compile.json
echo "    rocky compile: ok"

# Accept either GCP_PROJECT_ID (existing POC convention) or
# BIGQUERY_TEST_PROJECT (live integration-test convention). Default the
# canonical name from the test convention so the rest of the script can
# read GCP_PROJECT_ID uniformly.
PROJECT="${GCP_PROJECT_ID:-${BIGQUERY_TEST_PROJECT:-}}"
CREDS="${GOOGLE_APPLICATION_CREDENTIALS:-${BIGQUERY_TOKEN:-}}"

if [[ -z "$PROJECT" || -z "$CREDS" ]]; then
    echo
    echo "==> live demo: SKIPPED (BigQuery credentials not set)"
    echo "    Set GCP_PROJECT_ID (or BIGQUERY_TEST_PROJECT) plus"
    echo "    GOOGLE_APPLICATION_CREDENTIALS to exercise the live path."
    echo
    echo "POC compile smoke complete."
    exit 0
fi

# Normalize the canonical name so the existing live drivers (which read
# GCP_PROJECT_ID) inherit the right value when chained below.
export GCP_PROJECT_ID="$PROJECT"

# ---------------------------------------------------------------------------
# Mode 2: live demo. Full-refresh run against the sandbox.
# ---------------------------------------------------------------------------
#
# Mirrors live/run.sh but captures the run receipt at the POC root
# (expected/run.json) — that's the user-facing artifact a reader of the
# POC sees first. The follow-up `bq query` proves the row landed in the
# warehouse, not just that the rocky CLI exited 0.

LOCATION="${BQ_LOCATION:-EU}"
DATASET="poc_step4_poc"

drop_dataset() {
    bq --location="$LOCATION" --project_id="$GCP_PROJECT_ID" \
       rm -r -f -d "$DATASET" 2>/dev/null || true
}

drop_dataset
trap drop_dataset EXIT

echo
echo "==> live demo: full-refresh against $GCP_PROJECT_ID.$DATASET (location: $LOCATION)"

# Pre-create the target dataset. `run_transformation` does not honor
# auto_create_schemas (only the replication path does), so the dataset
# must exist before CREATE TABLE runs. See live/README.md finding 3.
bq --location="$LOCATION" --project_id="$GCP_PROJECT_ID" \
   mk --dataset "$DATASET" > /dev/null

# Stage live.rocky.toml + model files into a temp dir with
# `__GCP_PROJECT__` substituted and the dataset name pointed at the
# POC-level dataset (so the POC and the per-driver smoke don't collide).
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"; drop_dataset' EXIT
cp live/live.rocky.toml "$STAGE/"
cp -R live/models "$STAGE/"
find "$STAGE" -type f \( -name "*.toml" -o -name "*.sql" \) \
    -exec sed -i.bak \
        -e "s|__GCP_PROJECT__|${GCP_PROJECT_ID}|g" \
        -e "s|poc_step1_live|${DATASET}|g" \
        {} +
find "$STAGE" -name "*.bak" -delete

echo "==> rocky validate"
rocky -c "$STAGE/live.rocky.toml" validate 2>/dev/null > /dev/null
echo "    validate: ok"

echo "==> rocky run --output json (receipt: expected/run.json)"
ROCKY_SUPPRESS_DEPRECATION=1 \
    rocky -c "$STAGE/live.rocky.toml" run --output json 2>/dev/null \
    > expected/run.json
echo "    run: ok"

echo "==> bq query: verifying the row landed in $DATASET.full_refresh_demo"
ACTUAL_ROWS="$(bq --project_id="$GCP_PROJECT_ID" \
    query --use_legacy_sql=false --format=csv --quiet \
    "SELECT COUNT(*) FROM \`${GCP_PROJECT_ID}\`.\`${DATASET}\`.\`full_refresh_demo\`" \
    | tail -n 1)"

if [[ "$ACTUAL_ROWS" != "1" ]]; then
    echo "FAIL: expected 1 row in target, got '$ACTUAL_ROWS'"
    exit 1
fi
echo "    rows materialized = 1"

# Surface the rocky run receipt summary so a reader can see what the
# JSON output looks like without opening expected/run.json by hand.
echo "==> rocky run receipt (subset)"
python3 - "$HERE/expected/run.json" <<'PY'
import json, sys
out = json.load(open(sys.argv[1]))
mat = out["materializations"][0]
asset = ".".join(mat.get("asset_key", []))
strategy = mat.get("metadata", {}).get("strategy")
print(f"    asset_key       : {asset}")
print(f"    strategy        : {strategy}")
print(f"    duration_ms     : {mat.get('duration_ms')}")
print(f"    bytes_scanned   : {mat.get('bytes_scanned')}")
print(f"    cost_usd        : {mat.get('cost_usd')}")
print(f"    job_ids         : {mat.get('job_ids', [])}")
print(f"    status          : {out.get('status')}")
PY

echo
echo "POC complete: BigQuery full-refresh materialization verified live."
echo "Receipt: $HERE/expected/run.json"
echo
echo "For the full BigQuery surface tour (time-interval, merge, discover,"
echo "drift, cost cross-check), run each driver under live/:"
echo "    ./live/run.sh"
echo "    ./live/time-interval/run.sh"
echo "    ./live/merge/run.sh"
echo "    ./live/discover/run.sh"
echo "    ./live/drift/run.sh"
echo "    ./live/cost-cross-check/run.sh"
