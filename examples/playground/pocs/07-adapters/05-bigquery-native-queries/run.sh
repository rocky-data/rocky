#!/usr/bin/env bash
# BigQuery adapter — end-to-end live tour.
#
# Runs each live driver under `live/<scenario>/` in sequence against a
# real GCP project. Six scenarios cover every BigQuery-specific surface
# Rocky exercises:
#
#   live/                     full-refresh CTAS
#   live/time-interval/       BEGIN/DELETE/INSERT/COMMIT script per partition
#   live/merge/               WHEN NOT MATCHED THEN INSERT ROW + first-run bootstrap
#   live/discover/            BigQueryDiscoveryAdapter via INFORMATION_SCHEMA.SCHEMATA
#   live/drift/               replication-from-BQ + per-table drift detection
#                             (add_columns / drop_and_recreate / alter_column_types)
#   live/cost-cross-check/    bytes_scanned == bq show -j totalBytesBilled
#
# Each driver creates + drops its own `hc_phase*_*` dataset so cleanup is
# scoped per-driver. Total runtime ~3–5 minutes against the EU sandbox.
#
# Required env:
#   GCP_PROJECT_ID                  — GCP project to run against
#   GOOGLE_APPLICATION_CREDENTIALS  — path to SA JSON key (or BIGQUERY_TOKEN)
# Optional:
#   BQ_LOCATION                     — dataset location (default: EU)

set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID before running this POC}"
: "${GOOGLE_APPLICATION_CREDENTIALS:?Set GOOGLE_APPLICATION_CREDENTIALS or BIGQUERY_TOKEN}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

DRIVERS=(
    "live/run.sh                  full-refresh"
    "live/time-interval/run.sh    time-interval"
    "live/merge/run.sh            merge"
    "live/discover/run.sh         discover"
    "live/drift/run.sh            drift (incremental + ALTER)"
    "live/cost-cross-check/run.sh cost cross-check"
)

for entry in "${DRIVERS[@]}"; do
    # Split on first whitespace into path + label.
    path="${entry%% *}"
    label="${entry#* }"
    label="${label#"${label%%[![:space:]]*}"}"
    echo
    echo "============================================================"
    echo "==> $label  ($path)"
    echo "============================================================"
    "$HERE/$path"
done

echo
echo "POC complete: BigQuery adapter exercised end-to-end across $(echo "${#DRIVERS[@]}") scenarios."
