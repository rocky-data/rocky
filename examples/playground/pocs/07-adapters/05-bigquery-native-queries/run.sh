#!/usr/bin/env bash
# BigQuery adapter — end-to-end demo
set -euo pipefail
: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID before running this POC}"
: "${GOOGLE_APPLICATION_CREDENTIALS:?Set GOOGLE_APPLICATION_CREDENTIALS (path to SA JSON key) or BIGQUERY_TOKEN}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected

rocky validate
rocky compile --models models > expected/compile.json

echo
echo "POC complete: BigQuery adapter config validated and models compiled."
echo "(Live execution requires a GCP project with BigQuery API enabled.)"
