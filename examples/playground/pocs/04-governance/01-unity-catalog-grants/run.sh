#!/usr/bin/env bash
set -euo pipefail
: "${DATABRICKS_HOST:?Set DATABRICKS_HOST before running this POC}"
: "${DATABRICKS_TOKEN:?Set DATABRICKS_TOKEN before running this POC}"
: "${DATABRICKS_HTTP_PATH:?Set DATABRICKS_HTTP_PATH before running this POC}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected

rocky validate
rocky -c rocky.toml -o json plan --filter source=orders > expected/plan.json
rocky -c rocky.toml -o json run  --filter source=orders > expected/run.json

echo "POC complete: grants reconciled. See expected/run.json -> permissions block."
