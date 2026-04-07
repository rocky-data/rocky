#!/usr/bin/env bash
set -euo pipefail
: "${DATABRICKS_HOST:?Set DATABRICKS_HOST before running this POC}"
: "${DATABRICKS_TOKEN:?Set DATABRICKS_TOKEN before running this POC}"
: "${FIVETRAN_API_KEY:?Set FIVETRAN_API_KEY before running this POC}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected

rocky validate
rocky -c rocky.toml -o json discover > expected/discover.json
rocky -c rocky.toml -o json plan --filter tenant=acme > expected/plan_acme.json

echo "POC complete: discovered + planned for tenant=acme."
