#!/usr/bin/env bash
set -euo pipefail
: "${FIVETRAN_API_KEY:?Set FIVETRAN_API_KEY before running this POC}"
: "${FIVETRAN_API_SECRET:?Set FIVETRAN_API_SECRET before running this POC}"
: "${FIVETRAN_DESTINATION_ID:?Set FIVETRAN_DESTINATION_ID before running this POC}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected

rocky validate
rocky -c rocky.toml -o json discover > expected/discover.json

echo
echo "POC complete: discovered $(jq '.sources | length' expected/discover.json) Fivetran connectors."
