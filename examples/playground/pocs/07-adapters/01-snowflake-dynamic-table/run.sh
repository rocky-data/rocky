#!/usr/bin/env bash
set -euo pipefail
: "${SNOWFLAKE_ACCOUNT:?Set SNOWFLAKE_ACCOUNT before running this POC}"
: "${SNOWFLAKE_USERNAME:?Set SNOWFLAKE_USERNAME before running this POC}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected

rocky validate
rocky compile --models models > expected/compile.json

echo
echo "POC complete: dynamic_table strategy parsed; live execution requires the Snowflake adapter beta."
