#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb

rocky validate
rocky compile --models models > expected/compile.json
rocky test    --models models > expected/test.json

rocky lineage stg_orders          --models models > expected/lineage_stg_orders.json
rocky lineage fct_status_summary  --models models > expected/lineage_fct_status_summary.json
rocky lineage fct_status_summary  --models models --format dot > expected/lineage.dot

echo "POC complete: column-level lineage emitted for branching DAG."
