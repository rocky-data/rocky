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

# Column-level traces: `amount` dead-ends at stg_orders, `status` propagates on to fct_status_summary.
rocky lineage stg_orders --models models --column amount  > expected/lineage_stg_orders_amount.json
rocky lineage stg_orders --models models --column status  > expected/lineage_stg_orders_status.json

echo "POC complete: column-level lineage emitted for branching DAG."
