#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb

rocky validate
rocky compile --models models > expected/compile.json
rocky test    --models models > expected/test.json

rocky lineage fct_revenue --models models                           > expected/lineage_fct_revenue.json
rocky lineage fct_revenue --models models --column total            > expected/lineage_total.json
rocky lineage fct_revenue --models models --format dot              > expected/lineage.dot

echo
echo "POC complete: lineage emitted as JSON + DOT for fct_revenue."
