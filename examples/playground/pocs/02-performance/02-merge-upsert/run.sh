#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb

rocky validate
rocky compile --models models > expected/compile.json
rocky test --models models > expected/test.json

echo "POC complete: merge strategy parsed and models compiled."
echo "(Local DuckDB executor uses CREATE OR REPLACE TABLE for now; see README.)"
