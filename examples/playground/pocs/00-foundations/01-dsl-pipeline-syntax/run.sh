#!/usr/bin/env bash
# 01-dsl-pipeline-syntax — DSL operator demo
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

mkdir -p expected
rm -f .rocky-state.redb

rocky validate
rocky compile --models models > expected/compile.json
rocky test    --models models > expected/test.json
rocky lineage top_customers --models models > expected/lineage.json

echo "POC complete: DSL pipeline compiles, tests pass, lineage traced."
