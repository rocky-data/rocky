#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb playground.duckdb

rocky validate
rocky compile --models models > expected/compile.json
rocky test    --models models > expected/test.json

echo "POC complete: window functions compile and execute."
