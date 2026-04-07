#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb

rocky validate

echo "=== rocky doctor ==="
rocky -c rocky.toml -o json doctor > expected/doctor.json 2>&1 || true
head -25 expected/doctor.json

echo
echo "=== rocky ci ==="
rocky ci --models models > expected/ci.json 2>&1
head -25 expected/ci.json

echo
echo "POC complete: doctor + ci both produce JSON output. See .github/workflows/rocky-ci.yml for the GHA template."
