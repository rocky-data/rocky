#!/usr/bin/env bash
# 01-data-contracts-strict — every contract rule + a deliberately broken model
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb

echo "=== Compile ALL models with contracts ==="
echo "    (broken_metrics is expected to fail — that's the point of this POC)"
rocky compile --models models --contracts contracts > expected/compile.json
exit_code=$?
echo "    rocky compile exit code: $exit_code"
echo

echo "=== Diagnostic codes raised ==="
codes=$(grep -oE 'E01[0-9]' expected/compile.json | sort -u | tr '\n' ' ')
echo "    ${codes}"
echo

expected_codes="E010 E012 E013 "
if [ "$codes" != "$expected_codes" ]; then
    echo "ERROR: expected exactly '${expected_codes}' but got '${codes}'"
    exit 1
fi

echo "POC complete: good_metrics passes; broken_metrics raises E010 (required),"
echo "E012 (nullability), and E013 (protected). E011 (type mismatch) needs a live"
echo "warehouse to infer output types, so it is not surfaced on this compile path."
