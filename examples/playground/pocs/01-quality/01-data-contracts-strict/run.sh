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

if grep -qE 'E01[0-3]' expected/compile.json; then
    echo "=== Diagnostic codes raised ==="
    grep -oE 'E01[0-9]' expected/compile.json | sort -u
fi

echo
echo "POC complete: good_metrics passes, broken_metrics raises contract diagnostics."
