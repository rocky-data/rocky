#!/usr/bin/env bash
set -euo pipefail
: "${ANTHROPIC_API_KEY:?Set ANTHROPIC_API_KEY before running this POC}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected tests

rocky ai-test --all --save 2>&1 | tee expected/testgen.log

echo
echo "=== Generated tests ==="
ls tests/ 2>/dev/null || echo "    (no tests/ directory created — see log)"

echo
echo "POC complete: AI test generation invoked."
