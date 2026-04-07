#!/usr/bin/env bash
set -euo pipefail
: "${ANTHROPIC_API_KEY:?Set ANTHROPIC_API_KEY before running this POC}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected

rocky ai-sync --models models 2>&1 | tee expected/sync.log

echo
echo "POC complete: ai-sync proposed downstream updates."
