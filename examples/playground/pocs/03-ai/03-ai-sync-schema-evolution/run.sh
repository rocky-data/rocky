#!/usr/bin/env bash
set -euo pipefail
: "${ANTHROPIC_API_KEY:?Set ANTHROPIC_API_KEY before running this POC}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected

# ai-sync scans models with an `intent` field and proposes intent-guided
# updates. Upstream schema-change detection is not yet wired in the engine
# (see README) — proposals are driven by declared intent alone.
rocky ai-sync --models models 2>&1 | tee expected/sync.log

echo
echo "POC complete: ai-sync proposed intent-guided downstream updates."
