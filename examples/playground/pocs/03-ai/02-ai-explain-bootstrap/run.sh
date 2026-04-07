#!/usr/bin/env bash
set -euo pipefail
: "${ANTHROPIC_API_KEY:?Set ANTHROPIC_API_KEY before running this POC}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected

rocky ai-explain --all --save --models models 2>&1 | tee expected/explain.log

echo
echo "=== Resulting model TOML files ==="
for f in models/*.toml; do
    echo "--- $f ---"
    cat "$f"
done

echo
echo "POC complete: intent fields added to all models."
