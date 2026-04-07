#!/usr/bin/env bash
set -euo pipefail
: "${ANTHROPIC_API_KEY:?Set ANTHROPIC_API_KEY before running this POC}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected

# Generate a model from natural language. Rocky compiles + retries if it doesn't parse.
rocky ai "monthly revenue per customer from raw_orders, only completed orders" --format rocky 2>&1 | tee expected/generation.log

echo
echo "POC complete: AI-generated model produced (see expected/generation.log)."
