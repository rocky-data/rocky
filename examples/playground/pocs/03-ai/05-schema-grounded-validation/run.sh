#!/usr/bin/env bash
# Arc 5 — schema-grounded AI: every `rocky ai` output is compiler-validated
set -euo pipefail

: "${ANTHROPIC_API_KEY:?Set ANTHROPIC_API_KEY before running this POC}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected

echo "==> 1. Compile existing models so rocky ai can ground the prompt"
rocky compile --models models > expected/compile.json 2>/dev/null

echo "==> 2. Ask rocky ai for a derived model — schema grounding + compile-verify loop"
rocky ai "monthly revenue per customer from raw_orders, only completed orders" \
    --format rocky \
    --models models \
    2>&1 | tee expected/generation.log

echo
echo "==> What happened"
echo "  • ValidationContext: project_models + source_schemas + source_column_info,"
echo "    all drawn from the compiled project."
echo "  • The prompt carried the typed schema of raw_orders so the LLM could"
echo "    not invent columns like 'order_total' or 'currency' that don't exist."
echo "  • If the generated model failed to compile, Rocky fed the compiler"
echo "    errors back to the LLM up to 3 times. A successful return means"
echo "    at least one compile-clean output."
echo
echo "POC complete: AI output is schema-grounded and compiler-gated."
