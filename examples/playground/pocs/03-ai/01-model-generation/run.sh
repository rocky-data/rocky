#!/usr/bin/env bash
set -euo pipefail
: "${ANTHROPIC_API_KEY:?Set ANTHROPIC_API_KEY before running this POC}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f models/monthly_revenue.rocky models/monthly_revenue.toml

# Generate a model from natural language. Rocky compiles + retries if it
# doesn't parse, then writes both the body and a `[strategy]`/`[target]`
# sidecar alongside it under `models/`.
rocky ai "monthly revenue per customer from raw_orders, only completed orders" \
    --format rocky \
    --target poc.demo.monthly_revenue \
    --overwrite \
    2>&1 | tee expected/generation.log

echo
echo "==> Generated artifacts:"
ls -1 models/monthly_revenue.* | tee expected/generated_files.txt

echo
echo "==> Sidecar contents:"
cat models/monthly_revenue.toml | tee expected/monthly_revenue.toml.txt

echo
echo "POC complete: body + sidecar landed under models/, log at expected/generation.log."
