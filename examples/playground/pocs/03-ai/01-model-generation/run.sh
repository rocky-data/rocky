#!/usr/bin/env bash
set -euo pipefail
: "${ANTHROPIC_API_KEY:?Set ANTHROPIC_API_KEY before running this POC}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f models/monthly_revenue.rocky models/monthly_revenue.toml
rm -f models/orders_daily.rocky models/orders_daily.toml

# --- 1. Default full_refresh materialization ----------------------------------
#
# Generate a model from natural language. Rocky compiles + retries if it
# doesn't parse, then writes both the body and a `[strategy]`/`[target]`
# sidecar alongside it under `models/`. The default `--materialization`
# is `full_refresh`.
echo "=== generate (default full_refresh) ==="
rocky ai "monthly revenue per customer from raw_orders, only completed orders" \
    --format rocky \
    --target poc.demo.monthly_revenue \
    --overwrite \
    2>&1 | tee expected/generation.log

echo
echo "==> Generated artifacts (full_refresh):"
ls -1 models/monthly_revenue.* | tee expected/generated_files.txt

echo
echo "==> Sidecar contents (full_refresh):"
cat models/monthly_revenue.toml | tee expected/monthly_revenue.toml.txt

# --- 2. Incremental materialization with --watermark --------------------------
#
# Exercises the v1.26.0 sidecar emission unlock: --materialization=incremental
# requires --watermark and the emitted sidecar carries [strategy].type =
# "incremental" + timestamp_column = <watermark>.
echo
echo "=== generate (incremental + watermark) ==="
rocky ai "daily order facts from raw_orders, watermarked by ordered_at" \
    --format rocky \
    --materialization incremental \
    --watermark ordered_at \
    --target poc.demo.orders_daily \
    --overwrite \
    2>&1 | tee expected/generation-incremental.log

echo
echo "==> Generated artifacts (incremental):"
ls -1 models/orders_daily.* | tee expected/generated_files_incremental.txt

echo
echo "==> Sidecar contents (incremental — note [strategy] timestamp_column):"
cat models/orders_daily.toml | tee expected/orders_daily.toml.txt

echo
echo "POC complete: two pairs of body + sidecar landed under models/."
echo "  - monthly_revenue.{rocky,toml}  — full_refresh"
echo "  - orders_daily.{rocky,toml}     — incremental, watermark = ordered_at"
