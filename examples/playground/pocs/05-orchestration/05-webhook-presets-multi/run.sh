#!/usr/bin/env bash
# Multi-preset webhooks — end-to-end demo
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

duckdb poc.duckdb < data/seed.sql

rocky validate

echo "=== Configured webhooks ==="
rocky -c rocky.toml -o json hooks list > expected/hooks_list.json 2>&1 || true
cat expected/hooks_list.json 2>/dev/null | head -40 || echo "(hooks list output)"

echo
echo "=== Webhook presets configured ==="
echo "  on_pipeline_complete  → Slack     (formatted blocks)"
echo "  on_pipeline_error     → Teams     (adaptive card, 3 retries)"
echo "  on_materialize_error  → PagerDuty (incident trigger)"
echo "  on_drift_detected     → Datadog   (event API)"
echo "  on_anomaly_detected   → Generic   (custom JSON template)"

echo
echo "POC complete: 5 webhook presets configured across different event types."
echo "(Webhooks fire during pipeline execution — URLs are placeholders for this demo.)"
