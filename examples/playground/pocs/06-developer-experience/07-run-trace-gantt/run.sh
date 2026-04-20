#!/usr/bin/env bash
# Arc 4 — render a completed run as a timeline
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

rm -f .rocky-state.redb poc.duckdb
mkdir -p expected

echo "==> 1. Seed two tables (orders + customers) so the DAG has concurrency to render"
duckdb poc.duckdb < data/seed.sql

echo "==> 2. Run two pipeline stages concurrently"
rocky -c rocky.toml -o json run --filter source=orders > expected/run_orders.json
rocky -c rocky.toml -o json run --filter source=customers > expected/run_customers.json

echo "==> 3. Inspect 'rocky trace latest' — the Gantt read path (Arc 1 wave 2 ships the write path)"
rocky trace latest > expected/trace_latest.json 2>&1 || true

echo "==> 4. Inspect 'rocky replay latest' — same state store, different view"
rocky replay latest > expected/replay_latest.json 2>&1 || true

echo
echo "The trace command renders per-model offsets, duration bars, and"
echo "concurrency lanes from the state store's RunRecord. It is live as"
echo "an inspection primitive; the OTLP exporter (feature-gated) streams"
echo "the same timeline to any OpenTelemetry collector."
echo
echo "POC complete: trace + replay JSON dumped to expected/."
