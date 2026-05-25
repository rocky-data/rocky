#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

duckdb poc.duckdb < data/seed_initial.sql
echo "=== Source after seed_initial: $(duckdb poc.duckdb 'SELECT COUNT(*) FROM raw__events.events' -noheader -list) rows ==="

rocky validate
echo
target_rows() { duckdb poc.duckdb 'SELECT COUNT(*) FROM staging__events.events' -noheader -list; }

echo "=== Run 1 (initial) ==="
rocky -c rocky.toml -o json run --filter source=events > expected/run1.json
run1_rows=$(target_rows)
echo "    target rows: $run1_rows"
[ "$run1_rows" = "500" ] || { echo "FAIL: expected 500 target rows after the initial run, got $run1_rows" >&2; exit 1; }

echo "=== State after run 1 ==="
rocky state 2>&1 | tail -10

echo
echo "=== Append delta (25 new rows, later timestamps) ==="
duckdb poc.duckdb < data/seed_delta.sql
echo "    source rows: $(duckdb poc.duckdb 'SELECT COUNT(*) FROM raw__events.events' -noheader -list)"

echo
echo "=== Run 2 (incremental) ==="
rocky -c rocky.toml -o json run --filter source=events > expected/run2.json
run2_rows=$(target_rows)
echo "    target rows: $run2_rows"

echo "=== State after run 2 ==="
rocky state 2>&1 | tail -10

echo
# The incremental run must append the 25 delta rows to the 500 already loaded.
# Asserted, not just echoed: a bootstrap watermark of wall-clock time would
# filter the delta out and leave this at 500 silently. This POC now fails loudly
# if that regresses.
[ "$run2_rows" = "525" ] || { echo "FAIL: expected 525 target rows after the incremental run, got $run2_rows (delta not copied)" >&2; exit 1; }
echo "POC complete: incremental appended the 25 delta rows (target now 525)."
