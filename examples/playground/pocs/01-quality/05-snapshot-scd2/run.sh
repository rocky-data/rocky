#!/usr/bin/env bash
# Snapshot SCD-2 — end-to-end demo
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

# Load initial customer data
duckdb poc.duckdb < data/seed_v1.sql
echo "=== Source v1: $(duckdb poc.duckdb 'SELECT COUNT(*) FROM raw__crm.customers' -noheader -list) customers ==="

# Validate the snapshot pipeline
rocky validate

# Run 1: initial snapshot — all rows are new, valid_from = updated_at, valid_to = NULL.
# NOTE: on the local DuckDB path the engine currently emits invalid snapshot MERGE
# SQL, so `rocky run` exits non-zero and the history table stays empty. `|| true`
# keeps the demo walking the full validate → run → inspect flow; see README's
# "Known limitation" section and expected/run1.json for the underlying DuckDB errors.
echo
echo "=== Snapshot run 1 (initial) ==="
rocky -c rocky.toml -o json run > expected/run1.json 2>&1 || true
echo "    history rows: $(duckdb poc.duckdb 'SELECT COUNT(*) FROM snapshots.customers_history' -noheader -list 2>/dev/null || echo 'table not yet created')"

# Simulate changes: Alice upgraded, Charlie deleted, Dave added
duckdb poc.duckdb < data/seed_v2.sql
echo
echo "=== Source v2: Alice upgraded, Charlie deleted, Dave added ==="
echo "    source rows: $(duckdb poc.duckdb 'SELECT COUNT(*) FROM raw__crm.customers' -noheader -list)"

# Run 2: changed rows get new records; old Alice record gets valid_to set;
#         Charlie's record gets valid_to (hard delete invalidation)
echo
echo "=== Snapshot run 2 (changes detected) ==="
rocky -c rocky.toml -o json run > expected/run2.json 2>&1 || true

echo
echo "=== History table ==="
duckdb poc.duckdb "SELECT * FROM snapshots.customers_history ORDER BY customer_id, valid_from" 2>/dev/null || echo "(history table empty — snapshot MERGE SQL is invalid on the local DuckDB path; see README)"

echo
echo "POC complete: snapshot SCD-2 pipeline configured and validated."
