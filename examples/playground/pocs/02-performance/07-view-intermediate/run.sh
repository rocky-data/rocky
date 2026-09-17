#!/usr/bin/env bash
# A view as a shared intermediate — end-to-end demo.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

duckdb poc.duckdb < data/seed.sql

rocky validate
rocky -o json compile --models models > expected/compile.json

# Run both models. This is the step the old ephemeral version of this POC
# never took, which is how it stayed green while the strategy did not work.
rocky -o json run --models models > expected/run.json

# What landed in the warehouse: stg_events is a VIEW, user_metrics a TABLE.
duckdb poc.duckdb -c \
    "SELECT table_name, table_type
     FROM information_schema.tables
     WHERE table_schema = 'analytics'
     ORDER BY table_name;" \
    > expected/objects.txt

# The view stores no rows of its own, and the consumer reads it fine.
duckdb poc.duckdb -c \
    "SELECT
        (SELECT COUNT(*) FROM analytics.stg_events)   AS view_rows,
        (SELECT COUNT(*) FROM analytics.user_metrics) AS metric_rows;" \
    > expected/counts.txt

echo
echo "=== Objects in poc.analytics ==="
cat expected/objects.txt
echo
echo "=== Row counts ==="
cat expected/counts.txt

echo
echo "Key point: stg_events is a view. It copies no data, and user_metrics"
echo "reads it like any other relation."
echo
echo "POC complete: a view carried the intermediate, and the run went green."
