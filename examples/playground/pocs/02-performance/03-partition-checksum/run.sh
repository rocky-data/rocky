#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Reset state from previous runs.
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

# Bootstrap raw__orders.orders + the empty marts.fct_daily_orders target.
duckdb poc.duckdb < data/seed.sql

# Sanity check the rocky.toml.
rocky validate

# Compile the model — verifies the time_interval strategy parses, the
# @start_date / @end_date placeholders are present, and the column-shape
# checks pass. The model's `time_column = "order_date"` resolves to type
# Unknown here (raw DuckDB tables aren't declared in source_schemas), so
# Rocky skips E021/E022/E025 — the runtime catches any actual mismatch.
rocky -o json compile --models models/ > expected/compile.json

# ---------------------------------------------------------------------------
# Step 1 — Run partition 2026-04-07 in isolation.
# Expected: 2 output rows (customer 1 with 2 orders → 1 group + customer 2
# with 1 order → 1 group). The other 4 days are NOT touched.
# ---------------------------------------------------------------------------
rocky -c rocky.toml -o json run \
    --filter source=orders \
    --models models/ \
    --partition 2026-04-07 \
    > expected/run-partition-04-07.json

duckdb poc.duckdb -c \
    "SELECT order_date, customer_id, order_count, revenue
     FROM marts.fct_daily_orders
     ORDER BY order_date, customer_id;" \
    > expected/state-after-04-07.txt

# ---------------------------------------------------------------------------
# Step 2 — Backfill the entire range 2026-04-04 → 2026-04-08.
# Expected: 6 output rows total (one per (date, customer) pair across the
# 5 days). 04-07 is recomputed and produces the same 2 rows as Step 1.
# ---------------------------------------------------------------------------
rocky -c rocky.toml -o json run \
    --filter source=orders \
    --models models/ \
    --from 2026-04-04 --to 2026-04-08 \
    > expected/run-backfill.json

duckdb poc.duckdb -c \
    "SELECT order_date, customer_id, order_count, revenue
     FROM marts.fct_daily_orders
     ORDER BY order_date, customer_id;" \
    > expected/state-after-backfill.txt

# ---------------------------------------------------------------------------
# Step 3 — Late-arriving data scenario.
# A new order (customer 3, $999) shows up for 2026-04-07 AFTER the partition
# was already computed. With watermark-incremental this row would be missed
# (its timestamp doesn't advance the watermark beyond what was already seen).
# With time_interval, we just re-run --partition 2026-04-07 and the
# DELETE+INSERT cycle picks it up.
# ---------------------------------------------------------------------------
duckdb poc.duckdb -c \
    "INSERT INTO raw__orders.orders VALUES
        (10, 3, '2026-04-07 22:00:00', 999.0);"

rocky -c rocky.toml -o json run \
    --filter source=orders \
    --models models/ \
    --partition 2026-04-07 \
    > expected/run-late-correction.json

duckdb poc.duckdb -c \
    "SELECT order_date, customer_id, order_count, revenue
     FROM marts.fct_daily_orders
     WHERE order_date = '2026-04-07'
     ORDER BY customer_id;" \
    > expected/state-after-late-correction.txt

# Verify the late row was actually picked up. -csv strips duckdb's box-
# drawing chrome so we get a parseable single value.
late_row_count=$(duckdb -csv poc.duckdb \
    "SELECT COUNT(*) FROM marts.fct_daily_orders
     WHERE order_date = '2026-04-07' AND customer_id = 3;" \
    | tail -1 | tr -d '[:space:]')
if [[ "$late_row_count" != "1" ]]; then
    echo "FAIL: late-arriving row for customer 3 on 2026-04-07 was not picked up"
    echo "      expected 1 row, got: '$late_row_count'"
    exit 1
fi

echo
echo "✓ POC complete: time_interval handled the late correction cleanly."
echo "  - Step 1: ran partition 2026-04-07 in isolation (2 rows)"
echo "  - Step 2: backfilled 2026-04-04 → 2026-04-08 (6 rows total)"
echo "  - Step 3: re-ran 2026-04-07 after late insert → customer 3 picked up"
echo
echo "Inspect expected/*.json and expected/state-*.txt for the captured output."
