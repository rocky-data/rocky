#!/usr/bin/env bash
# 13-surrogate-keys — inject a deterministic, dbt_utils-compatible surrogate
# key via a [[surrogate_key]] sidecar block, then prove the hash column is
# present and populated in the real materialized table.
set -euo pipefail

export ROCKY_SUPPRESS_DEPRECATION=1

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

mkdir -p expected
# State lands under models/ for this transformation pipeline; clean it (plus the
# root path the template uses) so every run starts from a fresh redb + DuckDB.
rm -f poc.duckdb .rocky-state.redb models/.rocky-state.redb*

echo "=== seed ==="
duckdb poc.duckdb < data/seed.sql

echo "=== run (materialize order_keys; Rocky injects the order_key hash) ==="
rocky run > expected/run.json
echo "materialized in poc.main:"
duckdb -noheader -list poc.duckdb \
    "SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name"

echo "=== query the injected surrogate key ==="
duckdb -noheader -list poc.duckdb \
    "SELECT order_key FROM main.order_keys LIMIT 5"

echo "=== assert: order_key is present and holds a 32-char md5 hash ==="
if duckdb -noheader -list poc.duckdb \
        "SELECT order_key FROM main.order_keys LIMIT 5" \
        | grep -qE '^[0-9a-f]{32}$'; then
    echo "PASS: order_key holds a 32-char md5 surrogate key"
else
    echo "FAIL: order_key missing or not a 32-char md5 hash" >&2
    exit 1
fi

echo "=== assert: byte-identical to dbt_utils.generate_surrogate_key ==="
# Rocky's injected hash must equal the dbt-utils form hand-written here:
# md5( coalesce(cast(c as varchar), '_dbt_utils_surrogate_key_null_') joined by '-' ),
# in declared column order (order_id, customer_id). A NULL input coalesces to
# the fixed sentinel before hashing, exactly as dbt-utils does.
MISMATCHES=$(duckdb -noheader -list poc.duckdb "
    SELECT COUNT(*) FROM main.order_keys
    WHERE order_key <> md5(
        coalesce(cast(order_id as varchar),    '_dbt_utils_surrogate_key_null_') || '-' ||
        coalesce(cast(customer_id as varchar), '_dbt_utils_surrogate_key_null_')
    )")
if [ "$MISMATCHES" = "0" ]; then
    echo "PASS: every order_key matches the dbt_utils.generate_surrogate_key form (0 mismatches)"
else
    echo "FAIL: ${MISMATCHES} row(s) diverge from the dbt_utils surrogate-key hash" >&2
    exit 1
fi

echo "POC complete: order_key materialized as a 32-char md5 surrogate key, byte-identical to dbt_utils.generate_surrogate_key."
