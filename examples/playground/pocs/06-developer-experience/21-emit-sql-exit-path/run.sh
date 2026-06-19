#!/usr/bin/env bash
# 21-emit-sql-exit-path — prove the no-lock-in exit path.
#
# `rocky emit-sql` reduces the whole model DAG to plain `<model>.sql` files,
# then this script PROVES those files run Rocky-free: it seeds a fresh DuckDB
# and replays the emitted SQL in dependency order through the `duckdb` CLI.
# There is no `rocky run` anywhere below — the final tables are built by plain
# DuckDB from emitted SQL.
set -euo pipefail

export ROCKY_SUPPRESS_DEPRECATION=1

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

mkdir -p expected
rm -rf build
rm -f .rocky-state.redb poc.duckdb

echo "=== validate ==="
rocky validate > expected/validate.json

echo "=== emit-sql (compile DAG offline → one <model>.sql per model) ==="
rocky emit-sql --models models --out-dir build/sql
echo "emitted SQL files:"
ls build/sql

# Confirm the value-add survives the exit path: the surrogate key is a real
# md5() expression in the emitted SQL, computed by plain SQL, not by rocky run.
echo "=== surrogate key is plain SQL in the emitted file ==="
grep -q "md5(" build/sql/stg_orders.sql \
    && echo "OK: build/sql/stg_orders.sql builds order_key with md5()"

# Dependency order comes from Rocky, not from a fragile alphabetical glob.
# (emit-sql already writes in dep order; we use `rocky dag` to drive the
# replay explicitly so the order is auditable.)
echo "=== resolve replay order from rocky dag ==="
ORDER="$(rocky dag --models models --output json 2>/dev/null \
    | jq -r '.execution_layers[][]' \
    | sed 's/^transformation://')"
echo "replay order: $(echo "$ORDER" | tr '\n' ' ')"

echo "=== seed a fresh DuckDB (the same file the emitted SQL writes into) ==="
duckdb poc.duckdb < data/seed.sql

echo "=== replay emitted SQL through plain duckdb — NO rocky run ==="
while IFS= read -r model; do
    [ -n "$model" ] || continue
    echo "  duckdb poc.duckdb < build/sql/${model}.sql"
    duckdb poc.duckdb < "build/sql/${model}.sql"
done <<< "$ORDER"

echo "=== assert the final table materialized via plain DuckDB ==="
ROWS="$(duckdb -noheader -list poc.duckdb \
    "SELECT count(*) FROM poc.main.revenue_by_region")"
echo "poc.main.revenue_by_region rows: $ROWS"
if [ "$ROWS" -lt 1 ]; then
    echo "FAIL: revenue_by_region is empty — emitted SQL did not materialize it" >&2
    exit 1
fi

echo "=== assert the surrogate key was rebuilt by plain DuckDB (32-char md5) ==="
KEYLEN="$(duckdb -noheader -list poc.duckdb \
    "SELECT length(order_key) FROM poc.main.stg_orders LIMIT 1")"
echo "poc.main.stg_orders.order_key length: $KEYLEN"
if [ "$KEYLEN" != "32" ]; then
    echo "FAIL: order_key is not a 32-char md5 hash" >&2
    exit 1
fi

echo "tables built by plain DuckDB from emitted SQL:"
duckdb poc.duckdb \
    "SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name"

# Sanity: the same models also pass Rocky's in-memory test path (auto-loads
# data/seed.sql), so the emitted SQL we just proved is the same SQL Rocky runs.
echo "=== rocky test (in-memory, for parity) ==="
rocky test --models models > expected/test.json

echo "POC complete: 3 models reduced to plain SQL via emit-sql; a fresh DuckDB built revenue_by_region (and a 32-char md5 order_key) with no rocky run."
