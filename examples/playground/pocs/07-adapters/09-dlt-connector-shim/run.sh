#!/usr/bin/env bash
# dlt connector shim: a dlt source -> Parquet -> DuckDB, incremental through
# Rocky state.
#
# Installs dlt 1.32.0 into ./.venv, then runs
# `rocky load --connector shop_orders` three times over an offline source:
#
#   load 1  5 rows   orders with id 1..5
#   (append orders with id 6..8)
#   load 2  3 rows   only the new ones: dlt's cursor came back from Rocky state
#   load 3  0 rows   nothing new; the state still commits (commit_seq 3)
#
# Then it checks the target (8 rows, no duplicate id, no `customers` rows),
# checks that the saved dlt cursor reached id 8, and checks that a connector
# naming a stream the source lacks fails with a clear message.
#
# dlt cannot be installed (no uv/venv, or no package registry): `rocky
# validate` only, then exit 0 with a SKIP message. Set REQUIRE_DLT=1 to fail
# instead.
#
# Needs: rocky, the duckdb CLI, uv (or python3 -m venv) to install dlt once.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

rm -rf .rocky expected poc.duckdb poc.duckdb.wal shop_src/data
mkdir -p expected shop_src/data

echo "=== rocky validate (parse-only, no network) ==="
rocky -c rocky.toml validate

skip_or_fail() {
    if [ "${REQUIRE_DLT:-0}" = "1" ]; then
        echo "FAIL: REQUIRE_DLT=1 but $1"
        exit 1
    fi
    echo
    echo "SKIP: $1, so the live tour did not run."
    echo "      Fix that and re-run, or set REQUIRE_DLT=1 to make this a failure."
    exit 0
}

if ! command -v duckdb >/dev/null 2>&1; then
    echo "FAIL: 'duckdb' is not on PATH (the shim counts Parquet rows with it)"
    exit 1
fi

echo
echo "=== install dlt into ./.venv (pinned in requirements.txt) ==="
if ! .venv/bin/python -c "import dlt" >/dev/null 2>&1; then
    if command -v uv >/dev/null 2>&1; then
        uv venv -q .venv >/dev/null 2>&1 || true
        uv pip install -q --python .venv/bin/python -r requirements.txt \
            >/dev/null 2>&1 || true
    elif command -v python3 >/dev/null 2>&1; then
        python3 -m venv .venv >/dev/null 2>&1 || true
        .venv/bin/python -m pip install -q -r requirements.txt \
            >/dev/null 2>&1 || true
    fi
fi
if ! .venv/bin/python -c "import dlt" >/dev/null 2>&1; then
    skip_or_fail "dlt is not installed and could not be installed here"
fi
echo "    dlt $(.venv/bin/python -c 'import dlt; print(dlt.__version__)')"

# Seed the offline source: 5 orders and 2 customers.
for i in 1 2 3 4 5; do
    echo "{\"id\": $i, \"amount_cents\": $((i * 100))}"
done > shop_src/data/orders.jsonl
printf '%s\n' '{"id": 1, "name": "Ada"}' '{"id": 2, "name": "Grace"}' \
    > shop_src/data/customers.jsonl
printf '%s\n' '{"id": 1, "sku": "A"}' '{"id": 2, "sku": "B"}' '{"id": 3, "sku": "C"}' \
    > shop_src/data/order_items.jsonl
printf '%s\n' '{"id": 1, "reason": "late"}' '{"id": 2, "reason": "damaged"}' \
    > shop_src/data/refunds.jsonl
printf '%s\n' '{"id": 1, "items": [{"sku": "A"}, {"sku": "B"}]}' \
    '{"id": 2, "items": [{"sku": "C"}]}' > shop_src/data/nested_orders.jsonl

# load N EXPECTED_ROWS EXPECTED_COMMIT_SEQ [CONNECTOR]
load() {
    local n="$1" rows="$2" seq="$3" name="${4:-shop_orders}"
    echo
    echo "=== load $n: rocky load --connector $name (expect $rows rows) ==="
    RUST_LOG=info rocky -c rocky.toml --state-path .rocky/state.redb \
        load --connector "$name" --output json \
        > "expected/$name-$n.json" 2> "expected/$name-$n.log" || {
        echo "FAIL: load $n exited non-zero. Last log lines:"
        tail -n 20 "expected/$name-$n.log"
        exit 1
    }
    python3 - "expected/$name-$n.json" "$rows" "$seq" <<'PY'
import json, sys
path, rows, seq = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
out = json.load(open(path))
c = out["connector"]
problems = []
if c["status"] != "loaded":
    problems.append("status is %r (error: %r)" % (c["status"], c.get("error")))
if out["total_rows"] != rows:
    problems.append("total_rows is %d, expected %d" % (out["total_rows"], rows))
if c["commit_seq_after"] != seq:
    problems.append("commit_seq_after is %d, expected %d" % (c["commit_seq_after"], seq))
if problems:
    print("FAIL: " + "; ".join(problems))
    sys.exit(1)
print("    status=loaded total_rows=%d commit_seq %d -> %d"
      % (out["total_rows"], c["commit_seq_before"], c["commit_seq_after"]))
PY
}

echo
echo "=== at least once: a failure AFTER the shim succeeded must not move the state ==="
# rocky-contract-fail.toml adds a contract the batch cannot meet. The shim
# succeeds, the contract gate fails, and Rocky must keep the old state.
if RUST_LOG=info rocky -c rocky-contract-fail.toml --state-path .rocky/state.redb \
    load --connector shop_orders --output json \
    > expected/contract-fail.json 2> expected/contract-fail.log; then
    echo "FAIL: the contract-gated load exited 0"
    exit 1
fi
if ! grep -q "wrote 1 file(s), 5 row(s)" expected/contract-fail.log; then
    echo "FAIL: the shim did not extract the batch before the gate failed"
    exit 1
fi
LEFT="$(duckdb -readonly -csv -noheader poc.duckdb \
    "SELECT count(*) FROM information_schema.tables WHERE table_name = 'orders'" 2>/dev/null || echo 0)"
if [ "$LEFT" != "0" ] && [ "$(duckdb -readonly -csv -noheader poc.duckdb 'SELECT count(*) FROM poc.main.orders')" != "0" ]; then
    echo "FAIL: the failed load left rows in poc.main.orders"
    exit 1
fi
echo "    the load failed after the shim read 5 rows; no rows landed"
echo "    the next load must read the same 5 rows with commit_seq 0 -> 1:"

load 1 5 1

echo
echo "=== append orders with id 6..8 to the source ==="
for i in 6 7 8; do
    echo "{\"id\": $i, \"amount_cents\": $((i * 100))}"
done >> shop_src/data/orders.jsonl

load 2 3 2
load 3 0 3

echo
echo "=== target table poc.main.orders ==="
COUNTS="$(duckdb -readonly -csv -noheader poc.duckdb \
    "SELECT count(*), count(DISTINCT id) FROM poc.main.orders")"
if [ "$COUNTS" != "8,8" ]; then
    echo "FAIL: expected 8 rows and 8 distinct ids, got '$COUNTS'"
    exit 1
fi
echo "    rows=8 distinct ids=8"
TABLES="$(duckdb -readonly -csv -noheader poc.duckdb \
    "SELECT count(*) FROM information_schema.tables WHERE table_name = 'customers'")"
if [ "$TABLES" != "0" ]; then
    echo "FAIL: the unselected stream 'customers' reached the warehouse"
    exit 1
fi
echo "    the other stream (customers) was not loaded"

echo
echo "=== dlt state lives in Rocky state (the result of the last run) ==="
python3 - <<'PY'
import glob, json, os, sys
runs = sorted(glob.glob(".rocky/landing/shop_orders/*/_rocky_result.json"),
              key=os.path.getmtime)
if len(runs) != 4:
    print("FAIL: expected 4 kept scratch directories (the contract failure and 3 loads), found %d" % len(runs))
    sys.exit(1)
state = json.load(open(runs[-1]))["state"]["value"]
cursor = state["sources"]["shop"]["resources"]["orders"]["incremental"]["id"]
print("    dlt_version=%s last_value=%s" % (state["dlt_version"], cursor["last_value"]))
if cursor["last_value"] != 8:
    print("FAIL: expected the saved dlt cursor last_value to be 8")
    sys.exit(1)
PY

echo
echo "=== a stream the source does not have fails clearly ==="
if rocky -c rocky.toml --state-path .rocky/state.redb \
    load --connector shop_missing_stream --output json \
    > expected/missing.json 2> expected/missing.log; then
    echo "FAIL: loading a missing stream exited 0"
    exit 1
fi
if ! grep -q "has no stream 'refunds'" expected/missing.log; then
    echo "FAIL: the log does not say the stream is missing. Last log lines:"
    tail -n 10 expected/missing.log
    exit 1
fi
echo "    failed as expected: $(grep -o -m1 "the source has no stream[^\"]*" expected/missing.log)"

echo
echo "=== dlt renames tables: the shim follows dlt's schema ==="
# Resource "order-items" lands in dlt table order_items.
load 1 3 1 shop_order_items
# Resource "refunds_raw" has table_name = "shop_refunds".
load 1 2 1 shop_refunds
for pair in "order_items:3" "refunds:2"; do
    table="${pair%%:*}"
    got="$(duckdb -readonly -csv -noheader poc.duckdb "SELECT count(*) FROM poc.main.$table")"
    if [ "$got" != "${pair##*:}" ]; then
        echo "FAIL: poc.main.$table has $got rows, expected ${pair##*:}"
        exit 1
    fi
done
echo "    poc.main.order_items=3 rows, poc.main.refunds=2 rows"

echo
echo "=== a resource that writes two tables (nested list) fails clearly ==="
if rocky -c rocky.toml --state-path .rocky/state.redb \
    load --connector shop_nested --output json \
    > expected/nested.json 2> expected/nested.log; then
    echo "FAIL: loading a nested resource exited 0"
    exit 1
fi
if ! grep -q "wrote 2 tables with rows" expected/nested.log; then
    echo "FAIL: the log does not say the resource wrote two tables. Last log lines:"
    tail -n 10 expected/nested.log
    exit 1
fi
echo "    failed as expected: $(grep -o -m1 "stream 'nested_orders' wrote[^.]*" expected/nested.log)"

echo
echo "=== dlt's private work directory is gone ==="
if find .rocky -name 'dlt-work-*' -o -name 'trace.pickle' | grep -q .; then
    echo "FAIL: a dlt work directory or trace.pickle is left under .rocky"
    exit 1
fi
echo "    0 left"

echo
echo "POC complete: 3 loads (5, 3, 0 rows), 8 rows total, dlt cursor carried in Rocky state."
