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

# load N EXPECTED_ROWS EXPECTED_COMMIT_SEQ
load() {
    local n="$1" rows="$2" seq="$3"
    echo
    echo "=== load $n: rocky load --connector shop_orders (expect $rows rows) ==="
    RUST_LOG=info rocky -c rocky.toml --state-path .rocky/state.redb \
        load --connector shop_orders --output json \
        > "expected/load-$n.json" 2> "expected/load-$n.log" || {
        echo "FAIL: load $n exited non-zero. Last log lines:"
        tail -n 20 "expected/load-$n.log"
        exit 1
    }
    python3 - "expected/load-$n.json" "$rows" "$seq" <<'PY'
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
if len(runs) != 3:
    print("FAIL: expected 3 kept scratch directories, found %d" % len(runs))
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
echo "POC complete: 3 loads (5, 3, 0 rows), 8 rows total, dlt cursor carried in Rocky state."
