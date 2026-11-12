#!/usr/bin/env bash
# Meltano connector shim: a Singer tap run by Meltano -> Parquet -> DuckDB,
# incremental through Rocky state.
#
# Installs Meltano 4.4.1 into ./.venv and installs the project's tap
# (meltano_project/tap-orders, a tiny offline Singer SDK tap) once. Then it
# runs `rocky load --connector orders_tap` three times:
#
#   load 1  4 rows   orders with id 1..4
#   (append orders with id 5..7)
#   load 2  3 rows   only the new ones: the Singer bookmark came back from
#                    Rocky state and went to the tap with `--state`
#   load 3  0 rows   nothing new; the state still commits (commit_seq 3)
#
# Then it checks the target (7 rows, no duplicate id, no `customers` rows),
# checks that the saved bookmark reached id 7, and checks that a connector
# naming a stream the tap lacks fails with a clear message.
#
# Meltano or the tap cannot be installed (no uv/venv, or no package
# registry): `rocky validate` only, then exit 0 with a SKIP message. Set
# REQUIRE_MELTANO=1 to fail instead.
#
# Needs: rocky, python3 (3.8+), the duckdb CLI, uv (or python3 -m venv with
# Python 3.10+) to install Meltano once.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Meltano usage tracking off, in this script and in the shim's child.
export MELTANO_DISABLE_TRACKING=true
export MELTANO_PROJECT_ROOT="$HERE/meltano_project"

rm -rf .rocky expected poc.duckdb poc.duckdb.wal meltano_project/data
mkdir -p expected meltano_project/data

echo "=== rocky validate (parse-only, no network) ==="
rocky -c rocky.toml validate

skip_or_fail() {
    if [ "${REQUIRE_MELTANO:-0}" = "1" ]; then
        echo "FAIL: REQUIRE_MELTANO=1 but $1"
        exit 1
    fi
    echo
    echo "SKIP: $1, so the live tour did not run."
    echo "      Fix that and re-run, or set REQUIRE_MELTANO=1 to make this a failure."
    exit 0
}

for tool in duckdb python3; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "FAIL: '$tool' is not on PATH (the shim needs it)"
        exit 1
    fi
done

echo
echo "=== install Meltano into ./.venv, then the tap into the project ==="
if [ ! -x .venv/bin/meltano ]; then
    if command -v uv >/dev/null 2>&1; then
        uv venv -q --python '>=3.10' .venv >/dev/null 2>&1 || true
        uv pip install -q --python .venv/bin/python -r requirements.txt \
            >/dev/null 2>&1 || true
    else
        python3 -m venv .venv >/dev/null 2>&1 || true
        .venv/bin/python -m pip install -q -r requirements.txt \
            >/dev/null 2>&1 || true
    fi
fi
if [ ! -x .venv/bin/meltano ] || ! .venv/bin/meltano --version >/dev/null 2>&1; then
    skip_or_fail "Meltano is not installed and could not be installed here"
fi
if [ ! -x meltano_project/.meltano/extractors/tap-orders/venv/bin/tap-orders ]; then
    (cd meltano_project && ../.venv/bin/meltano install extractor tap-orders \
        > "$HERE/expected/meltano-install.log" 2>&1) \
        || skip_or_fail "the tap could not be installed (see expected/meltano-install.log)"
fi
echo "    $(.venv/bin/meltano --version)"

# Seed the offline tap: 4 orders and 1 customer.
for i in 1 2 3 4; do
    echo "{\"id\": $i, \"amount_cents\": $((i * 100))}"
done > meltano_project/data/orders.jsonl
echo '{"id": 1, "name": "Ada"}' > meltano_project/data/customers.jsonl

# load N EXPECTED_ROWS EXPECTED_COMMIT_SEQ
load() {
    local n="$1" rows="$2" seq="$3"
    echo
    echo "=== load $n: rocky load --connector orders_tap (expect $rows rows) ==="
    RUST_LOG=info rocky -c rocky.toml --state-path .rocky/state.redb \
        load --connector orders_tap --output json \
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

# A stale state file of Meltano's own must never reach the tap.
mkdir -p meltano_project/.meltano/run/tap-orders
echo '{"bookmarks": {"orders": {"replication_key": "id", "replication_key_value": 99}}}' \
    > meltano_project/.meltano/run/tap-orders/state.json

echo
echo "=== at least once: a failure AFTER the shim succeeded must not move the state ==="
# rocky-contract-fail.toml adds a contract the batch cannot meet. The shim
# succeeds, the contract gate fails, and Rocky must keep the old state.
if RUST_LOG=info rocky -c rocky-contract-fail.toml --state-path .rocky/state.redb \
    load --connector orders_tap --output json \
    > expected/contract-fail.json 2> expected/contract-fail.log; then
    echo "FAIL: the contract-gated load exited 0"
    exit 1
fi
if ! grep -q "read 4 record(s) of stream 'orders'" expected/contract-fail.log; then
    echo "FAIL: the shim did not read the batch before the gate failed"
    exit 1
fi
if grep -q "resuming from saved state" expected/contract-fail.log; then
    echo "FAIL: the first run resumed from a state (a stale file reached the tap?)"
    exit 1
fi
LEFT="$(duckdb -readonly -csv -noheader poc.duckdb \
    "SELECT count(*) FROM information_schema.tables WHERE table_name = 'orders'" 2>/dev/null || echo 0)"
if [ "$LEFT" != "0" ] && [ "$(duckdb -readonly -csv -noheader poc.duckdb 'SELECT count(*) FROM poc.main.orders')" != "0" ]; then
    echo "FAIL: the failed load left rows in poc.main.orders"
    exit 1
fi
echo "    the load failed after the shim read 4 records; no rows landed"
echo "    the next load must read the same 4 rows with commit_seq 0 -> 1:"

load 1 4 1

echo
echo "=== append orders with id 5..7 to the tap's data ==="
for i in 5 6 7; do
    echo "{\"id\": $i, \"amount_cents\": $((i * 100))}"
done >> meltano_project/data/orders.jsonl

load 2 3 2
load 3 0 3

echo
echo "=== target table poc.main.orders ==="
COUNTS="$(duckdb -readonly -csv -noheader poc.duckdb \
    "SELECT count(*), count(DISTINCT id) FROM poc.main.orders")"
if [ "$COUNTS" != "7,7" ]; then
    echo "FAIL: expected 7 rows and 7 distinct ids, got '$COUNTS'"
    exit 1
fi
echo "    rows=7 distinct ids=7"
TABLES="$(duckdb -readonly -csv -noheader poc.duckdb \
    "SELECT count(*) FROM information_schema.tables WHERE table_name = 'customers'")"
if [ "$TABLES" != "0" ]; then
    echo "FAIL: the deselected stream 'customers' reached the warehouse"
    exit 1
fi
echo "    the other stream (customers) was not loaded"

echo
echo "=== the Singer state lives in Rocky state ==="
if ! grep -q "resuming from saved state" expected/load-2.log; then
    echo "FAIL: load 2 did not pass the saved state to the tap"
    exit 1
fi
echo "    load 2 passed the saved state to the tap (--state)"
python3 - <<'PY'
import glob, json, os, sys
runs = sorted(glob.glob(".rocky/landing/orders_tap/*/_rocky_result.json"),
              key=os.path.getmtime)
if len(runs) != 4:
    print("FAIL: expected 4 kept scratch directories (the contract failure and 3 loads), found %d" % len(runs))
    sys.exit(1)
for label, path, expected in (("load 1", runs[1], 4), ("load 3", runs[-1], 7)):
    value = json.load(open(path))["state"]["value"]
    bookmark = value["bookmarks"]["orders"]
    print("    %s: bookmark %s=%s" % (label, bookmark["replication_key"],
                                      bookmark["replication_key_value"]))
    if sorted(value["bookmarks"]) != ["orders"] or bookmark["replication_key_value"] != expected:
        print("FAIL: expected a bookmark for 'orders' alone with value %d" % expected)
        sys.exit(1)
PY

echo
echo "=== a stream the tap does not have fails clearly ==="
if rocky -c rocky.toml --state-path .rocky/state.redb \
    load --connector orders_missing_stream --output json \
    > expected/missing.json 2> expected/missing.log; then
    echo "FAIL: loading a missing stream exited 0"
    exit 1
fi
if ! grep -q "the tap has no stream 'refunds'" expected/missing.log; then
    echo "FAIL: the log does not say the stream is missing. Last log lines:"
    tail -n 10 expected/missing.log
    exit 1
fi
echo "    failed as expected: $(grep -o -m1 "the tap has no stream[^\"]*" expected/missing.log)"

echo
echo "=== records under another stream name fail the run ==="
if rocky -c rocky.toml --state-path .rocky/state.redb \
    load --connector orders_name_mismatch --output json \
    > expected/mismatch.json 2> expected/mismatch.log; then
    echo "FAIL: a stream-name mismatch exited 0"
    exit 1
fi
if ! grep -q "none for stream 'orders'" expected/mismatch.log; then
    echo "FAIL: the log does not report the mismatch. Last log lines:"
    tail -n 10 expected/mismatch.log
    exit 1
fi
echo "    failed as expected: $(grep -o -m1 "emitted [0-9]* record(s) but none for stream[^;]*" expected/mismatch.log)"

echo
echo "=== no tap config or Meltano temp dir is left behind ==="
if find meltano_project .rocky -name 'tap.*.config.json' 2>/dev/null | grep -q . \
    || ls -d "${TMPDIR:-/tmp}"/meltano-sys-* >/dev/null 2>&1; then
    echo "FAIL: a tap.*.config.json (it can hold secrets) or a meltano-sys-* dir is left"
    exit 1
fi
echo "    0 left"

echo
echo "POC complete: 3 loads (4, 3, 0 rows), 7 rows total, Singer bookmark carried in Rocky state."
