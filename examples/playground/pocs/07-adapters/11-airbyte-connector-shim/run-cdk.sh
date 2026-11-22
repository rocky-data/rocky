#!/usr/bin/env bash
# Airbyte connector shim with no Docker: a declarative source manifest run
# through airbyte-cdk's `source-declarative-manifest`, state kept by Rocky.
#
# The source is this POC's own manifest (cdk/orders-manifest.yaml, Apache-2.0)
# against a local stub API (cdk/stub.py). No Airbyte connector manifest is
# shipped: most are Elastic License 2.0.
#
#   load 1   4 rows   orders 1 to 4; the last STATE (a cursor on updated_at)
#                     is saved
#   load 2   4 rows   orders 5 to 7 were added; Rocky hands the state back and
#                     the source asks the API for orders from the cursor on:
#                     order 4 again, then 5 to 7
#   load 3   1 row    nothing new; order 7, at the cursor, comes again
#
# airbyte-cdk starts the next read AT the saved cursor, not after it. This
# stub's lower bound is inclusive, so the row at the cursor loads again on
# every load. The target only appends, so that row is a duplicate. This is at-least-once delivery: deduplicate
# downstream on a key (here `id`).
#
# Then four failures: a manifest whose SHA-256 is not the pinned one, a
# manifest that names a custom Python component, and AIRBYTE_ENABLE_UNSAFE_CODE
# or a PYTHON* variable handed to the shim.
#
# No airbyte-cdk and it cannot be installed here: `rocky validate` only, then
# exit 0 with a SKIP message. Set REQUIRE_AIRBYTE_CDK=1 to fail instead.
#
# Needs: rocky, python3, the duckdb CLI, curl, and airbyte-cdk in cdk/.venv.
# airbyte-cdk 7.48.0 needs Python 3.11 to 3.14. This script installs it into
# cdk/.venv with uv (preferred) or a python3.11 to python3.14 it finds, from
# cdk/requirements.txt. CI installs the hash-pinned cdk/requirements.lock.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

PORT=18766
CONFIG=rocky-cdk.toml
STATE=.rocky/cdk-state.redb
CDK=cdk/.venv/bin/source-declarative-manifest

rm -rf .rocky/landing/orders_cdk* "$STATE" poc_cdk.duckdb poc_cdk.duckdb.wal cdk/data
mkdir -p expected cdk/data .rocky

echo "=== rocky validate (parse-only, no network) ==="
rocky -c "$CONFIG" validate

skip_or_fail() {
    if [ "${REQUIRE_AIRBYTE_CDK:-0}" = "1" ]; then
        echo "FAIL: REQUIRE_AIRBYTE_CDK=1 but $1"
        exit 1
    fi
    echo
    echo "SKIP: $1, so the live tour did not run."
    echo "      Fix that and re-run, or set REQUIRE_AIRBYTE_CDK=1 to make this a failure."
    exit 0
}

for tool in duckdb python3 curl; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "FAIL: '$tool' is not on PATH"
        exit 1
    fi
done

echo
echo "=== airbyte-cdk in cdk/.venv (pinned in cdk/requirements.txt) ==="
if [ ! -x "$CDK" ]; then
    if command -v uv >/dev/null 2>&1; then
        uv venv -q --python 3.12 cdk/.venv >/dev/null 2>&1 || true
        uv pip install -q --python cdk/.venv/bin/python -r cdk/requirements.txt \
            >/dev/null 2>&1 || true
    else
        for py in python3.12 python3.13 python3.11 python3.14; do
            if command -v "$py" >/dev/null 2>&1; then
                "$py" -m venv cdk/.venv >/dev/null 2>&1 || true
                cdk/.venv/bin/python -m pip install -q -r cdk/requirements.txt \
                    >/dev/null 2>&1 || true
                break
            fi
        done
    fi
fi
if [ ! -x "$CDK" ]; then
    skip_or_fail "airbyte-cdk is not installed in cdk/.venv and could not be installed here"
fi
echo "    airbyte-cdk $(cdk/.venv/bin/python -c 'import importlib.metadata as m; print(m.version("airbyte-cdk"))')"

# orders FIRST LAST: the stub's data, orders FIRST to LAST, one a day.
orders() {
    python3 - "$1" "$2" <<'PY'
import json, sys
first, last = int(sys.argv[1]), int(sys.argv[2])
rows = [{"id": i, "amount_cents": i * 100, "updated_at": "2026-11-%02dT10:00:00Z" % i}
        for i in range(first, last + 1)]
json.dump(rows, open("cdk/data/orders.json", "w"))
PY
}

if curl -s -o /dev/null "http://127.0.0.1:$PORT/"; then
    echo "FAIL: something already listens on 127.0.0.1:$PORT (the stub's port)"
    exit 1
fi
orders 1 4
python3 cdk/stub.py "$PORT" cdk/data/orders.json 2> expected/stub.log &
STUB=$!
trap 'kill "$STUB" 2>/dev/null || true' EXIT
for _ in $(seq 1 50); do
    curl -s -o /dev/null "http://127.0.0.1:$PORT/ready" && break
    sleep 0.2
done

# load CONNECTOR N EXPECTED_ROWS EXPECTED_COMMIT_SEQ
load() {
    local name="$1" n="$2" rows="$3" seq="$4"
    echo
    echo "=== load $n: rocky load --connector $name (expect $rows rows) ==="
    RUST_LOG=info rocky -c "$CONFIG" --state-path "$STATE" \
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

load orders_cdk 1 4 1
orders 1 7
load orders_cdk 2 4 2
load orders_cdk 3 1 3

if ! grep -q "resuming from saved STREAM state" expected/orders_cdk-2.log; then
    echo "FAIL: load 2 did not pass the saved state to the source"
    exit 1
fi
echo
echo "    load 2 passed the saved state to the source (--state)"
echo "    the requests the stub saw:"
sed 's/^/      /' expected/stub.log

echo
echo "=== target table ==="
COUNTS="$(duckdb -readonly -csv -noheader poc_cdk.duckdb \
    "SELECT count(*), count(DISTINCT id), min(id), max(id) FROM poc_cdk.main.orders")"
if [ "$COUNTS" != "9,7,1,7" ]; then
    echo "FAIL: expected 9 rows, ids 1 to 7, in poc_cdk.main.orders; got '$COUNTS'"
    exit 1
fi
DUPES="$(duckdb -readonly -csv -noheader poc_cdk.duckdb \
    "SELECT string_agg(id::VARCHAR, '-' ORDER BY id) FROM (SELECT id FROM poc_cdk.main.orders GROUP BY id HAVING count(*) > 1)")"
if [ "$DUPES" != "4-7" ]; then
    echo "FAIL: expected only orders 4 and 7 (the cursor rows) twice; got '$DUPES'"
    exit 1
fi
echo "    poc_cdk.main.orders: rows=9 distinct ids=7 (1 to 7); orders 4 and 7, at the cursor, twice"

fails() {
    # fails CONNECTOR PATTERN: the load must exit non-zero and say PATTERN.
    local name="$1" pattern="$2"
    if rocky -c "$CONFIG" --state-path "$STATE" \
        load --connector "$name" --output json \
        > "expected/$name.json" 2> "expected/$name.log"; then
        echo "FAIL: $name exited 0"
        exit 1
    fi
    if ! grep -q "$pattern" "expected/$name.log"; then
        echo "FAIL: the log of $name does not say '$pattern'. Last lines:"
        tail -n 10 "expected/$name.log"
        exit 1
    fi
    echo "    failed as expected: $(grep -o -m1 "${pattern}[^\"]*" "expected/$name.log" | cut -c1-120)"
}

echo
echo "=== a manifest that is not the pinned one fails before airbyte-cdk starts ==="
BEFORE="$(wc -l < expected/stub.log)"
fails orders_cdk_bad_hash "but options.manifest_sha256 is"
if [ "$(wc -l < expected/stub.log)" != "$BEFORE" ]; then
    echo "FAIL: the stub saw a request during the bad-hash run"
    exit 1
fi
echo "    the stub saw no request"

echo
echo "=== a manifest that names a custom Python component fails ==="
fails orders_cdk_custom_component "Custom connector code is not permitted"

echo
echo "=== the shim refuses AIRBYTE_ENABLE_UNSAFE_CODE, which would let it run ==="
AIRBYTE_ENABLE_UNSAFE_CODE=true fails orders_cdk_unsafe_code "AIRBYTE_ENABLE_UNSAFE_CODE is set"

echo
echo "=== the shim refuses a PYTHON* variable, which would load code the hash does not cover ==="
PYTHONDONTWRITEBYTECODE=1 fails orders_cdk_python_var "PYTHONDONTWRITEBYTECODE is set"

echo
echo "POC complete: 3 loads (4, 4, 1 rows) with no Docker, the Airbyte cursor carried in Rocky state."
