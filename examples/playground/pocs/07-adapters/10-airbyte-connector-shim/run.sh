#!/usr/bin/env bash
# Airbyte connector shim: airbyte/source-faker -> Parquet -> DuckDB, resumed
# from state kept by Rocky.
#
# Pulls airbyte/source-faker:6.2.21 once, then runs
# `rocky load --connector faker_users` three times (a fixed seed, 20 users):
#
#   load 1  20 rows   the incremental `users` stream; the last STATE is saved
#   load 2   0 rows   Rocky hands the state back; faker sees it and emits nothing
#   load 3   0 rows   same; the state still commits (commit_seq 3)
#
# A control connector with no saved state loads 20 rows from the same source,
# so the 0 rows of load 2 are the state at work. Then it checks the target,
# checks the saved state, and checks two failures: a stream the source lacks
# and a source that reports an error TRACE message.
#
# Docker absent, daemon down, or the image cannot be pulled: `rocky validate`
# only, then exit 0 with a SKIP message. Set REQUIRE_DOCKER=1 to fail instead.
#
# Needs: rocky, python3 (3.8+), the duckdb CLI, docker.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

IMAGE="airbyte/source-faker:6.2.21"

rm -rf .rocky expected poc.duckdb poc.duckdb.wal
mkdir -p expected

echo "=== rocky validate (parse-only, no network) ==="
rocky -c rocky.toml validate

skip_or_fail() {
    if [ "${REQUIRE_DOCKER:-0}" = "1" ]; then
        echo "FAIL: REQUIRE_DOCKER=1 but $1"
        exit 1
    fi
    echo
    echo "SKIP: $1, so the live tour did not run."
    echo "      Fix that and re-run, or set REQUIRE_DOCKER=1 to make this a failure."
    exit 0
}

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
    skip_or_fail "Docker is not reachable"
fi

for tool in duckdb python3; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "FAIL: '$tool' is not on PATH (the shim needs it)"
        exit 1
    fi
done

echo
echo "=== docker image $IMAGE (pulled once; the shim never pulls) ==="
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    docker pull -q "$IMAGE" >/dev/null 2>&1 || skip_or_fail "the image $IMAGE cannot be pulled"
fi
echo "    present"

# load CONNECTOR N EXPECTED_ROWS EXPECTED_COMMIT_SEQ
load() {
    local name="$1" n="$2" rows="$3" seq="$4"
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
    load --connector faker_users --output json \
    > expected/contract-fail.json 2> expected/contract-fail.log; then
    echo "FAIL: the contract-gated load exited 0"
    exit 1
fi
if ! grep -q "read 20 record(s) of stream 'users'" expected/contract-fail.log; then
    echo "FAIL: the shim did not read the batch before the gate failed"
    exit 1
fi
LEFT="$(duckdb -readonly -csv -noheader poc.duckdb \
    "SELECT count(*) FROM information_schema.tables WHERE table_name = 'users'" 2>/dev/null || echo 0)"
if [ "$LEFT" != "0" ] && [ "$(duckdb -readonly -csv -noheader poc.duckdb 'SELECT count(*) FROM poc.main.users')" != "0" ]; then
    echo "FAIL: the failed load left rows in poc.main.users"
    exit 1
fi
echo "    the load failed after the shim read 20 records; no rows landed"
echo "    the next load must read the same 20 rows with commit_seq 0 -> 1:"

load faker_users 1 20 1
load faker_users 2 0 2
load faker_users 3 0 3
load faker_users_fresh 1 20 1

echo
echo "=== target tables ==="
COUNTS="$(duckdb -readonly -csv -noheader poc.duckdb \
    "SELECT count(*), count(DISTINCT id) FROM poc.main.users")"
if [ "$COUNTS" != "20,20" ]; then
    echo "FAIL: expected 20 rows and 20 distinct ids in poc.main.users, got '$COUNTS'"
    exit 1
fi
echo "    poc.main.users: rows=20 distinct ids=20"
echo "    poc.main.users_fresh (control, no saved state): 20 rows"

echo
echo "=== the Airbyte state lives in Rocky state (kept scratch of load 1 and 3) ==="
python3 - <<'PY'
import glob, json, os, sys
runs = sorted(glob.glob(".rocky/landing/faker_users/*/_rocky_result.json"),
              key=os.path.getmtime)
if len(runs) != 4:
    print("FAIL: expected 4 kept scratch directories (the contract failure and 3 loads), found %d" % len(runs))
    sys.exit(1)
first = json.load(open(runs[0]))["state"]["value"]
last = json.load(open(runs[-1]))["state"]["value"]
for label, value in (("load 1", first), ("load 3", last)):
    stream_state = value["message"]["stream"]["stream_state"]
    print("    %s: state_type=%s stream=%s loop_offset=%s"
          % (label, value["state_type"],
             value["message"]["stream"]["stream_descriptor"]["name"],
             stream_state["loop_offset"]))
    if value["state_type"] != "STREAM" or stream_state["loop_offset"] != 20:
        print("FAIL: expected a STREAM state with loop_offset 20")
        sys.exit(1)
if first != last:
    print("FAIL: the state changed between load 1 and load 3")
    sys.exit(1)
PY

if ! grep -q "resuming from saved STREAM state" expected/faker_users-2.log; then
    echo "FAIL: load 2 did not pass the saved state to the source"
    exit 1
fi
echo "    load 2 passed the saved state to the source (--state)"

fails() {
    # fails CONNECTOR PATTERN: the load must exit non-zero and say PATTERN.
    local name="$1" pattern="$2"
    if rocky -c rocky.toml --state-path .rocky/state.redb \
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
    echo "    failed as expected: $(grep -o -m1 "$pattern[^\"]*" "expected/$name.log")"
}

echo
echo "=== a stream the source does not have fails clearly ==="
fails faker_bad_stream "the source has no stream 'orders'"

echo
echo "=== a source that reports an error TRACE message fails the run ==="
fails faker_bad_config "the source reported an error"

echo
echo "=== no container of this POC is left running ==="
if [ -n "$(docker ps -aq --filter name=rocky-)" ]; then
    echo "FAIL: a rocky-* container is left:"
    docker ps -a --filter name=rocky-
    exit 1
fi
echo "    0 left"

echo
echo "POC complete: 3 loads (20, 0, 0 rows), Airbyte STREAM state carried in Rocky state."
