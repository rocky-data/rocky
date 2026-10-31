#!/usr/bin/env bash
# Reference child connector: Postgres -> Parquet -> DuckDB, with a cursor.
#
# Boots postgres:16-alpine (20 seeded rows), then runs
# `rocky load --connector pg_events` three times:
#
#   load 1  19 rows   the row at the source maximum waits for the next run
#   (insert 5 rows with later timestamps)
#   load 2   5 rows   the held-back row plus four new rows
#   load 3   0 rows   no new row; the state still commits (commit_seq 3)
#
# Then it checks the target (24 rows, no duplicate id) and greps the
# Postgres password over every output, log, scratch dir and the state file.
#
# Docker absent or daemon down: `rocky validate` only, then exit 0 with a
# SKIP message. Set REQUIRE_DOCKER=1 to fail instead (the weekly CI does).
#
# Needs: rocky, python3 (3.8+), the duckdb CLI, docker compose.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Connection settings. The defaults match docker-compose.yml. The user name
# is not 8 bytes long and the password is longer than 8 bytes, so Rocky's
# log redaction covers both.
export PGHOST="${PGHOST:-127.0.0.1}"
export PGPORT="${PGPORT:-55432}"
export PGUSER="${PGUSER:-rocky_reader}"
export PGPASSWORD="${PGPASSWORD:-rocky-poc-reader-pw}"
if [ -z "$PGPASSWORD" ]; then
    echo "FAIL: PGPASSWORD is empty; unset it to use the POC default"
    exit 1
fi
export PGDATABASE="${PGDATABASE:-rocky_src}"

rm -rf .rocky expected poc.duckdb poc.duckdb.wal
mkdir -p expected

echo "=== rocky validate (parse-only, no network) ==="
rocky -c rocky.toml validate

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1 \
    || ! docker compose version >/dev/null 2>&1; then
    if [ "${REQUIRE_DOCKER:-0}" = "1" ]; then
        echo "FAIL: REQUIRE_DOCKER=1 but Docker (with compose) is not reachable"
        exit 1
    fi
    echo
    echo "SKIP: Docker (with compose) is not reachable, so the live tour did not run."
    echo "      Start Docker and re-run, or set REQUIRE_DOCKER=1 to make this a failure."
    exit 0
fi

for tool in duckdb python3; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "FAIL: '$tool' is not on PATH (the connector needs it)"
        exit 1
    fi
done

cleanup() {
    echo
    echo "=== docker compose down -v ==="
    docker compose down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo
echo "=== docker compose up -d --wait (postgres:16-alpine, 20 seeded rows) ==="
# A container left by a killed run would hold the port.
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d --wait

# Runs SQL inside the container, over the local socket.
psql_in_container() {
    docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -q \
        -U "$PGUSER" -d "$PGDATABASE" -c "$1"
}

# load N EXPECTED_ROWS EXPECTED_COMMIT_SEQ
load() {
    local n="$1" rows="$2" seq="$3"
    echo
    echo "=== load $n: rocky load --connector pg_events (expect $rows rows) ==="
    # Debug logging on purpose: the password grep below must cover it.
    RUST_LOG=debug rocky -c rocky.toml --state-path .rocky/state.redb \
        load --connector pg_events --output json \
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

load 1 19 1

echo
echo "=== insert 5 rows with later timestamps (2026-10-01 00:21 to 00:25 UTC) ==="
psql_in_container "INSERT INTO public.events (id, event_type, amount_cents, created_at)
    SELECT g, 'purchase', g * 100,
           timestamptz '2026-10-01 00:00:00+00' + g * interval '1 minute'
    FROM generate_series(21, 25) AS g;"

load 2 5 2
load 3 0 3

echo
echo "=== target table poc.main.events ==="
COUNTS="$(duckdb -readonly -csv -noheader poc.duckdb \
    "SELECT count(*) || ',' || count(DISTINCT id) FROM poc.main.events")"
if [ "$COUNTS" != "24,24" ]; then
    echo "FAIL: expected 24 rows and 24 distinct ids, got '$COUNTS'"
    exit 1
fi
echo "    rows=24 distinct ids=24"
HELD="$(duckdb -readonly -csv -noheader poc.duckdb \
    "SELECT bool_or(id = 20)::INT || ',' || bool_or(id = 25)::INT FROM poc.main.events")"
if [ "$HELD" != "1,0" ]; then
    echo "FAIL: expected id 20 loaded and id 25 held back, got '$HELD'"
    exit 1
fi
echo "    id 20 loaded on load 2; id 25, at the source maximum, waits until a newer row exists"

echo
echo "=== password grep: JSON outputs, debug logs, kept scratch dirs, saved state ==="
# keep_scratch = true, so the three runs' scratch directories must be there:
# otherwise the grep below would pass on an empty tree.
SCRATCH_DIRS="$(find .rocky/landing/pg_events -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')"
if [ "$SCRATCH_DIRS" -lt 3 ]; then
    echo "FAIL: expected 3 kept scratch directories under .rocky/landing/pg_events, found $SCRATCH_DIRS"
    exit 1
fi
if grep -r -a -F -l -- "$PGPASSWORD" expected .rocky; then
    echo "FAIL: the Postgres password appears in the files above"
    exit 1
fi
echo "    0 hits"

echo
echo "POC complete: 3 loads, 24 rows, cursor held back at the source maximum, no password leak."
