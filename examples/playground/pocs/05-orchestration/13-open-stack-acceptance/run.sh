#!/usr/bin/env bash
# Open-stack acceptance journey: one project, scripted, every step asserted.
#
#   1  load 1      rocky load --connector pg_events          19 rows from Postgres
#   2  run 1       rocky run --pipeline events               source 19 in range, output 3 in range,
#                                                            budget breached, breach hook fires once
#   3  status 1    rocky product status                      on time, no estimate yet
#   4  runs 2-5    rocky run x4                              no new breach event; 5 runs for an estimate
#   5  load 2      insert 5 rows, rocky load                 5 rows, the table now holds 24
#   6  wait        until the open minute deadline passes
#   7  run 6       rocky run                                 source 24 above_max (warning), late delivery
#   8  status 2    rocky product status                      late_by_seconds > 0, estimate known, at_risk
#   9  load 3      rocky load                                0 rows, state still commits
#  10  tick        rocky tick                                anchors the 23:59 cron; nothing is due
#  11  cost        rocky cost --month <YYYY-MM> --by product priced, breached, one event at run 1
#  12  serve       GET /api/v1/cost/month, /products/<name>, /schedule
#                                                            the schedule warns the cron starts too late
#  13  leak check  the Postgres password is in no output, log or state file
#
# Docker absent: `rocky validate` only, then exit 0 with SKIP. Set
# REQUIRE_DOCKER=1 to fail instead (the weekly CI does).
#
# Needs: rocky, python3 (3.8+), the duckdb CLI, curl, docker compose.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

ROCKY="${ROCKY_BIN:-rocky}"
PORT="${ROCKY_ACCEPTANCE_PORT:-8093}"
BASE="http://127.0.0.1:${PORT}"

# Connection settings. The defaults match docker-compose.yml. The password is
# longer than 8 bytes, so Rocky's log redaction covers it.
export PGHOST="${PGHOST:-127.0.0.1}"
export PGPORT="${PGPORT:-55433}"
export PGUSER="${PGUSER:-rocky_reader}"
export PGPASSWORD="${PGPASSWORD:-rocky-acceptance-reader-pw}"
export PGDATABASE="${PGDATABASE:-rocky_src}"
if [ -z "$PGPASSWORD" ]; then
    echo "FAIL: PGPASSWORD is empty; unset it to use the default"
    exit 1
fi

rm -rf .rocky expected poc.duckdb poc.duckdb.wal
mkdir -p expected

step() {
    echo
    echo "=== $* ==="
}

step "rocky validate (parse-only, no network)"
"$ROCKY" -c rocky.toml validate

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1 \
    || ! docker compose version >/dev/null 2>&1; then
    if [ "${REQUIRE_DOCKER:-0}" = "1" ]; then
        echo "FAIL: REQUIRE_DOCKER=1 but Docker (with compose) is not reachable"
        exit 1
    fi
    echo
    echo "SKIP: Docker (with compose) is not reachable, so the journey did not run."
    echo "      Start Docker and re-run, or set REQUIRE_DOCKER=1 to make this a failure."
    exit 0
fi

for tool in duckdb python3 curl; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "FAIL: '$tool' is not on PATH"
        exit 1
    fi
done

SERVE_PID=""
cleanup() {
    if [ -n "$SERVE_PID" ]; then
        kill "$SERVE_PID" 2>/dev/null || true
        wait "$SERVE_PID" 2>/dev/null || true
    fi
    echo
    echo "=== docker compose down -v ==="
    docker compose down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

check() {
    python3 check.py "$@"
}

# Every rocky call shares one state store and writes its JSON to expected/.
rocky_json() {
    local name="$1"
    shift
    RUST_LOG="${RUST_LOG:-info}" "$ROCKY" -c rocky.toml --state-path .rocky/state.redb \
        --output json "$@" > "expected/$name.json" 2> "expected/$name.log" || {
        echo "FAIL: rocky $* exited non-zero. Last log lines:"
        tail -n 20 "expected/$name.log"
        exit 1
    }
}

# retry N CMD...: runs CMD up to N times, 2 s, 4 s, 8 s ... apart.
retry() {
    local tries="$1" n=1 delay=2
    shift
    until "$@"; do
        if [ "$n" -ge "$tries" ]; then
            return 1
        fi
        echo "    attempt $n failed; retrying in ${delay}s"
        sleep "$delay"
        n=$((n + 1))
        delay=$((delay * 2))
    done
}

step "fetch the two network dependencies, with retries"
# The Postgres image from Docker Hub, and the DuckDB postgres extension from
# extensions.duckdb.org, into the cache next to the S-2 connector's main.py.
docker image inspect postgres:16-alpine >/dev/null 2>&1 \
    || retry 4 docker compose pull --quiet postgres || {
    echo "FAIL: could not pull the Postgres image after 4 attempts"
    exit 1
}
EXT_DIR="$(cd ../../07-adapters/08-postgres-child-connector/connector && pwd)/.duckdb-extensions"
retry 4 duckdb :memory: -c "SET extension_directories = ['$EXT_DIR']; INSTALL postgres;" >/dev/null || {
    echo "FAIL: could not install the DuckDB postgres extension after 4 attempts"
    exit 1
}
echo "    ready"

# The journey takes about 4 minutes. Started after 23:50 UTC, it could cross
# midnight (a new cost month) or reach the pipeline's 23:59 cron at the tick.
python3 - <<'PY'
import time
from datetime import datetime, timezone
now = datetime.now(timezone.utc)
if now.hour == 23 and now.minute >= 50:
    wait = (60 - now.minute) * 60 - now.second + 5
    print("    started near midnight UTC; sleeping %ds" % wait, flush=True)
    time.sleep(wait)
PY

step "docker compose up -d --wait (postgres:16-alpine, 20 seeded rows)"
docker compose down -v >/dev/null 2>&1 || true
retry 2 docker compose up -d --wait

step "1. load 1: rocky load --connector pg_events (expect 19 rows)"
RUST_LOG=debug rocky_json load-1 load --connector pg_events
check load expected/load-1.json 19 1

step "2. run 1: rocky run --pipeline events"
rocky_json run-1 run --pipeline events
check run expected/run-1.json first
RUN1_ID="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["product_budgets"][0]["breach_event"]["run_id"])' expected/run-1.json)"
check hook expected/budget-breach-hook.jsonl

step "3. rocky product status events_by_type (one delivery)"
rocky_json status-1 product status events_by_type
check status expected/status-1.json first

step "4. runs 2-5: four more runs, so the delivery estimate has 5 durations"
for n in 2 3 4 5; do
    rocky_json "run-$n" run --pipeline events
    check run "expected/run-$n.json" quiet
done
check hook expected/budget-breach-hook.jsonl

step "5. load 2: insert 5 rows in Postgres, then rocky load (expect 5 rows)"
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -q -U "$PGUSER" -d "$PGDATABASE" -c \
    "INSERT INTO public.events (id, event_type, amount_cents, created_at)
     SELECT g, 'purchase', g * 100, timestamptz '2026-10-01 00:00:00+00' + g * interval '1 minute'
     FROM generate_series(21, 25) AS g;"
RUST_LOG=debug rocky_json load-2 load --connector pg_events
check load expected/load-2.json 5 2

step "6. wait until the open deadline passes"
# deliver_by is every minute. The last delivery covered the next minute, so
# the deadline after it is still open. Wait past it, plus a margin.
WAIT_S="$(python3 - <<'PY'
from datetime import datetime, timezone
# Run 5 finished at or before now, so it covered a boundary no later than
# the next one. The open deadline is at most the boundary after that. Waking
# 60 s past the next boundary, plus a margin, puts run 6 after it.
now = datetime.now(timezone.utc)
print((60 - now.second) + 60 + 3)
PY
)"
echo "    sleeping ${WAIT_S}s"
sleep "$WAIT_S"

step "7. run 6: rocky run --pipeline events (source above range, late delivery)"
rocky_json run-6 run --pipeline events
check run expected/run-6.json late

step "8. rocky product status events_by_type (late delivery, known estimate)"
rocky_json status-2 product status events_by_type
check status expected/status-2.json late

step "9. load 3: rocky load (no new row; expect 0 rows, commit_seq 3)"
RUST_LOG=debug rocky_json load-3 load --connector pg_events
check load expected/load-3.json 0 3

step "10. rocky tick: the scheduler anchors the pipeline's cron"
rocky_json tick tick
check tick expected/tick.json

MONTH="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["cost_month"]["month"])' expected/status-2.json)"

step "11. rocky cost --month $MONTH --by product"
rocky_json cost-month cost --month "$MONTH" --by product
check cost expected/cost-month.json "$RUN1_ID"

step "12. rocky serve: the Delivery and cost panel's HTTP API"
"$ROCKY" -c rocky.toml --state-path .rocky/state.redb serve --port "$PORT" \
    > expected/serve.log 2>&1 &
SERVE_PID=$!
ready=""
for _ in $(seq 1 60); do
    if curl -sf "$BASE/api/v1/health" >/dev/null 2>&1; then ready=1; break; fi
    sleep 0.5
done
# A health 200 from another server on the same port would pass the loop.
# Our serve fails to bind then, so it must still be alive.
if ! kill -0 "$SERVE_PID" 2>/dev/null; then
    ready=""
fi
if [ -z "$ready" ]; then
    echo "FAIL: rocky serve did not become ready"
    tail -n 20 expected/serve.log
    exit 1
fi
curl -sf "$BASE/api/v1/cost/month?month=$MONTH" > expected/api-cost-month.json
check api-cost expected/api-cost-month.json expected/cost-month.json
curl -sf "$BASE/api/v1/products/events_by_type" > expected/api-product.json
check api-product expected/api-product.json expected/status-2.json
curl -sf "$BASE/api/v1/schedule" > expected/api-schedule.json
check api-schedule expected/api-schedule.json

step "13. password grep: JSON outputs, logs, scratch dirs, saved state"
# grep exits 1 for no hit, 0 for a hit and 2 for a read error. Only 1 passes.
rc=0
grep -r -a -F -l -- "$PGPASSWORD" expected .rocky || rc=$?
if [ "$rc" -eq 0 ]; then
    echo "FAIL: the Postgres password appears in the files above"
    exit 1
elif [ "$rc" -ne 1 ]; then
    echo "FAIL: the password grep could not read every file (exit $rc)"
    exit 1
fi
echo "    0 hits"

echo
echo "ACCEPTANCE PASS: connector, source and output volume, delivery on time and late,"
echo "                 monthly cost by product, budget breach and hook, HTTP API."
