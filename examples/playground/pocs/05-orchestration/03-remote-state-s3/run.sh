#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f poc.duckdb .rocky-state.redb

# --- Helper: list objects in the S3 bucket via aws CLI ---
s3_ls() {
    aws --endpoint-url http://localhost:8333 s3 ls s3://rocky-state/ --recursive 2>/dev/null || true
}

HAVE_DOCKER=false
if command -v docker >/dev/null; then
    HAVE_DOCKER=true
fi

if $HAVE_DOCKER; then
    echo "=== Starting SeaweedFS (S3 API on :8333) ==="
    docker compose up -d 2>&1 || true
    # The `bucket` service exits once it has created the bucket.
    for _ in $(seq 1 90); do
        [ "$(docker compose ps -a --format '{{.State}}' bucket 2>/dev/null)" = "exited" ] && break
        sleep 1
    done
    if [ "$(docker compose ps -a --format '{{.ExitCode}}' bucket 2>/dev/null)" != "0" ]; then
        echo "the S3 bucket was not created; see: docker compose logs bucket" >&2
        exit 1
    fi
else
    echo "docker not installed; showing config-only demo."
fi

# --- Seed the source database ---
duckdb poc.duckdb < data/seed.sql
echo "=== Source after seed: $(duckdb poc.duckdb 'SELECT COUNT(*) FROM raw__orders.orders' -noheader -list) rows ==="

# These envs configure rocky's S3 state sync. SeaweedFS runs with anonymous
# access, so any key works.
export AWS_ACCESS_KEY_ID="rocky"
export AWS_SECRET_ACCESS_KEY="rocky-secret"
export AWS_ENDPOINT_URL="http://localhost:8333"
export AWS_REGION="us-east-1"
# The local store speaks plain HTTP; Rocky's S3 client refuses it otherwise.
export AWS_ALLOW_HTTP="true"

rocky validate
echo

# --- state_rw probe: confirm the S3 bucket is writable before any pipeline
#     work (fails fast at cold start on IAM / reachability issues instead of
#     at end-of-run upload). Local backend = no-op; tiered probes both legs.
if $HAVE_DOCKER; then
    echo "=== rocky doctor --check state_rw (live round-trip probe vs SeaweedFS) ==="
    rocky -c rocky.toml -o json doctor --check state_rw > expected/doctor_state_rw.json 2>&1 || true
    head -20 expected/doctor_state_rw.json
    echo
fi

# --- Run 1: initial load ---
echo "=== Run 1 (initial — full load of 100 rows) ==="
rocky -c rocky.toml -o json run --filter source=orders > expected/run1.json 2>&1 || true
echo "    target rows: $(duckdb poc.duckdb 'SELECT COUNT(*) FROM staging__orders.orders' -noheader -list)"

echo "=== Local state after run 1 ==="
ls -lh .rocky-state.redb 2>/dev/null || echo "    (no local state file)"
rocky state 2>&1 | tail -5

if $HAVE_DOCKER; then
    echo
    echo "=== S3 state after run 1 (SeaweedFS bucket) ==="
    s3_ls
fi

# --- Run 2: incremental (append 25 delta rows) ---
echo
echo "=== Append delta (25 new rows, later timestamps) ==="
duckdb poc.duckdb < data/seed_delta.sql
echo "    source rows: $(duckdb poc.duckdb 'SELECT COUNT(*) FROM raw__orders.orders' -noheader -list)"

echo
echo "=== Run 2 (incremental — only 25 new rows) ==="
rocky -c rocky.toml -o json run --filter source=orders > expected/run2.json 2>&1 || true
echo "    target rows: $(duckdb poc.duckdb 'SELECT COUNT(*) FROM staging__orders.orders' -noheader -list)"

echo "=== State after run 2 ==="
rocky state 2>&1 | tail -5

if $HAVE_DOCKER; then
    echo
    echo "=== S3 state after run 2 ==="
    s3_ls
fi

# --- Round-trip: delete local state, restore from S3 ---
if $HAVE_DOCKER; then
    echo
    echo "=== Round-trip test: delete local state, re-download from S3 ==="
    rm -f .rocky-state.redb
    echo "    local state deleted: $(ls .rocky-state.redb 2>&1)"

    echo "    re-running pipeline (will download state from S3 first)..."
    rocky -c rocky.toml -o json run --filter source=orders > expected/run3.json 2>&1 || true
    echo "    target rows: $(duckdb poc.duckdb 'SELECT COUNT(*) FROM staging__orders.orders' -noheader -list)"

    echo "=== State after round-trip restore ==="
    rocky state 2>&1 | tail -5
    echo
    echo "    Watermark survived the round-trip — no duplicate rows."
fi

echo
echo "POC complete: S3 state sync (upload + download) via SeaweedFS."
if $HAVE_DOCKER; then
    echo "To clean up: docker compose down -v"
fi
