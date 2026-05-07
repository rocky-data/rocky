#!/usr/bin/env bash
# Trino adapter — Docker-backed full_refresh demo.
#
# Boots a single-node `trinodb/trino` coordinator, runs a Rocky
# full_refresh transformation that copies `tpch.tiny.nation` (25 rows,
# baked into Trino's built-in tpch connector) into the writable `memory`
# catalog, then asserts row count via Trino's REST API.
#
# Two execution modes:
#   - Docker available + reachable: full live tour (compose up, rocky run,
#     row-count assertion, compose down).
#   - Docker absent or daemon down: parse-only smoke (rocky validate +
#     rocky compile). Mirrors the pattern in
#     pocs/05-orchestration/03-remote-state-s3 / 06-valkey-distributed-cache.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected

HAVE_DOCKER=false
if command -v docker >/dev/null && docker info >/dev/null 2>&1; then
    HAVE_DOCKER=true
fi

echo "=== rocky validate (parse-only — no network) ==="
rocky -c rocky.toml validate

echo
echo "=== rocky compile (parse + type-check models, dialect-agnostic) ==="
rocky compile --models models/ --output json > expected/compile.json 2> expected/compile.log
HAS_ERRORS="$(python3 -c 'import json; print(json.load(open("expected/compile.json"))["has_errors"])')"
if [ "$HAS_ERRORS" != "False" ]; then
    echo "FAIL: compile reported errors — see expected/compile.json"
    exit 1
fi
echo "    compile ok (1 model, 0 diagnostics)"

if ! $HAVE_DOCKER; then
    echo
    echo "Docker daemon not reachable — skipping live tour."
    echo "Install Docker Desktop and re-run to exercise rocky run against Trino."
    echo
    echo "POC complete: validate + compile passed (live tour skipped)."
    exit 0
fi

cleanup() {
    echo
    echo "=== docker compose down ==="
    docker compose down -v 2>&1 || true
}
trap cleanup EXIT

echo
echo "=== docker compose up -d (booting trinodb/trino) ==="
docker compose up -d --wait

echo
echo "=== Waiting for Trino coordinator readiness ==="
READY=false
for i in {1..120}; do
    if curl -fsS http://localhost:8080/v1/info 2>/dev/null | grep -q '"starting":false'; then
        echo "    Trino ready after ${i}s"
        READY=true
        break
    fi
    sleep 1
done
if ! $READY; then
    echo "FAIL: Trino did not report ready within 120s — see 'docker compose logs trino'"
    exit 1
fi

# Helper: submit a SQL statement via Trino's REST API and aggregate
# every page of results. This is the same /v1/statement state machine
# rocky-trino's connector drives, just inlined in shell for the
# row-count assertion.
trino_query() {
    local sql="$1"
    local resp
    resp="$(curl -fsS -X POST \
        -H "X-Trino-User: trino" \
        -H "Content-Type: text/plain" \
        --data-binary "$sql" \
        http://localhost:8080/v1/statement)"
    local data
    data="$(echo "$resp" | python3 -c 'import json,sys; r=json.load(sys.stdin); print(json.dumps(r.get("data") or []))')"
    local next
    next="$(echo "$resp" | python3 -c 'import json,sys; r=json.load(sys.stdin); print(r.get("nextUri") or "")')"
    while [ -n "$next" ]; do
        resp="$(curl -fsS -H "X-Trino-User: trino" "$next")"
        local page
        page="$(echo "$resp" | python3 -c 'import json,sys; r=json.load(sys.stdin); print(json.dumps(r.get("data") or []))')"
        data="$(python3 -c "import json,sys; a=json.loads(sys.argv[1]); b=json.loads(sys.argv[2]); print(json.dumps(a+b))" "$data" "$page")"
        next="$(echo "$resp" | python3 -c 'import json,sys; r=json.load(sys.stdin); print(r.get("nextUri") or "")')"
    done
    echo "$data"
}

echo
echo "=== rocky run (full_refresh — CTAS into memory.rocky_demo) ==="
rocky -c rocky.toml run --output json > expected/run.json
echo "    run ok"

echo
echo "=== verifying row count via Trino REST ==="
ROW_DATA="$(trino_query 'SELECT COUNT(*) FROM "memory"."rocky_demo"."nations_copy"')"
ACTUAL_ROWS="$(python3 -c "import json,sys; d=json.loads(sys.argv[1]); print(d[0][0] if d else '0')" "$ROW_DATA")"

if [ "$ACTUAL_ROWS" != "25" ]; then
    echo "FAIL: expected 25 rows (tpch.tiny.nation), got '$ACTUAL_ROWS'"
    exit 1
fi
echo "    rows = 25 (matches tpch.tiny.nation cardinality)"

echo
echo "POC complete: rocky-trino v0 exercised end-to-end against a real Trino coordinator."
