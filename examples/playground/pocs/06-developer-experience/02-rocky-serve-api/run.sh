#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb

rocky validate

# Start the API server in the background.
rocky serve --models models --port 9876 > expected/serve.log 2>&1 &
PID=$!
trap 'kill -9 $PID 2>/dev/null || true' EXIT

# Wait for the server to come up.
for i in 1 2 3 4 5; do
    if curl -sf http://localhost:9876/api/v1/health >/dev/null; then
        echo "server up after ${i}s"
        break
    fi
    sleep 1
done

echo "=== /api/v1/health ==="
curl -s http://localhost:9876/api/v1/health | tee expected/health.json
echo
echo "=== /api/v1/models ==="
curl -s http://localhost:9876/api/v1/models | tee expected/models.json | head -30
echo
echo "=== /api/v1/models/customer_totals/lineage ==="
curl -s http://localhost:9876/api/v1/models/customer_totals/lineage | tee expected/lineage.json | head -30
echo
echo "=== /api/v1/dag ==="
curl -s http://localhost:9876/api/v1/dag | tee expected/dag.json | head -30

echo
echo "POC complete: rocky serve API exercised."
