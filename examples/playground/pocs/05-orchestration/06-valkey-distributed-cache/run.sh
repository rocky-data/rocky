#!/usr/bin/env bash
# Valkey distributed cache — end-to-end demo
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

# Start Valkey (if docker is available)
if command -v docker >/dev/null; then
    echo "=== Starting Valkey ==="
    docker compose up -d 2>&1 || true
    sleep 2
else
    echo "docker not installed; showing config-only demo."
fi

duckdb poc.duckdb < data/seed.sql

rocky validate

echo
echo "=== Cache configuration ==="
echo "  [cache]  valkey_url  → schema/metadata caching in Valkey"
echo "  [state]  backend     → tiered (local redb + Valkey)"
echo "  Tier 1: In-process LRU (memory.rs) — sub-millisecond"
echo "  Tier 2: Valkey (valkey.rs) — shared across instances"
echo "  Tier 3: Source API — warehouse/connector queries"

echo
echo "=== Run pipeline (cache-enabled) ==="
rocky -c rocky.toml -o json run --filter source=events > expected/run.json 2>&1 || true

echo
echo "=== Check Valkey state ==="
if command -v docker >/dev/null && docker compose ps --status running 2>/dev/null | grep -q valkey; then
    docker compose exec -T valkey valkey-cli KEYS 'rocky:*' 2>/dev/null || echo "(no keys yet — state sync requires warehouse adapter)"
    echo
    docker compose exec -T valkey valkey-cli INFO keyspace 2>/dev/null | grep -v "^#" | head -5
fi

echo
echo "POC complete: Valkey cache + tiered state backend configured."
echo "To clean up: docker compose down"
