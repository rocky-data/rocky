#!/usr/bin/env bash
# Arc 3 — validate the [adapter.retry] circuit-breaker config shape
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected

echo "==> 1. Validate the [adapter.retry] block — asserts the config parses"
rocky validate > expected/validate.json

echo
echo "==> 2. Three states of the breaker (fired from Databricks/Snowflake adapters)"
cat <<'STATES'
                    ┌─────────┐
          success   │ Closed  │  failure
       ┌───────────▶│ (N<thr) │◀───────────┐
       │            └─────────┘            │
       │                 │  Nth failure    │
       │                 ▼                 │
       │            ┌─────────┐            │
       │ success    │  Open   │  trial     │
       │  (new req) │ (rej'd) │  after     │
       │            └─────────┘  timeout   │
       │                 │                 │
       │                 ▼                 │
       │            ┌─────────┐            │
       └───────────▶│HalfOpen │            │
                    │(1 req)  │────────────┘
                    └─────────┘
STATES
echo
echo "  Events emitted on transitions: circuit_breaker_tripped, circuit_breaker_recovered."
echo
echo "==> 3. Note: DuckDB has no transient-error surface"
echo "  To see the breaker *fire*, point [adapter] at Databricks or Snowflake"
echo "  with a bad token or off-network host. The sibling POC"
echo "  07-adapters/02-databricks-materialized-view/ ships a live adapter config."
echo
echo "POC complete: [adapter.retry] config validated."
