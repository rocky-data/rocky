#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

duckdb poc.duckdb < data/seed.sql

rocky validate

# Scenario 1: fresh key → run claims it, proceeds.
# Like POC 04, the DuckDB execution path hits "no discovery adapter"
# before reaching materialization, so the run exits non-zero. The
# idempotency claim still happens: the key enters `InFlight` state
# inside the state store.
KEY="fivetran_sync:demo_connector:2026-04-23T10:00:00Z"
rocky -c rocky.toml -o json run \
  --filter source=orders \
  --idempotency-key "$KEY" > expected/run1_fresh.json || true
STATUS1=$(jq -r '.status // "error"' expected/run1_fresh.json)
KEY1=$(jq -r '.idempotency_key // "none"' expected/run1_fresh.json)
echo "Run 1 (fresh key, orders)      status = $STATUS1  (key echoed: $KEY1)"

# Scenario 2: same key again → idempotency short-circuit.
# The claim step inspects the prior entry (InFlight from Run 1 since
# the DuckDB path didn't finalize). The second caller sees an in-flight
# claim that's still within TTL and exits with skipped_in_flight plus
# the prior run_id.
#
# On a fully-successful Run 1 (tiered/valkey backend or real pipeline)
# the entry would transition to Succeeded, and Run 2 would see
# skipped_idempotent instead. The mechanism is identical; only the
# state transition differs.
rocky -c rocky.toml -o json run \
  --filter source=orders \
  --idempotency-key "$KEY" > expected/run2_repeat.json || true
STATUS2=$(jq -r '.status // "error"' expected/run2_repeat.json)
SKIPPED_BY=$(jq -r '.skipped_by_run_id // "none"' expected/run2_repeat.json)
echo "Run 2 (same key, orders)       status = $STATUS2  (skipped_by = $SKIPPED_BY)"

# Scenario 3: different key → claims fresh, proceeds (same end state as 1).
NEW_KEY="fivetran_sync:demo_connector:2026-04-23T11:00:00Z"
rocky -c rocky.toml -o json run \
  --filter source=customers \
  --idempotency-key "$NEW_KEY" > expected/run3_new_key.json || true
STATUS3=$(jq -r '.status // "error"' expected/run3_new_key.json)
KEY3=$(jq -r '.idempotency_key // "none"' expected/run3_new_key.json)
echo "Run 3 (new key, customers)     status = $STATUS3  (key echoed: $KEY3)"

echo
# Validate the dedup mechanism:
# Run 2 MUST short-circuit (skipped_idempotent or skipped_in_flight)
# and reference Run 1's run_id.
if [[ "$STATUS2" != "SkippedIdempotent" && "$STATUS2" != "SkippedInFlight" ]]; then
  echo "FAIL: Run 2 expected a skipped_* status but got '$STATUS2'"
  exit 1
fi
if [[ "$SKIPPED_BY" == "none" ]]; then
  echo "FAIL: Run 2 should reference Run 1's run_id in skipped_by_run_id"
  exit 1
fi
echo "POC complete: --idempotency-key dedups the second invocation (status=$STATUS2)"
echo "              while a different key (Run 3) proceeds independently."
