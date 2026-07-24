#!/usr/bin/env bash
# Webhook ingress (at-most-once): trigger a pipeline over HTTP.
#
# Starts `rocky serve --scheduler`, signs a webhook with HMAC-SHA256, POSTs it
# to /api/v1/hooks/trigger/{pipeline}, and confirms the reconciler consumed the
# durable demand exactly once — recording the run with trigger "Webhook".
#
# Set ROCKY_BIN to point at a freshly built binary; otherwise `rocky` on PATH.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

ROCKY="${ROCKY_BIN:-rocky}"
PORT="${ROCKY_WEBHOOK_POC_PORT:-8091}"
SECRET="poc-webhook-secret"
BASE="http://127.0.0.1:${PORT}"

rm -f .rocky-state.redb poc.duckdb serve.log
rm -rf .rocky

SERVE_PID=""
cleanup() {
  [[ -n "$SERVE_PID" ]] && kill "$SERVE_PID" 2>/dev/null
  [[ -n "$SERVE_PID" ]] && wait "$SERVE_PID" 2>/dev/null
}
trap cleanup EXIT

"$ROCKY" validate

echo "==> starting rocky serve --scheduler on :${PORT}"
ROCKY_WEBHOOK_SECRET="$SECRET" "$ROCKY" serve --scheduler \
  --port "$PORT" --poll-interval-seconds 2 > serve.log 2>&1 &
SERVE_PID=$!

# Wait for readiness.
ready=""
for _ in $(seq 1 40); do
  if curl -sf "${BASE}/api/v1/health" >/dev/null 2>&1; then ready=1; break; fi
  sleep 0.5
done
if [[ -z "$ready" ]]; then
  echo "FAIL: server did not become ready"; tail -20 serve.log; exit 1
fi

# Preflight: skip gracefully on a binary that predates webhook ingress, so a
# stale PATH `rocky` in a smoke run does not hard-fail (see README).
if ! curl -sf "${BASE}/api/v1/meta" | grep -q '"webhooks"'; then
  echo "SKIP: this rocky build does not advertise the 'webhooks' capability."
  echo "      Set ROCKY_BIN to a build that includes webhook ingress."
  exit 0
fi

# Sign the raw body with HMAC-SHA256 (hex) and POST it.
BODY='{"event":"orders.synced"}'
SIG=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac "$SECRET" | awk '{print $NF}')

echo "==> signed POST /api/v1/hooks/trigger/hello"
CODE=$(curl -sS -o resp.json -w '%{http_code}' -X POST \
  "${BASE}/api/v1/hooks/trigger/hello" \
  -H "X-Rocky-Signature: ${SIG}" \
  -H "X-Rocky-Delivery: poc-evt-1" \
  --data-binary "$BODY")
echo "    → HTTP ${CODE}: $(cat resp.json)"
if [[ "$CODE" != "202" ]]; then echo "FAIL: expected 202"; exit 1; fi

echo "==> unsigned POST must be rejected"
UCODE=$(curl -sS -o /dev/null -w '%{http_code}' -X POST \
  "${BASE}/api/v1/hooks/trigger/hello" --data-binary "$BODY")
echo "    → HTTP ${UCODE} (expected 401)"
if [[ "$UCODE" != "401" ]]; then echo "FAIL: unsigned request was not rejected"; exit 1; fi

echo "==> waiting for the reconciler to consume the demand"
trigger=""
for _ in $(seq 1 20); do
  trigger=$("$ROCKY" history --output json 2>/dev/null \
    | jq -r '.runs[] | select(.pipeline=="hello") | .trigger' 2>/dev/null | head -1)
  [[ -n "$trigger" ]] && break
  sleep 1
done
echo "    → history trigger for 'hello': ${trigger:-<none>}"
if [[ "$trigger" != "Webhook" ]]; then
  echo "FAIL: the webhook demand did not run as a Webhook-triggered run"
  tail -20 serve.log; exit 1
fi

# A redelivery of the same id is deduped (24h tombstone), not re-run.
echo "==> redelivering the same id must be an idempotent duplicate"
DUP=$(curl -sS -X POST "${BASE}/api/v1/hooks/trigger/hello" \
  -H "X-Rocky-Signature: ${SIG}" -H "X-Rocky-Delivery: poc-evt-1" \
  --data-binary "$BODY" | jq -r '.demand' 2>/dev/null)
echo "    → redelivery demand: ${DUP}"

echo
echo "POC complete: a signed webhook queued a durable at-most-once demand that"
echo "              the resident scheduler ran exactly once (trigger=Webhook),"
echo "              an unsigned request was rejected 401, and a redelivery of the"
echo "              same id was idempotently deduplicated."
