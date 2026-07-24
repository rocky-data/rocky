# Webhook ingress (at-most-once)

Trigger a Rocky pipeline over HTTP. `rocky serve --scheduler` exposes
`POST /api/v1/hooks/trigger/{pipeline}`; a signed webhook queues a **durable,
at-most-once** run demand that the resident reconciler consumes on its next
tick.

## Run it

```bash
./run.sh
```

The script starts the server, signs a webhook body with HMAC-SHA256, POSTs it,
and confirms the pipeline ran with `trigger: "Webhook"` in `rocky history`.

Point it at a freshly built binary with `ROCKY_BIN=/path/to/rocky ./run.sh`.
If the binary predates webhook ingress (no `webhooks` capability at
`/api/v1/meta`), the script prints `SKIP` and exits cleanly rather than failing.

## What it demonstrates

- **HMAC auth, not the Bearer token.** The webhook route is authenticated by an
  `X-Rocky-Signature` HMAC-SHA256 (hex) over the raw body, keyed on
  `ROCKY_WEBHOOK_SECRET`. An unsigned request is `401`.
- **Durable, at-most-once delivery.** The demand is `fsync`'d to
  `.rocky/pending-demands/` before the `202`, so a crash never loses an accepted
  webhook. The reconciler consumes it exactly once — one attempt, no retry on
  the delivery side.
- **Idempotent redelivery.** An `X-Rocky-Delivery` id deduplicates a redelivery
  for 24 hours after consumption, so a sender that retries the same event does
  not double-fire the pipeline.

## The signature

```bash
BODY='{"event":"orders.synced"}'
SIG=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac "$ROCKY_WEBHOOK_SECRET" | awk '{print $NF}')
curl -X POST http://127.0.0.1:8091/api/v1/hooks/trigger/hello \
  -H "X-Rocky-Signature: $SIG" -H "X-Rocky-Delivery: evt-1" --data-binary "$BODY"
```

## Note on delivery semantics

Webhook delivery is at-most-once: the one loss window is a reconciler crash
*after* it has claimed a demand *and* the child run also dying before it records
an outcome. For a webhook-only pipeline, pair the trigger with a `freshness`
schedule as a backstop. See
[Running without an orchestrator](../../../../docs/src/content/docs/guides/running-without-an-orchestrator.md).
