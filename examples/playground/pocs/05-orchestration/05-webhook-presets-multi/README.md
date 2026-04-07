# 05-webhook-presets-multi — Multi-Preset Webhooks

> **Category:** 05-orchestration
> **Credentials:** none (placeholder URLs for demo)
> **Runtime:** < 10s
> **Rocky features:** webhook presets (slack, teams, pagerduty, datadog, generic), event hooks, Mustache templates, HMAC signing, retry

## What it shows

All five built-in webhook presets configured on different pipeline lifecycle events:

| Event | Preset | What it does |
|---|---|---|
| `on_pipeline_complete` | **Slack** | Formatted block message on success |
| `on_pipeline_error` | **Teams** | Adaptive card with 3 retries |
| `on_materialize_error` | **PagerDuty** | Incident trigger via Events API v2 |
| `on_drift_detected` | **Datadog** | Event pushed to Datadog Events API |
| `on_anomaly_detected` | **Generic** | Custom Mustache JSON template |

Each preset provides sensible defaults (headers, body format) — you only need to set the `url`.

## Why it's distinctive

- **5 presets in one config** — shows the full webhook ecosystem, not just Slack
- **Event-specific routing** — different services for different severity levels
- **Custom templates** — `generic` preset with Mustache variables (`{{event}}`, `{{pipeline}}`, `{{table}}`, `{{duration_ms}}`)
- **Retry + timeout** — per-webhook retry_count, retry_delay_ms, timeout_ms
- **HMAC signing** — optional `secret` field for webhook signature verification
- Existing POC [`02-webhook-slack-preset`](../02-webhook-slack-preset/) only shows Slack; this covers all presets

## Layout

```
.
├── README.md         this file
├── rocky.toml        pipeline + 5 webhook hooks (one per preset)
├── run.sh            end-to-end demo (config validation + hooks list)
└── data/
    └── seed.sql      sample events (200 rows)
```

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI (`brew install duckdb`)
- For live webhooks, set the `*_WEBHOOK_URL` / `*_API_KEY` env vars

## Run

```bash
./run.sh
```

## Expected output

```text
Configured webhooks:
  5 hooks registered

Webhook presets configured:
  on_pipeline_complete  → Slack     (formatted blocks)
  on_pipeline_error     → Teams     (adaptive card, 3 retries)
  on_materialize_error  → PagerDuty (incident trigger)
  on_drift_detected     → Datadog   (event API)
  on_anomaly_detected   → Generic   (custom JSON template)

POC complete: 5 webhook presets configured across different event types.
```

## What happened

1. `rocky validate` parsed the config including all 5 webhook definitions
2. `rocky hooks list` enumerated the registered hooks with their presets
3. Each preset provides default HTTP headers and body templates:
   - **Slack:** Slack Block Kit JSON with `text` field
   - **Teams:** Adaptive Card JSON payload
   - **PagerDuty:** Events API v2 format with `routing_key`
   - **Datadog:** Events API format with `title`, `text`, `tags`
   - **Generic:** Raw JSON context (or custom `body_template`)
4. Webhooks fire during actual pipeline execution — this demo uses placeholder URLs

## Related

- Slack-only webhook POC: [`05-orchestration/02-webhook-slack-preset`](../02-webhook-slack-preset/)
- Shell hooks POC: [`05-orchestration/01-shell-hooks`](../01-shell-hooks/)
- Webhook source: `engine/crates/rocky-core/src/hooks/webhook.rs`
- Preset definitions: `engine/crates/rocky-core/src/hooks/presets.rs`
