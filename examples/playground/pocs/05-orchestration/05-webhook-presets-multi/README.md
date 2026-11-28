# 05-webhook-presets-multi — Multi-Preset Webhooks

> **Category:** 05-orchestration
> **Credentials:** none (placeholder URLs for demo)
> **Runtime:** < 10s
> **Rocky features:** webhook presets (slack, teams, pagerduty, datadog, generic), event hooks, Mustache templates, HMAC signing, retry

## What it shows

All five built-in webhook presets, each on a different lifecycle event:

| Event | Preset | What it does |
|---|---|---|
| `on_pipeline_complete` | **Slack** | Formatted block message on success |
| `on_pipeline_error` | **Teams** | Adaptive card with 3 retries |
| `on_materialize_error` | **PagerDuty** | Incident trigger via Events API v2 |
| `on_drift_detected` | **Datadog** | Event pushed to Datadog Events API |
| `on_anomaly_detected` | **Generic** | Custom Mustache JSON template |

Presets supply default headers and body templates; you only need the `url`. The
`generic` template can use `{{event}}`, `{{pipeline}}`, `{{table}}` and
`{{duration_ms}}`. Per webhook you can set `retry_count`, `retry_delay_ms`,
`timeout_ms` and an HMAC `secret`.

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI (`brew install duckdb`)
- For live webhooks, set the `*_WEBHOOK_URL` / `*_API_KEY` env vars

## Run

```bash
./run.sh
```

The URLs are placeholders. Webhooks fire during real pipeline execution.

## Expected output

`rocky validate` succeeds, `hooks list` reports `total: 0`, and the five presets
are printed by a static echo in `run.sh`, not by the CLI.

```text
=== Configured webhooks ===
{
  "hooks": [],
  "total": 0
}
```

> **Why does `hooks list` show `total: 0`?** `rocky hooks list --output json`
> currently enumerates **shell-command** hooks only (`[hook.on_<event>] command = …`);
> its JSON entries have no representation for webhook presets, so a webhook-only
> config reports an empty list. The five webhooks *are* parsed, preset-resolved,
> and validated — `rocky validate` succeeds — and they fire during pipeline
> execution. The sibling [`02-webhook-slack-preset`](../02-webhook-slack-preset/)
> POC documents the same limitation.
