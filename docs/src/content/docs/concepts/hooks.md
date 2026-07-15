---
title: Hooks and Webhooks
description: Lifecycle events, shell hooks, and webhook integrations
sidebar:
  order: 12
---

Rocky fires lifecycle events at key points during pipeline execution. You can attach shell scripts or HTTP webhooks to any event for notifications, gating, auditing, or custom integrations.

:::note[Not the same as seed hooks]
The lifecycle hooks on this page fire **shell commands and webhooks** on pipeline events. They are separate from the `pre_hook` / `post_hook` fields on a seed sidecar, which run **SQL statements** on the warehouse around a single [`rocky seed`](/reference/cli/#rocky-seed) load.
:::

## Lifecycle events

Events are organized into five phases:

### Pipeline phase

| Event | When | Use case |
|-------|------|----------|
| `pipeline_start` | Pipeline begins | Slack notification, deploy freeze gate |
| `discover_complete` | Source discovery finishes | Log connector/table counts |
| `compile_complete` | Compilation finishes | Validate types before execution |
| `pipeline_complete` | Pipeline succeeds | Success notification, metrics push |
| `pipeline_error` | Pipeline fails | PagerDuty alert, incident creation |

### Table phase

| Event | When | Use case |
|-------|------|----------|
| `before_materialize` | Before table copy | Audit logging |
| `after_materialize` | After table copy | Publish to data catalog |
| `materialize_error` | Table copy fails | Per-table alerting |

### Model phase

| Event | When | Use case |
|-------|------|----------|
| `before_model_run` | Before compiled model runs | Feature flag checks |
| `after_model_run` | After compiled model runs | Lineage metadata push |
| `model_error` | Model execution fails | Debug notification |

### Quality phase

| Event | When | Use case |
|-------|------|----------|
| `before_checks` | Quality checks begin | Mute downstream alerts during expected check windows |
| `check_result` | A quality check completes | Per-check threshold alerting |
| `after_checks` | All checks finish | Aggregate summary to dashboards, gating on total pass rate |
| `drift_detected` | Schema drift found | Schema change notification |
| `anomaly_detected` | Row count anomaly | Data quality alert |

### State phase

| Event | When | Use case |
|-------|------|----------|
| `state_synced` | State store synced | Backup confirmation |

### Budget phase

| Event | When | Use case |
|-------|------|----------|
| `budget_breach` | Observed run cost or duration exceeds a limit declared in [`[budget]`](/reference/configuration/#budget) | Page oncall on overspend; gate downstream runs on `on_breach = "error"` |

:::note[Event names carry an `on_` prefix in config]
The bare names above (`pipeline_start`, `budget_breach`, …) are what Rocky writes into the `event` field of the JSON context. When you **reference** an event — as a `[hook.*]` config key or as the `rocky hooks test` argument — prefix it with `on_`: `on_pipeline_start`, `on_budget_breach`, and so on. An unknown/unprefixed key is ignored with a warning rather than firing.
:::

## Shell hooks

Shell hooks execute a command and pipe the event context as JSON to stdin:

```toml
[[hook.on_pipeline_complete]]
command = "bash scripts/slack-notify.sh"
timeout_ms = 5000
on_failure = "warn"
```

The script receives JSON like:

```json
{
  "event": "pipeline_complete",
  "run_id": "run_20260402",
  "pipeline": "raw",
  "timestamp": "2026-04-02T14:30:00Z",
  "duration_ms": 45200,
  "metadata": {
    "table_count": 20
  }
}
```

### Failure handling

| Mode | Behavior |
|------|----------|
| `abort` | Stop the pipeline if the hook fails |
| `warn` | Log a warning and continue (default) |
| `ignore` | Silently continue |

Use `abort` for gating hooks (deploy freeze, approval gates). Use `warn` or `ignore` for notifications.

### Security: trust the `command` source

The `command` string is passed to `sh -c` verbatim. The event context is delivered to the script as JSON on **stdin** (it is not interpolated into the command line), so the runtime values Rocky exposes (`run_id`, `event`, etc.) cannot inject shell metacharacters into your command.

Still, **never build a hook `command` by string-formatting untrusted input** (a webhook payload, a Fivetran response, a value pulled from a row, anything you don't fully control). Use only static commands or commands templated from values you control yourself. If you need to react to dynamic input, hand it off through the JSON context to a script you wrote, and let that script decide what to do with quoting / validation.

## Webhooks

Webhooks send HTTP requests instead of running shell commands:

```toml
[hook.webhooks.on_pipeline_error]
url = "https://hooks.slack.com/services/T.../B.../xxx"
preset = "slack"
secret = "${WEBHOOK_SECRET}"
```

### Built-in presets

| Preset | Service | Body format |
|--------|---------|-------------|
| `slack` | Slack Incoming Webhook | Slack Block Kit JSON |
| `pagerduty` | PagerDuty Events API v2 | PD event payload |
| `datadog` | Datadog Events API | DD event JSON |
| `teams` | Microsoft Teams Webhook | Adaptive Card JSON |

Presets provide default body templates and headers. Override any field in your config.

### HMAC signing

When `secret` is set, Rocky signs the request body with HMAC-SHA256:

```
X-Rocky-Signature: sha256=<hex-encoded digest>
```

The receiving service can verify the signature to ensure the request came from Rocky.

### Body templates

Custom body templates use Mustache-style syntax:

```toml
body_template = """
{
  "text": "Pipeline {{event}}: {{metadata.tables_copied}} tables copied in {{duration_ms}}ms"
}
"""
```

Supported: `{{field}}`, `{{metadata.key}}`, `{{#if field}}...{{/if}}`.

## Testing hooks

Validate your hook configuration without running a real pipeline:

```bash
# List all configured hooks
rocky hooks list

# Fire a test event
rocky hooks test on_pipeline_start
```

The test command sends a synthetic event context to verify scripts execute correctly.
