---
title: Hooks and Webhooks
description: The 18 lifecycle events Rocky fires during a run, and how to attach a shell command or an HTTP webhook to any of them.
sidebar:
  order: 12
---

Rocky fires a lifecycle event at each notable point in a run. Attach a shell script or an HTTP webhook to any event. Use it to send a notification, gate the run, write an audit record, or drive your own integration.

:::note[Not the same as seed hooks]
The lifecycle hooks on this page fire **shell commands and webhooks** on pipeline events. They are separate from the `pre_hook` / `post_hook` fields on a seed sidecar, which run **SQL statements** on the warehouse around a single [`rocky seed`](/reference/cli/#rocky-seed) load.
:::

## How a hook is dispatched

Every event follows the same path, whichever kind of hook you attach to it.

```
  a lifecycle event fires
            │
            │ Rocky builds the event context as JSON:
            │ { event, run_id, pipeline, timestamp, duration_ms,
            │   metadata }
            │
      ┌─────┴──────────────────────┐
      ▼                            ▼
 ┌───────────────────┐     ┌───────────────────────┐
 │ shell hook        │     │ webhook               │
 │ [[hook.on_*]]     │     │ [hook.webhooks.on_*]  │
 ├───────────────────┤     ├───────────────────────┤
 │ sh -c "<command>" │     │ HTTP POST to `url`    │
 │ context on STDIN  │     │ body from a preset or │
 │                   │     │ your body_template    │
 └─────────┬─────────┘     └───────────┬───────────┘
           │ the hook fails            │ `secret` is set
           ▼                           ▼
   on_failure decides:          Rocky signs the body:
     abort  → stop the run        X-Rocky-Signature:
     warn   → log, continue         sha256=<hex digest>
     ignore → continue quietly
```

## Lifecycle events

Rocky fires 18 events, grouped into six phases:

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

A shell hook runs a command and pipes the event context to its stdin as JSON:

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

Use `abort` for a gating hook, such as a deploy freeze or an approval gate. Use `warn` or `ignore` for a notification.

### Security: trust the `command` source

Rocky passes the `command` string to `sh -c` verbatim. It delivers the event context to your script as JSON on **stdin**, never interpolated into the command line. The runtime values Rocky exposes, such as `run_id` and `event`, therefore cannot inject shell metacharacters into your command.

Even so, **never build a hook `command` by string-formatting untrusted input**. That covers a webhook payload, a Fivetran response, a value pulled from a row, and anything else you do not fully control. Use a static command, or template it from values you control yourself.

To react to dynamic input, pass it through the JSON context to a script you wrote. Let that script decide how to quote and validate it.

## Webhooks

A webhook sends an HTTP request instead of running a shell command:

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

Each preset supplies a default body template and the headers that service expects. Override any field in your config.

### HMAC signing

When you set `secret`, Rocky signs the request body with HMAC-SHA256 and sends the digest in a header:

```
X-Rocky-Signature: sha256=<hex-encoded digest>
```

Rocky only adds the header. Verifying it is the receiving service's job, and Rocky cannot tell whether it happens. To verify, recompute HMAC-SHA256 over the raw request body with the same secret. Compare your result against the header's lower-case hex digest.

### Body templates

A custom body template uses Mustache-style syntax:

```toml
body_template = """
{
  "text": "Pipeline {{event}}: {{metadata.tables_copied}} tables copied in {{duration_ms}}ms"
}
"""
```

Supported: `{{field}}`, `{{metadata.key}}`, `{{#if field}}...{{/if}}`.

## Testing hooks

Check your hook configuration without running a real pipeline:

```bash
# List all configured hooks
rocky hooks list

# Fire a test event
rocky hooks test on_pipeline_start
```

`rocky hooks test` sends a synthetic event context, so you can confirm your script runs and reads the JSON correctly.
