---
title: Hooks and Webhooks
description: The 18 lifecycle events Rocky fires during a run, and how to attach a shell command or an HTTP webhook to any of them.
sidebar:
  order: 12
---

Rocky fires a lifecycle event at each notable point in a run. Attach a shell script or an HTTP webhook to any event, for a notification, an audit record or your own integration.

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
     warn   → log, continue       X-Rocky-Signature:
     ignore → continue quietly      sha256=<hex digest>
     abort  → `rocky hooks test` only
```

## Lifecycle events

Rocky fires 18 events, in six phases:

| Phase | Event | When |
|---|---|---|
| Pipeline | `pipeline_start` | Pipeline begins |
| | `discover_complete` | Source discovery finishes |
| | `compile_complete` | Compilation finishes |
| | `pipeline_complete` | Pipeline succeeds |
| | `pipeline_error` | Pipeline fails |
| Table | `before_materialize` | Before table copy |
| | `after_materialize` | After table copy |
| | `materialize_error` | Table copy fails |
| Model | `before_model_run` | Before compiled model runs |
| | `after_model_run` | After compiled model runs |
| | `model_error` | Model execution fails |
| Quality | `before_checks` | Quality checks begin |
| | `check_result` | A quality check completes |
| | `after_checks` | All checks finish |
| | `drift_detected` | Schema drift found |
| | `anomaly_detected` | Row count anomaly |
| State | `state_synced` | State store synced |
| Budget | `budget_breach` | Observed run cost or duration exceeds a limit declared in [`[budget]`](/reference/configuration/#budget) |

`budget_breach` also fires when a data product goes over its monthly budget, declared in [`[product_budget.NAME]`](/reference/configuration/#product_budgetname). It fires once for each exceeded limit, at the end of the production run after which the product was first over. It does not fire again while the product stays over. An interrupted run and a backfill do not check budgets, so a breach they cause fires no hook; it still shows as an event in `rocky cost --month`. Its `metadata` then has three more keys, so a hook can tell the two apart:

| Key | Value |
|-----|-------|
| `scope` | `"product_month"`. A run-level breach has no `scope`. |
| `product` | The product name. |
| `month` | The month, `YYYY-MM`, in the project's `[schedule].timezone`. |

`limit_type` is `max_compute_hours`, `max_storage_gb_month` or `max_usd`; `limit` and `actual` are in hours, GB-months or USD. A product breach never changes the run's exit code. Unlike the pipeline events, it also fires for a model-only run such as `rocky run --model X`.

:::note[Event names carry an `on_` prefix in config]
The bare names above (`pipeline_start`, `budget_breach`, …) are what Rocky writes into the `event` field of the JSON context. When you **reference** an event — as a `[hook.*]` config key or as the `rocky hooks test` argument — prefix it with `on_`: `on_pipeline_start`, `on_budget_breach`, and so on. An unknown/unprefixed key is ignored with a warning rather than firing.
:::

### Which events fire for which pipeline type

`pipeline_start`, `pipeline_complete` and `pipeline_error` fire when `rocky run` executes a whole pipeline of any type: replication, transformation, quality, snapshot or load. They do not fire for `rocky run --model X`, for `rocky run --select` with several models, or for a transformation node run under `--dag`. Under `rocky run --dag`, quality, snapshot and load nodes each fire these hooks per node, and transformation nodes fire none. A run that is cancelled (Ctrl-C or SIGTERM; on Windows also Ctrl-Break, console close, logoff or shutdown) or that panics can fire `pipeline_start` with no end event. The other events depend on what the pipeline does.

| Event | Replication | Transformation | Quality, snapshot, load |
|-------|-------------|----------------|-------------------------|
| `pipeline_start`, `pipeline_complete`, `pipeline_error` | Yes | Yes | Yes |
| `discover_complete`, table phase, `drift_detected`, check events | Yes | No | No |
| `compile_complete`, model phase | No | Yes | No |

`pipeline_complete` carries `duration_ms` for every pipeline type. `metadata.table_count` is set for a replication run only. For a transformation run, read the model count from `metadata.model_count` on `compile_complete`.

## Shell hooks

A shell hook runs a command and gives it the event context as JSON on its stdin. The hook can ignore its stdin and exit early:

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
  "run_id": "run-20260402-143000-000",
  "pipeline": "raw",
  "timestamp": "2026-04-02T14:30:00Z",
  "duration_ms": 45200,
  "metadata": {
    "table_count": 20
  }
}
```

### Failure handling

| `on_failure` | Behavior |
|------|----------|
| `abort` | Mark the hook result as abort. Only `rocky hooks test` acts on it. |
| `warn` | Log a warning and continue (default) |
| `ignore` | Silently continue |

`rocky run` ignores every hook outcome. A hook cannot gate a run. Do not use a hook for a deploy freeze or an approval gate. Use hooks for notifications and audit records.

### Security: trust the `command` source

Rocky passes the `command` string to `sh -c` verbatim. It delivers the event context as JSON on **stdin**, never interpolated into the command line, so values such as `run_id` and `event` cannot inject shell metacharacters.

**Never build a hook `command` by string-formatting untrusted input**: a webhook payload, a Fivetran response, a value from a row, or anything else you do not control. Use a static command. To react to dynamic input, pass it through the JSON context to a script you wrote, and let that script quote and validate it.

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

Rocky only adds the header. The receiver verifies it: recompute HMAC-SHA256 over the raw request body with the same secret, and compare it with the header's lower-case hex digest.

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

`rocky hooks list` lists the configured hooks. `rocky hooks test on_pipeline_start` fires a synthetic event context, so you can confirm your script runs and reads the JSON without a real pipeline.
