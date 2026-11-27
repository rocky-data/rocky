# 02-webhook-slack-preset — Slack-formatted webhook on pipeline completion

> **Category:** 05-orchestration
> **Credentials:** none (uses webhook.site echo URL)
> **Runtime:** < 5s
> **Rocky features:** `[hook.webhooks.<event>] preset = "slack"`, JSON template, HMAC

## What it shows

A webhook on `on_pipeline_complete` whose built-in `slack` preset formats the
Slack JSON payload (`text`, `blocks`, etc.). Swap the event key to
`on_pipeline_error` for failure alerts. Presets also exist for PagerDuty, Datadog
and Teams; HMAC signing is optional via `secret`.

## Run

```bash
./run.sh
```

`run.sh` does not POST: it loads the seed and runs `rocky validate`, which
checks the webhook block and preset. The default `url` is
`https://webhook.site/00000000-0000-0000-0000-000000000000`. Replace it with
your own webhook.site URL and run `rocky run` to see the payload.

> **Note:** `rocky hooks list` currently surfaces shell-command hooks only —
> configured webhooks are validated by `rocky validate` but do not appear in
> `hooks list` output (see engine note in the run script).
