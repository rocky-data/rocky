# 02-webhook-slack-preset — Slack-formatted webhook on pipeline failure

> **Category:** 05-orchestration
> **Credentials:** none (uses webhook.site echo URL)
> **Runtime:** < 5s
> **Rocky features:** `[hook.webhooks.<event>] preset = "slack"`, JSON template, HMAC

## What it shows

Declarative webhook on a pipeline event with a built-in `slack` preset
that formats the JSON payload Slack expects (`text`, `blocks`, etc.) so
you don't write the format yourself.

## Why it's distinctive

- **No middleware** — point at any webhook and Rocky formats the payload.
- Built-in presets for Slack, PagerDuty, Datadog, Teams.
- HMAC signing optional via `hmac_secret`.

## Layout

```
.
├── README.md
├── rocky.toml          [hook.webhooks.pipeline_success] preset = "slack"
└── run.sh              Doesn't actually POST (point at https://webhook.site/...)
```

## Run

```bash
./run.sh
```

The default `url` is `https://webhook.site/00000000-0000-0000-0000-000000000000` — replace it with your own webhook.site URL to see the POSTed payload.
