# 02-webhook-slack-preset — Slack-formatted webhook on pipeline completion

> **Category:** 05-orchestration
> **Credentials:** none (uses webhook.site echo URL)
> **Runtime:** < 5s
> **Rocky features:** `[hook.webhooks.<event>] preset = "slack"`, JSON template, HMAC

## What it shows

Declarative webhook on a pipeline lifecycle event with a built-in `slack`
preset that formats the JSON payload Slack expects (`text`, `blocks`, etc.)
so you don't write the format yourself. This POC wires it to
`on_pipeline_complete`; swap the event key to `on_pipeline_error` for a
failure alert instead.

## Why it's distinctive

- **No middleware** — point at any webhook and Rocky formats the payload.
- Built-in presets for Slack, PagerDuty, Datadog, Teams.
- HMAC signing optional via the `secret` field.

## Layout

```
.
├── README.md
├── rocky.toml          [hook.webhooks.on_pipeline_complete] preset = "slack"
├── data/seed.sql       raw__orders.orders (10 rows)
└── run.sh              Doesn't actually POST (point at https://webhook.site/...)
```

## Run

```bash
./run.sh
```

`run.sh` loads the seed, then runs `rocky validate` — which parses and
confirms the `[hook.webhooks.on_pipeline_complete]` block, including the
`slack` preset. The default `url` is
`https://webhook.site/00000000-0000-0000-0000-000000000000`; replace it with
your own webhook.site URL and run a real `rocky run` to see the POSTed
payload.

> **Note:** `rocky hooks list` currently surfaces shell-command hooks only —
> configured webhooks are validated by `rocky validate` but do not appear in
> `hooks list` output (see engine note in the run script).
