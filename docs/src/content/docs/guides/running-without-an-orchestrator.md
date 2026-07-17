---
title: Running Rocky Without an Orchestrator
description: "Schedule Rocky from cron, systemd, GitHub Actions, or a warehouse-native scheduler: exit codes, failure notification, and health checks with no orchestration platform"
sidebar:
  order: 5.5
---

You do not need Dagster, Airflow, or any orchestration platform to run Rocky on a schedule. The engine already owns the parts that are hard to get right, and a plain timer supplies the one part it does not: deciding when to start.

This guide shows how to drive `rocky run` from cron, a systemd timer, GitHub Actions, or a warehouse-native scheduler, how to read the exit codes so your alerting is honest, and how to get failure notifications and health checks without a control plane.

If you already run Dagster or Airflow, keep them. The [`dagster-rocky`](/dagster/resource/) integration is first-class. This page is for the estates where a timer is enough.

## What the engine already does

A single `rocky run` is not a bare SQL script. Inside one invocation the engine:

- **Executes the model DAG in dependency order**, with configurable concurrency across independent branches.
- **Retries and self-heals within the run.** Failed statements are retried up to `max_retries`, and on the Databricks and Snowflake adapters a circuit breaker trips after a run of consecutive failures so one broken warehouse connection does not hammer the rest of the run. See the [retry and circuit-breaker settings](/reference/configuration/#retry).
- **Reports partial success instead of losing good work.** When some models materialize and others fail, the run does not discard what succeeded: it finishes with a partial-success exit code and enumerates the failures, so you keep the current data and know exactly what broke. See [per-table error containment](/advanced/per-table-error-containment/).
- **Records runs into embedded state** (a local redb store), so `rocky history` and the audit trail work with no external database. Run-record persistence is best-effort and most complete for replication and transformation runs today.
- **Resumes a failed replication run** with `rocky run --resume-latest` (or `--resume <run_id>`) — a flag on `rocky run`, not a separate command. It picks up from the latest recorded progress for the pipeline.
- **Deduplicates with idempotency keys.** Pass `rocky run --idempotency-key <key>`; an in-flight or previously *successful* run under the same key is skipped rather than double-applied. By default a *failed* run leaves the key claimable so a retry can proceed; set `dedup_on = "any"` to also skip after a failure (which forgoes retry under that key). Idempotency keys and resume cannot be combined.
- **Fires lifecycle [hooks](/concepts/hooks/)** on pipeline events, so notifications can live in the pipeline definition rather than in the scheduler. Hook coverage is still filling in — the failure hooks fire most reliably on the replication path today — so pair them with the exit-code routing below rather than relying on them alone.

What a timer adds is the trigger and, if you want it, retention of logs. Everything about *how the run behaves* is already in `rocky.toml` and the engine. That is the whole idea: the run's behavior lives with the pipeline definition, not in a separate system you have to keep in sync.

## The exit-code contract

Every recipe below keys off the process exit code. Rocky uses a distinct code per condition so a wrapper script or CI step can branch without parsing output:

| Code | Meaning | Emitted by |
|------|---------|------------|
| `0` | Success | every command |
| `1` | Generic hard failure (config error, unreadable state, or an error raised *after* some models already materialized — a budget breach, say) | most commands |
| `2` | **Partial success** — some models materialized, some failed | `rocky run` |
| `3` | A Critical health check | `rocky doctor` |
| `4` | Compile and tests passed but advisory warnings were emitted | `rocky ci` |
| `130` | Interrupted by SIGINT or SIGTERM | `rocky run` |

For a scheduled `rocky run` you will see `0`, `1`, `2`, or `130`. Codes `3` and `4` come from `rocky doctor` and `rocky ci`, which you may run as a pre-flight (below) or in CI.

### What to alert on

Alert on any non-zero exit. Beyond that, one rule earns its keep:

**Give exit `2` its own channel.** A partial success means the run kept going and produced real, current data for the models that worked, while a subset failed. That is a different operational situation from a hard failure (exit `1`). Note that exit `1` is generic: it often means nothing materialized, but it can also fire *after* some models landed (a budget breach, for one), so inspect the run's `--output json` result or `rocky history` rather than assume the estate is empty. Routing exit `1` and exit `2` to the same place trains people to ignore the alert. A hard failure is a page; a partial success is a ticket for the on-call to look at the failed models before the next run.

`130` (interrupted) usually means a deploy or a machine restart cut the run short; treat it as informational unless it repeats.

A small wrapper makes the routing explicit and is reusable across every scheduler:

```bash
#!/usr/bin/env bash
# rocky-run.sh — run a pipeline and route notifications by exit code.
set -uo pipefail
cd /srv/analytics

rocky run --output json >> /var/log/rocky/analytics.log 2>&1
code=$?

case "$code" in
  0)   ;;                                                   # success, stay quiet
  2)   notify "#data-partial" "Rocky partial success (exit 2): some models failed, run continued" ;;
  130) ;;                                                   # interrupted, informational
  *)   notify "#data-oncall" "Rocky run FAILED (exit $code)" ;;
esac

exit "$code"
```

Replace `notify` with your `curl` to Slack, `mail`, or whatever you already use. `--output json` writes the full per-model result to the log so the on-call can see exactly which models failed without re-running anything.

## cron

The classic timer. The only thing cron does not give you for free is overlap protection: if a run takes longer than the interval, the next tick will start a second run on top of the first. Guard it with `flock`.

```cron
# /etc/cron.d/rocky-analytics
# Run at 03:00 daily. flock -n makes a still-running previous run skip this tick
# rather than piling a second run on top of it.
0 3 * * * dataeng flock -n /var/lock/rocky-analytics.lock /srv/analytics/rocky-run.sh
```

`flock -n` (non-blocking) exits immediately if the lock is held. If you would rather queue the next run than skip it, drop `-n` and `flock` will wait for the lock instead. The wrapper script above owns the exit-code routing, so cron itself only needs to acquire the lock and start it.

If you do not want a wrapper, call the CLI directly and let cron mail you on any non-zero exit via `MAILTO`, but you lose the exit-`2`-specific channel:

```cron
MAILTO=data-oncall@example.com
0 3 * * * dataeng flock -n /var/lock/rocky-analytics.lock rocky -c /srv/analytics/rocky.toml run --output json
```

## systemd timer and service

On a systemd host, a `oneshot` service plus a timer is more observable than cron: you get `systemctl status`, `journalctl` history, and `OnFailure=` handlers.

```ini
# /etc/systemd/system/rocky-analytics.service
[Unit]
Description=Rocky analytics pipeline
After=network-online.target
Wants=network-online.target
# Fire a handler unit on any exit systemd considers a failure.
OnFailure=rocky-analytics-failed@%n.service

[Service]
Type=oneshot
User=dataeng
WorkingDirectory=/srv/analytics
# Pre-flight: fail fast (exit 3) if config or warehouse connectivity is broken.
ExecStartPre=/usr/local/bin/rocky doctor
ExecStart=/srv/analytics/rocky-run.sh
# The wrapper already routes partial success (exit 2) to its own channel, so tell
# systemd that 2 is not a unit failure — otherwise OnFailure fires for it too and
# you get the alert twice. Total failures (exit 1) still trip OnFailure as a backstop.
SuccessExitStatus=2
```

```ini
# /etc/systemd/system/rocky-analytics.timer
[Unit]
Description=Run the Rocky analytics pipeline daily

[Timer]
OnCalendar=*-*-* 03:00:00
Persistent=true
# Persistent=true runs a missed occurrence once on the next boot (e.g. after the
# host was down at 03:00), which matches how a single rocky run behaves: it does
# the current work once, it does not replay a backlog of windows.

[Install]
WantedBy=timers.target
```

Enable it with `systemctl enable --now rocky-analytics.timer`. The `ExecStartPre` guard turns a broken deploy into a clean "service failed to start" instead of a half-run against a bad config. If you prefer the platform to do the routing, drop the wrapper, set `ExecStart=/usr/local/bin/rocky run --output json`, and handle partial-vs-total in the `rocky-analytics-failed@` handler unit by reading `$EXIT_STATUS`.

> These systemd and timer units are illustrative templates; adapt the paths, user, and `OnCalendar` to your host. The exit-code behavior they rely on is verified against the CLI (see the notes at the end of this page).

## GitHub Actions on a schedule

If your warehouse is reachable from GitHub's runners, a scheduled workflow needs no infrastructure of your own. Key off the exit code so a partial success is visible but distinct from a hard failure.

```yaml
# .github/workflows/rocky-nightly.yml
name: Rocky nightly

on:
  schedule:
    - cron: "0 3 * * *"   # 03:00 UTC daily
  workflow_dispatch: {}    # allow manual runs from the Actions tab

jobs:
  run:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Rocky
        run: curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash

      - name: Pre-flight
        run: rocky doctor

      - name: Run pipeline
        id: rocky
        run: |
          set +e
          rocky run --output json | tee run.json
          # ${PIPESTATUS[0]} is rocky's code, not tee's — do not use a bare $? here.
          echo "exit_code=${PIPESTATUS[0]}" >> "$GITHUB_OUTPUT"

      - name: Classify outcome
        if: always()
        run: |
          case "${{ steps.rocky.outputs.exit_code }}" in
            0)   echo "Run succeeded." ;;
            2)   echo "::warning::Partial success — some models failed."; exit 0 ;;
            130) echo "Interrupted."; exit 0 ;;
            *)   echo "::error::Run failed."; exit 1 ;;
          esac
```

Because the run step captures the code instead of failing the job directly, the classify step decides what a red build means. Here a partial success is a warning annotation that keeps the job green (it produced current data), while a total failure fails the job and triggers your normal Actions failure notifications. Flip that policy to taste. Store warehouse credentials as [encrypted secrets](/reference/authentication/) and pass them as environment variables.

This is a scheduled *production* run. For running `rocky ci` on pull requests (compile and test with no warehouse), see the [CI/CD guide](/guides/ci-cd/).

## Databricks Workflows and warehouse-native schedulers

If your warehouse already has a scheduler, you often do not need a separate host at all. Any scheduler that can run a shell command can run Rocky. On Databricks, a Workflow with a shell or Python task installs the binary and calls the CLI:

```bash
# A Databricks task command (or any warehouse-native scheduler's shell step).
set -uo pipefail
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash
rocky doctor
rocky run --output json
```

The task's exit code propagates to the Workflow run, so the platform's own retry and alerting policies apply on top of Rocky's in-run retries. If your scheduler distinguishes exit codes, wire exit `2` to a warning and the rest to a failure exactly as above; if it only sees success or failure, decide whether a partial success should mark the task failed. Snowflake Tasks, Airflow's `BashOperator`, and cloud cron services (Cloud Scheduler, EventBridge Scheduler hitting a small runner) all follow the same shape: install, optional pre-flight, `rocky run`.

> The Databricks and warehouse-native snippets are illustrative. They use the same `rocky run` and `rocky doctor` invocations verified below, but the surrounding task configuration depends on your platform.

## Failure notification without a platform

You do not need an orchestrator's alerting to hear about a failed run. Rocky has two mechanisms that live in the pipeline definition.

### Webhook on pipeline error

A webhook hook posts an HTTP request to Slack, Teams, PagerDuty, or Datadog when the pipeline errors. It sends the event context (run id, event, metadata) directly over HTTP with an optional HMAC signature. Nothing else in your stack has to be running.

```toml
# rocky.toml
[hook.webhooks.on_pipeline_error]
url = "${SLACK_WEBHOOK_URL}"
preset = "slack"
secret = "${WEBHOOK_SECRET}"
```

The `preset` gives you a service-shaped body (Slack Block Kit here) for free; see [Hooks](/concepts/hooks/) for the full list of presets, custom body templates, and HMAC verification.

### A richer digest with rocky brief

For an email or Slack message that carries more than "it failed" (recent runs, drift, freshness, quality, and cost), use a command hook that renders [`rocky brief`](/reference/commands/administration/) and delivers it. `brief --output md` produces a Slack- and email-ready Markdown document:

```toml
# rocky.toml
[[hook.on_pipeline_error]]
command = "/srv/analytics/send-brief.sh"
on_failure = "warn"
```

```bash
#!/usr/bin/env bash
# send-brief.sh — post the estate digest when a run fails.
set -uo pipefail
cd /srv/analytics

# Pass --output md explicitly: when stdout is not a terminal (a hook, a pipe, a
# cron job), rocky defaults to JSON, so you must ask for markdown for the digest.
digest="$(rocky brief --since 24h --output md)"

curl -fsS -X POST "$SLACK_WEBHOOK_URL" \
  -H 'Content-Type: application/json' \
  --data "$(jq -n --arg text "$digest" '{text: $text}')"
```

A webhook hook and a command hook are two different things: the `[hook.webhooks.*]` block posts the event context over HTTP by itself, while a `[[hook.*]]` command block runs a program you provide (which is what lets it shell out to `rocky brief`). Use the webhook for a fast "it failed" ping and the command hook when you want the full digest.

To confirm Rocky picked up either block, run `rocky validate` — it parses the whole config, both hook tables included. `rocky hooks list` prints the *command* hooks Rocky loaded (a `[hook.webhooks.*]` block will not appear there), so a command hook showing up confirms its event key is one Rocky recognizes. An unknown or misspelled `on_<event>` key is skipped with a warning rather than firing.

One detail bites timers specifically. Config values like `${SLACK_WEBHOOK_URL}` are substituted from the environment, and a referenced variable that is not set makes config loading fail outright (`rocky validate` returns `1`, and so does the run). cron and systemd start with a nearly empty environment, so export the secrets your config references in the timer's own environment (`Environment=` or `EnvironmentFile=` for systemd, a sourced file for cron) or the run will not even start.

## Health checks

Two commands turn a blind timer into an observable one.

### rocky doctor as a pre-flight

`rocky doctor` runs config, state, adapter, and auth checks and **exits `3` if any check is Critical**. Putting it before `rocky run` (the `ExecStartPre` and pre-flight steps above) turns a broken config or an unreachable warehouse into a clean, early failure instead of a half-completed run — config problems and an unreachable warehouse are Critical. A degraded or unreadable local state store surfaces as a Warning (exit `0`), so it will not by itself fail the pre-flight. Run the full battery, or scope to a single check with `--check`:

```bash
rocky doctor                  # all checks, exits 3 on any Critical
rocky doctor --check config   # config only, offline and fast
rocky doctor --check auth     # ping the warehouse to catch a rotated credential
```

### rocky history for run archaeology

Runs are recorded in the embedded state store — most completely for replication and transformation runs today. `rocky history` reads it back with no warehouse round-trip, so it works from the same host your timer runs on:

```bash
rocky history                       # recent runs: id, start, status, model count, trigger
rocky history --model fct_revenue   # one model's execution history
rocky history --since 2026-03-01    # runs on or after a date
rocky history --audit               # include the governance audit trail
```

The `status` column tells partial from total after the fact (`Success`, `PartialFailure`, `Failure`), and `trigger` records how the run was started (CLI runs currently record `manual`). Combined with `rocky run --resume-latest` for replication pipelines, this is enough to see what a scheduled run did overnight and pick up a failed one where it left off.

## Where to go next

- [Observability](/guides/observability/) — export traces and metrics over OpenTelemetry so a scheduled estate is visible in Grafana, Tempo, or any OTLP backend, with no UI to host.
- [Hooks](/concepts/hooks/) — the full lifecycle-event surface behind the notification recipes above.
- [Failure modes](/advanced/failure-modes/) — how to read every failure Rocky can report, including partial success.
- [CI/CD integration](/guides/ci-cd/) — `rocky ci` for pull-request checks, the complement to the scheduled runs here.

---

**On verification.** The commands on this page were exercised against the playground pipeline with the current engine build: a clean `rocky run` returns `0`, a run where one model fails while the others materialize returns `2`, and `rocky doctor` returns `3` on a Critical check. `rocky validate`, `rocky hooks list`, and `rocky brief --output md` were run against the hook configuration shown above. Exit codes `1`, `4`, and `130` follow the CLI's documented convention. The systemd, GitHub Actions, and Databricks configurations are illustrative templates around those verified commands: adapt them to your host and platform.
