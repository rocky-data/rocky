---
title: Running Rocky Without an Orchestrator
description: "Run Rocky on a schedule from cron, a systemd timer, GitHub Actions, or your warehouse's own scheduler. Covers the exit codes, failure alerts, and health checks."
sidebar:
  order: 10
---

You do not need Dagster, Airflow, or any other orchestration platform to run Rocky on a schedule. A plain timer decides when to start. The engine does the rest.

You get:

- `rocky run` driven from cron, a systemd timer, GitHub Actions, or a warehouse-native scheduler;
- exit codes that let your alerting tell a partial success from a hard failure;
- failure notifications and health checks with no control plane;
- optional native scheduling with `rocky tick` (experimental).

| You want | Use | Section |
|---|---|---|
| A timer on a Linux host | cron or a systemd timer | [cron](#cron), [systemd](#systemd-timer-and-service) |
| No host at all | GitHub Actions or a warehouse scheduler | [GitHub Actions](#github-actions-on-a-schedule), [Databricks](#databricks-workflows-and-warehouse-native-schedulers) |
| An alert on failure | A webhook hook or `rocky brief` | [Failure notification](#failure-notification-without-a-platform) |
| Pre-flight and history | `rocky doctor`, `rocky history` | [Health checks](#health-checks) |
| Schedules declared in `rocky.toml` | `rocky tick` | [Native scheduling](#native-scheduling-with-rocky-tick-experimental) |

If you already run Dagster or Airflow, keep them. The [`dagster-rocky`](/dagster/resource/) integration is fully supported.

## What one `rocky run` already does

A single `rocky run` is not a bare SQL script. A timer adds only the trigger, and log retention if you want it. The run's behavior stays in `rocky.toml` and the engine.

| Behavior | What it does |
|---|---|
| DAG order | Runs models in dependency order. Independent branches run concurrently, at a concurrency you configure. |
| Retry | Retries a failed statement up to `max_retries`. On Databricks and Snowflake a circuit breaker trips after consecutive failures. See the [retry settings](/reference/configuration/#retry). |
| Keep good work | Keeps models that materialized when others fail. The run exits with the partial-success code and lists the failures. See [per-table error containment](/advanced/per-table-error-containment/). |
| Local records | Writes runs to the embedded [state store](/reference/glossary/#state-store) (a local redb file). `rocky history` and the audit trail need no external database. Persistence is best-effort, and most complete for replication and transformation runs today. |
| Resume | `rocky run --resume-latest` or `--resume <run_id>` resumes a failed replication run from recorded progress. A complete checkpoint without a terminal run record refuses resume. Follow its recovery guidance. See [Interrupted replication](/concepts/incremental/#recovering-an-interrupted-replication). |
| Idempotency key | `rocky run --idempotency-key <key>` skips a run whose key is in flight or already *successful*. A *failed* run leaves the key claimable by default. Set `dedup_on = "any"` to skip after a failure too, which forgoes retry under that key. You cannot combine a key with resume. |
| Hooks | Fires lifecycle [hooks](/concepts/hooks/) on pipeline events. Coverage is still filling in: failure hooks fire most reliably on the replication path today. Pair them with the exit-code routing below. |

## The exit-code contract

Every recipe below keys off the process exit code. Rocky uses a distinct code per condition, so a wrapper script or a CI step can branch without parsing output:

| Code | Meaning | Emitted by |
|------|---------|------------|
| `0` | Success | every command |
| `1` | Generic hard failure (config error, unreadable state, or an error raised *after* some models already materialized — a budget breach, say). For `rocky tick`, also an unreadable webhook spool | most commands |
| `2` | **Partial success** — some models materialized, some failed, or an error-severity check failed on a run that had already copied its data. Also: `rocky fulfill` is blocked and needs a human, and `rocky product verify` failed | `rocky run`, `rocky tick`, `rocky fulfill`, `rocky product verify` |
| `3` | A Critical health check. Also: `rocky fulfill` is parked at `applying_unknown` | `rocky doctor`, `rocky fulfill` |
| `4` | `rocky fulfill` applied a plan whose output fails a check the product declared | `rocky fulfill` |
| `130` | Interrupted: SIGINT or SIGTERM on Unix; Ctrl-C, Ctrl-Break, console close, logoff or shutdown on Windows | `rocky run` |

A scheduled `rocky run` returns `0`, `1`, `2`, or `130`. A quality pipeline's failed check gate returns `1`, not `2`. Codes `3` and `4` come from `rocky doctor` and `rocky fulfill`. Run `rocky doctor` as a pre-flight (below) or in CI.

**`rocky ci` returns `0` or `1`.** Its JSON `exit_code` is the same number. Advisory warnings do not change it. A CI step that wants to act on warnings reads the diagnostics:

```bash
n=$(rocky ci --output json | jq '[.diagnostics[] | select(.severity == "Warning")] | length')
[ "$n" -gt 0 ] && echo "advisory warnings"
```

**`rocky tick` and the webhook spool.** A tick that cannot read `.rocky/pending-demands` exits `1`, even when everything it did run succeeded. The scan fails only when something *is* at that path and cannot be read — a dangling symlink, a permission fault. A spool directory that does not exist yet reads as "no pending demand" and exits `0`, so this never fires on a project that has not used webhooks.

It is worth a page rather than a ticket. Nothing about it clears on its own, and while it stands, every webhook-triggered pipeline is silently not firing. That is also why it outranks exit `2`: a failing run is already visible in the tick's report and in run history, and letting it win would hide the spool fault for as long as the run kept failing.

### What to alert on

Alert on any non-zero exit. Beyond that, one distinction deserves a channel of its own.

**Give exit `2` its own channel.** A partial success means the run kept going. The models that worked produced real, current data, and a subset failed. That is a different operational situation from a hard failure. Exit `2` also covers a second case: every table copied, and then an error-severity check failed. Read `check_gate_failed` and `check_results[]` in the JSON to tell the two apart. A resume does not help that one. The data has landed, so fix the source and re-run the pipeline.

Exit `1` is generic. It often means nothing materialized, but it can also fire *after* some models landed — a budget breach, for one. So inspect the run's `--output json` result or `rocky history` rather than assume the estate is empty. Routing exit `1` and exit `2` to the same place trains people to ignore the alert. Treat a hard failure as a page. Treat a partial success as a ticket: the on-call looks at the failed models before the next run.

Exit `130` means an interrupt cut the run short, usually a deploy or a machine restart. Treat it as informational unless it repeats.

A small wrapper makes the routing explicit, and it is reusable across every scheduler:

```bash
#!/usr/bin/env bash
# rocky-run.sh — run a pipeline and route notifications by exit code.
set -uo pipefail
cd /srv/analytics

rocky run --output json >> /var/log/rocky/analytics.log 2>&1
code=$?

case "$code" in
  0)   ;;                                                   # success, stay quiet
  2)   notify "#data-partial" "Rocky partial success (exit 2): a model or a check failed, run continued" ;;
  130) ;;                                                   # interrupted, informational
  *)   notify "#data-oncall" "Rocky run FAILED (exit $code)" ;;
esac

exit "$code"
```

Replace `notify` with your `curl` to Slack, your `mail` command, or whatever you already use. `--output json` writes the full per-model result to the log, so the on-call sees which models failed without re-running anything.

## cron

cron is the classic timer. The one thing it does not give you for free is overlap protection: if a run takes longer than the interval, cron starts a second run on top of the first. Guard it with `flock`.

```cron
# /etc/cron.d/rocky-analytics
# Run at 03:00 daily. flock -n makes a still-running previous run skip this tick
# rather than piling a second run on top of it.
0 3 * * * dataeng flock -n /var/lock/rocky-analytics.lock /srv/analytics/rocky-run.sh
```

`flock -n` is non-blocking: it exits at once if the lock is held. Drop `-n` if you would rather queue the next run than skip it, and `flock` waits for the lock instead. The wrapper script owns the exit-code routing, so cron only acquires the lock and starts it.

To skip the wrapper, call the CLI directly and let cron mail you on any non-zero exit through `MAILTO`. You then lose the separate channel for exit `2`:

```cron
MAILTO=data-oncall@example.com
0 3 * * * dataeng flock -n /var/lock/rocky-analytics.lock rocky -c /srv/analytics/rocky.toml run --output json
```

## systemd timer and service

On a systemd host, a `oneshot` service plus a timer is more observable than cron. You get `systemctl status`, `journalctl` history, and `OnFailure=` handlers.

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

Enable it with `systemctl enable --now rocky-analytics.timer`. The `ExecStartPre` guard turns a broken deploy into a clean "service failed to start", instead of a half-run against a bad config. To let the platform do the routing instead, drop the wrapper, set `ExecStart=/usr/local/bin/rocky run --output json`, and read `$EXIT_STATUS` in the `rocky-analytics-failed@` handler unit.

> These systemd and timer units are illustrative templates. Adapt the paths, the user, and `OnCalendar` to your host. The exit-code behavior they rely on is verified against the CLI. See the note at the end of this page.

## GitHub Actions on a schedule

If your warehouse is reachable from GitHub's runners, a scheduled workflow needs no infrastructure of your own. Key off the exit code, so a partial success stays visible but distinct from a hard failure.

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

The run step captures the code instead of failing the job, so the classify step decides what a red build means. Here a partial success is a warning annotation and the job stays green, because the run produced current data. A total failure fails the job and triggers your normal Actions failure notifications. Flip that policy to taste. Store warehouse credentials as [encrypted secrets](/reference/authentication/) and pass them as environment variables.

This is a scheduled *production* run. To run `rocky ci` on pull requests (compile and test, with no warehouse), see the [CI/CD guide](/guides/ci-cd/).

## Databricks Workflows and warehouse-native schedulers

If your warehouse already has a scheduler, you often need no separate host at all. Any scheduler that can run a shell command can run Rocky. On Databricks, a Workflow with a shell or Python task installs the binary and calls the CLI:

```bash
# A Databricks task command (or any warehouse-native scheduler's shell step).
set -uo pipefail
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash
rocky doctor
rocky run --output json
```

The task's exit code propagates to the Workflow run. The platform's own retry and alerting policies then apply on top of Rocky's in-run retries. If your scheduler distinguishes exit codes, wire exit `2` to a warning and the rest to a failure, exactly as above. If it only sees success or failure, decide whether a partial success should mark the task failed.

Snowflake Tasks, Airflow's `BashOperator`, and cloud cron services (Cloud Scheduler, or EventBridge Scheduler hitting a small runner) all follow the same shape: install, optional pre-flight, `rocky run`.

> The Databricks and warehouse-native snippets are illustrative. They use the same `rocky run` and `rocky doctor` invocations verified below. The surrounding task configuration depends on your platform.

## Failure notification without a platform

You do not need an orchestrator's alerting to hear about a failed run. Rocky has two mechanisms, and both live in the pipeline definition.

### Webhook on pipeline error

A webhook hook posts an HTTP request when the pipeline errors. It can target Slack, Teams, PagerDuty, or Datadog. It sends the event context (run id, event, metadata) over HTTP, with an optional HMAC signature. Nothing else in your stack has to be running.

```toml
# rocky.toml
[hook.webhooks.on_pipeline_error]
url = "${SLACK_WEBHOOK_URL}"
preset = "slack"
secret = "${WEBHOOK_SECRET}"
```

The `preset` gives you a service-shaped body for free, Slack Block Kit in this case. See [Hooks](/concepts/hooks/) for the full list of presets, custom body templates, and HMAC verification.

### A richer digest with rocky brief

Use a command hook when you want a message that carries more than "it failed". The hook renders [`rocky brief`](/reference/commands/administration/) and delivers it by email or Slack. The digest covers recent runs, drift, freshness, quality, and cost. When the scheduler is in use, it also covers holds, failure streaks, and incidents. `brief --output md` produces a Markdown document ready for either destination:

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

A webhook hook and a command hook are two different things. The `[hook.webhooks.*]` block posts the event context over HTTP by itself. A `[[hook.*]]` block runs a program you provide, which is what lets it shell out to `rocky brief`. Use the webhook for a fast "it failed" ping. Use the command hook when you want the full digest.

To confirm Rocky picked up either block, run `rocky validate`. It parses the whole config, both hook tables included. `rocky hooks list` prints only the *command* hooks Rocky loaded, so a `[hook.webhooks.*]` block never appears there. A command hook that does show up confirms its event key is one Rocky recognizes. Rocky skips an unknown or misspelled `on_<event>` key with a warning rather than firing it.

One detail bites timers specifically. Rocky substitutes config values like `${SLACK_WEBHOOK_URL}` from the environment. A referenced variable that is not set makes config loading fail outright: `rocky validate` returns `1`, and so does the run. cron and systemd start with a nearly empty environment. So export the secrets your config references in the timer's own environment, or the run will not even start. Use `Environment=` or `EnvironmentFile=` for systemd, and a sourced file for cron.

## Health checks

Two commands turn a blind timer into an observable one.

### rocky doctor as a pre-flight

`rocky doctor` runs config, state, adapter, and auth checks. It **exits `3` if any check is Critical**. Put it before `rocky run`, as the `ExecStartPre` and pre-flight steps above do. A broken config or an unreachable warehouse then fails early and cleanly, instead of leaving a half-completed run. Config problems and an unreachable warehouse are Critical. A degraded or unreadable local state store is a Warning and exits `0`, so it does not fail the pre-flight on its own.

Run the full battery, or scope to a single check with `--check`:

```bash
rocky doctor                  # all checks, exits 3 on any Critical
rocky doctor --check config   # config only, offline and fast
rocky doctor --check auth     # ping the warehouse to catch a rotated credential
rocky doctor --check scheduler # is the reconciler alive and unwedged?
```

When any pipeline declares a `[schedule]`, `rocky doctor` also runs a `scheduler` check. It stays silent otherwise. The check reads only the filesystem and the state store, never the warehouse. So it works offline, and it covers both `rocky tick` and `rocky serve --scheduler`. It reports two conditions:

- **Critical** when `.rocky/tick.lock` is held but its heartbeat has gone stale. That is a wedged reconciler that no restart-free takeover can dislodge. The fix is to restart the holding process.
- **Warning** when no tick has evaluated any schedule for more than twice the shortest cron interval. The timer looks dead.

Give `--check scheduler` a cron of its own, on the host that runs the reconciler.

### rocky history for past runs

Rocky records runs in the embedded state store, most completely for replication and transformation runs today. `rocky history` reads them back with no warehouse round-trip, so it works from the same host your timer runs on:

```bash
rocky history                       # recent runs: id, start, status, model count, trigger
rocky history --model fct_revenue   # one model's execution history
rocky history --since 2026-03-01    # runs on or after a date
rocky history --audit               # include the governance audit trail
```

The `status` column tells partial from total after the fact: `Success`, `PartialFailure`, `Failure`. The `trigger` column records how the run started. A direct `rocky run` shows `"trigger": "Manual"`. A run launched by `rocky tick` (below) shows `"trigger": "Schedule"`, joined to the tick that started it by a shared `submission_id`. Pair this with `rocky run --resume-latest` on replication pipelines, and you can see what a scheduled run did overnight and pick up a failed one where it left off.

## Native scheduling with `rocky tick` (experimental)

Everything above drives `rocky run` from a timer, and the timer decides *which* pipeline runs *when*. `rocky tick` moves that decision into `rocky.toml`. Each pipeline declares its own demand: a `cron` schedule, an `after` dependency, or a `freshness` budget. One `rocky tick` evaluates all of it at once and runs what is due.

There is still no daemon. The tick comes from the same cron or systemd timer you already have. You point that timer at `rocky tick` on a short interval, instead of at one specific `rocky run`. A one-minute timer turns the declarations below into SLO, cron, and dependency scheduling, with nothing resident.

This is **experimental** while the reconciler soaks. External orchestrators stay fully supported. If you run Dagster or Airflow today, keep them.

Scheduling works on `replication`, `transformation`, `quality`, and `snapshot` pipelines. `load` pipelines cannot take part yet. A load re-ingests every discovered file on each run rather than incrementally, so scheduling one would duplicate data, and it records no run the scheduler can observe. `rocky validate` rejects a scheduled load, and rejects an `after` that references a load. Native load scheduling is a planned follow-up.

### Words this section uses

| Term | What it means |
|---|---|
| Demand | A recorded request to run one pipeline. A `cron` slot, an `after` dependency, a `freshness` budget, or a webhook each create one. |
| Tick | One pass that looks at all standing demand and runs what is due. |
| Reconciler | The code that performs a tick. `rocky tick` runs it once. `rocky serve --scheduler` runs it in a loop. |
| Claim | The marker the reconciler writes when it takes a demand to run. It stops a second tick taking the same one. |
| Stuck-claim resolver | Recovery on the demand path. When a due demand finds its claim still `submitted` from an earlier tick, Rocky resolves it from the child's run record. |
| Orphan sweep | A pass at the end of a tick over the `submitted` claims an earlier tick left behind. It finishes each one whose run record already shows a terminal outcome, whether or not the demand is due again. |
| Spool file | The file on disk that holds an accepted webhook demand. Rocky writes and `fsync`s it before it answers the caller. |
| Fail-closed | Rocky refuses to act when it cannot confirm the conditions. It never guesses in the permissive direction. |
| At-most-once | Rocky attempts the work one time and never retries it. The work can be lost, never duplicated. |
| `failure_backoff` | The state a pipeline enters after a scheduled run fails. Later ticks skip it until its `resume_at`. |
| Incident bundle | One JSON file under `.rocky/incidents/` holding the facts of one failed scheduled run. |

Terms used across the rest of the docs are in the [glossary](/reference/glossary/).

### The demand lifecycle

Every demand travels the same path, whatever created it.

```
  DECLARED DEMAND                      WEBHOOK DEMAND
  [schedule] in rocky.toml             POST /api/v1/hooks/trigger/{pipeline}
  cron | after | freshness                        │
              │                                   │ fsync, then 202
              │                       ┌───────────▼───────────┐
              │                       │ spool file on disk    │
              │                       └───────────┬───────────┘
              └─────────────────┬─────────────────┘
                                ▼
                     ┌─────────────────────┐
                     │ tick — the          │
                     │ reconciler evaluates├──► skipped[]  (not due,
                     │ all standing demand │    backoff, in flight)
                     └──────────┬──────────┘
                                │ due
                                ▼
                     ┌─────────────────────┐
                     │ claim — one owner   │
                     │ per demand          │
                     └──────────┬──────────┘
                                ▼
                     ┌─────────────────────┐
                     │ rocky run           │
                     └──────────┬──────────┘
                                ▼
                     ┌─────────────────────┐
                     │ outcome recorded    │
                     └───┬─────────────┬───┘
                success  │             │  failure or partial
                         ▼             ▼
                 next occurrence   failure_backoff +
                                   incident bundle
```

A declared demand is evaluated fresh at every tick. A webhook demand lands in the spool file first, and the next tick consumes it.

### Declare demand in the pipeline

```toml
# rocky.toml
[pipeline.raw]
type = "replication"
# ...adapter, source, target...
[pipeline.raw.schedule]
cron = "0 3 * * *"          # run at 03:00
timezone = "Europe/Lisbon"  # IANA name; default is the project [schedule].timezone, else UTC

[pipeline.staging]
type = "transformation"
models = "models/**"
[pipeline.staging.schedule]
after = ["raw"]             # run once raw has a newer success than staging's last
```

`cron`, `after`, and `freshness` can combine on one pipeline. Any one source being due makes the pipeline due. See the [`[pipeline.*.schedule]` reference](/reference/configuration/#pipelinenameschedule) for every key, the catch-up policy, and the freshness semantics.

### Drive it from a one-minute timer

```bash
# /etc/cron.d/rocky-tick
# Evaluate all standing demand every minute. flock makes a still-running tick
# skip the next one rather than stacking a second reconciler on top.
* * * * *  analytics  cd /srv/analytics && flock -n /var/lock/rocky-tick.lock rocky tick --output json >> /var/log/rocky/tick.log 2>&1
```

Or a systemd timer. It follows the units in the earlier systemd section: a `oneshot` service with `SuccessExitStatus=2`, because the wrapper routes exit `2`.

```ini
# /etc/systemd/system/rocky-tick.timer
[Timer]
OnCalendar=*:0/1
AccuracySec=1s

# /etc/systemd/system/rocky-tick.service
[Service]
Type=oneshot
WorkingDirectory=/srv/analytics
ExecStart=/usr/local/bin/rocky tick --output json
SuccessExitStatus=2
```

`rocky tick` takes its own non-blocking lock, `.rocky/tick.lock`, next to your config. Two ticks never reconcile at once, even if a run outlives its interval. The outer `flock` above is only a cheap early skip.

### The resident scheduler: `rocky serve --scheduler` (experimental)

The timer approach keeps nothing resident. cron or systemd wakes `rocky tick`, it reconciles once, and it exits. If you already run `rocky serve` for the HTTP API, the server can drive the reconciler in-process instead, on a poll interval, with the `--scheduler` flag:

```bash
# The API plus a resident reconciler that ticks every 15 seconds (the default).
rocky serve --scheduler

# Tune the cadence and the shutdown drain window.
rocky serve --scheduler --poll-interval-seconds 30 --drain-timeout-seconds 120
```

It is the same reconciler as `rocky tick`. Same `[schedule]` declarations, same `cron`/`after`/`freshness` evaluation, same `.rocky/tick.lock` and `schedule_state`. The only difference is that it is hosted inside a long-lived process instead of behind an external timer. The resident form adds four things:

- **Runs show up as jobs.** Each scheduled run is recorded through the same jobs model as `POST /api/v1/jobs/run`. So `GET /api/v1/jobs/{submission_id}` reports it, and a restarted server reports honest status for what it launched.
- **It coordinates with API mutations.** A scheduler tick and an API `run` or `apply` never collide on the state store. Whichever arrives second gets a clean `409 mutation_in_progress`. The scheduler instead skips the tick and re-evaluates next time, rather than racing the writer lock.
- **Config is re-read every tick.** Edit `rocky.toml` under a running server and the next tick picks it up. A parse error on one tick is logged and skipped. The loop never dies on a bad edit, and it never runs a stale schedule.
- **Shutdown drains.** On `SIGTERM` or `Ctrl-C` (on Windows also Ctrl-Break, console close, logoff or shutdown) the server stops starting new work. A run already in flight gets up to `--drain-timeout-seconds` to finish on its own, but never beyond that run's own `timeout_minutes`, and is then terminated. In-flight HTTP requests drain in the same window before the process exits. A run cut short by the drain is recorded as failed, and is not retried until its next occurrence. The scheduler also holds its first tick until the server has finished starting up: its job sweep and listener bind. So a scheduled run never starts before the server is up, and never outlives a failed startup.

You can read the scheduler's state over HTTP:

```
GET /api/v1/schedule           # what is scheduled and in flight
GET /api/v1/schedule/spool     # webhook demands accepted but not yet run
```

It reports every scheduled pipeline: its `cron`/`after`/`freshness` configuration, when it last evaluated and last fired, its next expected fire, any active backoff, and the claims currently in flight. It also reports the tick-lock state. Two things need reading correctly.

- **`tick_lock.state: free` is the normal steady state.** The lock is held only for the brief duration of a tick, so a healthy scheduler reports `free` on almost every request. `free` does not mean no scheduler is running. To check that, compare `last_evaluated_at` against the cadence. `held` means a tick is in progress right now. `wedged` means the lock's heartbeat has gone stale, and the reconciler needs restarting.
- **A `next_fire_at` in the past means overdue.** The projection is anchored on the last occurrence that actually fired, not on the clock. A stalled timer therefore reports the slot it missed, rather than a healthy-looking future one. A pipeline whose schedule cannot be resolved carries a `config_error` and never fires; the endpoint surfaces the reason rather than omitting the pipeline. The endpoint reads stored state only. It does not evaluate demand — that is `rocky tick --dry-run` — so it is a cheap, side-effect-free health read.
- **`delivery` answers "will it land in time?".** It appears on a pipeline that builds the output model of a product with a [`deliver_by` deadline](/reference/commands/products/#delivery-deadline). For a transformation pipeline, Rocky reads that from the pipeline's `models` glob, with the same root, models directory and glob as `rocky run`, so a pipeline that has never built the model yet still counts. Ephemeral models do not count: `rocky run` never builds one. Rocky loads the models but does not compile them, so a compile error is not detected here. When the models directory is absent or cannot be located, or a model fails to load, `rocky run` builds nothing, so the pipeline has no `delivery`: see `models_unreadable` below. For any other pipeline type, Rocky reads the pipeline's newest production run that built a deadlined model, within the newest 500 runs. It names the most urgent such product. `on_time` and `late_by_seconds` describe the latest delivery. `at_risk` is `true` when the pipeline's p90 run time is longer than the time left before `next_due_at`. The p90 comes from the pipeline's last 20 successful production runs. With fewer than 5 runs, `estimate.state` is `unknown` and `at_risk` is `null`: no verdict. Computing it loads each scheduled transformation pipeline's model files and reads run history, so once a product declares `deliver_by` the endpoint is no longer cheap: its cost grows with model files plus pipelines × recorded runs.
- **`latest_start` answers "does the cron start early enough?".** It appears on each pipeline that feeds a deadline: a pipeline with a `delivery`, a pipeline marked `models_unreadable` (below), and every pipeline either runs `after`, directly or through others. It is absent on every other pipeline, including one on an `after` cycle with no deadline downstream. `at` is the latest time the pipeline can start and still meet every deadline it feeds. `product` and `due_at` name the deadline that sets it. Rocky works back along `after` from each deadline. At each pipeline it takes the earliest start-by time of what that pipeline feeds, then subtracts the pipeline's own p90 run time.

  ```text
    raw (p90 20m) ──after──▶ revenue (p90 10m)  due 06:00  start by 05:50
                  └─after──▶ costs   (p90 30m)  due 04:00  start by 03:30
    raw: min(05:50, 03:30) - 20m = 03:10
  ```

  `cron_check` compares the pipeline's own cron with `at`. Rocky only warns. A deadline never changes a cron and never adds an `after` edge. The first value that applies wins:

  | `cron_check` | When | Text |
  |---|---|---|
  | `disabled` | The schedule is disabled or cannot be resolved, so it never fires. | `warning` |
  | `overdue` | `next_fire_at` is earlier than now: a cron slot was missed. The same test as `counts.overdue`. Checked even when `state` is `unknown`. When `at` is known and already passed, `cron_too_late` is `true`. | `warning` |
  | `not_judged` | `state` is `unknown`, unless the pipeline has no cron and an `after` upstream is known late (then `too_late`); or the cron has no next fire yet (no fire anchor); or the pipeline has neither a cron nor `after` (it runs by hand or by freshness). Or the pipeline has no cron, no `after` upstream is known late, and some `after` upstream cannot be judged: its `state` is `unknown` (for example, fewer than 5 runs) or its `cron_check` is `not_judged`. | none; in the last case, `note` names the upstreams that cannot be judged |
  | `on_time` | `next_fire_at` is at or before `at`. A fire exactly at `at` is in time. | none |
  | `too_late` | `next_fire_at` is later than `at`, and the pipeline has no `after`, or some `after` upstream does not qualify for `relies_on_after` (it is late or cannot be judged); `cron_too_late` is `true`. Or the pipeline has no cron, runs only `after` others, and some of them are known late: their `cron_check` is `too_late`, `overdue` or `disabled`. `cron_too_late` is then `false`, as it has no cron. This holds even when the pipeline's own `state` is `unknown`. | `warning`, naming the upstreams not in time |
  | `relies_on_after` | Every `after` upstream has a known `latest_start` and a `cron_check` of `on_time` or `relies_on_after`, and either `next_fire_at` is later than `at` or the pipeline has no cron. When the upstreams start in time, `after` starts this pipeline in time. `cron_too_late` stays `false`. | `note`: the cron alone would be late, or the pipeline has no cron |

  `cron_too_late` is `true` for `too_late` on a pipeline with a cron, and for `overdue` when `at` is known and already passed. It is `false` when `state` is `unknown`, and always `false` on a pipeline with no cron. To alert on every risk, alert on `warning`.

  When any term on the way is unknown, `state` is `unknown`, `at` is absent and `reasons` lists why: no guess. `missing_estimate` means a pipeline has fewer than 5 successful production runs; the `missing_estimate` field names those pipelines. `no_next_deadline` means a `deliver_by` cron has no next occurrence. `cycle` means `after` has a cycle on the way to the deadline. `overflow` means the subtraction left the date range. `models_unreadable` means a transformation pipeline's models directory is absent or cannot be located, or a model fails to load, so `rocky run` builds nothing for it. Every such pipeline reads `models_unreadable` whenever any product declares `deliver_by`: neither config nor run history can show that it builds no deadlined output model. That pipeline also carries a `warning`, and has no `delivery`. `rocky product status` still shows the product's last delivery facts from run history. Rocky does not know when an external source lands, so a late source does not show here. Each pipeline upstream of a delivery adds one more run-history read.

The resident reconciler is a single loop. A scheduled run that hangs holds the loop until the run finishes or is terminated: the server keeps serving HTTP, but it evaluates no further schedules. Set `timeout_minutes` on a `[schedule]` so a stuck run is terminated and the loop moves on. Without one, a hung run stalls scheduling until the process is restarted, and `rocky doctor --check scheduler` reports it as a dead timer. Automatic recovery of a wedged reconciler is a planned follow-up. Until it lands, bound long runs with `timeout_minutes` and watch the doctor check.

A minimal systemd unit for the resident form:

```ini
# /etc/systemd/system/rocky-serve.service
[Unit]
Description=Rocky API + resident scheduler
After=network-online.target

[Service]
WorkingDirectory=/opt/rocky/analytics
ExecStart=/usr/local/bin/rocky serve --scheduler
Restart=on-failure
# Give an in-flight scheduled run time to drain before SIGKILL.
TimeoutStopSec=180

[Install]
WantedBy=multi-user.target
```

Pick one form or the other against a given project, not both. A `rocky tick` timer *and* a `rocky serve --scheduler` on the same state file are two reconcilers (see below).

### Event-driven triggers: webhook ingress (experimental)

The resident scheduler can also accept an HTTP webhook that queues a run demand for a named pipeline. An external event then fires a pipeline without waiting for the next cron slot: a Fivetran sync completing, an upstream job finishing, or a manual "run now" button.

```
POST /api/v1/hooks/trigger/{pipeline}
```

The route is live only under `--scheduler`, because nothing else would consume the demand. It is authenticated by its own HMAC, not by the `--token` Bearer token the rest of the API uses. Set a shared secret, then sign the raw request body with HMAC-SHA256, hex-encoded, in the `X-Rocky-Signature` header:

```bash
export ROCKY_WEBHOOK_SECRET='a-long-random-secret'
rocky serve --scheduler   # in another shell

# Sign an (empty) body and trigger the `orders` pipeline.
BODY=''
SIG=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac "$ROCKY_WEBHOOK_SECRET" | awk '{print $2}')
curl -sS -X POST http://127.0.0.1:8080/api/v1/hooks/trigger/orders \
  -H "X-Rocky-Signature: $SIG" \
  -H 'X-Rocky-Delivery: evt-2026-05-02-0001' \
  --data-binary "$BODY"
# → 202 {"demand":"accepted","demand_uid":"…"}
```

`X-Rocky-Delivery` is an optional idempotency key, and it changes the guarantee you get. Every *accepted* demand is at-most-once either way. Whether the same *event* runs at most once depends on the header:

- **With `X-Rocky-Delivery`.** Rocky deduplicates a redelivery of the same id for 24 hours after the demand is consumed, answering `202 {"demand":"duplicate"}`. A sender that retries the same event does not double-fire the pipeline. Use this mode if your sender retries.
- **Without it, Rocky falls back to the body hash.** The demand deduplicates on that hash only while it is still queued. An identical body delivered again *after* consumption is a new demand, and it fires again. So for a sender with no delivery id, the guarantee across retries is **at-least-once**. Pair it with a `freshness` schedule (below), so a re-fire is at worst a redundant refresh rather than a correctness problem.

**Fail-closed.** With no `ROCKY_WEBHOOK_SECRET` set, the route answers `404`. The one exception is a server bound to loopback, a local-dev convenience that accepts without a signature. Running `serve` without `--scheduler` also answers `404`. Rocky sheds an over-rate flood with `429` and a `Retry-After` header before it writes anything. An unsigned or wrongly-signed request gets `401`. A request for a pipeline not in your config gets `404`, but only after the signature verifies, so an unauthenticated caller cannot use the endpoint to enumerate pipeline names.

**At-most-once delivery — read this before you depend on it.** Rocky writes an accepted webhook to a durable, `fsync`'d spool file *before* it answers `202`. A crash between the `202` and the next tick never loses it. The reconciler then consumes each spooled demand **at most once**: it attempts the demand exactly one time and never retries it.

One loss window is narrow but real. The reconciler can crash *after* it has claimed the demand. If the child run **also** dies before recording its outcome, that demand is finalized as a failure and is not re-run. A child that outlives a dead reconciler still records its run and is honored. A sender's own retries cover everything before the `202`.

Rocky does not retry a webhook on the delivery side. So **give a webhook-only pipeline a `freshness` schedule as a backstop**. If a delivery is ever dropped, the freshness trigger still brings the pipeline current within its budget:

```toml
[pipelines.orders.schedule]
freshness = true        # backstop: re-runs if it goes stale, even if a webhook is lost
```

A webhook-triggered run records `trigger: "webhook"` in `rocky history`, distinct from the `schedule` trigger a cron, `after`, or freshness run records. It also appears under `GET /api/v1/schedule`'s in-flight claims while it runs. A demand whose pipeline was removed from config after it was accepted is finalized without ever running, and logged loudly rather than left pending forever.

**Between acceptance and the next tick, a demand is in neither place.** `GET /api/v1/schedule` reports claims, and a claim exists only once a tick has picked the demand up. So a webhook that returned `202` and has not run yet appears in no claim and no history row. To see that queue:

```
GET /api/v1/schedule/spool          # or: rocky state schedule spool
```

It lists what the ingress accepted and no tick has consumed, with the `token` you can match against your provider's delivery log. Read it when a delivery succeeded on your provider's side and the pipeline never ran. A spool that is present but unreadable is reported as an error rather than an empty queue — "nothing is waiting" and "we cannot tell" are different answers.

### One scheduler instance per project

Run exactly one reconciler per project, meaning per state file. One `rocky tick` timer, or one `rocky serve --scheduler`. Not both, and not several.

All of a reconciler's mutual exclusion is local to one machine. The `.rocky/tick.lock` flock and the state store's own writer lock both live on that host's filesystem, and neither can see a second host. Scheduler state (the cursor and claim tables) is deliberately local-only as well: a remote `[state]` backend (S3, GCS, Valkey, tiered) never uploads it, and a download never overwrites it. Two hosts ticking the same project each keep an independent cursor, and both fire the same occurrence. Remote state is last-writer-wins today, with no cross-host compare-and-swap, so there is no fence to lean on across machines.

If you need timers on several hosts, give each host its own project and state file. Or keep a single timer, and let the other hosts invoke `rocky run` directly.

### Preview before you wire the timer

`rocky tick --dry-run` evaluates demand and reports exactly what *would* run. It starts no pipeline and advances no scheduler cursor. It still opens the state store read-write. On a fresh project it can create the state file. On an older store it can stamp the current schema version. Use it to confirm a new schedule does what you expect:

```bash
rocky tick --dry-run --now 2026-05-02T03:00:00Z --output json
```

`--now` pins the evaluation instant, in RFC3339, so you can preview a future occurrence or a catch-up window deterministically. Omit it and the tick uses the wall clock.

### Exit codes and honest alerting

`rocky tick` reuses the exit-code contract above:

- `0`: nothing was due, or every run the tick launched succeeded.
- `1`: the tick could not proceed at all (invalid config, unopenable state). It ran nothing and failed closed.
- `2`: at least one launched run failed or came back partial.

The same `rocky-run.sh` routing works unchanged.

**Exit `0` does not mean the estate is healthy.** After the tick that first observes a failure, a broken pipeline goes into `failure_backoff`. Later ticks correctly *skip* it, so they do not hammer it every minute, and they therefore exit `0`. The ongoing problem lives in the tick's JSON, not in its exit code. Each suppressed demand appears in `skipped[]` with a reason (`failure_backoff`, `partial_backoff`) and a `resume_at`. The `counts` block and the `consecutive_failures` metric carry the running total. Alert on those, and on the [scheduler metrics](/guides/observability/), not on exit codes alone.

A tick can also come back exit `0` having done nothing, because another `rocky` process held the state store when it tried to open it. That process may be a manual `rocky run`, or the tick's own child from a still-running prior tick. It shows up as a single `state_busy` entry in `skipped[]`. One is normal contention and self-heals on the next tick. A `state_busy` on every tick for many minutes means a wedged writer is holding the store, and is worth an alert.

### Incident bundles

When a scheduled run finalizes as a failure, the reconciler writes one JSON file under `.rocky/incidents/`. A full failure and a partial one both count, because both trip the scheduler's backoff.

The file holds the structured facts of that incident, with no narration:

- the pipeline;
- the demand source (`cron`, `after`, `freshness`, `webhook`);
- the outcome, and the occurrence it was for;
- the submission id and the exit code;
- the attempt count for the demand cycle;
- the consecutive-failure count after this failure;
- retrieval pointers: a `rocky history` command, plus the `/api/v1/jobs/{id}` and `/api/v1/schedule` endpoints under the resident scheduler.

A fact the emitting path cannot know is `null`, never a guessed zero. The exit code of a run recovered from a crashed owner is one such fact.

Two recovery paths write bundles as well: the stuck-claim resolver and the orphan sweep. Whichever one observes the failure writes the file. There is one deliberate exception. A child the spawner itself terminated for a shutdown drain gets no bundle, because a graceful shutdown is not an incident. A run that failed on its own while a drain happened to be in progress still records one.

Rocky keeps the newest 50 bundles. The sweep deletes only files the writer itself named, and it refuses to operate through a symlinked `incidents` directory.

The point of the format is that whoever picks up the page, a human or an agent, starts from citations instead of re-deriving what happened. `rocky brief` surfaces the count and the newest bundle's path in its Scheduler section, so the digest a failure hook posts already points at the file to open.

## Where to go next

- [Observability](/guides/observability/) — export traces and metrics over OpenTelemetry, so a scheduled estate is visible in Grafana, Tempo, or any OTLP backend, with no UI to host.
- [Hooks](/concepts/hooks/) — the full lifecycle-event surface behind the notification recipes above.
- [Failure modes](/advanced/failure-modes/) — how to read every failure Rocky can report, including partial success.
- [CI/CD integration](/guides/ci-cd/) — `rocky ci` for pull-request checks, the complement to the scheduled runs here.

---

**On verification.** The commands on this page were exercised against the playground pipeline with the current engine build. A clean `rocky run` returns `0`. A run where one model fails while the others materialize returns `2`, and so does a run that copied every table and then failed an error-severity check. `rocky doctor` returns `3` on a Critical check. `rocky validate`, `rocky hooks list`, and `rocky brief --output md` were run against the hook configuration shown above. Exit codes `1`, `4`, and `130` follow the CLI's documented convention. The systemd, GitHub Actions, and Databricks configurations are illustrative templates around those verified commands. Adapt them to your host and platform.
