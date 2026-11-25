---title: Running Rocky Without an Orchestrator
description: "Run Rocky on a schedule from cron, a systemd timer, GitHub Actions, or your warehouse's own scheduler. Covers the exit codes, failure alerts, and health checks."
sidebar:
  order: 10
---

You do not need Dagster, Airflow, or any other orchestrator to run Rocky on a schedule. A timer decides when to start. The engine does the rest.

| You want | Use | Section |
|---|---|---|
| A timer on a Linux host | cron or a systemd timer | [cron](#cron), [systemd](#systemd-timer-and-service) |
| No host at all | GitHub Actions or a warehouse scheduler | [GitHub Actions](#github-actions-on-a-schedule), [Databricks](#databricks-workflows-and-warehouse-native-schedulers) |
| An alert on failure | A webhook hook or `rocky brief` | [Failure notification](#failure-notification-without-a-platform) |
| Pre-flight and history | `rocky doctor`, `rocky history` | [Health checks](#health-checks) |
| Schedules declared in `rocky.toml` | `rocky tick` | [Native scheduling](#native-scheduling-with-rocky-tick-experimental) |

If you already run Dagster or Airflow, keep them. See [`dagster-rocky`](/dagster/resource/).

## What one `rocky run` already does

A timer adds only the trigger. A `rocky run` already does the rest, from `rocky.toml` and the engine: it runs models in DAG order (independent branches concurrently), retries a failed statement up to `max_retries` (see [retry settings](/reference/configuration/#retry)), keeps models that materialized when others fail (exit `2`, see [per-table error containment](/advanced/per-table-error-containment/)), writes runs to the embedded [state store](/reference/glossary/#state-store), and fires lifecycle [hooks](/concepts/hooks/) (failure hooks fire most reliably on the replication path).

Resume and re-run options:

| Flag | What it does |
|---|---|
| `--resume-latest`, `--resume <run_id>` | Resume a failed replication run. A complete checkpoint with no terminal run record refuses resume: follow its recovery guidance. See [Interrupted replication](/concepts/incremental/#recovering-an-interrupted-replication). |
| `--retry-failed [<run_id>]` | Rerun the failed and never-built models of a failed transformation run, and their downstream models. Needs a local `[state]` backend. See [Retrying a failed run](/reference/commands/core-pipeline/#retrying-a-failed-run). |
| `--idempotency-key <key>` | Skip a run whose key is in flight or already *successful*. A *failed* run leaves the key claimable. `dedup_on = "any"` skips after a failure too. You cannot combine a key with resume. |

## The exit-code contract

Every recipe below keys off the process exit code:

| Code | Meaning | Emitted by |
|------|---------|------------|
| `0` | Success | every command |
| `1` | Hard failure: config error, unreadable state, or an error raised *after* some models materialized (a budget breach). For `rocky tick`, also an unreadable webhook spool | most commands |
| `2` | **Partial success**: some models failed, or an error-severity check failed after the data was copied. Also `rocky fulfill` blocked and needing a human, and `rocky product verify` failed | `rocky run`, `rocky tick`, `rocky fulfill`, `rocky product verify` |
| `3` | A Critical health check. Also `rocky fulfill` parked at `applying_unknown` | `rocky doctor`, `rocky fulfill` |
| `4` | `rocky fulfill` applied a plan whose output fails a check the product declared | `rocky fulfill` |
| `130` | Interrupted: SIGINT or SIGTERM on Unix; Ctrl-C, Ctrl-Break, console close, logoff or shutdown on Windows | `rocky run` |

A scheduled `rocky run` returns `0`, `1`, `2` or `130`. A failed check gate on a quality pipeline returns `1`, not `2`. `rocky ci` returns `0` or `1`, and its JSON `exit_code` matches. Advisory warnings do not change it. To act on them, read the diagnostics:

```bash
n=$(rocky ci --output json | jq '[.diagnostics[] | select(.severity == "Warning")] | length')
[ "$n" -gt 0 ] && echo "advisory warnings"
```

### What to alert on

Alert on any non-zero exit. Give exit `2` its own channel.

| Exit | Meaning | Treat as |
|---|---|---|
| `1` | Usually nothing materialized, but it can fire *after* some models landed. Inspect `--output json` or `rocky history`. | A page |
| `2` | The models that worked hold current data. Read `check_gate_failed` and `check_results[]` to tell a failed model from a failed check. A resume does not fix a failed check: the data landed, so fix the source and re-run. | A ticket |
| `130` | An interrupt cut the run short, usually a deploy or restart. | Informational, unless it repeats |

One wrapper makes the routing reusable across every scheduler:

```bash
#!/usr/bin/env bash
# rocky-run.sh: run a pipeline and route notifications by exit code.
set -uo pipefail
cd /srv/analytics

rocky run --output json >> /var/log/rocky/analytics.log 2>&1
code=$?

case "$code" in
  0)   ;;
  2)   notify "#data-partial" "Rocky partial success (exit 2): a model or a check failed, run continued" ;;
  130) ;;
  *)   notify "#data-oncall" "Rocky run FAILED (exit $code)" ;;
esac

exit "$code"
```

Replace `notify` with your own Slack or mail command.

## cron

cron starts a second run on top of a slow first one. Guard it with `flock`. `-n` skips the tick when the lock is held. Drop `-n` to queue instead.

```cron
# /etc/cron.d/rocky-analytics
0 3 * * * dataeng flock -n /var/lock/rocky-analytics.lock /srv/analytics/rocky-run.sh
```

To skip the wrapper, call the CLI and let `MAILTO` mail you on any non-zero exit. You lose the separate channel for exit `2`:

```cron
MAILTO=data-oncall@example.com
0 3 * * * dataeng flock -n /var/lock/rocky-analytics.lock rocky -c /srv/analytics/rocky.toml run --output json
```

## systemd timer and service

A `oneshot` service plus a timer gives you `systemctl status`, `journalctl` history and `OnFailure=` handlers.

```ini
# /etc/systemd/system/rocky-analytics.service
[Unit]
Description=Rocky analytics pipeline
After=network-online.target
Wants=network-online.target
OnFailure=rocky-analytics-failed@%n.service

[Service]
Type=oneshot
User=dataeng
WorkingDirectory=/srv/analytics
# Pre-flight: exit 3 if config or warehouse connectivity is broken.
ExecStartPre=/usr/local/bin/rocky doctor
ExecStart=/srv/analytics/rocky-run.sh
# The wrapper already routes exit 2. Without this, OnFailure fires for it too.
SuccessExitStatus=2
```

```ini
# /etc/systemd/system/rocky-analytics.timer
[Unit]
Description=Run the Rocky analytics pipeline daily

[Timer]
OnCalendar=*-*-* 03:00:00
# Runs a missed occurrence once after boot. It does not replay a backlog.
Persistent=true

[Install]
WantedBy=timers.target
```

Enable it with `systemctl enable --now rocky-analytics.timer`. To let systemd route instead, drop the wrapper, set `ExecStart=/usr/local/bin/rocky run --output json`, and read `$EXIT_STATUS` in the `rocky-analytics-failed@` unit.

## GitHub Actions on a schedule

If GitHub's runners can reach your warehouse, a scheduled workflow needs no infrastructure. Store credentials as [encrypted secrets](/reference/authentication/) and pass them as environment variables.

```yaml
# .github/workflows/rocky-nightly.yml
on:
  schedule:
    - cron: "0 3 * * *"   # 03:00 UTC daily
  workflow_dispatch: {}
jobs:
  run:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash
      - run: rocky doctor
      - name: Run pipeline
        run: |
          set +e
          rocky run --output json | tee run.json
          code=${PIPESTATUS[0]}   # rocky's code, not tee's
          case "$code" in
            0|130) exit 0 ;;
            2)     echo "::warning::Partial success: some models failed."; exit 0 ;;
            *)     echo "::error::Run failed."; exit "$code" ;;
          esac
```

Here a partial success is a warning and the job stays green. A total failure fails the job. To run `rocky ci` on pull requests, see the [CI/CD guide](/guides/ci-cd/).

## Databricks Workflows and warehouse-native schedulers

Any scheduler that can run a shell command can run Rocky. The task's exit code propagates to the platform's own retry and alerting:

```bash
set -uo pipefail
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash
rocky doctor
rocky run --output json
```

Wire exit `2` to a warning and the rest to a failure if the scheduler distinguishes them. Snowflake Tasks, Airflow's `BashOperator` and cloud cron follow the same shape.

## Failure notification without a platform

Rocky has two mechanisms, both in `rocky.toml`.

### Webhook on pipeline error

A webhook hook posts the event context (run id, event, metadata) over HTTP, with an optional HMAC signature. It can target Slack, Teams, PagerDuty or Datadog.

```toml
[hook.webhooks.on_pipeline_error]
url = "${SLACK_WEBHOOK_URL}"
preset = "slack"
secret = "${WEBHOOK_SECRET}"
```

See [Hooks](/concepts/hooks/) for presets, body templates and HMAC verification.

### A richer digest with rocky brief

A command hook can render [`rocky brief`](/reference/commands/administration/) and send it by email or Slack. The digest covers runs, drift, freshness, quality and cost, plus holds, failure streaks and incidents when the scheduler is in use.

```toml
[[hook.on_pipeline_error]]
command = "/srv/analytics/send-brief.sh"
on_failure = "warn"
```

```bash
#!/usr/bin/env bash
# send-brief.sh: post the estate digest when a run fails.
set -uo pipefail
cd /srv/analytics
# Pass --output md: outside a terminal (hook, pipe, cron) rocky defaults to JSON.
digest="$(rocky brief --since 24h --output md)"
curl -fsS -X POST "$SLACK_WEBHOOK_URL" \
  -H 'Content-Type: application/json' \
  --data "$(jq -n --arg text "$digest" '{text: $text}')"
```

`[hook.webhooks.*]` posts the event context itself. `[[hook.*]]` runs a program you provide. Run `rocky validate` to confirm Rocky parsed either block. `rocky hooks list` shows only command hooks. Rocky skips an unknown or misspelled `on_<event>` key with a warning.

cron and systemd start with a nearly empty environment. A `${VAR}` in config that is unset makes `rocky validate` and the run fail with `1`. Export the secrets with `Environment=` or `EnvironmentFile=` (systemd) or a sourced file (cron).

## Health checks

### rocky doctor as a pre-flight

`rocky doctor` runs config, state, adapter and auth checks and **exits `3` if any is Critical** (a bad config, an unreachable warehouse). A degraded or unreadable local state store is a Warning and exits `0`.

```bash
rocky doctor                   # all checks, exits 3 on any Critical
rocky doctor --check config    # config only, offline and fast
rocky doctor --check auth      # ping the warehouse to catch a rotated credential
rocky doctor --check scheduler # is the reconciler alive and unwedged?
```

When any pipeline declares a `[schedule]`, `rocky doctor` also runs a `scheduler` check. It reads only the filesystem and the state store, and covers `rocky tick` and `rocky serve --scheduler`.

| Level | Condition |
|---|---|
| Critical | `.rocky/tick.lock` is held but its heartbeat is stale. The reconciler is wedged. Restart the holding process. |
| Warning | No tick has evaluated any schedule for more than twice the shortest cron interval. The timer looks dead. |

Give `--check scheduler` its own cron on the host that runs the reconciler.

### rocky history for past runs

`rocky history` reads the state store with no warehouse round-trip:

```bash
rocky history                       # recent runs: id, start, status, model count, trigger
rocky history --model fct_revenue   # one model's execution history
rocky history --since 2026-03-01    # runs on or after a date
rocky history --audit               # include the governance audit trail
```

`status` is `Success`, `PartialFailure` or `Failure`. `trigger` is `"Manual"` for a direct `rocky run` and `"Schedule"` for a run launched by `rocky tick`, joined to its tick by a shared `submission_id`.

## Native scheduling with `rocky tick` (experimental)

`rocky tick` moves the "which pipeline, when" decision into `rocky.toml`. Each pipeline declares a `cron` schedule, an `after` dependency or a `freshness` budget. One `rocky tick` evaluates all of it and runs what is due. Flags and exit codes: [`rocky tick`](/reference/commands/administration/#rocky-tick).

There is no daemon. Point your cron or systemd timer at `rocky tick` every minute instead of at one `rocky run`. The feature is **experimental** while the reconciler soaks.

Scheduling works on `replication`, `transformation`, `quality` and `snapshot` pipelines. `load` pipelines cannot take part: a load re-ingests every file each run, so scheduling one would duplicate data, and it records no run the scheduler can observe. `rocky validate` rejects a scheduled load, and an `after` that references one.

### Words this section uses

- **Demand:** a recorded request to run one pipeline, from a `cron` slot, an `after` dependency, a `freshness` budget or a webhook.
- **Tick:** one pass over all standing demand that runs what is due. The **reconciler** performs it: `rocky tick` once, `rocky serve --scheduler` in a loop.
- **Claim:** the marker written when the reconciler takes a demand, so a second tick cannot take it. The **stuck-claim resolver** settles a claim still `submitted` from an earlier tick using the child's run record. The **orphan sweep** at the end of a tick finishes each `submitted` claim whose run record is terminal.
- **Spool file:** holds an accepted webhook demand. Rocky writes and `fsync`s it before it answers.
- **Fail-closed:** Rocky refuses to act when it cannot confirm the conditions. **At-most-once:** attempted once, never retried: lost at worst, never duplicated.
- **`failure_backoff`:** the state after a scheduled run fails. Later ticks skip the pipeline until its `resume_at`.
- **Incident bundle:** one JSON file under `.rocky/incidents/` for one failed scheduled run.

### The demand lifecycle

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
                     │ tick: evaluate all  ├──► skipped[]  (not due,
                     │ standing demand     │    backoff, in flight)
                     └──────────┬──────────┘
                                │ due
                     ┌──────────▼──────────┐
                     │ claim: one owner    │
                     └──────────┬──────────┘
                     ┌──────────▼──────────┐
                     │ rocky run           │
                     └──────────┬──────────┘
                     ┌──────────▼──────────┐
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
[pipeline.raw]
type = "replication"
# ...adapter, source, target...
[pipeline.raw.schedule]
cron = "0 3 * * *"
timezone = "Europe/Lisbon"  # IANA name; default is the project [schedule].timezone, else UTC

[pipeline.staging]
type = "transformation"
models = "models/**"
[pipeline.staging.schedule]
after = ["raw"]             # run once raw has a newer success than staging's last
```

`cron`, `after` and `freshness` can combine. Any one source being due makes the pipeline due. Every key, the catch-up policy and the freshness semantics: [`[pipeline.*.schedule]`](/reference/configuration/#pipelinenameschedule).

### Drive it from a one-minute timer

```bash
# /etc/cron.d/rocky-tick
* * * * *  analytics  cd /srv/analytics && flock -n /var/lock/rocky-tick.lock rocky tick --output json >> /var/log/rocky/tick.log 2>&1
```

Or a systemd timer (`OnCalendar=*:0/1`, `AccuracySec=1s`) with a `oneshot` service that runs `/usr/local/bin/rocky tick --output json` with `SuccessExitStatus=2`, as above.

`rocky tick` takes its own non-blocking lock, `.rocky/tick.lock`, so two ticks never reconcile at once. The outer `flock` is a cheap early skip.

### The resident scheduler: `rocky serve --scheduler` (experimental)

If you already run `rocky serve`, the server can run the same reconciler in process on a poll interval. Same `[schedule]` declarations, same `.rocky/tick.lock` and `schedule_state`.

```bash
rocky serve --scheduler   # ticks every 15 seconds by default
rocky serve --scheduler --poll-interval-seconds 30 --drain-timeout-seconds 120
```

The resident form adds four things:

- **Runs show up as jobs.** Each scheduled run uses the same jobs model as `POST /api/v1/jobs/run`, so `GET /api/v1/jobs/{submission_id}` reports it.
- **It coordinates with API mutations.** A tick and an API `run` or `apply` never collide on the state store. The API caller gets `409 mutation_in_progress`. The scheduler skips the tick and re-evaluates next time.
- **Config is re-read every tick.** A parse error is logged and skipped. The loop never dies on a bad edit.
- **Shutdown drains.** On `SIGTERM` or `Ctrl-C` the server starts no new work. A run in flight gets up to `--drain-timeout-seconds`, never beyond its own `timeout_minutes`, then is terminated. In-flight HTTP requests drain in the same window. A run cut short is recorded as failed and not retried until its next occurrence. The first tick waits until the server has finished starting.

Read the state over HTTP:

```
GET /api/v1/schedule           # what is scheduled and in flight
GET /api/v1/schedule/spool     # webhook demands accepted but not yet run
```

`/api/v1/schedule` reports each scheduled pipeline: its configuration, last evaluation and fire, next expected fire, active backoff and in-flight claims, plus the tick-lock state. It reads stored state only and evaluates no demand (that is `rocky tick --dry-run`), so it is cheap and side-effect free, until a product declares `deliver_by`.

- **`tick_lock.state: free` is the normal state.** The lock is held only during a tick. To confirm a scheduler runs, compare `last_evaluated_at` with the cadence. `held` means a tick is running now. `wedged` means the heartbeat is stale: restart the reconciler.
- **A `next_fire_at` in the past means overdue.** The projection anchors on the last occurrence that fired, so a stalled timer reports the slot it missed. A pipeline whose schedule cannot be resolved carries a `config_error` and never fires.
- **`delivery` answers "will it land in time?".** It appears on a pipeline that builds the output model of a product with a [`deliver_by` deadline](/reference/commands/products/#delivery-deadline). For a transformation pipeline, Rocky reads the `models` glob with the same root, models directory and glob as `rocky run`, so a never-built model still counts. Ephemeral models do not count. Rocky loads but does not compile the models, so it detects no compile error. For any other pipeline type, Rocky reads the newest production run that built a deadlined model, within the newest 500 runs, and names the most urgent product. `on_time` and `late_by_seconds` describe the latest delivery. `at_risk` is `true` when the pipeline's p90 run time (from its last 20 successful production runs) is longer than the time left before `next_due_at`. With fewer than 5 runs, `estimate.state` is `unknown` and `at_risk` is `null`. Cost grows with model files plus pipelines x recorded runs.
- **`latest_start` answers "does the cron start early enough?".** It appears on each pipeline that feeds a deadline: one with a `delivery`, one marked `models_unreadable`, and every pipeline either runs `after`, directly or through others. `at` is the latest start that still meets every deadline it feeds. `product` and `due_at` name the deadline that sets it. Rocky works back along `after` from each deadline: at each pipeline it takes the earliest start-by time of what that pipeline feeds, then subtracts the pipeline's own p90 run time.

  ```text
    raw (p90 20m) ──after──▶ revenue (p90 10m)  due 06:00  start by 05:50
                  └─after──▶ costs   (p90 30m)  due 04:00  start by 03:30
    raw: min(05:50, 03:30) - 20m = 03:10
  ```

  `cron_check` compares the pipeline's own cron with `at`. Rocky only warns. A deadline never changes a cron and never adds an `after` edge. The first value that applies wins:

  | `cron_check` | When | Text |
  |---|---|---|
  | `disabled` | The schedule is disabled or cannot be resolved, so it never fires. | `warning` |
  | `overdue` | `next_fire_at` is earlier than now. Same test as `counts.overdue`. Checked even when `state` is `unknown`. When `at` is known and already passed, `cron_too_late` is `true`. | `warning` |
  | `not_judged` | `state` is `unknown` (unless the pipeline has no cron and an `after` upstream is known late: then `too_late`). Or the cron has no next fire yet. Or the pipeline has neither cron nor `after`. Or it has no cron, no upstream known late, and some `after` upstream cannot be judged. | none; in the last case, `note` names those upstreams |
  | `on_time` | `next_fire_at` is at or before `at`. | none |
  | `too_late` | `next_fire_at` is later than `at`, and the pipeline has no `after` or some `after` upstream does not qualify for `relies_on_after`; `cron_too_late` is `true`. Or the pipeline has no cron, runs only `after` others, and some are known late (`too_late`, `overdue` or `disabled`); `cron_too_late` is `false`. This holds even when `state` is `unknown`. | `warning`, naming the upstreams not in time |
  | `relies_on_after` | Every `after` upstream has a known `latest_start` and a `cron_check` of `on_time` or `relies_on_after`, and either `next_fire_at` is later than `at` or the pipeline has no cron. `cron_too_late` stays `false`. | `note` |

  `cron_too_late` is `true` for `too_late` on a pipeline with a cron, and for `overdue` when `at` is known and passed. It is `false` when `state` is `unknown` and on a pipeline with no cron. To alert on every risk, alert on `warning`.

  When any term is unknown, `state` is `unknown`, `at` is absent and `reasons` lists why:

  | Reason | Meaning |
  |---|---|
  | `missing_estimate` | A pipeline has fewer than 5 successful production runs. The `missing_estimate` field names them. |
  | `no_next_deadline` | A `deliver_by` cron has no next occurrence. |
  | `cycle` | `after` has a cycle on the way to the deadline. |
  | `overflow` | The subtraction left the date range. |
  | `models_unreadable` | A transformation pipeline's models directory is absent or cannot be located, or a model fails to load, so `rocky run` builds nothing. Whenever any product declares `deliver_by`, every such pipeline reads `models_unreadable`, carries a `warning` and has no `delivery`. `rocky product status` still shows the last delivery from run history. |

  Rocky does not know when an external source lands, so a late source does not show here.

The resident reconciler is one loop. A hung scheduled run stalls it: the server keeps serving HTTP but evaluates no schedule, and `rocky doctor --check scheduler` reports a dead timer. Set `timeout_minutes` on a `[schedule]` so a stuck run is terminated. Automatic recovery of a wedged reconciler is a planned follow-up.

A minimal systemd unit:

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

Use a `rocky tick` timer or `rocky serve --scheduler` per project, not both. See [One scheduler instance per project](#one-scheduler-instance-per-project).

### Event-driven triggers: webhook ingress (experimental)

Under `--scheduler`, an HTTP webhook can queue a run demand for a named pipeline: a Fivetran sync completing, an upstream job finishing, or a "run now" button.

```
POST /api/v1/hooks/trigger/{pipeline}
```

The route uses its own HMAC, not the `--token` Bearer token. Sign the raw body with HMAC-SHA256, hex-encoded, in the `X-Rocky-Signature` header:

```bash
export ROCKY_WEBHOOK_SECRET='a-long-random-secret'
rocky serve --scheduler   # in another shell

BODY=''
SIG=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac "$ROCKY_WEBHOOK_SECRET" | awk '{print $2}')
curl -sS -X POST http://127.0.0.1:8080/api/v1/hooks/trigger/orders \
  -H "X-Rocky-Signature: $SIG" \
  -H 'X-Rocky-Delivery: evt-2026-05-02-0001' \
  --data-binary "$BODY"
# → 202 {"demand":"accepted","demand_uid":"…"}
```

Every *accepted* demand is at-most-once. Whether the same *event* runs at most once depends on `X-Rocky-Delivery`, an optional idempotency key:

| Header | Dedup | Guarantee across sender retries |
|---|---|---|
| Present | Rocky deduplicates a redelivered id for 24 hours after consumption, answering `202 {"demand":"duplicate"}`. | At most once |
| Absent | Rocky deduplicates on the body hash, only while the demand is still queued. An identical body after consumption fires again. | At least once. Pair it with a `freshness` schedule. |

**Responses (fail-closed).**

| Status | When |
|---|---|
| `404` | No `ROCKY_WEBHOOK_SECRET` (a loopback-bound server accepts unsigned requests as a local-dev convenience). Or `serve` runs without `--scheduler`. Or the pipeline is not in your config, checked only after the signature verifies, so callers cannot enumerate names. |
| `401` | Unsigned or wrongly signed request. |
| `429` | Over-rate flood, with `Retry-After`, before anything is written. |

**At-most-once delivery.** Rocky writes an accepted webhook to a durable `fsync`'d spool file *before* it answers `202`, so a crash before the next tick never loses it. The reconciler then attempts each spooled demand exactly once. One narrow loss window exists: the reconciler crashes *after* claiming the demand, and the child run *also* dies before recording its outcome. Rocky finalizes that demand as a failure and does not re-run it. A child that outlives a dead reconciler still records its run. Rocky does not retry a webhook on delivery. **Give a webhook-only pipeline a `freshness` schedule as a backstop**:

```toml
[pipelines.orders.schedule]
freshness = true        # re-runs if it goes stale, even if a webhook is lost
```

A webhook run records `trigger: "webhook"` in `rocky history`, and a cron, `after` or freshness run records `schedule`. A demand whose pipeline left the config after acceptance is finalized without running, and logged. Between acceptance and the next tick, a demand is in no claim and no history row. To see that queue:

```
GET /api/v1/schedule/spool          # or: rocky state schedule spool
```

It lists what the ingress accepted and no tick consumed, with the `token` to match against your provider's delivery log. A present but unreadable spool is reported as an error, not an empty queue.

### One scheduler instance per project

Run exactly one reconciler per state file: one `rocky tick` timer or one `rocky serve --scheduler`. The `.rocky/tick.lock` flock and the state store's writer lock are local to one machine. Scheduler state (cursor and claim tables) is local-only: a remote `[state]` backend (S3, GCS, Valkey, tiered) never uploads it. Two hosts on one project keep independent cursors and both fire the same occurrence. Remote state is last-writer-wins, with no cross-host compare-and-swap.

For timers on several hosts, give each host its own project and state file, or keep one timer and let the other hosts call `rocky run` directly.

### Policy and freezes apply as they do to `rocky run`

A scheduled run is a plain `rocky run --pipeline <name>` in a child process. The [`[policy]`](/reference/configuration/#policy) plane governs it exactly like the same command typed by hand: the same freeze fence, drift auto-apply rules, cost `[budget]` and [`run` capability](/reference/configuration/#policy).

The child sets only `ROCKY_RUN_TRIGGER`, `ROCKY_SUBMISSION_ID` and the trace context. It never passes `--principal`, but it inherits `ROCKY_PRINCIPAL`. A scheduler started with `ROCKY_PRINCIPAL=agent` runs its children as an agent, and a `deny` that resolves for `run` refuses the scheduled run. Start it without that variable when it runs on a human's authority.

A freeze on a scheduled pipeline does not skip the tick. The child starts, the fence withholds its work, and the run records a failure plus a `hold` (`kind`, `freeze_id`, `scope`, `reason`) in the run record and `rocky run --output json`. The scheduler then:

- Does not retry a held run. `consecutive_failures` does not move, and the cursor records `freeze_hold`.
- Re-probes a standing (`after` or `freshness`) demand 15 minutes after its last attempt started. The interval does not grow.
- Finishes a held cron or webhook occurrence. It does not run again when the freeze lifts. A cron pipeline runs at its next occurrence.
- Writes one `freeze_hold` incident bundle for the first held run, naming the freeze. A run that is not held ends the episode. A different freeze starts a new one.
- Counts a run as a hold even if it also had other failures. Read the run in `rocky history` for the rest.
- Reports a hold under `counts.held`, not `counts.failed` or `counts.partial`. A hold alone does not make the tick exit non-zero.

When the fence cannot list the freeze markers, the run fails with no `hold`. That is a fault, so the usual retries, incident bundle and backoff follow.

### Preview before you wire the timer

`rocky tick --dry-run` reports what *would* run. It starts no pipeline and advances no cursor. It still opens the state store read-write: on a fresh project it can create the state file, and on an older store it can stamp the current schema version.

```bash
rocky tick --dry-run --now 2026-05-02T03:00:00Z --output json
```

`--now` pins the evaluation instant (RFC3339) to preview a future occurrence or a catch-up window.

### Exit codes and honest alerting

`rocky tick` uses the exit-code contract above: `0` when nothing was due or every launched run succeeded, `1` when the tick could not proceed (it ran nothing), `2` when a launched run failed or was partial. The same `rocky-run.sh` routing works.

**Exit `0` does not mean the estate is healthy.** After the tick that first sees a failure, the pipeline enters `failure_backoff`. Later ticks skip it and exit `0`. Each suppressed demand appears in `skipped[]` with a reason (`failure_backoff`, `partial_backoff`) and a `resume_at`. `counts` and the `consecutive_failures` metric carry the running total. Alert on those and on the [scheduler metrics](/guides/observability/).

A tick can also exit `0` having done nothing because another `rocky` process held the state store. It shows as one `state_busy` entry in `skipped[]`. One is normal and self-heals. A `state_busy` on every tick for many minutes means a wedged writer: alert on it.

### Incident bundles

When a scheduled run finalizes as a failure (full or partial, both trip backoff), the reconciler writes one JSON file under `.rocky/incidents/` with facts only: the pipeline, the demand source (`cron`, `after`, `freshness`, `webhook`), the outcome and occurrence, the submission id and exit code, the attempt count and consecutive-failure count after this failure, and retrieval pointers (a `rocky history` command, plus `/api/v1/jobs/{id}` and `/api/v1/schedule` under the resident scheduler). A fact the path cannot know is `null`, never a guessed zero, such as the exit code of a run recovered from a crashed owner.

The stuck-claim resolver and the orphan sweep also write bundles. The sweep catches a cron occurrence whose child never recorded a run (a power loss, a kill): after 5 minutes it records a lost failure, writes the bundle and does not re-run it. A child the spawner terminated for a shutdown drain gets no bundle, because a graceful shutdown is not an incident. A run that failed on its own during a drain still does.

Rocky keeps the newest 50 bundles. The sweep deletes only files the writer named, and refuses to work through a symlinked `incidents` directory.

## Where to go next

- [Observability](/guides/observability/): traces and metrics over OpenTelemetry.
- [Hooks](/concepts/hooks/): the lifecycle events behind the notification recipes.
- [Failure modes](/advanced/failure-modes/): how to read every failure Rocky reports.
- [CI/CD integration](/guides/ci-cd/): `rocky ci` for pull requests.
