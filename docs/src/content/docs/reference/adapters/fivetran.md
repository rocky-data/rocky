---
title: Fivetran adapter
description: The Fivetran source adapter, which reads connector and table metadata and, when both owners opt in, can start a sync
sidebar:
  order: 10
---

The Fivetran source adapter calls the Fivetran REST API to list connectors and their tables. It reads **metadata**. Rocky never moves data through it. By default it never starts a sync either: that needs two opt-ins, `allow_sync_trigger = true` on the adapter (the key owner) and `trigger = true` on the pipeline. See [Start a sync](#start-a-sync).

## Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `kind` | string | Yes | Must be `"discovery"`. Fivetran lists tables and moves no data, so Rocky refuses the block without it (`V032`). |
| `destination_id` | string | Yes | Fivetran destination ID. |
| `api_key` | string | Yes | Fivetran API key (Basic Auth). |
| `api_secret` | string | Yes | Fivetran API secret (Basic Auth). |
| `allow_sync_trigger` | bool | No | Default `false`. The owner of the key lets Rocky start syncs with it. A pipeline can then ask for that with `trigger = true`. Without this key the adapter sends no `POST`, and `rocky validate` refuses a pipeline that sets `trigger = true`. |

```toml
[adapter.fivetran]
type = "fivetran"
kind = "discovery"
destination_id = "${FIVETRAN_DESTINATION_ID}"
api_key = "${FIVETRAN_API_KEY}"
api_secret = "${FIVETRAN_API_SECRET}"
```

A pipeline names this adapter in `source.discovery`, not in `source.adapter`. The warehouse Fivetran writes to reads the rows (`V033` refuses a discovery-only `source.adapter`):

```toml
[pipeline.bronze.source]
adapter = "prod"            # the warehouse Fivetran lands tables in

[pipeline.bronze.source.discovery]
adapter = "fivetran"
```

## Wait for a running sync

By default `rocky run` reads whatever Fivetran has already landed, so a run during a sync may read half-landed data. To wait for that sync first, add a `sync` table to the discovery block:

```toml
[pipeline.bronze.source.discovery]
adapter = "fivetran"

[pipeline.bronze.source.discovery.sync]
wait = true
wait_timeout_secs = 3600   # whole wait, all connectors
poll_interval_secs = 30    # at least 10
on_sync_failure = "fail"   # or "warn"
```

With `wait = true` alone, Rocky only sends `GET` requests to the connector list of the destination (one paged read per poll). It never starts, pauses or cancels a sync. A connector that is not syncing when the run begins is not waited for, and a paused connector never is. With no `sync` table, the run sends no extra request.

```
rocky run
   |
   v
read connector state (GET)
   |-- nothing syncing ---------------> run as before
   |-- a sync is running: poll every poll_interval_secs
         |-- sync ended, new succeeded_at -> run
         |-- sync ended, new failed_at ----> on_sync_failure
         |-- still syncing at the timeout -> on_sync_failure
         '-- Ctrl-C / SIGTERM -------------> exit 130
```

Fivetran gives no sync id. Rocky notes each connector's `succeeded_at` and `failed_at` when it first sees the sync running. When the connector stops syncing, a `succeeded_at` that moved is a success and a `failed_at` that moved is a failure. If both moved, the later one wins. If neither moved after three more polls, the outcome is `unknown`, which counts as a failure. Both values come from Fivetran, so no clock on your machine takes part.

Keys (`wait`, `wait_timeout_secs`, `poll_interval_secs`, `on_sync_failure`, `trigger`) and their defaults: [`[pipeline.NAME.source.discovery.sync]`](/reference/configuration/#pipelinenamesourcediscoverysync). With `on_sync_failure = "warn"`, a sync that Rocky started and cannot call finished still stops the run: see [Start a sync](#start-a-sync).

The wait runs before the run takes its state session, so a long wait holds no state lock. Because no state store is open yet:

- A run stopped by the wait writes no run record, so `rocky history` does not show it. The `pipeline_start` then `pipeline_error` hooks fire, and the run JSON has `status: "Failure"` with the `source_syncs` rows.
- A Ctrl-C during the wait exits with code 130 and releases the idempotency claim.
- `rocky apply` never waits, for any plan: the reviewer did not see a wait. Only `rocky run` does.
- `--filter` limits which connectors the wait covers.

The run JSON lists each connector waited for in `source_syncs`: `id`, `schema`, `outcome` (`succeeded`, `failed`, `timeout`, `unknown`, `interrupted`, `error`, `paused` or `trigger_failed`), `waited_secs`, `last_sync_at`, `triggered` (only when Rocky started the sync) and, for `error` and `trigger_failed`, `detail`. The array is absent when nothing was waited for.

Fivetran reports `rescheduled` for a sync it paused and will resume. Rocky treats it as still running and keeps waiting. A connector that is `rescheduled` at the first poll is waited for. If it has not finished at `wait_timeout_secs`, the outcome is `timeout`.

Fivetran reports `rescheduled` for a sync it paused and will resume. Rocky treats it as still running. If it has not finished at `wait_timeout_secs`, the outcome is `timeout`.

`unknown` has two causes. Both fail closed on purpose. Use `on_sync_failure = "warn"` to run anyway.

- Fivetran writes `succeeded_at` before it clears `sync_state`. If the first poll sees the connector mid-sync with the new `succeeded_at` already set, Rocky records that value as the starting point, and nothing moves past it.
- The connector leaves `setup_state = "connected"` during the wait, so it drops out of the connector list.

If the status reads fail, Rocky abandons the wait. After three failed polls in a row, every open connector ends as `error` with the text in `detail`. With `on_sync_failure = "fail"`, an API outage at the first poll stops every run that has `wait = true` until the API answers or you set `wait = false`. With `"warn"` the run goes on, and `source_syncs` holds one `error` row (`id: "*"` when the first poll failed).

## Start a sync

With both opt-ins set, `rocky run` starts a sync on each selected connector before it reads, then waits for that sync to end. The wait runs on the same terms as above.

```toml
[adapter.fivetran]
type = "fivetran"
kind = "discovery"
destination_id = "${FIVETRAN_DESTINATION_ID}"
api_key = "${FIVETRAN_API_KEY}"
api_secret = "${FIVETRAN_API_SECRET}"
allow_sync_trigger = true            # the key owner's opt-in

[pipeline.bronze.source.discovery.sync]
wait = true                          # required
trigger = true                       # the pipeline's opt-in
```

What Rocky sends: one `POST /v1/connections/{id}/sync` per connector, with `{"force": false}`. Rocky never forces a sync, never resyncs, never unpauses a connector and never cancels a sync. Fivetran moves the data.

```
for each connector the run selects (--filter limits it)
   |
   |-- paused ---------------------------> no request, outcome `paused`
   |-- a sync is running now -> wait for it to end (not counted)
   |-- idle  -> note succeeded_at and failed_at at that read (the baseline)
   |             one POST; T_pre = Fivetran's own clock (the `Date` header of
   |             the answer to the POST)
   |             poll until:
   |               succeeded_at moved AND > T_pre + 5 s -> succeeded
   |               failed_at    moved AND > T_pre + 5 s -> failed (the later one wins)
```

Fivetran answers a sync request with no sync id and no start time, so Rocky decides which sync is its own from timestamps. A timestamp counts as the end of Rocky's sync only if two tests pass:

1. It moved past the value the connector had at the idle read.
2. It is later than `T_pre` plus 5 seconds. `T_pre` is the later of the `Date` header of the `POST` answer and of the idle read, so a sync Fivetran scheduled itself and that ended before the `POST` is not counted. With no `Date`, your machine's clock is used, and test 1 still applies.

The 5 second margin can reject a real sync that ends within 5 seconds of the `POST` (an empty incremental sync). If its timestamp lands after `T_pre` but inside the margin, it ends as `unknown` at once. If a poll saw the connector syncing first, it ends as `unknown` after a few idle polls. If nothing moved and no poll saw a sync, Rocky waits the full `wait_timeout_secs` and ends as `timeout`. All count as a failure.

One window stays open. A sync that starts after the idle read and before the `POST` is answered, and is still running then, is counted as Rocky's. With `force = false` the `POST` joins the running sync, so Rocky cannot tell them apart. The window can last minutes when earlier connectors hit `429` backoff.

| Situation | What Rocky does |
|---|---|
| A sync is running when the run begins | Waits it out, does not count it. The `POST` goes out only after a read shows the connector idle. |
| Connector is paused | Outcome `paused`, no request. Does not stop the run. |
| `403` | The key's role cannot start syncs. Outcome `trigger_failed`, no more requests on that key. Use a key whose role can run syncs, or set `trigger = false`. |
| Timeout or `5xx` | Never retried, because the sync may have started. The row stays `triggered: true` with a `detail`, and Rocky keeps waiting. It reads the connector once more: a sync seen running is counted as its own; if that read fails the row ends `unknown`; otherwise `timeout` or `unknown`, which `on_sync_failure = "warn"` cannot cover. |
| `409` | Treated as "the connector is busy" (Fivetran's docs do not say what it means). Waits for idle without counting that sync, then sends the request again. After a second `409` in the run, no more requests: Rocky waits for three idle reads in a row, then ends as `trigger_failed` with `triggered: false`. A connector gets at most two requests per run. |
| Connect error or `429` | Fivetran did not take the request. Retries within the retry budget. When they run out, or on `400` or `404`: `trigger_failed`, `triggered: false`, no `source_sync_triggered` event, no further read. |
| Signal while the request is in flight | The row is `interrupted` with `triggered: true` and a `detail`. |
| `rocky run --watch` | Never starts a sync (every save would start one). Still waits for a running one. |
| `--resume <run-id>`, `--resume-latest`, `--no-sync-trigger` | Never start a sync. Still wait for a running one. `--no-sync-trigger` does not change the config, so `rocky discover` still reports `sync_trigger: true`. |
| `rocky apply` | Never starts a sync, for any plan. |

`on_sync_failure = "warn"` does not cover a sync that Rocky started and cannot call finished (`timeout`, `error` or `unknown` with `triggered: true`): it may still be writing the tables the run would read, so the run stops as with `fail`. `warn` applies where Rocky started nothing, and to a sync that ended `failed`.

Every request Fivetran took appears in `source_syncs` (`triggered: true`) and as a `source_sync_triggered` event (run id, connector id, schema, `idle_seen_at`, which is `T_pre`).

`rocky validate` refuses `trigger = true` without `wait = true`, and without `allow_sync_trigger = true` on the adapter. It warns (`V063`) when the pipeline also has a `[schedule]` cron, because every tick then starts a vendor sync and spends vendor compute. It warns (`V064`) when two pipelines set `trigger = true` on the same discovery adapter with overlapping `schema_pattern` prefixes: in one `rocky run --dag` each sends its own request for a shared connector, and the vendor runs two syncs. Rocky keeps no record of requests across the sub-runs of a DAG, so set `trigger = true` on one of them.

### Dagster

A pipeline that starts its own syncs advances each connector's `last_sync_at` itself. A Dagster sensor that runs the pipeline whenever `last_sync_at` moves would start a second run per sync. So `rocky discover` reports `sync_trigger: true` for such a pipeline, and `rocky_source_sensor` stands down: it advances its cursor and emits no run. Pass `include_rocky_triggered=True` to keep the old behaviour. Then a sync Fivetran starts on its own schedule no longer fires the sensor either, and the next Rocky run picks it up. If you start syncs with Dagster's own Fivetran integration, set `trigger = false` and keep `wait = true`.

## Authentication

HTTP Basic Auth using `api_key` and `api_secret`. Source-adapter authentication is separate from warehouse authentication — see [Authentication](/reference/authentication/#source-adapter-authentication).

## See also

- [`[adapter.NAME]`](/reference/configuration/#adaptername) — fields shared by every adapter type, including the retry policy.
- [Fivetran state cache](/reference/fivetran-state-cache/) — the `cache`, `ratelimit`, `stampede` and `circuit_breaker` blocks that share work across processes.
- [`rocky discover --emit-fivetran-state-to`](/reference/commands/core-pipeline/#emitting-the-fivetran-state-envelope) — write the state envelope to a file.
