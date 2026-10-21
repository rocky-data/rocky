---
title: Fivetran adapter
description: The Fivetran source adapter, which reads connector and table metadata and, when both owners opt in, can start a sync
sidebar:
  order: 10
---

The Fivetran source adapter calls the Fivetran REST API to list connectors and their tables. It reads **metadata**. Rocky never moves data through this adapter. By default it never starts a sync either. Rocky may start a sync only when two opt-ins are set: `allow_sync_trigger = true` on the adapter (the owner of the key) and `trigger = true` on the pipeline. See [Start a sync](#start-a-sync).

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

A pipeline names this adapter in `source.discovery`, not in `source.adapter`. The warehouse that Fivetran writes to reads the rows (`V033` refuses a discovery-only `source.adapter`):

```toml
[pipeline.bronze.source]
adapter = "prod"            # the warehouse Fivetran lands tables in

[pipeline.bronze.source.discovery]
adapter = "fivetran"
```

## Wait for a running sync

By default `rocky run` reads whatever Fivetran has already landed. If a sync is running at that moment, the run may read half-landed data. To wait for that sync first, add a `sync` table to the pipeline's discovery block:

```toml
[pipeline.bronze.source.discovery]
adapter = "fivetran"

[pipeline.bronze.source.discovery.sync]
wait = true
wait_timeout_secs = 3600   # whole wait, all connectors
poll_interval_secs = 30    # at least 10
on_sync_failure = "fail"   # or "warn"
```

With `wait = true` alone, Rocky only reads. It sends `GET` requests to the connector list of the destination (one paged read per poll, whatever the connector count). It never starts, pauses or cancels a sync. A connector that is not syncing when the run begins is not waited for, and a paused connector never is. With no `sync` table, the run sends no extra request.

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

How Rocky decides how a sync ended: Fivetran gives no sync id. Rocky notes each connector's `succeeded_at` and `failed_at` when it first sees the sync running. When the connector stops syncing, a `succeeded_at` that moved is a success and a `failed_at` that moved is a failure. If both moved, the later one wins. If neither moved after three more polls, the outcome is `unknown`, which counts as a failure. Both values come from Fivetran, so no clock on your machine takes part.

| Key | Default | Meaning |
|-----|---------|---------|
| `wait` | `false` | Turn the wait on. The other keys do nothing without it. |
| `wait_timeout_secs` | `3600` | Longest wait for the whole phase. A sync still running at the limit counts as a failure (`timeout`). `rocky validate` requires at most half of `[state.idempotency] in_flight_ttl_hours`, so another run cannot reap this run's idempotency claim while it waits. |
| `poll_interval_secs` | `30` | Seconds between polls. At least 10. |
| `on_sync_failure` | `"fail"` | `fail` stops the run before any warehouse write. `warn` logs the failure and runs on the data Fivetran already landed, except after a sync that Rocky started and cannot call finished: see [Start a sync](#start-a-sync). |
| `trigger` | `false` | Start a sync first. Needs `wait = true` and `allow_sync_trigger = true` on the adapter. See [Start a sync](#start-a-sync). |

The wait runs before the run takes its state session, so a long wait holds no state lock. Because no state store is open yet:

- A run stopped by the wait writes no run record. `rocky history` does not show it. The `pipeline_start` then `pipeline_error` hooks fire, and the run JSON has `status: "Failure"` with the `source_syncs` rows.
- A Ctrl-C during the wait exits with code 130 and releases the idempotency claim.
- `rocky apply` never waits, for any plan. The reviewer did not see a wait, so apply must not cause one. Only `rocky run` does.
- `--filter` limits which connectors the wait covers.

The run JSON lists each connector that was waited for in `source_syncs`: `id`, `schema`, `outcome` (`succeeded`, `failed`, `timeout`, `unknown`, `interrupted`, `error`, `paused` or `trigger_failed`), `waited_secs`, `last_sync_at`, `triggered` (only when Rocky started the sync) and, for `error` and `trigger_failed`, `detail`. The array is absent when nothing was waited for.

Fivetran reports `rescheduled` for a sync it paused and will resume. Rocky treats it as still running and keeps waiting. A connector that is `rescheduled` at the first poll is waited for. If it has not finished at `wait_timeout_secs`, the outcome is `timeout`.

`unknown` has two causes:

- Fivetran writes `succeeded_at` before it clears `sync_state`. If the first poll sees the connector mid-sync with the new `succeeded_at` already set, Rocky records that new value as the starting point. When the sync then ends, nothing moved past it.
- The connector leaves `setup_state = "connected"` during the wait, so it drops out of the connector list.

Both fail closed on purpose. Use `on_sync_failure = "warn"` if you prefer to run anyway.

If the status reads fail, Rocky abandons the wait. After three failed polls in a row, every open connector ends as `error` with the error text in `detail`. With `on_sync_failure = "fail"`, a Fivetran API outage at the first poll stops every run that has `wait = true`: no run starts until the API answers or you set `wait = false`. With `"warn"` the run goes on, and `source_syncs` holds one row with `outcome: "error"` (`id: "*"` when the first poll failed, because no connector is known yet).

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

What Rocky sends: one `POST /v1/connections/{id}/sync` per connector, with `{"force": false}`. Rocky never forces a sync, never resyncs, never unpauses a connector and never cancels a sync. It still never moves data itself. Fivetran does.

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

Fivetran answers a sync request with no sync id and no start time. So Rocky decides which sync is its own from timestamps, and a timestamp counts as the end of Rocky's sync only if two tests pass:

1. It moved past the value the connector had at the idle read. Both values come from Fivetran, so no clock takes part.
2. It is later than `T_pre` plus 5 seconds. `T_pre` is the later of the `Date` header of Fivetran's answer to the `POST` and the `Date` of the idle read, so a sync that Fivetran scheduled itself and that ended before the `POST` is not counted, for example while the `POST` waited out a `429`. If neither has a `Date`, the clock of your machine is used. Test 1 still applies then, so a clock difference alone cannot make a sync look like Rocky's.

The 5 second margin covers the gap between the clock behind the `Date` header and the clock behind `succeeded_at`. It can reject a real sync that ends within 5 seconds of the `POST`, for example an empty incremental sync. If its timestamp lands after `T_pre` but inside the margin, it ends as `unknown` at once. If a poll saw the connector syncing first, it ends as `unknown` after a few idle polls. If nothing moved and no poll saw a sync, Rocky keeps waiting and it ends as `timeout` after the full `wait_timeout_secs`. All of these count as a failure. It fails closed.

One window stays open. A sync that starts after the idle read and before Rocky's `POST` is answered, and is still running then, is counted as Rocky's. Rocky sends `force = false`, so Fivetran starts no second sync: the `POST` joins the sync that runs, and Rocky cannot tell the two apart. The window can last minutes when earlier connectors hit `429` backoff.

Details:

- A sync that is running when the run begins is waited out first and is not counted. The request goes out only after a read shows the connector idle.
- A paused connector gets the outcome `paused`, no request, and does not stop the run: a person paused it on purpose.
- A `403` means the key's role cannot start syncs. The connector ends as `trigger_failed`, no further request goes out on that key, and the message says: use a key whose role can run syncs, or set `trigger = false`.
- A request is never retried after a timeout or a `5xx`, because the sync may have started. Rocky treats the request as sent: the row stays `triggered: true` with a `detail` saying the request may have been sent, and Rocky keeps waiting, as it does for a slow start. A sync that starts later is waited out. Rocky reads the connector once more right after that answer, and a sync that ended before that read is not counted. If that read fails, the row ends as `unknown` with `triggered: true` at once. If that read shows a sync running, Rocky counts it as its own (the same open window as above). If Rocky's own sync ended before that read, Rocky misses it and the row ends as `timeout`. If none is seen, the row ends as `timeout` or `unknown` with `triggered: true`, which `on_sync_failure = "warn"` cannot cover (see below).
- A `409` is taken as "the connector is busy". Fivetran's public docs do not say what a `409` on this endpoint means, so Rocky takes the safe side: it treats the connector as syncing, waits for it to be idle without counting that sync, and then sends the request again. After a second `409` (counted over the whole run) Rocky sends no more requests to that connector: it waits until three reads in a row show the connector idle (the status can lag behind the sync the `409` announced), then ends it as `trigger_failed` with `triggered: false`. A connector never gets more than two requests in one run. Three idle reads make it unlikely, but cannot rule out, that the run reads while the busy sync writes.
- A connect error or a `429` means Fivetran did not take the request, so those retry within the retry budget. When the retries run out, or Fivetran answers `400` or `404`, the request was refused: the connector ends as `trigger_failed` with `triggered: false`, no `source_sync_triggered` event fires, and Rocky does not read the connector again, because a sync that Fivetran started itself is not Rocky's.
- `rocky run --watch` never starts a sync, because every file save would start one. It still waits for a running sync.
- `rocky run --resume <run-id>` and `--resume-latest` never start a sync, the same as `--no-sync-trigger`. They still wait for a sync that is already running.
- `rocky run --no-sync-trigger` skips the request for one run. The run still waits for a sync that is already running. It does not change the config, so `rocky discover` still reports `sync_trigger: true`.
- Every request that Fivetran took appears in `source_syncs` (`triggered: true`) and as a `source_sync_triggered` event (run id, connector id, schema, `idle_seen_at`, which is `T_pre`). A refused request (`403`, `400`, `404`, an exhausted `429`) does not: its row has `triggered: false`. If a signal arrives while the request is in flight, the row is `interrupted` with `triggered: true` and a `detail` saying the request may have been sent.
- `on_sync_failure = "warn"` does not cover a sync that Rocky started and cannot call finished (`timeout`, `error` or `unknown` on a row with `triggered: true`). That sync may still be writing the tables the run would read, so the run stops as with `fail`. `warn` keeps its meaning for rows where Rocky started nothing, and for a sync that finished as `failed`.
- `rocky apply` never starts a sync, for any plan.
- `rocky validate` refuses `trigger = true` without `wait = true`, and without `allow_sync_trigger = true` on the adapter. It warns (`V063`) when the pipeline also has a `[schedule]` cron, because every tick then starts a vendor sync and spends vendor compute. It warns (`V064`) when two pipelines set `trigger = true` on the same discovery adapter with overlapping `schema_pattern` prefixes: in one `rocky run --dag` each pipeline sends its own request for a connector both select, and the vendor runs two syncs. Rocky keeps no record of requests across the sub-runs of a DAG, so set `trigger = true` on one of them.

### Dagster

A pipeline that starts its own syncs advances each connector's `last_sync_at` itself. A Dagster sensor that runs the pipeline whenever `last_sync_at` moves would start a second run for each sync. So `rocky discover` reports `sync_trigger: true` for such a pipeline, and `rocky_source_sensor` stands down: it advances its cursor and emits no run. Pass `include_rocky_triggered=True` to keep the old behaviour. The cost: a sync that Fivetran starts on its own schedule no longer fires the sensor either, and the next Rocky run picks it up. If you start syncs with Dagster's own Fivetran integration instead, set `trigger = false` and keep `wait = true`.

## Authentication

HTTP Basic Auth using `api_key` and `api_secret`. Source-adapter authentication is separate from warehouse authentication — see [Authentication](/reference/authentication/#source-adapter-authentication).

## See also

- [`[adapter.NAME]`](/reference/configuration/#adaptername) — fields shared by every adapter type, including the retry policy.
- [Fivetran state cache](/reference/fivetran-state-cache/) — the `cache`, `ratelimit`, `stampede` and `circuit_breaker` blocks that share work across processes.
- [`rocky discover --emit-fivetran-state-to`](/reference/commands/core-pipeline/#emitting-the-fivetran-state-envelope) — write the state envelope to a file.
