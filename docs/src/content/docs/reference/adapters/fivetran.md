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
| `on_sync_failure` | `"fail"` | `fail` stops the run before any warehouse write. `warn` logs the failure and runs on the data Fivetran already landed. |
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
   |-- idle  -> T_pre = Fivetran's own clock (the `Date` header) at that read
   |             one POST
   |             poll until:
   |               succeeded_at > T_pre + 5 s -> succeeded
   |               failed_at    > T_pre + 5 s -> failed (the later one wins)
```

Fivetran answers a sync request with no sync id and no start time. So Rocky decides which sync is its own from timestamps. The connector was idle at `T_pre`, so a sync that ends after `T_pre` began after it. The 5 second margin covers the gap between the clock behind the `Date` header and the clock behind `succeeded_at`. The margin can reject a real sync that ends within 5 seconds of the idle read, for example an empty incremental sync. That ends as `unknown`, which counts as a failure. It fails closed. One window stays open: a sync that Fivetran schedules itself between the idle read and the `POST` is taken as the one Rocky started, and its data can predate the request by that gap.

Details:

- A sync that is running when the run begins is waited out first and is not counted. The request goes out only after a read shows the connector idle.
- A paused connector gets the outcome `paused`, no request, and does not stop the run: a person paused it on purpose.
- A `403` means the key's role cannot start syncs. The connector ends as `trigger_failed`, no further request goes out on that key, and the message says: use a key whose role can run syncs, or set `trigger = false`.
- A request is never retried after a timeout or a `5xx`, because the sync may have started. Rocky reads the connector once more. If a sync is running or ended past `T_pre`, it is accepted. Otherwise the connector ends as `trigger_failed`. A connect error or a `429` means Fivetran did not take the request, so those retry within the retry budget.
- `rocky run --watch` never starts a sync, because every file save would start one. It still waits for a running sync.
- `rocky run --no-sync-trigger` skips the request for one run. The run still waits for a sync that is already running. It does not change the config, so `rocky discover` still reports `sync_trigger: true`.
- Every request appears in `source_syncs` (`triggered: true`) and as a `source_sync_triggered` event (run id, connector id, schema, `idle_seen_at`).
- `rocky apply` never starts a sync, for any plan.
- `rocky validate` refuses `trigger = true` without `wait = true`, and without `allow_sync_trigger = true` on the adapter. It warns (`V063`) when the pipeline also has a `[schedule]` cron, because every tick then starts a vendor sync and spends vendor compute.

### Dagster

A pipeline that starts its own syncs advances each connector's `last_sync_at` itself. A Dagster sensor that runs the pipeline whenever `last_sync_at` moves would start a second run for each sync. So `rocky discover` reports `sync_trigger: true` for such a pipeline, and `rocky_source_sensor` stands down: it advances its cursor and emits no run. Pass `include_rocky_triggered=True` to keep the old behaviour. The cost: a sync that Fivetran starts on its own schedule no longer fires the sensor either, and the next Rocky run picks it up. If you start syncs with Dagster's own Fivetran integration instead, set `trigger = false` and keep `wait = true`.

## Authentication

HTTP Basic Auth using `api_key` and `api_secret`. Source-adapter authentication is separate from warehouse authentication — see [Authentication](/reference/authentication/#source-adapter-authentication).

## See also

- [`[adapter.NAME]`](/reference/configuration/#adaptername) — fields shared by every adapter type, including the retry policy.
- [Fivetran state cache](/reference/fivetran-state-cache/) — the `cache`, `ratelimit`, `stampede` and `circuit_breaker` blocks that share work across processes.
- [`rocky discover --emit-fivetran-state-to`](/reference/commands/core-pipeline/#emitting-the-fivetran-state-envelope) — write the state envelope to a file.
