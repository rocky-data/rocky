---
title: Fivetran adapter
description: The Fivetran source adapter, which reads connector and table metadata and nothing else
sidebar:
  order: 10
---

The Fivetran source adapter calls the Fivetran REST API to list connectors and their tables. It reads **metadata only**. Rocky never moves data through this adapter, and never starts a sync.

## Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `kind` | string | Yes | Must be `"discovery"`. Fivetran lists tables and moves no data, so Rocky refuses the block without it (`V032`). |
| `destination_id` | string | Yes | Fivetran destination ID. |
| `api_key` | string | Yes | Fivetran API key (Basic Auth). |
| `api_secret` | string | Yes | Fivetran API secret (Basic Auth). |

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

Rocky only reads. It sends `GET` requests to the connector list of the destination (one paged read per poll, whatever the connector count). It never starts, pauses or cancels a sync. A connector that is not syncing when the run begins is not waited for, and a paused connector never is. With no `sync` table, the run sends no extra request.

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

The wait runs before the run takes its state session, so a long wait holds no state lock. Because no state store is open yet:

- A run stopped by the wait writes no run record. `rocky history` does not show it. The `pipeline_start` then `pipeline_error` hooks fire, and the run JSON has `status: "Failure"` with the `source_syncs` rows.
- A Ctrl-C during the wait exits with code 130 and releases the idempotency claim.
- `rocky apply` never waits, for any plan. The reviewer did not see a wait, so apply must not cause one. Only `rocky run` does.
- `--filter` limits which connectors the wait covers.

The run JSON lists each connector that was waited for in `source_syncs`: `id`, `schema`, `outcome` (`succeeded`, `failed`, `timeout`, `unknown`, `interrupted` or `error`), `waited_secs`, `last_sync_at` and, for `error`, `detail`. The array is absent when nothing was waited for.

Fivetran reports `rescheduled` for a sync it paused and will resume. Rocky treats it as still running and keeps waiting. A connector that is `rescheduled` at the first poll is waited for. If it has not finished at `wait_timeout_secs`, the outcome is `timeout`.

`unknown` has two causes:

- Fivetran writes `succeeded_at` before it clears `sync_state`. If the first poll sees the connector mid-sync with the new `succeeded_at` already set, Rocky records that new value as the starting point. When the sync then ends, nothing moved past it.
- The connector leaves `setup_state = "connected"` during the wait, so it drops out of the connector list.

Both fail closed on purpose. Use `on_sync_failure = "warn"` if you prefer to run anyway.

If the status reads fail, Rocky abandons the wait. After three failed polls in a row, every open connector ends as `error` with the error text in `detail`. With `on_sync_failure = "fail"`, a Fivetran API outage at the first poll stops every run that has `wait = true`: no run starts until the API answers or you set `wait = false`. With `"warn"` the run goes on, and `source_syncs` holds one row with `outcome: "error"` (`id: "*"` when the first poll failed, because no connector is known yet).

## Authentication

HTTP Basic Auth using `api_key` and `api_secret`. Source-adapter authentication is separate from warehouse authentication — see [Authentication](/reference/authentication/#source-adapter-authentication).

## See also

- [`[adapter.NAME]`](/reference/configuration/#adaptername) — fields shared by every adapter type, including the retry policy.
- [Fivetran state cache](/reference/fivetran-state-cache/) — the `cache`, `ratelimit`, `stampede` and `circuit_breaker` blocks that share work across processes.
- [`rocky discover --emit-fivetran-state-to`](/reference/commands/core-pipeline/#emitting-the-fivetran-state-envelope) — write the state envelope to a file.
