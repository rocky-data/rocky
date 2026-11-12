---
title: Airbyte adapter
description: The Airbyte source adapter, which lists connections and their streams and, when both owners opt in, can start a sync
sidebar:
  order: 11
---

The Airbyte source adapter calls the Airbyte API (`{host}/v1/...`) to list connections and their streams. It reads **metadata**. Rocky never moves data through this adapter. By default it never starts a sync either. Rocky may start a sync only when two opt-ins are set: `allow_sync_trigger = true` on the adapter (the owner of the token) and `trigger = true` on the pipeline. See [Start a sync](#start-a-sync).

## Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `kind` | string | Yes | Must be `"discovery"`. Airbyte lists tables and moves no data. |
| `host` | string | Yes | Airbyte API root, for example `https://api.airbyte.com`. |
| `token` | string | No | Bearer token. Airbyte Cloud needs one. A self-hosted instance may not. |
| `allow_sync_trigger` | bool | No | Default `false`. The owner of the token lets Rocky start syncs with it. A pipeline can then ask for that with `trigger = true`. Without this key the adapter sends no `POST`, and `rocky validate` refuses a pipeline that sets `trigger = true`. |

```toml
[adapter.airbyte]
type = "airbyte"
kind = "discovery"
host = "https://api.airbyte.com"
token = "${AIRBYTE_TOKEN}"
```

Discovery returns the `active` connections whose `namespaceFormat` or name starts with the pipeline's `schema_pattern` prefix. A pipeline names this adapter in `source.discovery`. The warehouse that Airbyte writes to reads the rows:

```toml
[pipeline.bronze.source]
adapter = "prod"            # the warehouse Airbyte lands tables in

[pipeline.bronze.source.discovery]
adapter = "airbyte"
```

## Wait for a running sync

`[pipeline.NAME.source.discovery.sync] wait = true` works on Airbyte the same way it works on Fivetran. The keys, the outcomes and the run JSON are the same. See [Wait for a running sync](/reference/adapters/fivetran/#wait-for-a-running-sync) on the Fivetran page. This section covers only what is different on Airbyte.

With `wait = true` alone, Rocky only reads. Each poll sends one `GET /v1/connections`, then one `GET /v1/jobs` for each active connection the prefix selects, up to 10 at a time. An inactive connection gets no jobs read. The jobs read asks for the 25 newest jobs, newest first (`orderBy=createdAt|DESC`). If the jobs are not in that order (job ids not falling), the read fails. Rocky builds each connection's state from them. One failed jobs read fails the whole poll. The pipeline's `include` filter is applied before the jobs reads, so a connection it leaves out costs no jobs read and cannot fail the poll.

| State | Where it comes from |
|-------|---------------------|
| syncing | The newest job has not ended: `pending`, `running`, `incomplete`, or a status Rocky does not know. Every job type counts, a reset or clear too, because it also writes to the destination. Airbyte runs one job per connection. A job that has not ended but is older than one that has is stale, and Rocky ignores it. |
| paused | The connection is `inactive`, or has a status Rocky does not know. Rocky never starts a sync on it and does not read its jobs. |
| `succeeded_at` | The latest `lastUpdatedAt` of a `sync` or `refresh` job that `succeeded`. |
| `failed_at` | The latest `lastUpdatedAt` of a `sync` or `refresh` job that `failed` or was `cancelled`. A cancelled sync is not a success, so Rocky counts it as a failure. |

A `reset` or `clear` job never sets `succeeded_at` or `failed_at`. It is not the sync Rocky asked for. A connection that has only had such jobs ends a triggered sync as `unknown`.

A `deprecated` connection is deleted in Airbyte. Rocky leaves it out: discovery never returns it, so the run never reads it.

Why `lastUpdatedAt`: Airbyte sets it when a job changes state. For a job that ended, that is the time it ended. `startTime` is not an end. A job that ended with no `lastUpdatedAt` gives no end time, so it never counts as the end of Rocky's sync, and the wait fails closed. A `lastUpdatedAt` that is not RFC 3339 fails the read: Rocky never guesses a time.

Only the newest 25 jobs are read. A connection whose last success is older than that has no `succeeded_at`. That is safe: a sync that ends during the wait still sets a new one.

## Start a sync

With both opt-ins set, `rocky run` starts a sync on each selected connection before it reads, then waits for it. The rules are the same as on Fivetran. See [Start a sync](/reference/adapters/fivetran/#start-a-sync).

```toml
[adapter.airbyte]
type = "airbyte"
kind = "discovery"
host = "https://api.airbyte.com"
token = "${AIRBYTE_TOKEN}"
allow_sync_trigger = true            # the token owner's opt-in

[pipeline.bronze.source.discovery.sync]
wait = true                          # required
trigger = true                       # the pipeline's opt-in
```

What Rocky sends: one `POST /v1/jobs` per connection, with `{"connectionId": "<id>", "jobType": "sync"}`. Rocky never sends a `reset` or clear job, never activates an inactive connection and never cancels a job. It still never moves data itself. Airbyte does.

How Rocky reads each answer:

| Answer | Meaning |
|--------|---------|
| `2xx` with a `sync` job in the body | The sync started. Its `Date` header is the lower bound for the end of the sync (`T_pre`). |
| `2xx` with any other body (not a job, or a job that is not a `sync`) | Rocky does not trust it. The connection ends as `trigger_failed`. A redirected `POST` ends up here. |
| `403` | The token cannot start syncs. The message says: use a token whose role can run syncs on this workspace, or set `trigger = false`. No more requests go out. |
| `409` | A sync already runs. Rocky waits for it to end, does not count it, and asks once more. This follows Airbyte's API description. It has not been checked against a live Airbyte yet. |
| `400`, `404`, a `429` after the retries, a connect error (a connect timeout too) | The request was refused. The connection ends as `trigger_failed`. |
| A timeout, a `5xx`, a connection lost mid-request | The answer is unknown. Rocky treats the request as sent and never sends it again. |

Airbyte answers a sync request with a job id. Rocky logs it but does not use it. It decides which sync is its own from the timestamps, with the same two tests as on Fivetran: the timestamp moved past its value at the idle read, and it is later than `T_pre` plus 5 seconds.
