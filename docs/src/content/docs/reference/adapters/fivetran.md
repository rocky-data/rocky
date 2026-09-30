---
title: Fivetran adapter
description: The Fivetran source adapter, which reads connector and table metadata and nothing else
sidebar:
  order: 5
---

The Fivetran source adapter calls the Fivetran REST API to list connectors and their tables. It reads **metadata only**. Rocky never moves data through this adapter.

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

## Authentication

HTTP Basic Auth using `api_key` and `api_secret`. Source-adapter authentication is separate from warehouse authentication — see [Authentication](/reference/authentication/#source-adapter-authentication).

## See also

- [`[adapter.NAME]`](/reference/configuration/#adaptername) — fields shared by every adapter type, including the retry policy.
- [Fivetran state cache](/reference/fivetran-state-cache/) — sharing the resolved state envelope across processes.
- [`rocky discover --emit-fivetran-state-to`](/reference/commands/core-pipeline/#emitting-the-fivetran-state-envelope) — write the state envelope to a file.
