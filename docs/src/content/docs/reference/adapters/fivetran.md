---
title: Fivetran adapter
description: Fivetran source adapter — metadata-only discovery over the Fivetran REST API
sidebar:
  order: 5
---

Fivetran source adapter. Calls the Fivetran REST API to discover connectors and tables. **Metadata only**: Rocky never moves data through this adapter.

## Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `destination_id` | string | Yes | Fivetran destination ID. |
| `api_key` | string | Yes | Fivetran API key (Basic Auth). |
| `api_secret` | string | Yes | Fivetran API secret (Basic Auth). |

```toml
[adapter.fivetran]
type = "fivetran"
destination_id = "${FIVETRAN_DESTINATION_ID}"
api_key = "${FIVETRAN_API_KEY}"
api_secret = "${FIVETRAN_API_SECRET}"
```

## Authentication

HTTP Basic Auth using `api_key` and `api_secret`. Source-adapter authentication is separate from warehouse authentication — see [Authentication](/reference/authentication/#source-adapter-authentication).

## See also

- [`[adapter.NAME]`](/reference/configuration/#adaptername) — fields shared by every adapter type, including the retry policy.
- [Fivetran state cache](/reference/fivetran-state-cache/) — sharing the resolved state envelope across processes.
