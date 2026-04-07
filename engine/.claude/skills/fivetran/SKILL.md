---
name: fivetran-api
description: Fivetran REST API reference for Rocky's source adapter. Use when implementing connector discovery, schema config parsing, sync detection, pagination, or any Fivetran API integration in the rocky-fivetran crate.
---

# Fivetran REST API Reference

## Quick Reference

For complete endpoint details, request/response examples, and parameters:
- `references/rest_api_collection.json` — Full Postman collection (17k+ lines)

## Authentication

Basic Auth with API key and secret:

```
Authorization: Basic base64({api_key}:{api_secret})
```

Base URL: `https://api.fivetran.com`

## Endpoints Used by Rocky

### Connectors (Primary — rocky-fivetran/connector.rs)

| Operation | Method | Endpoint |
|---|---|---|
| List all in group | GET | `/v1/groups/{groupId}/connectors` |
| Get details | GET | `/v1/connectors/{connectorId}` |
| Get state | GET | `/v1/connectors/{connectorId}/state` |
| Sync now | POST | `/v1/connectors/{connectorId}/sync` |

Key fields in connector response:
- `id` — Connector ID
- `group_id` — Destination/group ID
- `service` — Connector type (e.g., "facebook_ads")
- `schema` — Schema name (e.g., "q__raw__acme__na__fb_ads")
- `status.setup_state` — "connected", "incomplete", etc.
- `status.sync_state` — "syncing", "scheduled", "paused"
- `succeeded_at` — Last successful sync timestamp (ISO 8601)
- `failed_at` — Last failed sync timestamp

### Schemas (Primary — rocky-fivetran/schema.rs)

| Operation | Method | Endpoint |
|---|---|---|
| Get schema config | GET | `/v1/connectors/{connectorId}/schemas` |
| Get table columns | GET | `/v1/connectors/{connectorId}/schemas/{schemaName}/tables/{tableName}/columns` |

Schema config response structure (nested):
```json
{
  "schemas": {
    "schema_name": {
      "enabled": true,
      "tables": {
        "table_name": {
          "enabled": true,
          "sync_mode": "SOFT_DELETE",
          "columns": {
            "column_name": {
              "enabled": true,
              "hashed": false
            }
          }
        }
      }
    }
  }
}
```

### Groups/Destinations

| Operation | Method | Endpoint |
|---|---|---|
| List connectors in group | GET | `/v1/groups/{groupId}/connectors` |
| Get destination | GET | `/v1/destinations/{destinationId}` |

## Pagination

Cursor-based pagination on all list endpoints:

```
GET /v1/groups/{groupId}/connectors?cursor={next_cursor}&limit=100
```

Response:
```json
{
  "code": "Success",
  "data": {
    "items": [...],
    "next_cursor": "eyJza..."
  }
}
```

When `next_cursor` is absent or null, you've reached the last page.

## Rate Limits

- 100 requests per minute
- Implement exponential backoff on HTTP 429
- Cache GET responses (5-min TTL in memory, shared via Valkey)

## Response Format

```json
{
  "code": "Success",
  "data": { ... }
}
```

Error codes: `NotFound`, `BadRequest`, `Unauthorized`, `RateLimitExceeded`

## Implementation Notes for Rocky

1. **Connector discovery** — List all connectors in the destination group, filter by schema pattern (`q__raw__*`)
2. **Schema config** — Fetch per-connector, parse nested structure to get enabled tables
3. **Sync detection** — Compare `succeeded_at` against cursor timestamp
4. **Caching** — Cache connector list and schema configs in memory (5-min TTL) and Valkey (shared across pods)
5. **Error handling** — Fivetran API can return HTML on 5xx errors; handle non-JSON responses gracefully
