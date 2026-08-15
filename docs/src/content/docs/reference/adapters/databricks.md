---
title: Databricks adapter
description: The Databricks adapter's connection fields, and how it picks between a token and OAuth
sidebar:
  order: 2
---

The Databricks warehouse adapter runs your SQL through the Statement Execution REST API. It also manages Unity Catalog governance: catalogs, schemas, tags, and grants.

## Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `host` | string | Yes | Workspace hostname (e.g., `"workspace.cloud.databricks.com"`). |
| `http_path` | string | Yes | SQL warehouse HTTP path (e.g., `"/sql/1.0/warehouses/abc123"`). |
| `token` | string | No | Personal Access Token. Tried first if set. |
| `client_id` | string | No | OAuth M2M client ID (service principal). Used as fallback when `token` is not set. |
| `client_secret` | string | No | OAuth M2M client secret. Required if `client_id` is set. |
| `timeout_secs` | integer | No | Statement execution timeout in seconds (default `120`). Increase for large full-refresh queries. |

```toml
[adapter.prod]
type = "databricks"
host = "${DATABRICKS_HOST}"
http_path = "${DATABRICKS_HTTP_PATH}"
token = "${DATABRICKS_TOKEN}"
```

OAuth M2M instead of PAT:

```toml
[adapter.prod]
type = "databricks"
host = "${DATABRICKS_HOST}"
http_path = "${DATABRICKS_HTTP_PATH}"
client_id = "${DATABRICKS_CLIENT_ID}"
client_secret = "${DATABRICKS_CLIENT_SECRET}"
```

## Authentication

Rocky tries the personal access token (PAT) first. If `token` is empty, it falls back to OAuth machine-to-machine (M2M), which authenticates a service principal with `client_id` and `client_secret`.

Whichever one wins covers every Databricks call Rocky makes: SQL statement execution, Unity Catalog operations, and workspace bindings.

[Authentication](/reference/authentication/) covers the detection order, token refresh, and validation in full.

## See also

- [`[adapter.NAME]`](/reference/configuration/#adaptername) — fields shared by every adapter type, including the retry policy.
- [Permissions](/reference/permissions/) — declarative Unity Catalog grants.
