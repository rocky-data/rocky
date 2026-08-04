---
title: Databricks adapter
description: Databricks SQL warehouse adapter — connection fields, PAT and OAuth M2M auth
sidebar:
  order: 2
---

Databricks SQL warehouse adapter. Executes SQL via the Statement Execution REST API and manages Unity Catalog governance.

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

PAT is tried first; OAuth M2M is the fallback when the PAT token is empty. Both apply to every Databricks API call — SQL Statement Execution, Unity Catalog operations, and workspace bindings.

[Authentication](/reference/authentication/) covers the detection order, token refresh, and validation in full.

## See also

- [`[adapter.NAME]`](/reference/configuration/#adaptername) — fields shared by every adapter type, including the retry policy.
- [Permissions](/reference/permissions/) — declarative Unity Catalog grants.
