---
title: Snowflake adapter
description: Snowflake warehouse adapter — connection fields and the PAT / OAuth / key-pair / password priority order
sidebar:
  order: 3
---

Snowflake warehouse adapter. Supports Programmatic Access Token (PAT), OAuth, key-pair (RS256 JWT), and password authentication.

## Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `account` | string | Yes | Snowflake account identifier (e.g., `"org-account"`). |
| `warehouse` | string | Yes | Warehouse name for query execution. |
| `database` | string | No | Default database. |
| `schema` | string | No | Default schema. |
| `role` | string | No | Role to assume. |
| `username` | string | No | Username for key-pair or password auth. |
| `password` | string | No | Password for password auth. |
| `private_key_path` | string | No | Path to PKCS#8 PEM private key for key-pair JWT auth. |
| `oauth_token` | string | No | Pre-supplied OAuth token from an IdP. |
| `pat` | string | No | Programmatic Access Token (issued via Snowsight User Profile). Sent as a Bearer token with the `PROGRAMMATIC_ACCESS_TOKEN` token-type header, distinct from `oauth_token`. |

## Authentication

Priority order: **PAT** (highest) > **OAuth** > **key-pair JWT** > **password** (lowest).

```toml
# Programmatic Access Token (PAT) auth — recommended for trial accounts and
# scripts; issue via Snowsight → User Profile → Personal Access Tokens.
[adapter.snow]
type = "snowflake"
account = "${SNOWFLAKE_ACCOUNT}"
warehouse = "COMPUTE_WH"
pat = "${SNOWFLAKE_PAT}"

# Key-pair JWT auth — recommended for production (rotateable, scoped per user).
[adapter.snow]
type = "snowflake"
account = "${SNOWFLAKE_ACCOUNT}"
warehouse = "COMPUTE_WH"
username = "${SNOWFLAKE_USER}"
private_key_path = "${SNOWFLAKE_KEY_PATH}"

# Password auth
[adapter.snow]
type = "snowflake"
account = "${SNOWFLAKE_ACCOUNT}"
warehouse = "COMPUTE_WH"
username = "${SNOWFLAKE_USER}"
password = "${SNOWFLAKE_PASSWORD}"
```

## See also

- [`[adapter.NAME]`](/reference/configuration/#adaptername) — fields shared by every adapter type, including the retry policy.
