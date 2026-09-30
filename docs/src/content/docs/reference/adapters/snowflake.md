---
title: Snowflake adapter
description: The Snowflake adapter's connection fields, and the order it tries four kinds of credential
sidebar:
  order: 3
---

The Snowflake warehouse adapter accepts four kinds of credential: a Programmatic Access Token (PAT), an OAuth token, a key pair (RS256 JWT), and a password.

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

Rocky tries the credentials in a fixed order and uses the first one the config supplies.

```
  pat                        ──set──► Programmatic Access Token
     │ not set
     ▼
  oauth_token                ──set──► OAuth
     │ not set
     ▼
  private_key_path + username ──set──► key-pair JWT (RS256)
     │ not set
     ▼
  username + password         ──set──► password
     │ not set
     ▼
  error: no authentication configured
```

So a config that sets both `pat` and `password` authenticates with the PAT, and never uses the password.

Pick one. Each example is a complete adapter declaration, not a complete `rocky.toml`. These three are not the full set: Rocky also accepts an `oauth_token`, which the order above ranks second.

**Programmatic Access Token (PAT).** Use this for trial accounts and scripts. Issue the token in Snowsight.

```toml
[adapter.snow]
type = "snowflake"
account = "${SNOWFLAKE_ACCOUNT}"
warehouse = "COMPUTE_WH"
pat = "${SNOWFLAKE_PAT}"
```

**Key-pair JWT.** Use this in production. You can rotate the key, and it is scoped to one user.

```toml
[adapter.snow]
type = "snowflake"
account = "${SNOWFLAKE_ACCOUNT}"
warehouse = "COMPUTE_WH"
username = "${SNOWFLAKE_USER}"
private_key_path = "${SNOWFLAKE_KEY_PATH}"
```

**Password.**

```toml
[adapter.snow]
type = "snowflake"
account = "${SNOWFLAKE_ACCOUNT}"
warehouse = "COMPUTE_WH"
username = "${SNOWFLAKE_USER}"
password = "${SNOWFLAKE_PASSWORD}"
```

## See also

- [`[adapter.NAME]`](/reference/configuration/#adaptername) — fields shared by every adapter type, including the retry policy.
