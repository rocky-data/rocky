---
title: Authentication
description: PAT and OAuth M2M authentication for Databricks
sidebar:
  order: 4
---

Rocky supports two authentication methods for connecting to **Databricks** warehouses. They are auto-detected based on which credentials are available. These apply to all Databricks API calls — SQL Statement Execution, Unity Catalog operations, and workspace bindings.

## PAT (Personal Access Token)

**Tried first.** Simple token-based authentication, good for development.

- Set via environment variable: `DATABRICKS_TOKEN`
- Or in config: `token = "${DATABRICKS_TOKEN}"`
- Sent as: `Authorization: Bearer <token>`

## OAuth M2M (Service Principal)

**Used as fallback when the PAT token is empty.** Recommended for production deployments.

- Environment variables: `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`
- Token endpoint: `https://<host>/oidc/v1/token`
- Grant type: `client_credentials`
- Scope: `all-apis`
- Returns a short-lived access token that Rocky automatically refreshes

## Configuration

Authentication is configured on the Databricks adapter:

```toml
[adapter.prod]
type = "databricks"
host = "${DATABRICKS_HOST}"
http_path = "${DATABRICKS_HTTP_PATH}"

# PAT (takes precedence)
token = "${DATABRICKS_TOKEN}"

# OAuth M2M (used if token is empty)
# client_id = "${DATABRICKS_CLIENT_ID}"
# client_secret = "${DATABRICKS_CLIENT_SECRET}"
```

## Environment Variable Substitution

Rocky substitutes `${VAR_NAME}` references in `rocky.toml` at parse time. This means you can keep secrets out of your config files and inject them from the environment, CI/CD variables, or a secrets manager.

## Detection Order

1. If `token` is set and non-empty, Rocky uses PAT authentication
2. If `token` is empty or unset, Rocky checks for `client_id` and `client_secret`
3. If neither is configured, Rocky reports an error at startup

## Validation

Run `rocky validate` to check that at least one authentication method is properly configured before executing a pipeline.

## Source Adapter Authentication

Authentication for source adapters is separate from warehouse authentication. Each source adapter is its own `[adapter.NAME]` block. For Fivetran, authentication uses HTTP Basic Auth with `api_key` and `api_secret`. DuckDB and `manual` sources require no authentication.

```toml
[adapter.fivetran]
type = "fivetran"
destination_id = "${FIVETRAN_DESTINATION_ID}"
api_key = "${FIVETRAN_API_KEY}"
api_secret = "${FIVETRAN_API_SECRET}"
```
