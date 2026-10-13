---
title: Authentication
description: Where each adapter reads its credentials, and how Rocky picks between a PAT and OAuth M2M on Databricks
sidebar:
  order: 9
---

Each adapter reads its credentials from its own `[adapter.NAME]` block, except BigQuery, which reads them from the environment. Put `${VAR}` references in the block, never the secret itself.

| Adapter | Credentials | Details |
|---|---|---|
| Databricks | `token` (PAT), else `client_id` + `client_secret` (OAuth M2M) | This page |
| Snowflake | `pat`, `oauth_token`, key pair, or password | [Snowflake](/reference/adapters/snowflake/#authentication) |
| BigQuery | `BIGQUERY_TOKEN` or `GOOGLE_APPLICATION_CREDENTIALS` | [BigQuery](/reference/adapters/bigquery/#authentication) |
| PostgreSQL, Redshift | `username` + `password` | [PostgreSQL](/reference/adapters/postgres/#authentication), [Redshift](/reference/adapters/redshift/#authentication) |
| SQL Server | SQL login, Entra ID token, or service principal | [SQL Server](/reference/adapters/sqlserver/#authentication) |
| ClickHouse | `username` + `password` | [ClickHouse](/reference/adapters/clickhouse/#authentication-and-tls) |
| Spark | optional bearer `token` | [Spark](/reference/adapters/spark/#fields) |
| Fivetran | `api_key` + `api_secret` | [Fivetran](/reference/adapters/fivetran/#authentication) |
| DuckDB, `manual` | none | — |

The rest of this page covers Databricks. Rocky connects to it two ways. You do not choose between them explicitly: Rocky looks at which credentials you supplied and picks. The choice applies to every Databricks API call — SQL statement execution, Unity Catalog operations, and workspace bindings.

## Detection Order

Rocky checks the personal access token first, and falls back to the service principal:

```
   read [adapter.NAME]
          │
          ▼
   ┌─────────────────┐   yes   ┌──────────────────────────┐
   │ `token` set and ├────────►│ PAT                      │
   │ non-empty?      │         │ Authorization: Bearer …  │
   └────────┬────────┘         └──────────────────────────┘
            │ no
            ▼
   ┌──────────────────┐  yes   ┌──────────────────────────┐
   │ `client_id` and  ├───────►│ OAuth M2M                │
   │ `client_secret`? │        │ POST /oidc/v1/token      │
   └────────┬─────────┘        │ short-lived access token │
            │ no               └──────────────────────────┘
            ▼
   error at startup
```

## PAT (Personal Access Token)

A single long-lived token. Rocky tries this first. Good for development.

- Set `token` in the adapter block, for example `token = "${DATABRICKS_TOKEN}"`.
- Rocky sends it as `Authorization: Bearer <token>`.

## OAuth M2M (Service Principal)

A client ID and secret that Rocky exchanges for a short-lived token. Rocky uses this when the PAT is empty. Prefer it in production.

- Set `client_id` and `client_secret` in the adapter block, for example from `${DATABRICKS_CLIENT_ID}` and `${DATABRICKS_CLIENT_SECRET}`.
- Rocky calls the token endpoint `https://<host>/oidc/v1/token`.
- Grant type: `client_credentials`. Scope: `all-apis`.
- The endpoint returns a short-lived access token, and Rocky refreshes it for you.

## Configuration

Both methods live on the Databricks adapter block:

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

Rocky replaces every `${VAR_NAME}` reference in `rocky.toml` when it parses the file. Keep the secrets themselves out of the config and inject them from the environment, from CI/CD variables, or from a secrets manager.

## Validation

Run `rocky validate` before a pipeline. It warns when a Databricks block sets neither `token` nor `client_id` (`V013`).

## Source Adapter Authentication

Source adapters authenticate separately from the warehouse. Each one is its own `[adapter.NAME]` block. Fivetran uses HTTP Basic Auth with `api_key` and `api_secret` — see the [Fivetran adapter](/reference/adapters/fivetran/) for the full block. DuckDB and `manual` sources need no credentials at all.
