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

The rest of this page covers Databricks. Rocky checks which credentials you supplied and picks the method, for every Databricks API call (SQL statement execution, Unity Catalog operations, workspace bindings):

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

- **PAT (personal access token).** One long-lived token, good for development. Set `token`. Rocky sends `Authorization: Bearer <token>`.
- **OAuth M2M (service principal).** Used when the PAT is empty. Prefer it in production. Set `client_id` and `client_secret`. Rocky calls `https://<host>/oidc/v1/token` with grant type `client_credentials` and scope `all-apis`, and refreshes the short-lived token for you.

```toml
[adapter.prod]
type = "databricks"
host = "${DATABRICKS_HOST}"
http_path = "${DATABRICKS_HTTP_PATH}"
token = "${DATABRICKS_TOKEN}"                   # PAT (takes precedence)
# client_id = "${DATABRICKS_CLIENT_ID}"         # OAuth M2M (used if token is empty)
# client_secret = "${DATABRICKS_CLIENT_SECRET}"
```

Rocky resolves every `${VAR_NAME}` when it parses the config ([Environment variables](/reference/configuration/#environment-variables)), so inject secrets from the environment, CI/CD variables or a secrets manager. `rocky validate` warns when a Databricks block sets neither `token` nor `client_id` (`V013`).

## Source Adapter Authentication

Source adapters authenticate separately from the warehouse. Each one is its own `[adapter.NAME]` block. Fivetran uses HTTP Basic Auth with `api_key` and `api_secret` — see the [Fivetran adapter](/reference/adapters/fivetran/) for the full block. DuckDB and `manual` sources need no credentials at all.
