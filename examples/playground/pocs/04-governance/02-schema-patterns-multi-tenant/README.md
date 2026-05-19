# 02-schema-patterns-multi-tenant — `src__{tenant}__{regions...}__{source}`

> **Category:** 04-governance
> **Credentials:** `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_HTTP_PATH`,
> `FIVETRAN_API_KEY`, `FIVETRAN_API_SECRET`, `FIVETRAN_DESTINATION_ID` required
> **Runtime:** depends on Databricks + Fivetran APIs
> **Rocky features:** `source.schema_pattern` with multi-component layouts

## What it shows

Multi-tenant schema patterns: Rocky parses source schema names like
`src__acme__us_west__shopify` into structured components and routes them
to per-tenant target catalogs/schemas. Components support a variadic
`...` suffix for collecting regions.

## Why it's distinctive

- **Multi-tenant data platform pattern** that dbt struggles to express
  declaratively.
- The variadic `regions...` component matches one-or-more values without
  needing per-tenant duplication.

## Run

```bash
export DATABRICKS_HOST="..."
export DATABRICKS_TOKEN="..."
export DATABRICKS_HTTP_PATH="..."
export FIVETRAN_API_KEY="..."
export FIVETRAN_API_SECRET="..."
export FIVETRAN_DESTINATION_ID="..."
./run.sh
```
