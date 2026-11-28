# 02-schema-patterns-multi-tenant — `src__{tenant}__{regions...}__{source}`

> **Category:** 04-governance
> **Credentials:** `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_HTTP_PATH`,
> `FIVETRAN_API_KEY`, `FIVETRAN_API_SECRET`, `FIVETRAN_DESTINATION_ID` required
> **Runtime:** depends on Databricks + Fivetran APIs
> **Rocky features:** `source.schema_pattern` with multi-component layouts

## What it shows

Rocky parses source schema names like `src__acme__us_west__shopify` into
components and routes them to per-tenant target catalogs/schemas. The variadic
`regions...` component matches one or more values.

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

## Expected output

- `expected/discover.json`: Fivetran connectors and tables, each source schema
  parsed into `{tenant}__{regions...}__{source}`.
- `expected/plan_acme.json`: dry-run plan for `--filter tenant=acme`, routing to
  the `acme_warehouse` catalog and `staging__{regions}__{source}` schemas.

The final line prints `POC complete: discovered + planned for tenant=acme.`
