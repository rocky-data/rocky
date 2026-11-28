# 04-tagging-lifecycle — `[governance.tags]` propagation

> **Category:** 04-governance
> **Credentials:** `DATABRICKS_HOST` + `DATABRICKS_TOKEN` + `DATABRICKS_HTTP_PATH` required
> **Runtime:** depends on Databricks API
> **Rocky features:** `[pipeline.<name>.target.governance.tags]`, `ALTER ... SET TAGS`

## What it shows

`rocky run` applies the `[pipeline.poc.target.governance.tags]` tags to
catalogs, schemas and tables via `ALTER ... SET TAGS`. Schema-pattern components
(here `source`) are applied as tags too.

## Run

```bash
export DATABRICKS_HOST="..."
export DATABRICKS_TOKEN="..."
export DATABRICKS_HTTP_PATH="..."
./run.sh
```

The source table `main.raw__orders.orders` must exist in your workspace. Edit
`[[adapter.local_discovery.schemas]]` to point at a table you have.

## Expected output

`expected/run.json`: the `tagged_demo` catalog, its `staging__orders` schema and
the copied table carry the three tags from `rocky.toml` (`managed_by`, `team`,
`environment`) plus the `source` component tag.

For per-model tags on a model's own table or view, see the model sidecar
`[governance.tags]` block in the model format reference.
