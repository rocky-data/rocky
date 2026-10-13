# 04-tagging-lifecycle — `[governance.tags]` propagation

> **Category:** 04-governance
> **Credentials:** `DATABRICKS_HOST` + `DATABRICKS_TOKEN` + `DATABRICKS_HTTP_PATH` required
> **Runtime:** depends on Databricks API
> **Rocky features:** `[pipeline.<name>.target.governance.tags]`, `ALTER ... SET TAGS`

## What it shows

Tags declared in `[pipeline.poc.target.governance.tags]` are applied to catalogs/schemas/tables
during `rocky run` via `ALTER ... SET TAGS`. Schema-pattern components
(here `source`, from `components = ["source"]`) are also auto-applied as
tags so you can filter by them in Unity Catalog.

## Run

```bash
export DATABRICKS_HOST="..."
export DATABRICKS_TOKEN="..."
export DATABRICKS_HTTP_PATH="..."
./run.sh
```

The source table comes from the manual discovery adapter in `rocky.toml`,
which lists `raw__orders.orders`. It must exist as `main.raw__orders.orders`
in your workspace; edit `[[adapter.local_discovery.schemas]]` to point at a
table you have.

## Expected output

`run.sh` validates the config, then writes golden JSON to `expected/`:

- `expected/run.json`: the executed result. The `tagged_demo` catalog, its
  `staging__orders` schema and the copied table carry the three tags from
  `rocky.toml` (`managed_by`, `team`, `environment`), plus the `source`
  component tag.

For per-model tags on a model's own table or view, see the model sidecar
`[governance.tags]` block in the model format reference.
