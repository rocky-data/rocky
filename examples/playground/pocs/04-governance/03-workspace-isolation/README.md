# 03-workspace-isolation — Catalog isolation + workspace bindings

> **Category:** 04-governance
> **Credentials:** `DATABRICKS_HOST` + `DATABRICKS_TOKEN` + `DATABRICKS_HTTP_PATH` required
> **Runtime:** depends on Databricks API
> **Rocky features:** `[governance.isolation]`, workspace bindings, ISOLATED catalog mode

## What it shows

Sets Unity Catalog catalogs to ISOLATED mode and binds them to workspace IDs.
Each `[[governance.isolation.workspace_ids]]` binding has an access level
(`binding_type = "READ_WRITE"` or `"READ_ONLY"`). Rocky applies both as part of
target setup on `rocky run`.

## Run

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/<warehouse-id>"
./run.sh
```

The source table `main.raw__orders.orders` must exist in your workspace. Edit
`[[adapter.local_discovery.schemas]]` to point at a table you have.

## Expected output

`expected/run.json`: the `isolated_demo` catalog is created in ISOLATED mode and
bound to the workspace ID under
`[[pipeline.poc.target.governance.isolation.workspace_ids]]`.
