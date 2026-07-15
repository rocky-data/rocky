# 03-workspace-isolation — Catalog isolation + workspace bindings

> **Category:** 04-governance
> **Credentials:** `DATABRICKS_HOST` + `DATABRICKS_TOKEN` + `DATABRICKS_HTTP_PATH` required
> **Runtime:** depends on Databricks API
> **Rocky features:** `[governance.isolation]`, workspace bindings, ISOLATED catalog mode

## What it shows

Set Databricks Unity Catalog catalogs to ISOLATED mode and bind them to
specific workspace IDs. This is the standard Databricks security pattern
for multi-tenant data platforms.

## Why it's distinctive

- **Declarative isolation** — the ISOLATED catalog mode and its
  `[[governance.isolation.workspace_ids]]` bindings live in `rocky.toml`;
  Rocky applies them as part of target setup on `rocky run`.
- Each binding carries an access level (`binding_type = "READ_WRITE"` or
  `"READ_ONLY"`), so a workspace can be granted read-only visibility into an
  otherwise isolated catalog.

## Run

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/<warehouse-id>"
./run.sh
```

## Expected output

`run.sh` validates the config, then writes golden JSON to `expected/`:

- `expected/run.json` — the executed result. The target catalog
  (`isolated_demo`) is created in ISOLATED mode and bound to the workspace
  ID declared under `[[pipeline.poc.target.governance.isolation.workspace_ids]]`.
