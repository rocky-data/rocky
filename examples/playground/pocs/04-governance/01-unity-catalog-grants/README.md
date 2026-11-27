# 01-unity-catalog-grants — Declarative GRANT on Databricks

> **Category:** 04-governance
> **Credentials:** `DATABRICKS_HOST` + `DATABRICKS_TOKEN` + `DATABRICKS_HTTP_PATH` required
> **Runtime:** depends on Databricks API
> **Rocky features:** `[[pipeline.<name>.target.governance.grants]]`, `[[…governance.schema_grants]]`, grants applied during `rocky run`

## What it shows

Rocky reads the `[[pipeline.poc.target.governance.grants]]` (catalog) and
`[[pipeline.poc.target.governance.schema_grants]]` (schema) blocks and issues a
`GRANT` for each declared permission. Rocky never revokes. A grant that you
remove from `rocky.toml` stays on the warehouse until you revoke it yourself.

## Run

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/<warehouse-id>"
./run.sh
```

The source table `main.raw__orders.orders` must exist in your workspace (the
manual discovery adapter lists it). Edit `[[adapter.local_discovery.schemas]]`
to point at a table you have.

## Expected output

- `expected/plan.json`: the dry-run replication plan (`CREATE CATALOG`,
  `CREATE SCHEMA`, copy). Grants are not in the plan; Rocky applies them during
  the run.
- `expected/run.json`: the `permissions` block shows the grants applied.
  `grants_revoked` is always `0`.

A second `./run.sh` changes nothing on the warehouse if the declared grants did
not change (a repeated `GRANT` is a no-op).
