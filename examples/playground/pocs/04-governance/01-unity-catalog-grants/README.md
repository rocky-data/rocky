# 01-unity-catalog-grants — Declarative GRANT on Databricks

> **Category:** 04-governance
> **Credentials:** `DATABRICKS_HOST` + `DATABRICKS_TOKEN` + `DATABRICKS_HTTP_PATH` required
> **Runtime:** depends on Databricks API
> **Rocky features:** `[[pipeline.<name>.target.governance.grants]]`, `[[…governance.schema_grants]]`, grants applied during `rocky run`

## What it shows

Declarative permissions for Unity Catalog. Rocky reads the
`[[pipeline.poc.target.governance.grants]]` (catalog) and
`[[pipeline.poc.target.governance.schema_grants]]` (schema) blocks and issues
a `GRANT` statement for each declared permission. Rocky never revokes. A grant
that you remove from `rocky.toml` stays on the warehouse until you revoke it
yourself.

## Why it's distinctive

- **GitOps for permissions** — store grants in `rocky.toml` and let Rocky
  apply them on every run.
- A second run changes nothing on the warehouse if the declared grants did not
  change. Rocky grants again, and a repeated `GRANT` is a no-op.

## Run

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/<warehouse-id>"
./run.sh
```

The source table comes from the manual discovery adapter in `rocky.toml`,
which lists `raw__orders.orders`. It must exist as `main.raw__orders.orders`
in your workspace; edit `[[adapter.local_discovery.schemas]]` to point at a
table you have.

## Expected output

`run.sh` writes golden JSON to `expected/`:

- `expected/plan.json` — the dry-run replication plan: the `CREATE CATALOG`,
  `CREATE SCHEMA` and copy statements for `raw__orders.orders`. Grants are not
  in the plan; Rocky applies them during the run.
- `expected/run.json` — the executed result; inspect its `permissions` block to
  see the grants that were applied. `grants_revoked` is always `0`.

A second `./run.sh` changes nothing on the warehouse if the declared grants did not change.
