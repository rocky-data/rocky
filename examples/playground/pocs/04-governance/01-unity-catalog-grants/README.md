# 01-unity-catalog-grants — Declarative GRANT/REVOKE on Databricks

> **Category:** 04-governance
> **Credentials:** `DATABRICKS_HOST` + `DATABRICKS_TOKEN` + `DATABRICKS_HTTP_PATH` required
> **Runtime:** depends on Databricks API
> **Rocky features:** `[[governance.grants]]`, `[[governance.schema_grants]]`, `SHOW GRANTS` reconciliation

## What it shows

Declarative permissions for Unity Catalog. Rocky compares the
`[[governance.grants]]` block against `SHOW GRANTS` output and emits the
diff (`GRANT` and `REVOKE` statements) to converge to the desired state.

## Why it's distinctive

- **GitOps for permissions** — store grants in `rocky.toml` and let Rocky
  reconcile them on every run.
- Idempotent: subsequent runs are no-ops if nothing changed.

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
  in the plan; Rocky reconciles them against `SHOW GRANTS` during the run.
- `expected/run.json` — the executed result; inspect its `permissions` block to
  see the grants that were applied.

A second `./run.sh` is a no-op on the permissions block if nothing changed.
