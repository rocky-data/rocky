# 01-unity-catalog-grants — Declarative GRANT/REVOKE on Databricks

> **Category:** 04-governance
> **Credentials:** `DATABRICKS_HOST` + `DATABRICKS_TOKEN` (or OAuth M2M) required
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
./run.sh
```
