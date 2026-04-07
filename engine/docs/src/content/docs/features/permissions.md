---
title: Permissions
description: Declarative RBAC with automatic GRANT/REVOKE reconciliation for Databricks Unity Catalog
sidebar:
  order: 3
---

Rocky manages **Databricks Unity Catalog** permissions declaratively. Define grants inline in `rocky.toml` and Rocky reconciles them during each `rocky run` — there is no separate permissions command. Permissions are applied as part of the governance setup phase, before parallel table processing begins.

## Inline Grants (Recommended)

Define grants directly under the pipeline target in `rocky.toml`. These apply to **every** managed catalog and schema created by that pipeline:

```toml
[pipeline.bronze.target.governance]
auto_create_catalogs = true
auto_create_schemas = true

# Grants applied to every managed catalog
[[pipeline.bronze.target.governance.grants]]
principal = "group:data_engineers"
permissions = ["USE CATALOG", "MANAGE"]

[[pipeline.bronze.target.governance.grants]]
principal = "group:analysts"
permissions = ["BROWSE", "USE CATALOG"]

# Grants applied to every managed schema
[[pipeline.bronze.target.governance.schema_grants]]
principal = "group:data_engineers"
permissions = ["USE SCHEMA", "SELECT", "MODIFY"]

[[pipeline.bronze.target.governance.schema_grants]]
principal = "group:analysts"
permissions = ["USE SCHEMA", "SELECT"]
```

Inline grants are applied as best-effort during `rocky run`. If a grant fails (e.g., the principal doesn't exist), Rocky logs a warning and continues.

## Reconciliation Flow

Permission reconciliation runs during `rocky run`, integrated into the catalog and schema creation sequence:

1. **Read** desired permissions from `[pipeline.<name>.target.governance.grants]` and `[pipeline.<name>.target.governance.schema_grants]` in config
2. **Query** current state with Databricks `SHOW GRANTS ON CATALOG` and `SHOW GRANTS ON SCHEMA`
3. **Compute diff**: determine which grants need to be added and which need to be revoked
4. **Execute** the necessary Databricks `GRANT` and `REVOKE` statements

## Workspace Isolation

Rocky can isolate catalogs to specific Databricks workspaces using the **Databricks Unity Catalog** workspace bindings REST API (`PATCH /api/2.1/unity-catalog/bindings/catalog/{name}`). Each binding declares a workspace ID and an access level (`READ_WRITE` or `READ_ONLY`):

```toml
[pipeline.bronze.target.governance.isolation]
enabled = true

[[pipeline.bronze.target.governance.isolation.workspace_ids]]
id = 123456789
binding_type = "READ_WRITE"

[[pipeline.bronze.target.governance.isolation.workspace_ids]]
id = 987654321
binding_type = "READ_ONLY"
```

`binding_type` defaults to `"READ_WRITE"` if omitted. When enabled, Rocky:
1. Binds each managed catalog to the specified workspaces with their declared access level
2. Sets the catalog's isolation mode to `ISOLATED`

This prevents other workspaces from accessing the catalog. Workspace binding and isolation are applied as best-effort — failures are logged but don't block the run.

## Managed Permissions

Rocky manages the following permission types:

- `BROWSE`
- `USE CATALOG`
- `USE SCHEMA`
- `SELECT`
- `MODIFY`
- `MANAGE`

## Skipped Permissions

The following permission types are ignored during reconciliation and will never be granted or revoked by Rocky:

- `OWNERSHIP`
- `ALL PRIVILEGES`
- `CREATE SCHEMA` (non-managed)

## Principal Validation

Principal names are validated against the pattern `^[a-zA-Z0-9_ \-\.@]+$`. In generated SQL, principals are always wrapped in backticks to handle spaces and special characters safely:

```sql
GRANT USE CATALOG ON CATALOG acme_warehouse TO `group:data_engineers`
```

## Tagging

Rocky sets tags on catalogs, schemas, and tables using Databricks-specific `ALTER ... SET TAGS` SQL, combining parsed schema components with configured governance tags:

```toml
[pipeline.bronze.target.governance.tags]
managed_by = "rocky"
```

Tags are applied at three levels:
- **Catalogs**: `ALTER CATALOG ... SET TAGS (...)` — parsed components + governance tags
- **Schemas**: `ALTER SCHEMA ... SET TAGS (...)` — parsed components + governance tags
- **Tables**: `ALTER TABLE ... SET TAGS (...)` — governance tags applied to each replicated table

## Output

Permission reconciliation results are included in the run output:

```json
{
  "grants_added": 3,
  "grants_revoked": 0,
  "catalogs_created": 1,
  "schemas_created": 2
}
```
