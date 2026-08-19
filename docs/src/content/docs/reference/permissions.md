---
title: Permissions
description: Declare who gets access in rocky.toml, and let Rocky issue the GRANT and REVOKE that match
sidebar:
  order: 10
---

Declare who should have access to your **Databricks Unity Catalog** objects, and Rocky issues the statements that make it so. You never write a `GRANT` by hand.

This is a [declarative](/reference/glossary/#declarative) surface: you state the end result, and Rocky works out the steps. It [reconciles](/reference/glossary/#reconcile) the grants during every `rocky run`, before it starts processing tables in parallel. There is no separate permissions command.

## Inline Grants (Recommended)

Declare grants under the pipeline target. They apply to **every** catalog and schema that pipeline manages:

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

Rocky applies inline grants on a best-effort basis during `rocky run`. When one fails — the principal does not exist, say — Rocky logs a warning and carries on with the run.

## Reconciliation Flow

Rocky never blindly re-issues every grant. It reads what the warehouse has now, compares that against what you declared, and emits only the difference:

```
  rocky.toml                        Databricks Unity Catalog
  ──────────                        ────────────────────────
  [[…governance.grants]]            SHOW GRANTS ON CATALOG
  [[…governance.schema_grants]]     SHOW GRANTS ON SCHEMA
        │                                     │
        │ desired state                       │ current state
        ▼                                     ▼
      ┌─────────────────────────────────────────┐
      │              compute diff               │
      └────────┬───────────────────────┬────────┘
               │ missing               │ extra
               ▼                       ▼
          GRANT … TO `p`         REVOKE … FROM `p`
```

Rocky runs this during `rocky run`, inside the same sequence that creates catalogs and schemas.

## Workspace Isolation

Restrict a catalog to named Databricks workspaces so no other workspace can reach it. Rocky uses the Unity Catalog workspace-bindings API (`PATCH /api/2.1/unity-catalog/bindings/catalog/{name}`). Each binding names a workspace ID and an access level, `READ_WRITE` or `READ_ONLY`:

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

`binding_type` defaults to `"READ_WRITE"` when you omit it. With `enabled = true`, Rocky does two things to each managed catalog:

1. Binds it to the workspaces you listed, at the access level each one declares.
2. Sets the catalog's isolation mode to `ISOLATED`.

Binding and isolation are best-effort, like grants: Rocky logs a failure and the run continues.

## Managed Permissions

Rocky manages these permission types:

- `BROWSE`
- `USE CATALOG`
- `USE SCHEMA`
- `SELECT`
- `MODIFY`
- `MANAGE`

## Skipped Permissions

Rocky never grants or revokes these, and ignores them during reconciliation:

- `OWNERSHIP`
- `ALL PRIVILEGES`
- `CREATE SCHEMA` (non-managed)

## Principal Validation

Rocky checks every principal name against the pattern `^[a-zA-Z0-9_ \-\.@]+$`, then wraps it in backticks in the generated SQL so spaces and other characters are safe:

```sql
GRANT USE CATALOG ON CATALOG acme_warehouse TO `group:data_engineers`
```

## Tagging

Rocky labels the objects it manages with Databricks `ALTER … SET TAGS` SQL. It combines the components it parsed from the schema name with the tags you declare:

```toml
[pipeline.bronze.target.governance.tags]
managed_by = "rocky"
```

Rocky tags at three levels:

| Level | Statement | Tags applied |
|---|---|---|
| Catalog | `ALTER CATALOG … SET TAGS (…)` | parsed components + governance tags |
| Schema | `ALTER SCHEMA … SET TAGS (…)` | parsed components + governance tags |
| Table | `ALTER TABLE … SET TAGS (…)` | governance tags, on each replicated table |

## Output

`rocky run` reports what reconciliation did under the `permissions` key:

```json
{
  "permissions": {
    "grants_added": 3,
    "grants_revoked": 0,
    "catalogs_created": 1,
    "schemas_created": 2
  }
}
```
