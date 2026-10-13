---
title: Permissions
description: Declare who gets access in rocky.toml, and let Rocky issue the GRANT statements on a replication target
sidebar:
  order: 10
---

Declare who should have access to the catalogs and schemas a **replication** pipeline manages, and Rocky issues the `GRANT` statements. You never write a `GRANT` by hand. Grants run on **Databricks** (Unity Catalog) and **Snowflake**. BigQuery logs a warning and skips them; use Google Cloud IAM. Other adapters do nothing.

Rocky applies the grants during `rocky run`, before it starts processing tables in parallel. There is no separate permissions command.

:::caution[Grants are added, never revoked]
Rocky grants every declared permission on every run. It does not read the current grants, and it never revokes one. Removing a grant from `rocky.toml` leaves it in place on the warehouse. Revoke it yourself.
:::

:::caution[Grants follow the `auto_create_*` flags]
Catalog grants, catalog tags, workspace bindings and isolation run only when `auto_create_catalogs = true`. Schema grants and schema tags run only when `auto_create_schemas = true`. With both flags off, only table tags apply.
:::

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

## How Rocky applies grants

Rocky runs governance once per managed catalog, in the same step that creates catalogs and schemas:

```
  for each target catalog (once per run):
    auto_create_catalogs = true ──► CREATE CATALOG, catalog tags,
                                    workspace bindings, isolation,
                                    GRANT each [[grants]] permission
  for each target schema:
    auto_create_schemas = true  ──► CREATE SCHEMA, schema tags,
                                    GRANT each [[schema_grants]] permission
  after the tables load ──────────► table tags
```

A repeated `GRANT` of a permission the principal already holds changes nothing on the warehouse.

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

`binding_type` defaults to `"READ_WRITE"` when you omit it. Workspace bindings are reconciled, unlike grants. Rocky reads the catalog's current bindings and:

1. Binds each listed workspace at the declared access level, when it is not already bound that way.
2. **Removes every binding that is not in `rocky.toml`.** Declare a binding you added by hand, or the next run removes it.
3. Sets the catalog's isolation mode to `ISOLATED` when `enabled = true`.

Binding and isolation are best-effort, like grants: Rocky logs a failure and the run continues. Workspace isolation is Databricks only.

## Accepted permissions

Rocky grants only these names. It logs a warning and skips any other name, such as `OWNERSHIP` or `ALL PRIVILEGES`.

| Block | Accepted permissions |
|---|---|
| `[[…governance.grants]]` (catalog) | `BROWSE`, `USE CATALOG`, `USE SCHEMA`, `SELECT`, `MODIFY`, `MANAGE` |
| `[[…governance.schema_grants]]` (schema) | `USE SCHEMA`, `SELECT`, `MODIFY` |

## Principal Validation

Rocky checks every principal name against the pattern `^[a-zA-Z0-9_ \-\.@]+$`, then wraps it in backticks in the generated SQL so spaces and other characters are safe:

```sql
GRANT USE CATALOG ON CATALOG acme_warehouse TO `group:data_engineers`
```

## Tagging

Rocky labels the objects it manages. Databricks gets `ALTER … SET TAGS` (shown below), Snowflake gets `ALTER … SET TAG`, and BigQuery gets schema and table labels. Rocky combines the components it parsed from the schema name with the tags you declare:

```toml
[pipeline.bronze.target.governance.tags]
managed_by = "rocky"
```

Rocky tags at three levels:

| Level | Statement | Tags applied |
|---|---|---|
| Catalog | `ALTER CATALOG … SET TAGS (…)` | parsed components + governance tags (with `auto_create_catalogs = true`) |
| Schema | `ALTER SCHEMA … SET TAGS (…)` | parsed components + governance tags (with `auto_create_schemas = true`) |
| Table | `ALTER TABLE … SET TAGS (…)` | governance tags, on each replicated table |

## Output

`rocky run` reports what governance did under the `permissions` key. `grants_added` counts the declared catalog permissions Rocky sent, not the ones that changed. `grants_revoked` stays `0`.

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
