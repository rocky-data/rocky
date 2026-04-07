---
name: databricks-api
description: Databricks REST API and SQL reference for Rocky's warehouse adapter. Use when implementing SQL execution, Unity Catalog management, workspace bindings, authentication, permission reconciliation, or any Databricks integration in the rocky-databricks crate.
---

# Databricks API Reference for Rocky

## SQL Statement Execution API

Primary execution path for all SQL. No SDK needed — pure REST.

### Submit Statement

```
POST https://{host}/api/2.0/sql/statements
Authorization: Bearer {token}
Content-Type: application/json

{
  "warehouse_id": "{warehouse_id}",
  "statement": "SELECT * FROM catalog.schema.table LIMIT 10",
  "wait_timeout": "30s",
  "disposition": "INLINE",
  "format": "JSON_ARRAY"
}
```

`warehouse_id` is extracted from the HTTP path: `/sql/1.0/warehouses/{warehouse_id}`

Response (immediate if fast):
```json
{
  "statement_id": "abc-123",
  "status": { "state": "SUCCEEDED" },
  "manifest": {
    "schema": {
      "columns": [
        { "name": "col1", "type_name": "STRING", "position": 0 }
      ]
    },
    "total_row_count": 10
  },
  "result": {
    "data_array": [["value1"], ["value2"]]
  }
}
```

### Poll Statement (if not immediately complete)

```
GET https://{host}/api/2.0/sql/statements/{statement_id}
Authorization: Bearer {token}
```

States: `PENDING` → `RUNNING` → `SUCCEEDED` | `FAILED` | `CANCELED` | `CLOSED`

Poll strategy: 100ms → 200ms → 500ms → 1s → 2s (exponential backoff, cap at 2s)

### Cancel Statement

```
POST https://{host}/api/2.0/sql/statements/{statement_id}/cancel
```

### Important Notes

- `wait_timeout: "0s"` returns immediately with `PENDING` — useful for fire-and-forget
- `wait_timeout: "30s"` waits up to 30s inline before returning — avoids polling for fast queries
- `disposition: "INLINE"` returns data in response body (good for small results)
- `disposition: "EXTERNAL_LINKS"` returns presigned URLs for large results (future: Arrow Flight)
- Max statement size: 100KB
- Max concurrent statements per warehouse: varies by warehouse size

## Authentication

### PAT (Personal Access Token)

```
Authorization: Bearer {DATABRICKS_TOKEN}
```

### OAuth M2M (Service Principal)

Token request:
```
POST https://{host}/oidc/v1/token
Content-Type: application/x-www-form-urlencoded

grant_type=client_credentials&
client_id={DATABRICKS_CLIENT_ID}&
client_secret={DATABRICKS_CLIENT_SECRET}&
scope=all-apis
```

Response:
```json
{
  "access_token": "eyJ...",
  "token_type": "Bearer",
  "expires_in": 3600
}
```

Implementation:
- Cache the token
- Refresh when `expires_in` is within 60s of expiry
- Use the access_token as `Authorization: Bearer {access_token}`

### Auto-Detection Logic

```
if DATABRICKS_TOKEN is set and non-empty:
    use PAT auth
else if DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET are set:
    use OAuth M2M
else:
    error: no auth configured
```

## Unity Catalog APIs

### Catalog Isolation

```
PATCH https://{host}/api/2.1/unity-catalog/catalogs/{catalog_name}
Authorization: Bearer {token}
Content-Type: application/json

{
  "isolation_mode": "ISOLATED"
}
```

### Workspace Bindings

Get current bindings:
```
GET https://{host}/api/2.1/unity-catalog/bindings/catalog/{catalog_name}
```

Response:
```json
{
  "bindings": [
    { "workspace_id": 12345, "binding_type": "BINDING_TYPE_READ_WRITE" }
  ]
}
```

Update bindings (add/remove):
```
PATCH https://{host}/api/2.1/unity-catalog/bindings/catalog/{catalog_name}
Content-Type: application/json

{
  "add": [
    { "workspace_id": 67890, "binding_type": "BINDING_TYPE_READ_WRITE" }
  ],
  "remove": [
    { "workspace_id": 11111 }
  ]
}
```

## SQL Statements Rocky Must Generate

### Catalog Lifecycle

```sql
-- Create
CREATE CATALOG IF NOT EXISTS <catalog>

-- Tag
ALTER CATALOG <catalog> SET TAGS (
    'client' = '<client>',
    'product' = '<product_name>',
    'managed_by' = '<pipeline_name>'
)

-- Inspect
DESCRIBE CATALOG <catalog>
-- Returns rows: (info_name, info_value) — check for 'Catalog Name' row

-- Discover managed catalogs
SELECT catalog_name
FROM system.information_schema.catalog_tags
WHERE tag_name = 'managed_by' AND tag_value = '<pipeline_name>'
```

### Schema Lifecycle

```sql
-- Create
CREATE SCHEMA IF NOT EXISTS <catalog>.<schema>

-- Tag (dynamic hierarchy tags)
ALTER SCHEMA <catalog>.<schema> SET TAGS (
    'client' = '<client>',
    'layer' = 'raw',
    'hierarchy-1' = '<h1>',
    'hierarchy-2' = '<h2>',
    'connector' = '<connector>',
    'product' = '<product_name>',
    'managed_by' = '<pipeline_name>'
)

-- List schemas
SHOW SCHEMAS IN <catalog>
```

### Incremental Copy (Core Operation)

```sql
-- Full refresh
SELECT *, CAST(NULL AS STRING) AS permission_key
FROM <source_catalog>.<source_schema>.<table>

-- Incremental (append new rows since last watermark)
SELECT *, CAST(NULL AS STRING) AS permission_key
FROM <source_catalog>.<source_schema>.<table>
WHERE _fivetran_synced > (
    SELECT COALESCE(MAX(_fivetran_synced), TIMESTAMP '1970-01-01')
    FROM <target_catalog>.<target_schema>.<table>
)
```

### Schema Drift Detection

```sql
-- Get column info for comparison
DESCRIBE TABLE <catalog>.<schema>.<table>
-- Returns rows: (col_name, data_type, comment)

-- On type mismatch between source and target:
DROP TABLE IF EXISTS <target_catalog>.<target_schema>.<table>
-- Then full refresh on next run
```

### Permission Reconciliation

```sql
-- Inspect current grants
SHOW GRANTS ON CATALOG <catalog>
-- Returns rows: (principal, action_type, object_type, object_name)

SHOW GRANTS ON SCHEMA <catalog>.<schema>

-- Grant (principal always backtick-quoted)
GRANT BROWSE ON CATALOG <catalog> TO `<principal>`
GRANT USE CATALOG ON CATALOG <catalog> TO `<principal>`
GRANT SELECT ON CATALOG <catalog> TO `<principal>`
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<principal>`

-- Revoke
REVOKE BROWSE ON CATALOG <catalog> FROM `<principal>`

-- Supported permission types for reconciliation:
-- BROWSE, USE CATALOG, USE SCHEMA, SELECT, MANAGE, MODIFY
-- Skip these (non-managed): OWNERSHIP, ALL PRIVILEGES, CREATE SCHEMA
```

### Data Quality Checks

```sql
-- Single table row count
SELECT COUNT(*) FROM <catalog>.<schema>.<table>

-- Batched row counts (UNION ALL, batches of 200)
SELECT 'cat1' AS c, 'sch1' AS s, 'tbl1' AS t, COUNT(*) AS cnt FROM cat1.sch1.tbl1
UNION ALL
SELECT 'cat1' AS c, 'sch1' AS s, 'tbl2' AS t, COUNT(*) AS cnt FROM cat1.sch1.tbl2
UNION ALL
...

-- Column introspection (batched by schema)
SELECT lower(table_schema), lower(table_name), lower(column_name)
FROM <catalog>.information_schema.columns
WHERE table_schema IN ('schema1', 'schema2', ...)
ORDER BY table_schema, table_name, ordinal_position
```

## Validation Rules

**SQL identifiers** (catalogs, schemas, tables):
```
^[a-zA-Z0-9_]+$
```
Reject anything that doesn't match. Never use `format!()` with unvalidated strings.

**Principal names** (for GRANT/REVOKE):
```
^[a-zA-Z0-9_ \-\.@]+$
```
Always wrap in backticks: `` `principal_name` ``

## Error Handling

Common Databricks errors to handle:
- `TEMPORARILY_UNAVAILABLE` (503) — Retry with exponential backoff
- `INVALID_PARAMETER_VALUE` — Bad SQL or missing object
- `RESOURCE_DOES_NOT_EXIST` — Table/catalog/schema not found
- `PERMISSION_DENIED` — Missing privileges
- `InvalidOperationHandle` — Statement expired, re-submit
- Rate limiting — Warehouse concurrency limit reached, back off

Retry strategy: 3 attempts, exponential backoff (1s → 3s → 9s), only on transient errors (503, rate limit, InvalidOperationHandle).
