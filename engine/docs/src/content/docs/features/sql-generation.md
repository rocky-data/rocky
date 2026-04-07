---
title: SQL Generation
description: All SQL statements Rocky generates for Databricks
sidebar:
  order: 5
---

Rocky generates all SQL from its internal IR (Intermediate Representation). No Jinja templates, no string concatenation with untrusted input. Every identifier is validated before it reaches a SQL statement.

## Catalog Lifecycle

```sql
CREATE CATALOG IF NOT EXISTS <catalog>
```

```sql
ALTER CATALOG <catalog> SET TAGS ('managed_by' = 'rocky', 'tenant' = 'acme')
```

```sql
DESCRIBE CATALOG <catalog>
```

## Schema Lifecycle

```sql
CREATE SCHEMA IF NOT EXISTS <catalog>.<schema>
```

```sql
ALTER SCHEMA <catalog>.<schema> SET TAGS ('managed_by' = 'rocky', 'source' = 'shopify')
```

```sql
SHOW SCHEMAS IN <catalog>
```

## Table Tagging

Governance tags are applied to each replicated table:

```sql
ALTER TABLE <catalog>.<schema>.<table> SET TAGS ('managed_by' = 'rocky')
```

## Incremental Copy

The core replication operation. Copies only rows newer than the latest timestamp in the target:

```sql
INSERT INTO <target_catalog>.<target_schema>.<table>
SELECT *, CAST(NULL AS STRING) AS _loaded_by
FROM <source_catalog>.<source_schema>.<table>
WHERE _fivetran_synced > (
    SELECT COALESCE(MAX(_fivetran_synced), TIMESTAMP '1970-01-01')
    FROM <target_catalog>.<target_schema>.<table>
)
```

## Full Refresh

Used when drift is detected or when explicitly configured:

```sql
CREATE OR REPLACE TABLE <target> AS SELECT * FROM <source>
```

## Merge (Upsert)

For tables that require key-based deduplication:

```sql
MERGE INTO <target> AS t
USING (SELECT ... FROM <source>) AS s
ON t.key = s.key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

## Transformation SQL

User-defined SQL is wrapped in the appropriate statement depending on the materialization strategy:

- **Table**: `CREATE OR REPLACE TABLE ... AS <user_sql>`
- **Incremental**: `INSERT INTO ... <user_sql>`
- **Merge**: `MERGE INTO ... USING (<user_sql>) ...`

## Drift Detection

```sql
DESCRIBE TABLE <catalog>.<schema>.<table>
```

When drift is found:

```sql
DROP TABLE IF EXISTS <target_catalog>.<target_schema>.<table>
```

## Permission Reconciliation

```sql
SHOW GRANTS ON CATALOG <catalog>
```

```sql
SHOW GRANTS ON SCHEMA <catalog>.<schema>
```

```sql
GRANT <PERMISSION> ON CATALOG <catalog> TO `<principal>`
```

```sql
REVOKE <PERMISSION> ON CATALOG <catalog> FROM `<principal>`
```

## Data Quality Checks

**Row count** (batched):

```sql
SELECT '<catalog>', '<schema>', '<table>', COUNT(*)
FROM <catalog>.<schema>.<table>
UNION ALL
SELECT '<catalog>', '<schema>', '<table>', COUNT(*)
FROM <catalog>.<schema>.<table>
-- ... up to 200 tables per batch
```

**Freshness**:

```sql
SELECT MAX(<timestamp_column>) FROM <catalog>.<schema>.<table>
```

**Null rate** (sampled):

```sql
SELECT ... FROM <catalog>.<schema>.<table> TABLESAMPLE (N PERCENT)
```

## Workspace Isolation

Rocky uses the Databricks REST API for workspace binding and isolation (not SQL):

- `PATCH /api/2.1/unity-catalog/bindings/catalog/{name}` — Bind catalog to workspace IDs
- `PATCH /api/2.1/unity-catalog/catalogs/{name}` — Set `isolation_mode: "ISOLATED"`

## Catalog Discovery

Find catalogs managed by Rocky using tags:

```sql
SELECT catalog_name
FROM system.information_schema.catalog_tags
WHERE tag_name = 'managed_by' AND tag_value = 'rocky'
```

## SQL Safety

All SQL generation follows strict safety rules:

- **Identifiers** (catalogs, schemas, tables, tenants, regions, sources) are validated against `^[a-zA-Z0-9_]+$`
- **Principal names** are validated against `^[a-zA-Z0-9_ \-\.@]+$` and always wrapped in backticks
- Rocky never uses `format!()` to interpolate untrusted input into SQL
- All validation happens in `rocky-sql/validation.rs` before any SQL is constructed
