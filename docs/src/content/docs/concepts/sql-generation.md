---
title: SQL Generation
description: The statements Rocky emits for each strategy, in each warehouse dialect
sidebar:
  order: 7.5
---

Rocky builds every statement it sends to a warehouse. A model's statements come out of the compiler's typed IR. The IR (intermediate representation) is one typed record per model: its SQL, its columns, its target table, and its materialization strategy.

A replication pipeline needs no model. Rocky renders its catalog, schema, copy and other replication statements from the pipeline config instead. Both paths end in the dialect the target warehouse speaks. See the [glossary](/reference/glossary/) for both terms.

```
  models/fct_orders.sql  +  models/fct_orders.toml
                     │
                     │ compile: parse, resolve the DAG, type check
                     ▼
          ┌──────────────────────┐
          │ typed IR             │  one record per model:
          │                      │  its SQL, columns, target, strategy
          └──────────┬───────────┘
                     │
  rocky.toml ────────┤  replication needs no model: its catalog,
  (a replication     │  schema, copy and other statements are
   pipeline)         │  rendered from the pipeline config
                     │
                     │ generate, in the target warehouse's dialect
     ┌───────────┬───┴────────┬────────────┬───────────┐
     ▼           ▼            ▼            ▼           ▼
  Databricks  Snowflake   BigQuery      Trino      DuckDB
                  the SQL below, ready to execute
```

The rest of this page is the catalog of statements Rocky emits. Rocky validates every identifier before it reaches a statement — see [SQL Safety](#sql-safety) at the end.

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

Rocky tags each replicated table for governance:

```sql
ALTER TABLE <catalog>.<schema>.<table> SET TAGS ('managed_by' = 'rocky')
```

## Incremental Copy

This is the core replication statement. It copies only the rows newer than the last watermark Rocky recorded for the table:

```sql
INSERT INTO <target_catalog>.<target_schema>.<table>
SELECT *, CAST(NULL AS STRING) AS _loaded_by
FROM <source_catalog>.<source_schema>.<table>
WHERE _fivetran_synced > TIMESTAMP '<last watermark>'
```

Rocky reads the previous run's `MAX(_fivetran_synced)` from its state store and writes it in as a literal. There is no correlated subquery against the target table.

## Full Refresh

Rocky rebuilds the whole table when it finds drift it cannot fix in place, and when you configure a full refresh:

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

Rocky wraps the SQL you wrote in a statement chosen by the model's materialization strategy:

- **Table**: `CREATE OR REPLACE TABLE ... AS <user_sql>`
- **Incremental**: `INSERT INTO ... <user_sql>`
- **Merge**: `MERGE INTO ... USING (<user_sql>) ...`
- **Materialized View**: `CREATE OR REPLACE MATERIALIZED VIEW ... AS <user_sql>` (Databricks)
- **Dynamic Table**: `CREATE OR REPLACE DYNAMIC TABLE ... TARGET_LAG = '<lag>' AS <user_sql>` (Snowflake)
- **Time Interval**: Per-partition `INSERT OVERWRITE` with `@start_date`/`@end_date` substitution

## Surrogate Key Columns

A model declares deterministic surrogate keys with `[[surrogate_key]]` blocks in its `.toml` sidecar:

```toml
[[surrogate_key]]
name = "order_key"
columns = ["order_id"]
```

At `rocky run` and `rocky emit-sql`, Rocky wraps the model's `SELECT` and appends each key as a top-level column. DuckDB is shown here:

```sql
SELECT *, CAST(md5(cast(coalesce(cast(order_id as VARCHAR), '_dbt_utils_surrogate_key_null_') as VARCHAR)) AS VARCHAR) AS order_key
FROM (
<user_sql>
) AS __rocky_keyed
```

The hash expression follows dbt-utils' `generate_surrogate_key`. Rocky casts each input to text, replaces a NULL with the `_dbt_utils_surrogate_key_null_` sentinel, joins the values with a `-` separator, and MD5-hashes the result. Most warehouses concatenate with `||` and differ only in the text type they cast to:

- **DuckDB / Snowflake** cast to `VARCHAR`:

  ```sql
  md5(cast(coalesce(cast(a as VARCHAR), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(b as VARCHAR), '_dbt_utils_surrogate_key_null_') as VARCHAR))
  ```

- **Databricks** casts to `STRING` (Spark SQL rejects an unsized `VARCHAR`):

  ```sql
  md5(cast(coalesce(cast(a as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(b as STRING), '_dbt_utils_surrogate_key_null_') as STRING))
  ```

- **BigQuery** wraps the hash in `to_hex(...)` (its `MD5()` returns `BYTES`) and concatenates with `concat(...)` instead of `||`, casting to `STRING`:

  ```sql
  to_hex(md5(cast(concat(coalesce(cast(a as STRING), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(b as STRING), '_dbt_utils_surrogate_key_null_')) as STRING)))
  ```

The hash digest is the same one dbt-utils' `generate_surrogate_key` produces for the same columns, so a key Rocky computes joins against the same key in an upstream dbt model.

## Materialized View (Databricks)

```sql
CREATE OR REPLACE MATERIALIZED VIEW <catalog>.<schema>.<table> AS
<user_sql>
```

## Dynamic Table (Snowflake)

```sql
CREATE OR REPLACE DYNAMIC TABLE <catalog>.<schema>.<table>
TARGET_LAG = '<lag>'
WAREHOUSE = <warehouse>
AS <user_sql>
```

## Time-Interval Partition Processing

Each warehouse gets its own statement shape for partition-keyed materialization. See [Time interval materialization](/concepts/time-interval/) for the strategy itself.

**Databricks (Delta, atomic):**

```sql
INSERT INTO <target>
REPLACE WHERE <time_column> >= '<start>' AND <time_column> < '<end>'
<user_sql with @start_date/@end_date substituted>
```

**Snowflake (multi-statement transaction):**

```sql
BEGIN;
DELETE FROM <target> WHERE <time_column> >= '<start>' AND <time_column> < '<end>';
INSERT INTO <target> <user_sql>;
COMMIT;
```

## Drift Detection

Rocky reads both schemas before it copies a table:

```sql
DESCRIBE TABLE <catalog>.<schema>.<table>
```

When it finds a change it cannot apply in place, it drops the target and rebuilds it:

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

Rocky finds the catalogs it manages by reading its own tags back:

```sql
SELECT catalog_name
FROM system.information_schema.catalog_tags
WHERE tag_name = 'managed_by' AND tag_value = 'rocky'
```

## SQL Safety

Rocky applies the same rules to every statement it builds:

- **Identifiers** (catalogs, schemas, tables, tenants, regions, sources) must match `^[a-zA-Z0-9_]+$`
- **Principal names** must match `^[a-zA-Z0-9_ \-\.@]+$`, and Rocky always wraps them in backticks
- Rocky never uses `format!()` to interpolate untrusted input into SQL
- Every check runs in `rocky-sql/validation.rs`, before Rocky constructs any SQL
