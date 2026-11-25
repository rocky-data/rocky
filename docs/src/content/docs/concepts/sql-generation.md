---
title: SQL Generation
description: The statements Rocky emits for each strategy, in each warehouse dialect
sidebar:
  order: 7.5
---

Rocky builds every statement it sends to a warehouse. A model's statements come from the compiler's typed IR: one record per model with its SQL, columns, target table and materialization strategy. A replication pipeline needs no model: Rocky renders its catalog, schema and copy statements from the pipeline config. Both paths end in the dialect the target warehouse speaks.

```
  models/*.sql + *.toml ──▶ compile ──▶ typed IR ──┐
                                                   ├──▶ generate in the target dialect ──▶ SQL
  rocky.toml (replication pipeline) ───────────────┘
```

Every warehouse has its own dialect. This page shows the Databricks form unless it names another warehouse. Rocky validates every identifier before it reaches a statement: see [SQL Safety](#sql-safety).

## Catalog and Schema Lifecycle

```sql
CREATE CATALOG IF NOT EXISTS <catalog>
ALTER CATALOG <catalog> SET TAGS ('managed_by' = 'rocky', 'tenant' = 'acme')
DESCRIBE CATALOG <catalog>
CREATE SCHEMA IF NOT EXISTS <catalog>.<schema>
ALTER SCHEMA <catalog>.<schema> SET TAGS ('managed_by' = 'rocky', 'source' = 'shopify')
SHOW SCHEMAS IN <catalog>
```

## Table Tagging

Rocky applies the governance tags you configure to each replicated table:

```sql
ALTER TABLE <catalog>.<schema>.<table> SET TAGS ('managed_by' = 'rocky')
```

## Incremental Copy

This is the core replication statement. It copies the rows newer than the last watermark Rocky recorded for the table, and older than the newest source timestamp. The `_loaded_by` column appears only when the pipeline declares it in `metadata_columns`:

```sql
INSERT INTO <target_catalog>.<target_schema>.<table>
SELECT *, CAST(NULL AS STRING) AS _loaded_by
FROM <source_catalog>.<source_schema>.<table>
WHERE _fivetran_synced > TIMESTAMP '<last watermark>'
  AND _fivetran_synced < TIMESTAMP '<newest source timestamp>'
```

Rocky reads the previous run's `MAX(_fivetran_synced)` from its state store and writes it in as a literal, with no correlated subquery against the target. Just before the copy, it reads `MAX(_fivetran_synced)` over the source and writes that in as the second literal. The newest batch waits for a later run, so a late row with the same timestamp is not skipped. `rocky plan` shows the statement without the second bound. See [Incremental Processing](/concepts/incremental/#how-rocky-advances-the-watermark).

On DuckDB, each literal is a string with a UTC offset and no `TIMESTAMP` keyword: `'2026-03-02 10:00:00.123456789+00:00'`. DuckDB converts it to the column's type. So it keeps all nine digits on a `TIMESTAMP_NS` column, and it means the same instant on a `TIMESTAMPTZ` column in any session time zone.

## Full Refresh

Rocky rebuilds the whole table when it finds drift it cannot fix in place, and when you configure a full refresh:

```sql
CREATE OR REPLACE TABLE <target> AS SELECT * FROM <source>
```

A full refresh never drops the target before the new table exists. If the `SELECT` fails, the old table stays. Trino has no `CREATE OR REPLACE` on every connector, so there Rocky builds the new table under a stage name and swaps it in by rename:

```sql
CREATE TABLE <target>__rocky_stage AS SELECT * FROM <source>
ALTER TABLE IF EXISTS <target> RENAME TO <target>__rocky_old
ALTER TABLE <target>__rocky_stage RENAME TO <target>
DROP TABLE IF EXISTS <target>__rocky_old
```

If the third statement fails, Rocky renames the old table back. If a run stops between the two renames, the next run restores `<target>` from `<target>__rocky_old` before it does anything else. Rocky never drops a `__rocky_stage` or `__rocky_old` table it did not create in the same run: it stops and names the table instead. A target whose name ends in `__rocky_stage` or `__rocky_old` is refused on Trino. Readers can get "table not found" for the short moment between the two renames. The design is in [ADR-REPLACE](https://github.com/rocky-data/rocky/blob/main/docs/adr/ADR-REPLACE.md).

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

Rocky wraps the SQL you wrote in a statement chosen by the model's materialization strategy (`rocky-ir::ir::MaterializationStrategy`, one per model). SQL generation lives in `rocky-core::sql_gen`.

| Strategy | Behavior | Statement |
|---|---|---|
| `FullRefresh` | Rebuilds the whole table every run | `CREATE OR REPLACE TABLE ... AS <user_sql>` |
| `Incremental` | Inserts rows newer than the watermark. The state store supplies it at generation time, so recipe-hash inputs stay free of runtime state | `INSERT INTO ... <user_sql>` |
| `Merge` | Upserts on a unique key | `MERGE INTO ... USING (<user_sql>) ...` |
| `View` | No storage. Every read re-runs the `SELECT`. Every warehouse supports it | `CREATE OR REPLACE VIEW` |
| `MaterializedView` | Databricks, Snowflake, BigQuery. PostgreSQL and Redshift drop and re-create the view. Other warehouses refuse it | `CREATE OR REPLACE MATERIALIZED VIEW ... AS <user_sql>` |
| `DynamicTable` | Snowflake only | `CREATE OR REPLACE DYNAMIC TABLE ... TARGET_LAG = '<lag>' WAREHOUSE = <warehouse> AS <user_sql>` |
| `TimeInterval` | Partition-keyed. `@start_date` and `@end_date` take per-partition timestamps. Flags: `--partition`, `--from/--to`, `--latest`, `--missing` | Per-partition `INSERT OVERWRITE` (see below) |
| `DeleteInsert` | Deletes rows matching the partition key, then inserts. Cheaper than `Merge` when the key identifies the rewritten rows | |
| `Microbatch` | Alias for `TimeInterval` with `hour` granularity (dbt-compatible name) | |
| `Ephemeral` | Never materialized. The compiler inlines its SQL as a CTE in each reader | |
| `Snapshot` | SCD Type 2 history. Each run closes changed versions and inserts new ones | |
| `ContentAddressed` | Content-addressed Parquet plus a Delta log commit, written by `rocky-iceberg` for cross-engine reads. SQL generation does not run. See [Content-Addressed Materialization](/concepts/content-addressed/) | |

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

The hash expression follows dbt-utils' `generate_surrogate_key`, so a key Rocky computes joins against the same key in an upstream dbt model. Rocky casts each input to text, replaces a NULL with the `_dbt_utils_surrogate_key_null_` sentinel, joins the values with `-`, and MD5-hashes the result. Warehouses differ in the text type and the join:

| Warehouse | Text type | Join | Hash |
|---|---|---|---|
| DuckDB, Snowflake | `VARCHAR` | `\|\|` | `md5(...)` |
| Databricks | `STRING` (Spark SQL rejects an unsized `VARCHAR`) | `\|\|` | `md5(...)` |
| BigQuery | `STRING` | `concat(...)` | `to_hex(md5(...))` (its `MD5()` returns `BYTES`) |

Two columns `a` and `b` on Databricks give `md5(cast(coalesce(cast(a as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(b as STRING), '_dbt_utils_surrogate_key_null_') as STRING))`.

## Time-Interval Partition Processing

Each warehouse gets its own partition statement: Databricks `INSERT INTO ... REPLACE WHERE`, Snowflake and DuckDB a `BEGIN; DELETE; INSERT; COMMIT;` transaction, BigQuery one transaction script. See [Time interval materialization](/concepts/time-interval/#per-warehouse-sql).

## Drift Detection

Rocky runs `DESCRIBE TABLE <catalog>.<schema>.<table>` on both sides before it copies a table. A change it cannot apply in place triggers a [full refresh](#full-refresh). See [Schema Drift](/concepts/schema-drift/).

## Permission Grants

`rocky run` issues a `GRANT` for each declared permission. It never revokes. See [Permissions](/reference/permissions/).

```sql
GRANT <PERMISSION> ON CATALOG <catalog> TO `<principal>`
GRANT <PERMISSION> ON SCHEMA <catalog>.<schema> TO `<principal>`
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
SELECT COUNT(*), MAX(<timestamp_column>) FROM <catalog>.<schema>.<table>
```

The count rides beside the maximum so that a NULL maximum can be told apart: zero rows is an empty table with nothing to measure, one or more rows with no value in the column is a check Rocky could not evaluate.

**Null rate** (sampled):

```sql
SELECT ... FROM <catalog>.<schema>.<table> TABLESAMPLE (N PERCENT)
```

## Workspace Isolation

Rocky uses the Databricks REST API for workspace binding and isolation (not SQL):

- `PATCH /api/2.1/unity-catalog/bindings/catalog/{name}` — Bind catalog to workspace IDs
- `PATCH /api/2.1/unity-catalog/catalogs/{name}` — Set `isolation_mode: "ISOLATED"`

## Catalog Discovery

Rocky finds the catalogs it manages by reading back a tag you configured, for
example `managed_by = 'rocky'`:

```sql
SELECT catalog_name
FROM system.information_schema.catalog_tags
WHERE tag_name = 'managed_by' AND tag_value = 'rocky'
```

## SQL Safety

Rocky applies the same rules to every statement it builds:

- **Identifiers** (catalogs, schemas, tables, tenants, regions, sources) must match `^[a-zA-Z0-9_]+$`. A BigQuery project ID may also hold hyphens, and has its own rule
- **Principal names** must match `^[a-zA-Z0-9_ \-\.@]+$`, and Rocky always wraps them in backticks
- Rocky never uses `format!()` to interpolate untrusted input into SQL
- Every check runs in `rocky-sql/validation.rs`, before Rocky constructs any SQL
