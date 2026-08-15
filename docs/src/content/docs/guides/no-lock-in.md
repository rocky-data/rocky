---
title: No lock-in
description: Reduce any Rocky project to plain runnable SQL with rocky emit-sql, so adopting Rocky is never a one-way door.
sidebar:
  order: 8
---

Rocky compiles your models to ordinary SQL. `rocky emit-sql` hands you that SQL directly. If you ever step away from the engine, you keep runnable files rather than a proprietary format.

## `rocky emit-sql`

`rocky emit-sql` renders the SQL each transformation model would produce. It needs no warehouse connection and it runs nothing:

```bash
# Print the SQL for every model to stdout
rocky emit-sql --models models/

# Write one <model>.sql file per model
rocky emit-sql --models models/ --out-dir build/sql/

# Just one model
rocky emit-sql --models models/ --model fct_orders
```

Rocky generates that output through the same code path `rocky run` uses, including any declared [surrogate-key columns](/reference/model-format/#surrogate_key). The SQL dialect comes from the target adapter configured in your `rocky.toml`: Databricks, Snowflake, BigQuery, Trino, or DuckDB. With no resolvable config, it defaults to DuckDB.

A full-refresh model emits a complete `CREATE OR REPLACE TABLE … AS …`. That statement runs as-is, and it matches what a run executes.

An incremental or merge model emits its steady-state statement instead: a bare `INSERT` or `MERGE` against an existing target. `rocky run` creates that target on the first build, and it threads the incremental [watermark](/reference/glossary/#watermark) (the timestamp of the newest row already loaded) from its state store. A static SQL file can carry neither, so those files include a short note. Treat them as the recurring operation, not as a from-scratch build.

Rocky emits models in dependency order: a model never appears before one it reads. The stdout form is therefore a single ordered script you can pipe straight to your warehouse:

```bash
rocky emit-sql --models models/ > build/all.sql
duckdb mart.db < build/all.sql
```

The `--out-dir` form writes one `<model>.sql` per model, for inspection, editing, or dropping into dbt. To run those files directly, follow the same dependency order. `rocky dag` prints it:

```bash
rocky dag --models models/        # execution order
duckdb mart.db < build/sql/raw_orders.sql
duckdb mart.db < build/sql/fct_orders.sql
```

## The fallback recipe

To stop depending on Rocky for a model, or for the whole project:

1. Run `rocky emit-sql --models models/ --out-dir build/sql/` to capture the SQL.
2. Run `rocky dag --models models/` to capture the run order.
3. Run the files against your warehouse, or drop each `<model>.sql` into a dbt model, a scheduled query, or a hand-maintained script. The SQL has no Rocky-specific syntax.

For full-refresh models this path is exact. A CI test emits the SQL and executes it directly against DuckDB, so the path stays runnable as the engine evolves rather than becoming documentation that quietly rots.

## What it doesn't cover

`emit-sql` renders transformation models only.

Replication pipelines are incremental source-to-target copies driven by the engine's watermark state. Their SQL preview lives behind the live `rocky plan` path instead.

Some models produce no standalone statement. Rocky reports those on stderr rather than dropping them silently. Two cases produce no statement. One is an ephemeral model, which is inlined as a CTE. The other is a strategy that needs a live connection to render. A Snowflake dynamic table is one example: it resolves its compute-warehouse name at runtime.

## Related

- [SQL Generation](/concepts/sql-generation/) — how Rocky compiles each strategy to the SQL that `emit-sql` renders.
- [Model Format](/reference/model-format/#surrogate_key) — the sidecar fields, including the `[[surrogate_key]]` block `emit-sql` carries through.
- [Migrating from dbt](/guides/migrate-from-dbt/) — uses `emit-sql` for a connection-free side-by-side and as the exit door.
