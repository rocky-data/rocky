---
title: No lock-in
description: Reduce any Rocky project to plain runnable SQL with rocky emit-sql, so adopting Rocky is never a one-way door.
sidebar:
  order: 8
---

Adopting a transformation tool shouldn't be a one-way door. Rocky compiles your models to ordinary SQL, and `rocky emit-sql` hands you that SQL directly. If you ever need to step away from the engine, you keep runnable artifacts, not a proprietary format.

## `rocky emit-sql`

`rocky emit-sql` renders the SQL each transformation model would produce, without a warehouse connection and without running anything:

```bash
# Print the SQL for every model to stdout
rocky emit-sql --models models/

# Write one <model>.sql file per model
rocky emit-sql --models models/ --out-dir build/sql/

# Just one model
rocky emit-sql --models models/ --model fct_orders
```

The output is generated through the same code path `rocky run` uses, including any declared surrogate-key columns. The dialect comes from your configured target adapter in `rocky.toml` (Databricks, Snowflake, BigQuery, Trino, or DuckDB); with no resolvable config it defaults to DuckDB.

Full-refresh models emit a complete `CREATE OR REPLACE TABLE … AS …` that runs as-is and matches what a run executes. Incremental and merge models emit their steady-state statement, a bare `INSERT` or `MERGE` against an existing target. `rocky run` creates that target on the first build and threads the incremental watermark from its state store, neither of which a static SQL file can carry, so those files include a short note. Treat them as the recurring operation, not a from-scratch build.

Models are emitted in dependency order: a model never appears before one it reads. The stdout form is a single ordered script you can pipe straight to your warehouse:

```bash
rocky emit-sql --models models/ > build/all.sql
duckdb mart.db < build/all.sql
```

The `--out-dir` form writes one `<model>.sql` per model for inspection, editing, or dropping into dbt. To run those files directly, follow the same dependency order, which `rocky dag` also prints:

```bash
rocky dag --models models/        # execution order
duckdb mart.db < build/sql/raw_orders.sql
duckdb mart.db < build/sql/fct_orders.sql
```

## The fallback recipe

If you want to stop depending on Rocky for a model, or for the whole project:

1. `rocky emit-sql --models models/ --out-dir build/sql/` to capture the SQL.
2. `rocky dag --models models/` to capture the run order.
3. Run the files directly against your warehouse, or drop each `<model>.sql` into a dbt model, a scheduled query, or a hand-maintained script. The SQL has no Rocky-specific syntax.

For full-refresh models this is exact: a CI test emits the SQL and executes it directly against DuckDB, so the path stays runnable as the engine evolves rather than being documentation that quietly rots.

## What it doesn't cover

`emit-sql` renders transformation models. Replication pipelines (incremental source-to-target copies) are driven by the engine's watermark state, so their SQL preview lives behind the live `rocky plan` path instead. Models that produce no standalone statement are reported on stderr rather than dropped silently: ephemeral models (inlined as CTEs) and strategies that need a live connection to render, such as Snowflake dynamic tables that resolve a compute-warehouse name at runtime.

For incremental and merge models, remember the limits above: the emitted statement assumes the target already exists and carries no watermark, so reproducing a run from scratch means creating the target first (and, for incremental, deciding how to seed it).
