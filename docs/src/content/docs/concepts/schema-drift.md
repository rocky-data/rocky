---
title: Schema Drift
description: Automatic detection and graduated resolution of schema changes
sidebar:
  order: 10.7
---

Rocky automatically detects schema drift between source and target tables and resolves it using **graduated evolution** -- safe type widenings are handled with `ALTER TABLE` (preserving data), while unsafe changes trigger a full refresh.

![Two rocky run invocations sandwiching an ALTER TABLE; the second run reports "Drift: 1/1 tables drifted"](/demo-drift-recover.gif)

## What It Detects

Schema drift in Rocky means **column type mismatches** between source and target tables. For example, a column that was `STRING` in the source but is `INT` in the target, or an `INT` that widened to `BIGINT`.

## How It Works

1. Runs `DESCRIBE TABLE` on both the source and target tables
2. Compares column types (case-insensitive)
3. Classifies each type change as **safe** or **unsafe**
4. Safe widenings are resolved with `ALTER TABLE ALTER COLUMN`
5. Unsafe changes trigger `DROP TABLE IF EXISTS` followed by full refresh

## Graduated Evolution

### Safe Type Widenings

These type changes preserve data and are handled with `ALTER TABLE` without a full refresh:

| From | To | Example |
|---|---|---|
| `INT` | `BIGINT` | Integer widening |
| `FLOAT` | `DOUBLE` | Float precision widening |
| `DECIMAL(p1, s)` | `DECIMAL(p2, s)` | Decimal precision increase (p2 > p1) |
| `VARCHAR(n1)` | `VARCHAR(n2)` | String length increase (n2 > n1) |

```sql
ALTER TABLE acme_warehouse.staging__us_west__shopify.orders
ALTER COLUMN amount TYPE DECIMAL(12, 2)
```

### Unsafe Type Changes

Any type change not in the safe allowlist triggers a full refresh:

```sql
DROP TABLE IF EXISTS acme_warehouse.staging__us_west__shopify.orders
-- followed by full refresh from source
```

Examples: `STRING` to `INT`, `BIGINT` to `INT` (narrowing), `DATE` to `TIMESTAMP`.

## What Is NOT Drift

- **New columns in source** -- not a type mismatch, so not "drift" in the narrow sense, but still handled: Rocky issues `ALTER TABLE ADD COLUMN` for each before the next `INSERT ... SELECT *` (surfaced as the `add_columns` action). Without it, BigQuery / Snowflake / Databricks reject the INSERT.
- **Columns removed from source** -- extra columns in the target table are ignored

## Output

Drift detection runs inline during `rocky run`; the actions taken are reported in the `drift` section of the run JSON output:

```json
{
  "drift": {
    "tables_checked": 45,
    "tables_drifted": 1,
    "actions_taken": [
      {
        "table": "acme_warehouse.staging__us_west__shopify.events",
        "action": "drop_and_recreate",
        "reason": "column 'status' changed STRING -> INT"
      }
    ]
  }
}
```

Use `rocky plan` to preview the SQL Rocky would emit (including any drop statements) without executing:

```bash
rocky plan --filter client=acme --output json
```

Three drift actions are surfaced in the run output: `drop_and_recreate` (unsafe type change -- the target is dropped and rebuilt from source), `alter_column_types` (every drifted column is a safe widening -- executed inline via `ALTER TABLE ALTER COLUMN`, preserving data), and `add_columns` (new source columns added via `ALTER TABLE ADD COLUMN` before the INSERT). Safe widenings are classified as `AlterColumnTypes` in `rocky-core` and applied during the run.
