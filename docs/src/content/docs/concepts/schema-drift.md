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

The check runs on every table the run copies. With the opt-in `prune_unchanged` optimization, a table whose source reports an unchanged change-marker since the last successful copy is skipped entirely — copy, drift check, and data checks — so drift is only re-evaluated once the source changes again.

## Graduated Evolution

### Safe Type Widenings

These type changes preserve data and are handled with `ALTER TABLE` without a full refresh:

| From | To | Example |
|---|---|---|
| `INT` | `BIGINT` | Integer widening (also `TINYINT`/`SMALLINT` upward) |
| `FLOAT` | `DOUBLE` | Float precision widening |
| `DECIMAL(p1, s)` | `DECIMAL(p2, s)` | Decimal precision increase (p2 > p1, same scale) |
| `VARCHAR(n1)` | `VARCHAR(n2)` | String length increase (n2 > n1) |
| numeric / `BOOLEAN` | `STRING` | Representation change (lossless) |

```sql
ALTER TABLE acme_warehouse.staging__us_west__shopify.orders
ALTER COLUMN amount TYPE DECIMAL(12, 2)
```

Classification is per-dialect. The table above is the engine's default allowlist, verified end-to-end on DuckDB. Snowflake and BigQuery override it with narrower rules matching exactly what their `ALTER COLUMN` accepts: Snowflake allows only `NUMBER(p,s)` precision widening and `VARCHAR` length widening (integer types all canonicalize to `NUMBER(38,0)` in its `DESCRIBE TABLE` output, so integer widening never surfaces as drift there), and BigQuery allows only `INT64 → NUMERIC`, `INT64 → BIGNUMERIC`, and `NUMERIC → BIGNUMERIC` — numeric → `STRING` is not assignable on BigQuery and falls through to a full refresh.

:::caution[Databricks and Trino execution gaps]
Databricks and Trino currently inherit the default allowlist, and their `ALTER` execution paths have known gaps: Delta tables reject `ALTER COLUMN ... TYPE` changes unless the type-widening table feature is enabled (Rocky does not set it) and never accept numeric → `STRING`, and Trino requires `SET DATA TYPE` syntax the default statement doesn't use. On those warehouses, a drift classified as a safe widening fails that table's run with the warehouse's error — loud, never a silent divergence — instead of evolving the column in place; Rocky does not yet fall back to a full refresh on a failed `ALTER`. Tracked in [#1115](https://github.com/rocky-data/rocky/issues/1115).
:::

### Unsafe Type Changes

Any type change not in the safe allowlist triggers a full refresh:

```sql
DROP TABLE IF EXISTS acme_warehouse.staging__us_west__shopify.orders
-- followed by full refresh from source
```

Examples: `STRING` to `INT`, `BIGINT` to `INT` (narrowing), `DATE` to `TIMESTAMP`.

## What Is NOT Drift

- **New columns in source** -- handled additively rather than as drift: the runtime issues `ALTER TABLE ADD COLUMN` for each (nullable, so historical rows stay `NULL`) before the copy, surfaced as an `add_columns` action
- **Columns removed from source** -- extra columns in the target table are ignored

## Output

Drift detection runs inline on replication runs; the actions taken are reported in the `drift` section of the run JSON output:

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

Three actions are surfaced in the run output: `alter_column_types` (all drifted columns passed the safe-widening check and were altered in place), `drop_and_recreate` (at least one incompatible change; target rebuilt from the source), and `add_columns` (source-only columns added to the target).

By default these mutations are applied automatically. With the opt-in drift-governance gate (`auto_apply_additive_drift` plus a `[policy]` grant for `schema_change.additive`), only provably additive, policy-allowed changes proceed; anything else is refused before it touches the target and surfaced as a require-review failure for that table.
