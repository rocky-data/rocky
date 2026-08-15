---
title: Schema Drift
description: How Rocky spots a changed column type and repairs the target table
sidebar:
  order: 10.7
---

Schema drift is a column whose type in the source no longer matches its type in the target table. Rocky checks for it as it copies each table, and fixes what it safely can. This is **graduated evolution**: a safe widening becomes an `ALTER TABLE` that keeps the data, and anything else becomes a full refresh.

![Two rocky run invocations sandwiching an ALTER TABLE; the second run reports "Drift: 1/1 tables drifted"](/demo-drift-recover.gif)

## What It Detects

Drift in Rocky means **column type mismatches** between the source and the target. A column that is `STRING` in the source and `INT` in the target has drifted. So has an `INT` that widened to `BIGINT`.

## How Rocky classifies and resolves a change

Rocky runs the check inside each table's copy, whenever the target already exists.

```
  per table, when the target already exists
        │
        ├─► DESCRIBE TABLE source ─┐
        │                          ├─► compare types, case-insensitive
        └─► DESCRIBE TABLE target ─┘             │
                                                 ▼
                                       ┌───────────────────┐
                                       │ any type changed? │
                                       └──┬─────────────┬──┘
                                       no │             │ yes
                                          ▼             ▼
                                  copy the rows   ┌────────────────┐
                                                  │ safe widening? │
                                                  └──┬──────────┬──┘
                                               yes   │          │  no
                                                     ▼          ▼
                              ALTER TABLE ALTER COLUMN      DROP TABLE IF
                              data preserved                EXISTS, then
                                                            full refresh
```

Two cases bypass the check.

- **The source has not changed and `prune_unchanged` is on.** This opt-in optimization skips the whole table when the source reports the same change-marker as the last successful copy. That skips the copy, the drift check, and the data checks. Rocky re-evaluates drift once the source changes again.
- **`DESCRIBE TABLE` fails.** Rocky swallows the error instead of failing the run. It treats an unreadable target as absent and rebuilds it with a full refresh. An unreadable source produces no drift result for that run, so the copy proceeds unchecked.

## Graduated Evolution

### Safe Type Widenings

These type changes keep the data. Rocky applies them with `ALTER TABLE` and skips the full refresh:

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

Classification is per-dialect. The table above is the engine's default allowlist, verified end-to-end on DuckDB. Snowflake and BigQuery override it with narrower rules that match what their own `ALTER COLUMN` accepts.

- **Snowflake** allows `NUMBER(p,s)` precision widening and `VARCHAR` length widening, and nothing else. Its `DESCRIBE TABLE` output canonicalizes every integer type to `NUMBER(38,0)`, so integer widening never surfaces as drift there.
- **BigQuery** allows `INT64 → NUMERIC`, `INT64 → BIGNUMERIC`, and `NUMERIC → BIGNUMERIC`, and nothing else. A numeric → `STRING` change is not assignable on BigQuery, so it falls through to a full refresh.

:::caution[Databricks and Trino execution gaps]
Databricks and Trino inherit the default allowlist, but their `ALTER` execution paths have known gaps:

- Delta tables reject `ALTER COLUMN ... TYPE` unless the type-widening table feature is enabled, and Rocky does not set it. Delta never accepts numeric → `STRING`.
- Trino requires `SET DATA TYPE` syntax, which the default statement does not use.

On those two warehouses a safe widening does not evolve the column in place. The table's run fails with the warehouse's own error. The failure is loud, never a silent divergence, but Rocky does not yet fall back to a full refresh after a failed `ALTER`. Tracked in [#1115](https://github.com/rocky-data/rocky/issues/1115).
:::

### Unsafe Type Changes

Any type change outside the safe allowlist costs a full refresh. Rocky drops the target and rebuilds it from the source:

```sql
DROP TABLE IF EXISTS acme_warehouse.staging__us_west__shopify.orders
-- followed by full refresh from source
```

Examples: `STRING` to `INT`, `BIGINT` to `INT` (narrowing), `DATE` to `TIMESTAMP`.

## What Is NOT Drift

- **New columns in the source.** Rocky adds them rather than treating them as drift. Before the copy it issues one `ALTER TABLE ADD COLUMN` per new column, each nullable, so historical rows keep `NULL`. The run reports an `add_columns` action.
- **Columns removed from the source.** Rocky ignores extra columns in the target table.

## Output

Drift detection runs inline on a replication run. Rocky reports what it did in the `drift` section of the run JSON output:

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

Run `rocky plan` to preview the SQL, including any drop statements, without executing it:

```bash
rocky plan --filter client=acme --output json
```

The run output surfaces three actions:

| Action | What Rocky did |
|---|---|
| `alter_column_types` | Every drifted column passed the safe-widening check, so Rocky altered them in place. |
| `drop_and_recreate` | At least one change was incompatible, so Rocky rebuilt the target from the source. |
| `add_columns` | Rocky added source-only columns to the target. |

By default Rocky applies these mutations automatically. The opt-in drift-governance gate (`auto_apply_additive_drift` plus a `[policy]` grant for `schema_change.additive`) narrows that. Only provably additive, policy-allowed changes proceed. Rocky refuses anything else before it touches the target, and reports a require-review failure for that table.
