---
title: Schema Drift
description: Automatic detection and graduated resolution of schema changes
sidebar:
  order: 2
---

Rocky automatically detects schema drift between source and target tables and resolves it using **graduated evolution** -- safe type widenings are handled with `ALTER TABLE` (preserving data), while unsafe changes trigger a full refresh.

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

- **New columns in source** -- these are picked up automatically by `SELECT *` and do not require special handling
- **Columns removed from source** -- extra columns in the target table are ignored

## Standalone Drift Check

Use `rocky drift` to check for drift without executing any changes:

```bash
rocky drift --filter client=acme --output json
```

## Output

Drift actions are reported in the `drift` section of the run output:

```json
{
  "drift": {
    "tables_checked": 45,
    "tables_drifted": 2,
    "actions_taken": [
      {
        "table": "acme_warehouse.staging__us_west__shopify.orders",
        "action": "alter_column",
        "reason": "column 'amount' widened DECIMAL(10,2) -> DECIMAL(12,2)"
      },
      {
        "table": "acme_warehouse.staging__us_west__shopify.events",
        "action": "drop_and_recreate",
        "reason": "column 'status' changed STRING -> INT"
      }
    ]
  }
}
```

Drift detection runs automatically as part of `rocky run`. Use `rocky plan` to preview drift actions without executing them:

```bash
rocky plan --filter client=acme --output json
```
