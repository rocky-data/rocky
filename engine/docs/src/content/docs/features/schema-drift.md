---
title: Schema Drift
description: Automatic detection and resolution of schema changes
sidebar:
  order: 2
---

Rocky automatically detects schema drift between source and target tables and resolves it by dropping and recreating the affected target table.

## What It Detects

Schema drift in Rocky means **column type mismatches** between source and target tables. For example, a column that was `STRING` in the source but is `INT` in the target.

## How It Works

1. Runs `DESCRIBE TABLE` on both the source and target tables
2. Compares column types (case-insensitive)
3. If a type mismatch is found, triggers a `DropAndRecreate` action

## What Is NOT Drift

- **New columns in source** -- these are picked up automatically by `SELECT *` and do not require special handling
- **Columns removed from source** -- extra columns in the target table are ignored

## Auto-Remediation

When drift is detected, Rocky automatically:

1. Drops the target table with `DROP TABLE IF EXISTS`
2. Performs a full refresh of the table from source

This ensures the target schema always matches the source, without manual intervention.

## Output

Drift actions are reported in the `drift` section of the run output:

```json
{
  "drift": {
    "tables_checked": 45,
    "tables_drifted": 1,
    "actions_taken": [
      {
        "table": "acme_warehouse.staging__us_west__shopify.orders",
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
