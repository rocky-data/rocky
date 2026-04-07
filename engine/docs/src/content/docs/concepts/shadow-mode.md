---
title: Shadow Mode
description: Validate pipeline changes by comparing shadow tables against production
sidebar:
  order: 14
---

## Overview

Shadow mode writes pipeline output to shadow tables instead of (or alongside) production tables. This lets you validate changes — new logic, schema migrations, adapter upgrades — without affecting production data.

## How It Works

1. Rocky rewrites target table names by appending a suffix (default: `_rocky_shadow`) or routing to a dedicated schema
2. The pipeline runs normally, writing to shadow targets
3. A comparison engine checks row counts, schemas, and optionally sample data between shadow and production
4. Results show pass/warn/fail with detailed diffs

## Shadow Target Rewriting

### Suffix Mode (default)

```
production: analytics.marts.fct_revenue
shadow:     analytics.marts.fct_revenue_rocky_shadow
```

### Schema Override Mode

```
production: analytics.marts.fct_revenue
shadow:     analytics.rocky_shadow.fct_revenue
```

Schema override keeps the table name clean and groups all shadow tables together.

## Comparison Engine

The comparison evaluates three dimensions:

### Row Count

Compares the number of rows between shadow and production:

```
shadow:     148,203 rows
production: 148,205 rows
diff:       -2 rows (-0.001%)
verdict:    PASS (within 0.01% threshold)
```

### Schema Diff

Compares column names, types, and order:

| Diff Type | Description |
|-----------|-------------|
| `ColumnAdded` | Column in shadow but not production |
| `ColumnRemoved` | Column in production but not shadow |
| `ColumnTypeDiff` | Same column, different type |
| `ColumnOrderDiff` | Same columns, different order |

### Sample Comparison

Hash-based comparison of sample rows to detect value differences even when row counts match.

## Thresholds

Configure pass/warn/fail thresholds:

| Threshold | Default | Description |
|-----------|---------|-------------|
| `row_count_diff_pct_warn` | 0.01 (1%) | Warn if row count differs by more than this |
| `row_count_diff_pct_fail` | 0.05 (5%) | Fail if row count differs by more than this |
| `allow_column_order_diff` | true | Whether column reordering is acceptable |

## Verdicts

| Verdict | Meaning |
|---------|---------|
| **Pass** | All comparisons within thresholds |
| **Warn** | Minor differences detected (e.g., row count within warn threshold, column order change) |
| **Fail** | Significant differences (e.g., row count beyond fail threshold, missing columns, type changes) |

## Use Cases

- **Schema migrations**: Verify a column rename doesn't change output
- **Logic changes**: Compare old vs new calculation results
- **Adapter testing**: Validate a new warehouse adapter against the production adapter
- **dbt migration**: Compare Rocky output against dbt output (via `rocky validate-migration`)
