---
title: Data Quality Checks
description: Inline data quality checks that run during replication
sidebar:
  order: 1
---

Rocky runs data quality checks **inline during replication** -- same process, same connection. There is no separate testing step like `dbt test`. Checks execute immediately after each table is replicated, and results are included in the run output.

## Configuration

Enable and configure checks per pipeline in `rocky.toml`:

```toml
[pipeline.bronze.checks]
enabled = true
row_count = true
column_match = true
freshness = { threshold_seconds = 86400 }
```

## Check Types

### Row Count

Compares `COUNT(*)` between source and target tables. The check passes if counts match.

Rocky uses **batched queries** with `UNION ALL` for efficiency. Instead of running one query per table (5N queries for N tables), it batches up to 200 tables per query, reducing the total to roughly 3 queries for a typical pipeline.

**JSON output:**

```json
{
  "name": "row_count",
  "passed": true,
  "source_count": 15000,
  "target_count": 15000
}
```

### Column Match

Compares column sets between source and target tables (case-insensitive). Reports any missing or extra columns.

This check uses **cached columns from drift detection**, so it does not require an additional query.

**JSON output:**

```json
{
  "name": "column_match",
  "passed": true,
  "missing": [],
  "extra": []
}
```

### Freshness

Checks the time since the last data sync by comparing `MAX(timestamp_column)` against the current time.

The threshold is configurable in seconds. A table that has not received new data within the threshold is flagged.

**JSON output:**

```json
{
  "name": "freshness",
  "passed": true,
  "lag_seconds": 300,
  "threshold_seconds": 86400
}
```

### Null Rate (Infrastructure Ready)

Samples the table using `TABLESAMPLE` and calculates the null percentage per column. This avoids scanning the entire table.

**Configuration:**

```toml
null_rate = { columns = ["email"], threshold = 0.05 }
```

The generated SQL uses `TABLESAMPLE (N PERCENT)` for efficiency, making it practical even on very large tables.

### Custom Checks (Infrastructure Ready)

User-provided SQL templates with a `{target}` placeholder that Rocky substitutes at execution time. The query result is compared against a threshold.

**Configuration:**

```toml
custom = [
  { name = "no_future_dates", sql = "SELECT COUNT(*) FROM {target} WHERE date > CURRENT_DATE()", threshold = 0 }
]
```

The check passes if the query result is less than or equal to the threshold.

## Checks vs. declarative tests

Rocky has two complementary data quality mechanisms:

- **Data quality checks** (`[pipeline.<name>.checks]` in `rocky.toml`) run **inline during `rocky run`** — same process, same connection. Use these as production quality gates.
- **Declarative tests** (`[[tests]]` in model sidecar `.toml` files) run via `rocky test --declarative` — a separate command that executes per-model assertions against the warehouse. Use these for development and CI validation.

See [Testing and Contracts — Declarative tests](/concepts/testing/#declarative-tests-tests) for the full declarative test reference.

## Batched Execution

Row count and freshness checks use batched `UNION ALL` queries in groups of 200 tables. This minimizes round trips to the warehouse and keeps execution fast even when replicating hundreds of tables in a single run.
