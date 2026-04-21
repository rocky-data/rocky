---
title: Data Quality Checks
description: Inline data quality checks that run during replication, plus per-model declarative assertions
sidebar:
  order: 1
---

Rocky ships two complementary quality surfaces, both executed inline against the warehouse — there is no separate testing step like `dbt test`:

1. **Pipeline-level checks** — configured per pipeline in `rocky.toml` under `[pipeline.<name>.checks]`. These run after each table is replicated: row count, column match, freshness, null rate, anomaly detection, custom SQL.
2. **Model-level declarative assertions** — configured per model in the model's sidecar TOML (or directly under `[pipeline.<name>.checks]`) via repeated `[[assertions]]` blocks. These cover the DQX parity surface: `not_null`, `unique`, `accepted_values`, `relationships`, `expression`, `row_count_range`, `in_range`, `regex_match`, `aggregate`, `composite`, plus the time-window sugar `not_in_future` and `older_than_n_days`.

Both surfaces share the same JSON output shape (`check_results[]`) and the same severity / quarantine plumbing, so orchestrators don't need to distinguish between them.

Compile-time contract diagnostics run even earlier — before any check executes — so a model that violates its contract never reaches the warehouse:

![rocky compile surfaces E010 and E013 contract diagnostic codes on a broken model](/demo-data-contracts.gif)

## Pipeline-level checks

Enable and configure checks per pipeline in `rocky.toml`:

```toml
[pipeline.bronze.checks]
enabled = true
row_count = true
column_match = true
freshness = { threshold_seconds = 86400 }
```

### Row Count

Compares `COUNT(*)` between source and target tables. The check passes if counts match.

Rocky uses batched queries with `UNION ALL` for efficiency. Instead of running one query per table (5N queries for N tables), it batches up to 200 tables per query, reducing the total to roughly 3 queries for a typical pipeline.

```json
{
  "name": "row_count",
  "passed": true,
  "source_count": 15000,
  "target_count": 15000
}
```

### Column Match

Compares column sets between source and target tables (case-insensitive). Reports any missing or extra columns. Uses cached columns from drift detection, so it does not require an additional query.

```json
{
  "name": "column_match",
  "passed": true,
  "missing": [],
  "extra": []
}
```

### Freshness

Checks the time since the last data sync by comparing `MAX(timestamp_column)` against the current time. A table that has not received new data within the threshold is flagged.

```json
{
  "name": "freshness",
  "passed": true,
  "lag_seconds": 300,
  "threshold_seconds": 86400
}
```

### Null Rate

Samples the table using `TABLESAMPLE` and calculates the null percentage per column. This avoids scanning the entire table.

```toml
null_rate = { columns = ["email"], threshold = 0.05 }
```

The generated SQL uses `TABLESAMPLE (N PERCENT)` for efficiency, making it practical even on very large tables.

### Custom SQL

User-provided SQL templates with a `{target}` placeholder that Rocky substitutes at execution time. The query result is compared against a threshold.

```toml
custom = [
  { name = "no_future_dates", sql = "SELECT COUNT(*) FROM {target} WHERE date > CURRENT_DATE()", threshold = 0 }
]
```

The check passes if the query result is less than or equal to the threshold.

### Batched execution

Row count and freshness checks use batched `UNION ALL` queries in groups of 200 tables. This minimizes round trips to the warehouse and keeps execution fast even when replicating hundreds of tables in a single run.

## Model-level declarative assertions (DQX parity)

Declarative assertions are defined as repeated `[[assertions]]` (or `[[tests]]` in a model sidecar) blocks. Each one declares a `type`, optional `column`, optional `severity`, optional `filter`, and type-specific parameters.

```toml
[[assertions]]
type = "not_null"
column = "order_id"

[[assertions]]
type = "accepted_values"
column = "status"
values = ["pending", "shipped", "delivered"]
severity = "warning"

[[assertions]]
type = "in_range"
column = "amount_cents"
min = "0"
max = "100000000"
filter = "region = 'US'"
```

### Assertion kinds

| Type | Level | Parameters | Description |
|---|---|---|---|
| `not_null` | row | — | Column contains no NULL values. |
| `unique` | set | — | Column contains only unique values. |
| `accepted_values` | row | `values: [String]` | Every non-NULL value is in the fixed set. |
| `relationships` | row | `to_table`, `to_column` | Every non-NULL value exists in `to_table.to_column` (referential integrity). |
| `expression` | row | `expression: String` | Custom SQL boolean predicate must hold per row. |
| `row_count_range` | table | `min`, `max` (both optional) | Table row count falls within the inclusive range. |
| `in_range` | row | `min`, `max` (both optional, numeric) | Column's values fall within the numeric range. NULLs pass. |
| `regex_match` | row | `pattern: String` | Column matches the dialect-specific regex. NULLs pass. Patterns are validated against a strict allowlist (no single quotes, backticks, or semicolons). |
| `aggregate` | table | `op`, `cmp`, `value` | Aggregate comparison holds: `op(column) cmp value` must be `TRUE`. `op` ∈ `sum`, `count`, `avg`, `min`, `max`. `cmp` ∈ `lt`, `lte`, `gt`, `gte`, `eq`, `ne` (or their symbolic aliases). |
| `composite` | set | `kind: "unique"`, `columns: [String]` | Multi-column uniqueness. At least two columns required — use `unique` for single-column. |
| `not_in_future` | row | — (sugar for `col <= CURRENT_TIMESTAMP()`) | Timestamp column cannot contain future values. NULLs pass. |
| `older_than_n_days` | row | `days: u32` | Every timestamp must be at least `days` old. NULLs pass. Dialect-aware. |

Row-level assertions are **quarantinable** (see below). Set-based and table-level assertions (`unique`, `composite`, `row_count_range`, `aggregate`) are evaluated post-hoc and cannot be quarantined.

### Severity and `fail_on_error`

Each assertion takes an optional `severity` (`error` by default, or `warning`) and the pipeline takes an optional `fail_on_error` (`true` by default).

- `severity = "error"` + `fail_on_error = true` — a failing assertion causes the pipeline to exit non-zero (partial-success code 2 if other tables succeeded).
- `severity = "warning"` — a failing assertion appears in `check_results[]` with `passed = false` and `severity = "warning"`, but never fails the pipeline.
- `fail_on_error = false` at the pipeline level downgrades every `error` to a non-fatal result (useful for shadow runs and observation modes).

```toml
[pipeline.silver.checks]
fail_on_error = true  # default

[[pipeline.silver.checks.assertions]]
type = "not_null"
column = "order_id"
severity = "error"   # default — a null order_id fails the run

[[pipeline.silver.checks.assertions]]
type = "accepted_values"
column = "status"
values = ["pending", "shipped", "delivered"]
severity = "warning"  # unknown status reports but doesn't fail
```

### Per-assertion `filter`

Every assertion kind accepts an optional `filter` — a SQL boolean predicate that scopes the check to a subset of rows. Rows where `(filter)` evaluates to `TRUE` are subject to the assertion; rows where it's `FALSE` or `NULL` pass unconditionally.

```toml
[[assertions]]
type = "in_range"
column = "amount_cents"
min = "0"
filter = "region = 'US' AND status != 'cancelled'"
```

Filter is user-supplied SQL — the caller is responsible for valid SQL in the target dialect. Rocky validates identifiers inside structured parameters (columns, values) but passes the filter expression through verbatim.

### Row quarantine

Row-level assertions can quarantine failing rows instead of just reporting a count. Configure quarantine at the pipeline level:

```toml
[pipeline.silver.checks.quarantine]
mode = "split"   # or "tag" or "drop"
```

| Mode | Behavior |
|---|---|
| `split` | Rocky materializes two tables: `<target>` with passing rows, `<target>__quarantine` with failing rows. Downstream models see only the clean table. |
| `tag` | A `__dqx_valid` boolean column is added to `<target>`; failing rows stay in the table with `__dqx_valid = FALSE`. Useful for observation without rewiring downstream. |
| `drop` | Failing rows are dropped from `<target>`. Quarantine count is still reported in `check_results[]`. |

Set-based and table-level assertions are not quarantinable — they run as post-hoc checks regardless of mode.

The quarantine predicate is built from every quarantinable assertion, combined with AND. Filters compose via `CASE WHEN (filter) THEN base_valid_pred ELSE TRUE END`, so out-of-scope rows stay on the valid side even when the base predicate would fail them.

### Output

Every assertion produces a `check_results[]` entry in the `rocky run` JSON output:

```json
{
  "name": "orders.order_id.not_null",
  "passed": false,
  "severity": "error",
  "failing_count": 3,
  "quarantined": true,
  "sql": "SELECT COUNT(*) FROM orders WHERE order_id IS NULL"
}
```

Consumers (dagster-rocky, the VS Code lineage view, custom scripts) parse this shape via the generated Pydantic / TypeScript bindings — see the [JSON Output](/reference/json-output/) reference.
