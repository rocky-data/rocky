---
title: Data Quality Checks
description: Inline data quality checks that run during replication, plus per-model declarative assertions
sidebar:
  order: 11.5
---

Rocky ships two complementary quality surfaces, both executed inline against the warehouse. There is no separate testing step like `dbt test`:

1. **Pipeline-level checks** — configured per pipeline in `rocky.toml` under `[pipeline.<name>.checks]`. These run after each table is replicated: row count, column match, freshness, null rate, anomaly detection, custom SQL.
2. **Model-level declarative assertions** — configured per model in the model's sidecar TOML (or directly under `[pipeline.<name>.checks]`) via repeated `[[assertions]]` blocks. These cover the DQX parity surface: `not_null`, `unique`, `unique_expr`, `accepted_values`, `relationships`, `expression`, `row_count_range`, `in_range`, `regex_match`, `aggregate`, `composite`, plus the time-window sugar `not_in_future` and `older_than_n_days`.

Assertions run on **every** pipeline type (including **replication**, not just transformation/quality), so a target table doubled by the same source arriving twice is caught at load time. For the cross-*table* variant (the same key arriving through two sibling sources that later get `UNION`-ed together), see [Cross-source overlap](#cross-source-overlap).

Both surfaces share the same JSON output shape (`check_results[]`) and the same severity / quarantine plumbing, so orchestrators don't need to distinguish between them.

Compile-time contract diagnostics run even earlier, before any check executes, so a model that violates its contract never reaches the warehouse:

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

Rocky uses batched queries with `UNION ALL`. Instead of running one query per table (5N queries for N tables), it batches up to 200 tables per query, reducing the total to roughly 3 queries for a typical pipeline. Freshness checks batch the same way.

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

The generated SQL uses `TABLESAMPLE (N PERCENT)`, so it stays practical on large tables.

### Custom SQL

User-provided SQL templates with a `{target}` placeholder that Rocky substitutes at execution time. The query result is compared against a threshold.

```toml
custom = [
  { name = "no_future_dates", sql = "SELECT COUNT(*) FROM {target} WHERE date > CURRENT_DATE()", threshold = 0 }
]
```

The check passes if the query result is less than or equal to the threshold.

## Model-level declarative assertions (DQX parity)

Declarative assertions are defined as repeated `[[assertions]]` (or `[[tests]]` in a model sidecar) blocks. Each one declares a `type`, optional `column`, optional `severity`, optional `filter`, and type-specific parameters.

**Don't confuse `[[tests]]` with `[[test]]`.** They differ by one letter and run on different paths. The plural `[[tests]]` (and the equivalent `[[assertions]]`) are the declarative assertions on this page. Assertions written under `[pipeline.<name>.checks]` run inline against the warehouse during `rocky run`/`apply`; the model-sidecar `[[tests]]` form runs standalone with `rocky test --declarative`, which executes each assertion against the configured warehouse adapter. The singular `[[test]]` is a separate surface: fixture-driven unit tests that mock upstream inputs (`given`) and assert expected output rows (`expect`), run locally on DuckDB by plain `rocky test`. The assertions check rows already in the warehouse; the unit tests check the model's SQL against fixtures, with no warehouse connection.

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
| `unique_expr` | set | `key_expr: String` | A derived **key expression** is unique across rows (`GROUP BY <expr> HAVING COUNT(*) > 1`). For when the meaningful identity is a *computed* value (e.g. a surrogate built to be stable across a multi-tenant union) that neither `unique` (single column) nor `composite` (column tuple) can express. `key_expr` is passed through verbatim (trusted config, like `expression`); NULL keys are not excluded — use `filter` to scope them out. |
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

Most row-level assertions are **quarantinable** (see below): `not_null`, `accepted_values`, `expression`, `in_range`, `regex_match`, `not_in_future`, `older_than_n_days`. Set-based, table-level, and referential assertions (`unique`, `unique_expr`, `composite`, `row_count_range`, `aggregate`, `relationships`) are evaluated post-hoc and cannot be quarantined — `relationships` needs a join, not a per-row predicate.

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

### Reusable named tests

When the same assertion is applied across many models, define it once in `models/test_definitions.toml` and reference it by name. A definition is any assertion `type` plus its parameters and an optional default `column`:

```toml
# models/test_definitions.toml
[positive_amount]
type = "expression"
expression = "amount > 0"

[known_status]
type = "accepted_values"
column = "status"
values = ["pending", "shipped", "delivered"]
```

A model applies one with a `[[use_test]]` block, optionally binding or overriding the column, severity, and filter at the use site. Inline `[[tests]]` and `[[use_test]]` references coexist:

```toml
# models/fct_orders.toml
[[tests]]
type = "unique"
column = "order_id"

[[use_test]]
name = "positive_amount"
column = "amount"

[[use_test]]
name = "known_status"   # uses the definition's default column
severity = "warning"
```

References resolve into ordinary assertions at load, so they run identically to inline ones. A `[[use_test]]` naming no definition fails the load with a clear error, so a typo can't silently drop a check.

### Per-assertion `filter`

Every assertion kind accepts an optional `filter`, a SQL boolean predicate that scopes the check to a subset of rows. Rows where `(filter)` evaluates to `TRUE` are subject to the assertion; rows where it's `FALSE` or `NULL` pass unconditionally.

```toml
[[assertions]]
type = "in_range"
column = "amount_cents"
min = "0"
filter = "region = 'US' AND status != 'cancelled'"
```

Filter is user-supplied SQL: the caller is responsible for valid SQL in the target dialect. Rocky validates identifiers inside structured parameters (columns, values) but passes the filter expression through verbatim.

### Row quarantine

Row-level assertions can quarantine failing rows instead of just reporting a count. Configure quarantine at the pipeline level:

```toml
[pipeline.silver.checks.quarantine]
enabled = true
mode = "split"   # or "tag" or "drop"
```

| Mode | Behavior |
|---|---|
| `split` | Rocky materializes two new tables: `<target>__valid` with the passing rows and `<target>__quarantine` with the failing rows (plus per-assertion `_error_<name>` label columns marking which assertion each row failed). The original `<target>` is left untouched; point downstream models at `<target>__valid`. |
| `tag` | Rocky rewrites `<target>` in place, adding a per-assertion `_error_<name>` column populated on failing rows (NULL on passing rows). Every row stays in the table. Useful for observation without a second table — rewrites the source, so use with care on a raw replication target. |
| `drop` | Only `<target>__valid` (the passing rows) is written; failing rows are discarded. Quarantine count is still reported in `check_results[]`. |

Set-based, table-level, and referential assertions are not quarantinable; they run as post-hoc checks regardless of mode.

The quarantine predicate is built from every quarantinable assertion, combined with AND. Filters compose via `CASE WHEN (filter) THEN base_valid_pred ELSE TRUE END`, so out-of-scope rows stay on the valid side even when the base predicate would fail them.

### Output

Every assertion produces a `check_results[]` entry in the `rocky apply` JSON output:

```json
{
  "name": "not_null:order_id",
  "passed": false,
  "severity": "error",
  "kind": "not_null",
  "column": "order_id",
  "failing_rows": 3
}
```

The `name` is the assertion's explicit `name` when set, else a synthesized `"{kind}:{column}"`. The type-specific detail fields (`kind`, `column`, `failing_rows`) are flattened onto the result, consistent with every other check.

Consumers (dagster-rocky, the VS Code lineage view, custom scripts) parse this shape via the generated Pydantic / TypeScript bindings (see the [JSON Output](/reference/json-output/) reference).

## Cross-source overlap

The assertions above check one table at a time. They can't catch a subtler duplication: the **same business key arriving through two different sources** that later get `UNION`-ed into one consolidation target. Each source table is internally unique (every per-table `unique` check passes), yet the consolidation double-counts every shared key.

This is the classic "same account onboarded twice under two paths" failure. `cross_source_overlap` is the cross-table check that sees it.

```toml
[pipeline.bronze.checks.cross_source_overlap]
keys = ["order_id"]          # or: key_expr = "md5(a || '-' || b)"
severity = "warning"
max_overlap_rows = 0          # any overlap fails; raise to tolerate a known set
sample = 20                   # overlapping keys attached to the result for triage
```

Exactly one of `keys` (a column tuple) or `key_expr` (a derived SQL expression, passed through verbatim) is required, mirroring `unique` / `unique_expr`.

**How it works.** The runner buckets the pipeline's managed source tables into **sibling groups**: tables with the same source type and table name that landed in more than one target schema (the tenant/region fan-out that gets unioned downstream). It tags each sibling's rows with its source identity and runs:

```sql
SELECT order_id, COUNT(DISTINCT _src) AS _n_src
FROM (
  SELECT order_id, '<table_1>' AS _src FROM <table_1> WHERE order_id IS NOT NULL
  UNION ALL
  SELECT order_id, '<table_2>' AS _src FROM <table_2> WHERE order_id IS NOT NULL
  -- … one arm per sibling
) _u
GROUP BY order_id
HAVING COUNT(DISTINCT _src) > 1
```

The `COUNT(DISTINCT _src)` is the crux: it counts how many *distinct sources* a key appears in, so a single source's own internal duplicates never false-flag; only a key spanning two or more siblings does. (With a `key_expr` or multi-column `keys`, the projected key list changes accordingly.) A key appearing under more than one source is an overlap. Sibling tables whose key can't be evaluated (missing column / keyless) are **skipped with a logged reason** rather than failing the check. The result is a `check_results[]` entry named `cross_source_overlap:<source_type>.<table>`, carrying the overlap count, the contributing tables, and a bounded `sample` of overlapping keys (the detail fields are flattened onto the result, consistent with every other check):

```json
{
  "name": "cross_source_overlap:shopify.orders",
  "passed": false,
  "severity": "warning",
  "overlap_count": 3,
  "contributing_tables": ["raw__us__shopify.orders", "raw__eu__shopify.orders"],
  "sample": ["ord_1001", "ord_1002", "ord_1003"]
}
```

### Preventive vs detective

Rocky catches cross-source duplication at two points:

| Layer | Mechanism | When it runs | Config |
|---|---|---|---|
| **Preventive** | `on_collision` | `rocky discover` — before a stray catalog is even created | `[pipeline.NAME.source.discovery] on_collision` → `collision_candidates` |
| **Detective** | `cross_source_overlap` | `rocky run` — after the sibling tables are materialized | `[pipeline.NAME.checks.cross_source_overlap]` |

The preventive layer needs an adapter that resolves external object ids (e.g. Fivetran) and inspects connector metadata; the detective layer works on any warehouse by querying the materialized tables directly. They're complementary: use both for defense in depth, or just the detective check if your sources don't expose object ids at discover time. See [discovery configuration](/reference/configuration/#pipelinenamesourcediscovery) for `on_collision`.
