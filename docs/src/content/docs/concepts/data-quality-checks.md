---
title: Data Quality Checks
description: The two quality surfaces Rocky runs inline against the warehouse - pipeline-level checks and per-model declarative assertions.
sidebar:
  order: 11.5
---

Rocky has two quality surfaces. Both run inline against the warehouse during a run.

1. **Pipeline-level checks** — configured per pipeline in `rocky.toml` under `[pipeline.<name>.checks]`. They run after each table is replicated: row count, column match, freshness, null rate, anomaly detection, custom SQL.
2. **Model-level declarative assertions** — configured per model in the model's sidecar TOML, or directly under `[pipeline.<name>.checks]`, as repeated `[[assertions]]` blocks. They cover `not_null`, `unique`, `unique_expr`, `accepted_values`, `relationships`, `expression`, `row_count_range`, `in_range`, `regex_match`, `aggregate`, and `composite`, plus the time-window shorthands `not_in_future` and `older_than_n_days`.

A third check runs before either of them. The compiler validates each model against its contract, so a model that breaks its contract never reaches the warehouse:

```
        rocky compile                rocky run / rocky apply
        ─────────────                ───────────────────────
   ┌────────────────────┐
   │ contract           │  no errors    ┌──────────────────┐
   │ diagnostics        │──────────────►│ pipeline-level   │
   │ E010 / E013        │               │ checks, after    │
   └─────────┬──────────┘               │ each table lands │
             │ an error                 └────────┬─────────┘
             ▼                                   │
   the run stops. Nothing      ┌──────────────┐  │
   reaches the warehouse.      │ model-level  │  │
                               │ assertions,  │  │
                               │ on any       │  │
                               │ pipeline type│  │
                               └───────┬──────┘  │
                                       └────┬────┘
                                            ▼
                                   check_results[] — one
                                   shape for both surfaces
```

![rocky compile surfaces E010 and E013 contract diagnostic codes on a broken model](/demo-data-contracts.gif)

Assertions run on **every** pipeline type, **replication** included, not just transformation and quality. So a target table doubled by the same source arriving twice is caught at load time. For the cross-*table* version of that problem, where the same key arrives through two sibling sources that later get `UNION`-ed together, see [Cross-source overlap](#cross-source-overlap).

Both surfaces share one JSON output shape (`check_results[]`) and the same severity and quarantine plumbing. An orchestrator does not need to tell them apart.

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

Compares `COUNT(*)` between the source and target tables. The check passes when the counts match.

Rocky batches these queries with `UNION ALL` rather than running one per table. A naive approach costs 5N queries for N tables. Batching up to 200 tables per query brings a typical pipeline down to about 3 queries. Freshness checks batch the same way.

```json
{
  "name": "row_count",
  "passed": true,
  "source_count": 15000,
  "target_count": 15000
}
```

### Column Match

Compares the source and target column sets, ignoring case, and reports any missing or extra column. It reuses the columns cached by drift detection, so it costs no extra query.

```json
{
  "name": "column_match",
  "passed": true,
  "missing": [],
  "extra": []
}
```

### Freshness

Measures how long ago the table last received data, by comparing `MAX(timestamp_column)` against the current time. A table that has seen nothing new within the threshold is flagged.

```json
{
  "name": "freshness",
  "passed": true,
  "lag_seconds": 300,
  "threshold_seconds": 86400
}
```

### Null Rate

Samples the table with `TABLESAMPLE` and works out the null percentage per column. Sampling means it never scans the whole table.

```toml
null_rate = { columns = ["email"], threshold = 0.05 }
```

The generated SQL uses `TABLESAMPLE (N PERCENT)`, so the check stays practical on a large table.

### Custom SQL

Your own SQL, with a `{target}` placeholder that Rocky substitutes at execution time. Rocky compares the query result against a threshold.

```toml
custom = [
  { name = "no_future_dates", sql = "SELECT COUNT(*) FROM {target} WHERE date > CURRENT_DATE()", threshold = 0 }
]
```

The check passes if the query result is less than or equal to the threshold.

## Model-level declarative assertions

Write a declarative assertion as a repeated `[[assertions]]` block, or as `[[tests]]` in a model sidecar. Each block declares a `type`, an optional `column`, an optional `severity`, an optional `filter`, and the parameters that type needs. Together they match the assertion surface of Databricks Labs' DQX.

**Don't confuse `[[tests]]` with `[[test]]`.** They differ by one letter and run on different paths:

| Block | What it checks | How it runs |
|---|---|---|
| `[[assertions]]` under `[pipeline.<name>.checks]` | rows already in the warehouse | inline during `rocky run` / `rocky apply` |
| `[[tests]]` in a model sidecar | rows already in the warehouse | standalone with `rocky test --declarative`, against the configured warehouse adapter |
| `[[test]]` (singular) in a model sidecar | the model's SQL against fixtures | locally on DuckDB with plain `rocky test`, no warehouse connection |

The plural `[[tests]]`, and the equivalent `[[assertions]]`, are the declarative assertions on this page. The singular `[[test]]` is a separate surface: a fixture-driven unit test that mocks the upstream inputs (`given`) and asserts the expected output rows (`expect`).

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
| `unique_expr` | set | `key_expr: String` | A derived **key expression** is unique across rows (`GROUP BY <expr> HAVING COUNT(*) > 1`). For when the meaningful identity is a *computed* value (e.g. a surrogate built to be stable across a multi-tenant union) that neither `unique` (single column) nor `composite` (column tuple) can express. `key_expr` is passed through as written (like `expression`), subject to the one narrow refusal described under **Filters**. NULL keys are not excluded — use `filter` to scope them out. |
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

Most row-level assertions are **quarantinable**, meaning Rocky can route the failing rows aside instead of only counting them: `not_null`, `accepted_values`, `expression`, `in_range`, `regex_match`, `not_in_future`, `older_than_n_days`. See [Row quarantine](#row-quarantine) below.

The set-based, table-level, and referential assertions (`unique`, `unique_expr`, `composite`, `row_count_range`, `aggregate`, `relationships`) run after the fact and cannot be quarantined. `relationships`, for instance, needs a join, not a per-row predicate.

### Severity and `fail_on_error`

Each assertion takes an optional `severity`, either `error` (the default) or `warning`. Each pipeline takes an optional `fail_on_error`, which defaults to `true`.

- `severity = "error"` + `fail_on_error = true` — a failing assertion exits the pipeline non-zero. That is code 2, partial success, if other tables succeeded.
- `severity = "warning"` — a failing assertion appears in `check_results[]` with `passed = false` and `severity = "warning"`. It never fails the pipeline.
- `fail_on_error = false` at the pipeline level downgrades every `error` to a non-fatal result. Use it for shadow runs and observation modes.

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

To apply the same assertion across many models, define it once in `models/test_definitions.toml` and reference it by name. A definition is any assertion `type`, plus its parameters and an optional default `column`:

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

A model applies one with a `[[use_test]]` block. At the use site you may bind or override the column, the severity, and the filter. Inline `[[tests]]` and `[[use_test]]` references sit side by side:

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

A reference resolves into an ordinary assertion at load, so it runs exactly like an inline one. A `[[use_test]]` naming no definition fails the load with a clear error, so a typo cannot silently drop a check.

### Per-assertion `filter`

Every assertion kind accepts an optional `filter`: a SQL boolean predicate that scopes the check to a subset of rows. A row where `(filter)` is `TRUE` is subject to the assertion. A row where it is `FALSE` or `NULL` passes unconditionally.

```toml
[[assertions]]
type = "in_range"
column = "amount_cents"
min = "0"
filter = "region = 'US' AND status != 'cancelled'"
```

The filter is your SQL. You are responsible for making it valid in the target dialect. Rocky validates identifiers inside structured parameters, such as columns and values, but it passes the filter expression through as written.

One check does apply, to `filter`, `expression` and `key_expr` alike. Rocky refuses a fragment that could end the query it is building, and refuses anything it cannot read the same way on every warehouse it targets:

- a statement terminator `;` outside a string literal, a quoted identifier or a comment — including a trailing one;
- an unbalanced quote, or a `/* */` comment that never closes;
- a backslash inside a quoted literal, a triple-quoted string, a `$$…$$` dollar quote, a backtick, a `//` comment, or a nested `/*`. Snowflake, Databricks, BigQuery, DuckDB and Trino do not agree on these, so Rocky refuses rather than guess which reading applies.

The check runs when Rocky builds the query, and it names the field and the table so you know which line to fix.

Be clear about what this does not do. It stops the fragment ending Rocky's statement and starting another. It does **not** make the fragment a single expression: Rocky does not track parentheses, so a fragment can still close the parenthesis Rocky wraps it in and add its own clauses. And it does not limit what the expression may read — a subquery runs with the same warehouse credentials as the rest of the pipeline. Treat a check expression as code you are running, because it is.

### Row quarantine

A row-level assertion can move its failing rows aside instead of only reporting a count. Configure quarantine at the pipeline level:

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

Set-based, table-level, and referential assertions are never quarantinable. They run as after-the-fact checks whatever the mode.

Rocky builds the quarantine predicate from every quarantinable assertion, combined with AND. A filter composes into it as `CASE WHEN (filter) THEN base_valid_pred ELSE TRUE END`. An out-of-scope row therefore stays on the valid side, even when the base predicate would fail it.

### Output

Every assertion produces one `check_results[]` entry in the `rocky apply` JSON output:

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

The `name` is the assertion's explicit `name` when you set one, and otherwise a synthesized `"{kind}:{column}"`. The type-specific detail fields (`kind`, `column`, `failing_rows`) are flattened onto the result, as they are for every other check.

Consumers parse this shape through the generated Pydantic and TypeScript bindings: dagster-rocky, the VS Code lineage view, and your own scripts. See the [JSON Output](/reference/json-output/) reference.

## Cross-source overlap

The assertions above check one table at a time. They cannot catch a subtler duplication: the **same business key arriving through two different sources** that later get `UNION`-ed into one consolidation target.

Each source table is internally unique, so every per-table `unique` check passes. The consolidation still double-counts every shared key. This is the classic "same account onboarded twice under two paths" failure, and `cross_source_overlap` is the cross-table check that sees it.

```toml
[pipeline.bronze.checks.cross_source_overlap]
keys = ["order_id"]          # or: key_expr = "md5(a || '-' || b)"
severity = "warning"
max_overlap_rows = 0          # any overlap fails; raise to tolerate a known set
sample = 20                   # overlapping keys attached to the result for triage
```

Give exactly one of `keys` (a column tuple) or `key_expr` (a derived SQL expression, passed through as written). This mirrors `unique` and `unique_expr`.

**How it works.** The runner buckets the pipeline's managed source tables into **sibling groups**. Siblings share a source type and a table name, and they landed in more than one target schema. That is the tenant or region fan-out that gets unioned downstream. Rocky tags each sibling's rows with its source identity and runs:

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

The `COUNT(DISTINCT _src)` is the crux. It counts how many *distinct sources* a key appears in, so one source's own internal duplicates never raise a false flag. Only a key that spans two or more siblings does. With a `key_expr` or multi-column `keys`, the projected key list changes to match.

Some sibling tables cannot be evaluated: the key column is missing, or the table is keyless. Rocky **skips them with a logged reason** rather than failing the check.

The result is a `check_results[]` entry named `cross_source_overlap:<source_type>.<table>`. It carries the overlap count, the contributing tables, and a bounded `sample` of overlapping keys. The detail fields are flattened onto the result, as with every other check:

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

The preventive layer needs an adapter that resolves external object ids, such as Fivetran, and that inspects connector metadata. The detective layer works on any warehouse, because it queries the materialized tables directly.

Use both for defense in depth. Use the detective check alone if your sources expose no object ids at discover time. See [discovery configuration](/reference/configuration/#pipelinenamesourcediscovery) for `on_collision`.
