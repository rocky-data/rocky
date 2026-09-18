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

On Databricks, Rocky batches these queries with `UNION ALL` rather than running one per table. A naive approach costs 5N queries for N tables. Batching up to 200 tables per query brings a typical pipeline down to about 3 queries. Freshness batches the same way.

No other warehouse batches these two checks today. Snowflake and BigQuery batch the schema describe only, and run row count and freshness one query per table, as every other adapter does.

```json
{
  "name": "row_count",
  "passed": true,
  "source_count": 15000,
  "target_count": 15000
}
```

### Anomaly detection

Compares this run's target row count against the table's recent history in the state store. A deviation above `anomaly_threshold_pct` is reported in the run's `anomalies` list. The default threshold is 50%.

The detector does not run for every table. It runs for a table only when all of these are true:

- Row-count checks are on for the pipeline.
- At least one table in the batch was checked for row counts.
- `anomaly_threshold_pct` is above 0. A value of 0 or less switches detection off.
- The run has a state store, which holds the history.
- This table's row count was measured, and its history could be read.

`rocky run` therefore reports, per table, whether the detector evaluated it:

```json
"anomaly_evaluated": [
  { "table": "analytics.marts.orders", "evaluated": true },
  {
    "table": "analytics.marts.line_items",
    "evaluated": false,
    "not_evaluated_reason": "this run has no state store, so there is no row-count history to compare against"
  }
]
```

Read the two lists together. An empty `anomalies` list on its own means both "the detector found nothing" and "the detector never looked":

```
anomalies: []  +  evaluated: true   ──►  the count is normal
anomalies: []  +  evaluated: false  ──►  nothing was measured; the reason says why
```

`not_evaluated_reason` names one missing condition, not all of them, because the remedies differ. Two are a line in `rocky.toml`, one is how the run was invoked, and two are about the table.

Detection is a heuristic. A row-count swing is often real business behavior, so an anomaly is a warning, not a failed run.

### Column Match

Compares the source and target column sets, ignoring case, and reports any missing or extra column. Rocky reads both column lists again after the copy, one schema read per side, so the check costs two extra metadata queries per table. It reads them after the copy so it sees the schema the copy produced, including a column the run itself added. A read that fails is retried once, but only when the adapter classifies the failure as retryable and it is not a rate limit. A rate limit, a permanent error, or a second failure reports the check as not evaluated instead of comparing against a list nobody read.

```json
{
  "name": "column_match",
  "passed": true,
  "missing": [],
  "extra": []
}
```

### Freshness

Measures how long ago the table last received data, by comparing `MAX(timestamp_column)` against the current time. When there is a timestamp to measure, a table that has seen nothing new within the threshold is flagged. It is not always measured — see below.

```json
{
  "name": "freshness",
  "passed": true,
  "lag_seconds": 300,
  "threshold_seconds": 86400
}
```

**An empty table emits no freshness check.** `MAX()` answers `NULL` over no rows, and there is nothing to be fresh. Rocky asks for `COUNT(*)` in the same query, so a non-empty table whose timestamp column holds no value is told apart from an empty one: that case is reported as a failed `freshness_not_evaluated` check whose reason gives the row count, because rows exist and nothing says how fresh they are.

A freshness query that **fails**, or that returns a cell Rocky cannot read as a timestamp or as a count, is reported the same way. Of the tables Rocky queries, only an empty one goes silent.

A table can also go silent without any query at all. `prune_unchanged` is off by default. With it on, and only where the adapter can report a source change-marker, a table whose source has not changed skips its data checks entirely — freshness included. [Schema drift](/concepts/schema-drift/) covers pruning and what it takes.

In the CLI's JSON, that silence looks the same as a table with no `freshness` configured: the check is absent, not failing.

Dagster is different. In Pipes mode nothing is filled in, so the check is absent there as it is in the JSON.

Outside Pipes mode a configured check has a declared spec, so Dagster never leaves it blank: it fills the gap with a placeholder, and what the placeholder says depends on why the result is missing. A **pruned** table carries forward its last recorded verdict, or passes with a note saying it was never checked. A table that was checked and produced no freshness result fails at `WARN` severity, marked `not produced by rocky`. A table that was never materialized fails at `WARN` too, with its own reason.

If freshness matters for a table, confirm its `timestamp_column` is populated: a table with rows and no value in that column fails its freshness check as not evaluated until it is.

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
| `unique_expr` | set | `key_expr: String` | A derived **key expression** is unique across rows (`GROUP BY <expr> HAVING COUNT(*) > 1`). For when the meaningful identity is a *computed* value (e.g. a surrogate built to be stable across a multi-tenant union) that neither `unique` (single column) nor `composite` (column tuple) can express. `key_expr` goes through the same gate as `expression`, plus the two key-position rules under **Filters**. NULL keys are not excluded — use `filter` to scope them out. |
| `accepted_values` | row | `values: [String]` | Every non-NULL value is in the fixed set. |
| `relationships` | row | `to_table`, `to_column` | Every non-NULL value exists in `to_table.to_column` (referential integrity). |
| `expression` | row | `expression: String` | Custom SQL boolean predicate must hold per row. Bounded: one expression over the row's own columns, calling only allowlisted pure scalar functions — no subquery, no qualified function. See **Filters** below. |
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

- `severity = "error"` + `fail_on_error = true` — a failing assertion fails the run. A replication run that already copied data exits `2`, partial success. A quality pipeline's failed check gate exits `1`.
- `severity = "warning"` — a failing assertion appears in `check_results[]` with `passed = false` and `severity = "warning"`. It never fails the pipeline.
- `fail_on_error = false` at the pipeline level downgrades every `error` to a non-fatal result. Use it for shadow runs and observation modes.

For the replication checks, `severity` describes a check that ran and found a problem. It does not apply to a check Rocky could not run at all.

A check whose query fails is reported with `passed = false` and a `not_evaluated` field saying why. That result always carries `severity = "error"`, whatever the config says, so it fails the run while `fail_on_error` is on. This holds for every kind: `row_count`, `column_match`, `freshness`, `null_rate`, `[[checks.custom]]`, `[[assertions]]`, `cross_source_overlap`, and a quarantine plan Rocky could not compile.

```
the query answers, the data is bad   -> your severity   (warning stays advisory)
the query never ran                  -> always error    (gates, unless
                                                         fail_on_error = false)
```

The two say different things. Writing `severity = "warning"` on `freshness` means "a stale table is only a warning". It does not mean "a freshness query I could not run is only a warning" — that is an unknown, not a tolerated result.

One result is the exception, and it is not a failure. Rocky counts how many siblings in a `cross_source_overlap` group carry the key, then splits three ways:

```
0 siblings carry the key  -> not evaluated, error severity, fails   (a typo)
1 sibling carries it      -> passes, your severity, does not gate   (nothing
                                                                     to compare)
2 or more carry it        -> measured across those siblings, and it can fail
```

So a group of three where two share a duplicate key still fails, even though the third sibling has no such column. A group whose query failed, or whose key expression was refused, is an unevaluated failure like every other kind.

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

The filter is your SQL, and it must be valid in the target dialect. Rocky does not rewrite it. It does check it, with one gate that covers these fields:

- an assertion's `filter` and its `expression`;
- a `unique_expr` `key_expr` and a `cross_source_overlap` `key_expr`;
- the same `expression` and `filter` again when quarantine lowers that assertion into its own statements. `[checks.quarantine]` itself takes only `enabled`, `mode` and the two suffixes;
- a `metadata_columns[].value`;
- a check an agent drafts through the `draft_check` MCP tool, which parses under the generic dialect because it has no target yet.

One config field is **not** gated: a `[[checks.custom]]` `sql` query. Rocky substitutes `{target}` into it and runs it as written, so treat a custom check as code you are running.

Rocky parses the fragment under the target dialect and accepts exactly one expression over the row's own columns. It refuses:

- anything left over after that one expression, so the fragment cannot close the parenthesis Rocky wraps it in and add clauses of its own;
- any subquery, in any position;
- any qualified function name, such as `schema.fn(...)` — that is how user-defined, remote and plugin functions are reached;
- any function that is not on Rocky's allowlist of pure scalar functions;
- a lambda, because its body is the same question one level down;
- a placeholder (`$name`, `$1`, `?`), and an unquoted session-identity word such as `current_user`, `session_user`, `current_role`, `current_catalog` or `current_warehouse`. Each reads session state rather than the row.

Before that, Rocky refuses a fragment that could end the query it is building, or that the five target dialects do not lex the same way:

- a statement terminator `;` outside a string literal, a quoted identifier or a comment — including a trailing one;
- an unbalanced quote, or a `/* */` comment that never closes;
- a backslash inside a quoted literal, a triple-quoted string, a `$$…$$` dollar quote, a backtick, a `//` or `#` line comment, or a nested `/*`. Snowflake, Databricks, BigQuery, DuckDB and Trino do not agree on these, so Rocky refuses rather than guess which reading applies.

Comparisons, `CASE`, `CAST`, `BETWEEN`, `IN (...)` with literals, and functions such as `coalesce`, `nullif`, `abs`, `round`, `length`, `lower`, `upper`, `trim`, `regexp_like`, `md5` and `date_trunc` pass. Functions that read a file, a secret, a session variable or a remote endpoint do not, whatever their name looks like — DuckDB's `read_text`, Snowflake's `GETVARIABLE` and Databricks' `secret` all sit in ordinary scalar position and are refused by name.

One position adds a rule, because the expression is used differently there:

| Position | Extra rule |
|---|---|
| `unique_expr` `key_expr`, `cross_source_overlap` `key_expr` | No clock function (`now()`, `current_timestamp`), and no `COLLATE`. A value that changes between evaluations is not a key, and a collation changes what equality means. |
A quarantine `expression` or `filter` accepts a clock function such as `now()` in every mode, as an ordinary check `filter` does. So do `not_in_future` and `older_than_n_days` assertions. `split` evaluates each predicate once, so a clock read cannot put a row in both outputs.

`random()` and `uuid()` need no rule here. Neither is on the allowlist, so both are refused in every position.

The gate runs before anything executes, and how a refusal reaches you depends on the field:

| Field | What a refusal does |
|---|---|
| An assertion `filter` or `expression`, a `key_expr` | The check is reported as failing at error severity, with a `not_evaluated` reason naming the field, the table and the construct to remove. |
| A quarantine `expression` or `filter` | Reported the same way under the name `quarantine:compile`. No quarantine table is written, and the run fails, whatever `fail_on_error` says. |
| `metadata_columns[].value` | The config load fails, so no command runs. |
| The `draft_check` MCP tool | The tool refuses the write, so the bad check is never saved. |

### Row quarantine

A row-level assertion can move its failing rows aside instead of only reporting a count. Configure quarantine on a `quality` pipeline. Rocky ignores the block on any other pipeline type:

```toml
[pipeline.silver.checks.quarantine]
enabled = true
mode = "split"   # or "tag" or "drop"
```

| Mode | Behavior |
|---|---|
| `split` | Rocky materializes two new tables: `<target>__valid` with the passing rows and `<target>__quarantine` with the failing rows (plus per-assertion `_error_<name>` label columns marking which assertion each row failed). When the run completes, and the two suffixes name two different tables in your warehouse, each row lands in exactly one of them. The original `<target>` is left untouched; point downstream models at `<target>__valid`. Not available on Trino. |
| `tag` | Rocky rewrites `<target>` in place, adding a per-assertion `_error_<name>` column populated on failing rows (NULL on passing rows). Every row stays in the table. Useful for observation without a second table — rewrites the source, so use with care on a raw replication target. |
| `drop` | Only `<target>__valid` (the passing rows) is written; failing rows are discarded. Quarantine count is still reported in `check_results[]`. |

Set-based, table-level, and referential assertions are never quarantinable. They run as after-the-fact checks whatever the mode.

Rocky builds the quarantine predicate from every quarantinable assertion, combined with AND. A filter composes into it as `CASE WHEN (filter) THEN base_valid_pred ELSE TRUE END`. An out-of-scope row therefore stays on the valid side, even when the base predicate would fail it.

`split` evaluates that predicate once. It writes the source rows, plus one label column per assertion, to a new table in the source's schema named `_quarantine_labels_<token>`. It builds `__valid` and `__quarantine` from those labels, then drops the label table. The token is a random UUID for each run, so two runs never share a label table. A run killed between those statements leaves the label table behind. Two runs of one pipeline at the same time can still overwrite each other's `__valid` and `__quarantine` tables.

`__valid` keeps exactly the source's columns. Rocky writes it with `SELECT * EXCLUDE (...)` on DuckDB and Snowflake, and `SELECT * EXCEPT (...)` on Databricks and BigQuery. A dialect with no such form refuses `split` with a `quarantine:compile` check. Trino is one, and so is any adapter whose dialect does not provide it.

A quarantine that fails or is refused fails the run, whatever `fail_on_error` says. The table counts in `tables_failed` and is listed in `errors`. What the failure leaves behind depends on when it happened:

- **Refused** (`quarantine:compile`): nothing runs, so no table is written. Downstream reads the previous run's rows.
- **Failed at the warehouse** (`quarantine:execute`): the check names the statement's role and the warehouse error. `split` runs its statements in order: the label table, then `__quarantine`, then `__valid`, then the drop. Statements that already finished keep what they wrote, so `__quarantine` can be new while `__valid` is the previous run's. Rocky still tries to drop the label table after a failure, so it is normally gone. A failed drop leaves it behind. A statement that times out may still have committed.

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

Give exactly one of `keys` (a column tuple) or `key_expr` (a derived SQL expression). This mirrors `unique` and `unique_expr`, and `key_expr` goes through the same gate, including the two key-position rules under [Per-assertion `filter`](#per-assertion-filter).

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
