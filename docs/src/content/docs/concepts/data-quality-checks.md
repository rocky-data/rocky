---
title: Checks and Tests
description: Which check runs where, what it costs, and how to configure contracts, unit tests, declarative tests, and the inline checks of a run.
sidebar:
  order: 11
---

Rocky checks a model at four points. Each point runs different checks and has a different cost. Start with this table.

| Check | Config | Runs at | Needs | Cost |
|---|---|---|---|---|
| [Contract](#data-contracts) | `{model}.contract.toml` | `rocky compile` | Nothing | Compares the inferred schema. Reads no rows. |
| [Model execution](#rocky-test) | None | `rocky test`, `rocky ci` | Nothing | Runs each model on in-memory DuckDB. |
| [Unit test](#unit-tests-test) | `[[test]]` | `rocky test` | Nothing | Runs one model on DuckDB per test, with your fixtures. |
| [Declarative test](#declarative-tests-tests) | `[[tests]]` | `rocky test --declarative` | `rocky.toml` and a reachable warehouse | One SQL query per assertion. `--batch-tests` groups `unique` and `not_null` per model. |
| [Pipeline-level check](#pipeline-level-checks) | `[pipeline.<name>.checks]` | `rocky run`, `rocky apply` | The warehouse | A few queries per table. See each check. |
| [Assertion](#assertions-in-a-run) | `[[assertions]]` | `rocky run`, `rocky apply` | The warehouse | One SQL query per assertion. |
| [Cross-source overlap](#cross-source-overlap) | `checks.cross_source_overlap` | `rocky run` | The warehouse | One `UNION ALL` query per sibling group. |

```
  rocky compile ──► contracts          E010 E011 E012 E013 E014
        │            an error withholds the model and its downstream
        ▼
  rocky test ─────► model execution + [[test]] unit tests     (DuckDB, local)
  rocky test --declarative ──► [[tests]]                      (your warehouse)
        │
  rocky ci = compile + model execution, in that order
        │
        ▼
  rocky run / apply ► pipeline-level checks, then [[assertions]]
                      both write check_results[], one shape
```

![rocky compile surfaces E010 and E013 contract diagnostic codes on a broken model](/demo-data-contracts.gif)

Two blocks differ by one letter. Do not mix them up.

| Block | Where | What it checks | Runs |
|---|---|---|---|
| `[[test]]` (singular) | model sidecar | Model logic against fixtures you supply | Locally on DuckDB, with `rocky test` |
| `[[tests]]` (plural) | model sidecar | Rows already in the warehouse | `rocky test --declarative`, against the warehouse |
| `[[assertions]]` | `[pipeline.<name>.checks]` | Rows already in the warehouse | Inline during `rocky run` and `rocky apply` |

## Data contracts

A data contract declares what a model's output schema must look like. The compiler compares the schema it inferred against the contract. It catches a missing column, a wrong type, and a nullability violation before any row is written.

A contract is a `{model_name}.contract.toml` file. The compiler reads it from next to the model file, or from the project `contracts/` directory beside the models directory (`--contracts <DIR>` replaces it). A file in the directory wins. Example:

```toml
# orders_summary.contract.toml
[[columns]]
name = "customer_id"
type = "Int64"
nullable = false
description = "Unique customer identifier"

[[columns]]
name = "total_revenue"
type = "Decimal"
nullable = false

[rules]
required = ["customer_id", "total_revenue"]
protected = ["customer_id"]
```

Each `[[columns]]` entry takes `name` (required), `type`, `nullable`, and `description`. If `nullable = false`, the column must be non-nullable. `description` is for readers and is not validated. Type names match the `RockyType` variants: `Boolean`, `Int32`, `Int64`, `Float32`, `Float64`, `Decimal`, `String`, `Binary`, `Date`, `Timestamp`, `TimestampNtz`, `Array`, `Map`, `Struct`, `Variant`.

| `[rules]` key | Effect |
|---|---|
| `required` | These columns must exist in the output. A missing one is `E010`. |
| `protected` | These columns must never be removed. A removed one is `E013`. |
| `no_new_nullable` | Off by default. When on, a nullable output column that `[[columns]]` does not declare is `E014`. A contract that sets it and declares no `[[columns]]` is also `E014`, because there is no baseline. |
| `no_extra_columns` | Off by default. When on, an output column that the contract does not name in `[[columns]]`, `required` or `protected` is `E063`. If Rocky cannot prove the model's column list (a `SELECT *`, or an expression with no alias), that is `E063` too, because it cannot show there is no extra column. |

| Code | Severity | Meaning |
|---|---|---|
| `E010` | Error | Required column missing from the model output |
| `E011` | Error | Column type mismatch |
| `E012` | Error | Nullability violation: the contract says non-nullable, the model says nullable |
| `E013` | Error | Protected column removed |
| `E014` | Error | Undeclared nullable column while `no_new_nullable` is on |
| `E062` | Error | The output repeats a name the contract constrains (in `[[columns]]`, `required` or `protected`). The contract checks one of the columns; the warehouse writes the other under another name (DuckDB: `orders_1`) |
| `E063` | Error | Undeclared output column, or an unprovable column list, while `no_extra_columns` is on |
| `W010` | Warning | The contract defines a column that is not in the output and is not required |
| `W011` | Warning | A contract exists for a model not found in the project |

### When Rocky cannot prove a column is never NULL

`nullable = false` is a compile-time proof. Rocky reads no rows. It passes only when the SQL shows the column is never NULL, for example a `COALESCE` with a value that is never NULL, or a column that is NOT NULL in a source schema Rocky has. A `WHERE col IS NOT NULL` filter does not count, because Rocky does not read the WHERE clause when it infers nullability.

When the data keeps the column non-null but the SQL cannot show it, check the data instead. Set `nullable = true` in the contract, and add a `not_null` test to the model sidecar:

```toml
# models/orders_summary.toml
[[tests]]
type = "not_null"
column = "customer_id"
```

With `rocky run --tests enforce`, or `[run] tests_on_run = "enforce"`, a NULL fails the model, and its downstream models do not build. `[run] tests_on_run` defaults to `enforce`, so a run enforces tests unless you set `report` or `off`. The test runs after the write, so the rows stay in the model's table. `rocky plan promote` refuses a branch when the model's newest `rocky run --branch` evidence is that failure. A later run that rebuilds the model without tests replaces that evidence, and promote then only warns. See [Tests during a run](/reference/commands/core-pipeline/#tests-during-a-run) and [Declarative tests](#declarative-tests-tests).

Two cases differ. A data-product model gets its contract from the product spec: edit the spec, not the generated contract (a spec column with `nullable = false` already lowers to a `not_null` test). A `content_addressed` model writes no warehouse table, so its `[[tests]]` do not run.

:::caution
Contract files ignore unknown keys. Rocky does not reject them.

- A misspelt `[rules]` key, such as `requierd`, is ignored. It enforces nothing, and no error tells you.
- A misspelt `name` key in a `[[columns]]` entry makes the whole contract fail to load.
- A misspelt `type` or `nullable` key is ignored. That column check does not run.

After you edit a contract, break the model on purpose and confirm the expected `E010` to `E014` code appears.
:::

A column can end up with type `Unknown`. The compiler could not infer its type, so no type check against a contract runs. Rocky reports the column as `I003` at info severity, which changes no exit code. To refuse such a column, run with `--strict-contracts` or set `[contracts] strict = true`. Each `I003` then becomes the error `E059`. [Who Enforces What](/concepts/capability-matrix/#contracts-columns-types-required-protected) says how to give the compiler the types it needs.

## rocky test

`rocky test` compiles your models and runs them on a local DuckDB. It needs no warehouse connection. It reports a pass or fail line per model. Flags and JSON: [`rocky test`](/reference/commands/modeling/#rocky-test).

:::note[Time-interval models under `rocky test`]
A `time_interval` model's SQL uses the `@start_date` and `@end_date` bounds. `rocky test` replaces them with the widest window the local engine holds: `'0001-01-01 00:00:00'` to `'9999-12-31 23:59:59'`. This applies to model execution and to `[[test]]` unit tests. `rocky ci` runs model execution only.

The end is exclusive, as in `rocky run`. A model that filters `< @end_date` drops a row at `9999-12-31 23:59:59` or later.
:::

### Unit tests (`[[test]]`)

A unit test feeds a model mocked input rows and asserts on the rows it produces. It never touches the warehouse. It runs on the default `rocky test` path.

A unit test lives in the model's `.toml` sidecar. It names the test, mocks inputs under `[[test.given]]`, and declares the output under `[test.expect]`:

```toml
# models/orders_summary.toml
[[test]]
name = "high_value_orders"
description = "Orders over $100 should be flagged as high value"

[[test.given]]
ref = "orders"
rows = [
    { id = 1, amount = 150.0, status = "completed" },
    { id = 2, amount = 50.0, status = "completed" },
]

[test.expect]
rows = [
    { id = 1, amount = 150.0, is_high_value = true },
]
```

The runner seeds DuckDB with each `[[test.given]]` as a table named after its `ref`. It runs the model's compiled SQL and compares the result to `[test.expect]`.

| Field | Required | Meaning |
|---|---|---|
| `name` | Yes | Test name, unique within the model |
| `description` | No | For readers |
| `[[test.given]]` | No | A mocked input. `ref` is the model or source name (it matches `from` / `depends_on`). `rows` is an inline list. Repeat the block for more inputs. |
| `[test.expect]` | Yes | `rows` is the expected rows. `ordered` is an optional boolean. |

- **Multiset by default.** Order does not matter. Duplicate counts do. Rocky runs `EXCEPT ALL` in both directions, so a missing row and an extra row are both reported.
- **`ordered = true`** compares rows by position, against the declaration order.
- **Only asserted columns are compared.** Extra columns in the model output are ignored.
- **Empty `rows`** asserts that the model produces no rows for the given inputs.

With any `[[test]]` block, the `--output json` payload gains a `unit_tests` object. A failed unit test fails the run with a non-zero exit code.

## Declarative tests (`[[tests]]`)

A declarative test asserts a fact about data already in the warehouse. It uses the same assertion kinds as the [pipeline assertions](#assertion-kinds) below. Write `[[tests]]` in a model sidecar:

```toml
# models/orders_summary.toml
[[tests]]
type = "not_null"
column = "customer_id"

[[tests]]
type = "accepted_values"
column = "status"
values = ["pending", "shipped", "delivered"]
severity = "warning"
```

Run them with `rocky test --declarative`. They execute on the configured warehouse adapter, not DuckDB, so they need a `rocky.toml` and a warehouse Rocky can reach. With no `--pipeline` and no `--models`, each transformation pipeline runs the tests of its own models, against the warehouse that pipeline targets. Rocky reports `pass`, `fail`, or `error`. A failed `error`-severity assertion (the default) exits non-zero. A `warning` reports without failing the run.

Add `--batch-tests` to send fewer queries. A model's default-config `unique` tests then share one query, and its `not_null` tests share another, when it has two or more of that type. The results are the same as without the flag.

## rocky ci

`rocky ci` is `rocky compile` plus model execution, in that order, for a CI system. It exits `1` on a type error, a contract violation or a model that fails locally. Warnings do not change the exit code. Flags, steps and JSON: [`rocky ci`](/reference/commands/modeling/#rocky-ci).

## Pipeline-level checks

Configure these per pipeline in `rocky.toml`. They run after each table is replicated.

```toml
[pipeline.bronze.checks]
enabled = true
row_count = true
column_match = true
freshness = { error_after = "24h" }
```

### Row count

Compares `COUNT(*)` between the source and target tables. The check passes when the counts match. Its result carries `source_count` and `target_count`.

On Databricks, Rocky batches row count and freshness with `UNION ALL`, up to 200 tables per query. A typical pipeline costs about 3 queries instead of 5N. Snowflake and BigQuery batch the schema describe only. Every other adapter runs one query per table.

### Anomaly detection

Compares this run's target row count against the table's recent history in the state store. A deviation above `anomaly_threshold_pct` (default 50) goes into the run's `anomalies` list. An anomaly is a warning, not a failed run. A row-count swing is often real business behavior.

The detector runs for a table only when row-count checks are on, at least one table in the batch was row-counted, `anomaly_threshold_pct` is above 0 (0 or less switches it off), the run has a state store with readable history, and this table's count was measured.

`rocky run` reports per table whether the detector looked, in `anomaly_evaluated` (`table`, `evaluated`, and `not_evaluated_reason` when false). Read it with `anomalies`:

```
anomalies: []  +  evaluated: true   ──►  the count is normal
anomalies: []  +  evaluated: false  ──►  nothing was measured; the reason says why
```

`not_evaluated_reason` names one missing condition, not all of them, because the remedies differ.

### Column match

Compares the source and target column sets, ignoring case. It reports each missing or extra column. Rocky reads both column lists again after the copy, so the check costs two extra metadata queries per table. It sees the schema the copy produced, including a column the run added.

A failed read is retried once, only when the adapter classifies the failure as retryable and it is not a rate limit. A rate limit, a permanent error, or a second failure reports the check as not evaluated.

### Freshness

Compares `MAX(timestamp_column)` against the current time. A table older than `error_after` fails at error severity. A table older than `warn_after` fails at warning severity. Both levels use a duration such as `"12h"` (see [Freshness durations](/reference/configuration/#freshness-durations)). The result carries `lag_seconds` and `threshold_seconds`. If freshness matters for a table, confirm its `timestamp_column` is populated.

Rocky does not always measure freshness:

| Case | Result |
|---|---|
| The table is empty | No freshness check. `MAX()` is `NULL` over no rows. |
| The table has rows but the timestamp column holds no value | A failed `freshness_not_evaluated` check. Its reason gives the row count. |
| The query fails, or returns a cell Rocky cannot read as a timestamp or a count | The same failed `freshness_not_evaluated` check. |
| The newest timestamp is more than 60 seconds in the future | A failed `freshness_not_evaluated` check, marked warning severity. Like every check that is not evaluated, it still fails the run while `fail_on_error` is on. Up to 60 seconds ahead counts as fresh (clock skew). |
| `prune_unchanged` is on, the adapter reports a source change-marker, and the source has not changed | No query. The table skips its data checks, freshness included. The check is absent from the JSON. [Schema drift](/concepts/schema-drift/) covers pruning. |

`prune_unchanged` is off by default. An absent check looks the same as a table with no `freshness` configured: it is absent, not failing.

Dagster fills the gap in `check_results` per its own rules. See [Check Results](/dagster/checks/).

### Null rate

Samples the table with `TABLESAMPLE (N PERCENT)` and works out the null percentage per column. It never scans the whole table.

```toml
null_rate = { columns = ["email"], threshold = 0.05 }
```

### Custom SQL

Your SQL, with a `{target}` placeholder that Rocky replaces at run time. The check passes if the query result is less than or equal to the threshold.

```toml
custom = [
  { name = "no_future_dates", sql = "SELECT COUNT(*) FROM {target} WHERE date > CURRENT_DATE()", threshold = 0 }
]
```

Rocky does not gate a custom `sql` query. It runs the query as written. Treat a custom check as code you run.

## Assertions in a run

An assertion is a repeated `[[assertions]]` block under `[pipeline.<name>.checks]`. The same block in a model sidecar is `[[tests]]`. Each block declares a `type`, and may declare `column`, `severity`, `filter`, and the parameters its type needs. The set matches the assertion surface of Databricks Labs' DQX.

```toml
[[pipeline.silver.checks.assertions]]
type = "not_null"
column = "order_id"

[[pipeline.silver.checks.assertions]]
type = "in_range"
column = "amount_cents"
min = "0"
max = "100000000"
filter = "region = 'US'"
```

### Assertion kinds

| Type | Level | Passes when |
|---|---|---|
| `not_null` | row | The column has no NULL. |
| `unique` | set | The column has only unique values. |
| `unique_expr` | set | A computed key is unique (`GROUP BY <expr> HAVING COUNT(*) > 1`). Use it when neither `unique` nor `composite` can express the identity. NULL keys are not excluded. Use `filter` to scope them out. |
| `accepted_values` | row | Every non-NULL value is in the set. |
| `relationships` | row | Every non-NULL value exists in `to_table.to_column`. |
| `expression` | row | The SQL boolean predicate holds per row. It is one expression over the row's own columns. It calls only allowlisted pure scalar functions. It has no subquery and no qualified function. |
| `row_count_range` | table | The row count is in the inclusive range. |
| `in_range` | row | Values are in the numeric range. NULLs pass. |
| `regex_match` | row | The column matches the dialect regex. NULLs pass. The pattern allows no single quote, backtick, or semicolon. |
| `aggregate` | table | `op(column) cmp value` is `TRUE`. `op` is `sum`, `count`, `avg`, `min`, or `max`. `cmp` is `lt`, `lte`, `gt`, `gte`, `eq`, or `ne` (or symbolic aliases). |
| `composite` | set | A column tuple is unique. It needs at least two columns. Use `unique` for one. |
| `not_in_future` | row | The timestamp is not in the future (`col <= CURRENT_TIMESTAMP()`). NULLs pass. |
| `older_than_n_days` | row | Every timestamp is at least `days` old. NULLs pass. |

Parameters per type: [Assertions](/reference/configuration/#pipelinenamechecksassertions).

Seven row-level kinds are quarantinable: `not_null`, `accepted_values`, `expression`, `in_range`, `regex_match`, `not_in_future`, `older_than_n_days`. See [Row quarantine](#row-quarantine). The set, table, and referential kinds (`unique`, `unique_expr`, `composite`, `row_count_range`, `aggregate`, `relationships`) run after the fact and cannot be quarantined. `relationships` needs a join, not a per-row predicate.

### Severity and `fail_on_error`

Each assertion takes `severity`: `error` (the default) or `warning`. Each pipeline takes `fail_on_error`, which defaults to `true`.

| Setting | Effect |
|---|---|
| `severity = "error"` and `fail_on_error = true` | A failing assertion fails the run. A replication run that already copied data exits `2` (partial success). A failed quality-pipeline gate exits `1`. |
| `severity = "warning"` | The result has `passed = false` and `severity = "warning"`. It never fails the pipeline. |
| `fail_on_error = false` | Every `error` becomes a non-fatal result. Use it for shadow runs and observation modes. |

A check whose query fails has `passed = false` and a `not_evaluated` field that says why. It always has `severity = "error"`, whatever the config says. It fails the run while `fail_on_error` is on. This holds for every kind: `row_count`, `column_match`, `freshness`, `null_rate`, `[[checks.custom]]`, `[[assertions]]`, `cross_source_overlap`, and a quarantine plan Rocky could not compile.

```
the query answers, the data is bad   -> your severity   (warning stays advisory)
the query never ran                  -> always error    (gates, unless
                                                         fail_on_error = false)
```

`severity = "warning"` on `freshness` means a stale table is only a warning. It does not make a freshness query that could not run a warning.

### Reusable named tests

To apply one assertion to many models, define it once in `models/test_definitions.toml`. A definition is any assertion `type` with its parameters and an optional default `column`:

```toml
# models/test_definitions.toml
[positive_amount]
type = "expression"
expression = "amount > 0"
```

A model applies it with a `[[use_test]]` block. At the use site you may set the column, the severity, and the filter. Inline `[[tests]]` and `[[use_test]]` sit side by side:

```toml
# models/fct_orders.toml
[[tests]]
type = "unique"
column = "order_id"

[[use_test]]
name = "positive_amount"
column = "amount"
severity = "warning"
```

A reference resolves into an ordinary assertion at load. A `[[use_test]]` that names no definition fails the load with an error, so a typo cannot silently drop a check.

### Per-assertion `filter`

Every assertion accepts a `filter`: a SQL boolean predicate that scopes the check. A row where `(filter)` is `TRUE` is checked. A row where it is `FALSE` or `NULL` passes.

```toml
[[assertions]]
type = "in_range"
column = "amount_cents"
min = "0"
filter = "region = 'US' AND status != 'cancelled'"
```

The filter is your SQL and must be valid in the target dialect. Rocky does not rewrite it. One gate checks every SQL fragment you supply, before anything runs:

- an assertion `filter` and `expression`;
- a `unique_expr` `key_expr` and a `cross_source_overlap` `key_expr`;
- the same `expression` and `filter` when quarantine lowers the assertion;
- a `metadata_columns[].value`;
- a check an agent drafts through the `draft_check` MCP tool, validated under the target adapter's dialect. With several pipelines, Rocky infers the dialect when they all agree. Otherwise pass `pipeline`.

The gate parses the fragment under the target dialect. It accepts exactly one expression over the row's own columns. It refuses:

| Refused | Reason |
|---|---|
| Anything after the one expression | The fragment cannot close Rocky's wrapping parenthesis and add clauses. |
| A subquery, in any position | |
| A qualified function name, such as `schema.fn(...)` | That is how user-defined, remote and plugin functions are reached. |
| A function not on the allowlist of pure scalar functions | `random()` and `uuid()` are not on it. DuckDB `read_text`, Snowflake `GETVARIABLE` and Databricks `secret` are refused by name. |
| A lambda | |
| A placeholder (`$name`, `$1`, `?`), or an unquoted `current_user`, `session_user`, `current_role`, `current_catalog`, `current_warehouse` | Each reads session state, not the row. |
| A `;` outside a string literal, a quoted identifier or a comment, including a trailing one | It could end the query Rocky builds. |
| An unbalanced quote, or a `/* */` comment that never closes | |
| A backslash in a quoted literal, a triple-quoted string, a `$$...$$` quote, a backtick, a `//` or `#` line comment, or a nested `/*` | Snowflake, Databricks, BigQuery, DuckDB and Trino lex these differently. Rocky refuses rather than guess. |

Comparisons, `CASE`, `CAST`, `BETWEEN`, `IN (...)` with literals, and `coalesce`, `nullif`, `abs`, `round`, `length`, `lower`, `upper`, `trim`, `regexp_like`, `md5` and `date_trunc` pass.

**The allowlist checks a function's name, not the code behind it.** A session can rebind a built-in under an unqualified call, and then the allowlisted call runs the rebound body. Rocky measured this on DuckDB v1.5.5 with a persistent file: the rebind wins, even for a new session. On Databricks (Unity Catalog) it does not, because only a qualified override exists and Rocky refuses qualified names. Snowflake, BigQuery and Trino are not probed. On a persistent DuckDB file the binding needs file-write access that already lets you edit the data. So the allowlist is not a boundary against that admin.

Six allowlisted names are refused in one shape, because that shape reads a Snowflake session parameter:

| Function | Refused | Accepted |
|---|---|---|
| `to_date`, `to_timestamp`, `to_char` | One argument, such as `to_date(order_date)`. It reads a session default format. | An explicit format, such as `to_date(order_date, 'YYYY-MM-DD')` |
| `date_trunc`, `datediff` (and `date_diff`) | A `week` part or synonym (`w`, `wk`, `weekofyear`, `woy`, `wy`). It reads `WEEK_START`. | Any other part, such as `day`, `month`, `year`, or the Monday-start `week_iso` |
| `date_part` | A `week` part or synonym reads `WEEK_START`. A `dayofweek` (`weekday`, `dow`, `dw`) or `yearofweek` part reads `WEEK_OF_YEAR_POLICY` and `WEEK_START`. | Any other part, including `dayofyear`, the `epoch_*` and `timezone_*` parts, and the ISO-fixed `week_iso`, `dayofweekiso`, `yearofweekiso` |

`EXTRACT(<part> FROM <expr>)` is refused on the same three part families. On Snowflake, `EXTRACT(<part>, <expr>)` is refused too.

Two positions add a rule. A `unique_expr` or `cross_source_overlap` `key_expr` takes no clock function (`now()`, `current_timestamp`) and no `COLLATE`: a changing value is not a key, and a collation changes what equality means. A quarantine `expression` or `filter`, `not_in_future` and `older_than_n_days` accept a clock function in every mode, because `split` evaluates each predicate once.

What a refusal does depends on the field:

| Field | A refusal |
|---|---|
| An assertion `filter` or `expression`, a `key_expr` | The check fails at error severity, with a `not_evaluated` reason naming the field, the table and the construct to remove. |
| A quarantine `expression` or `filter` | The check `quarantine:compile` fails the same way. No quarantine table is written. The run fails, whatever `fail_on_error` says. |
| `metadata_columns[].value` | The config load fails, so no command runs. |
| The `draft_check` MCP tool | The tool refuses the write. The bad check is never saved. |

### Row quarantine

A row-level assertion can move its failing rows aside instead of only counting them. Configure quarantine on a `quality` pipeline. Rocky ignores the block on any other pipeline type. `[checks.quarantine]` takes only `enabled`, `mode` and the two suffixes.

```toml
[pipeline.silver.checks.quarantine]
enabled = true
mode = "split"   # or "tag" or "drop"
```

| Mode | Behavior |
|---|---|
| `split` | Rocky writes `<target>__valid` (passing rows) and `<target>__quarantine` (failing rows, plus an `_error_<name>` label column per assertion). When the two suffixes name two different tables, each row lands in exactly one. `<target>` is untouched. Point downstream models at `<target>__valid`. Not available on Trino. |
| `tag` | Rocky rewrites `<target>` in place and adds an `_error_<name>` column per assertion. It is populated on failing rows and NULL on passing rows. Every row stays. It rewrites the source, so use it with care on a raw replication target. Not available on Trino. |
| `drop` | Rocky writes only `<target>__valid` and discards failing rows. The quarantine count is still in `check_results[]`. On Trino, Rocky drops the previous valid table before it creates the new one. |

Rocky builds the predicate from every quarantinable assertion, joined with AND. A filter composes as `CASE WHEN (filter) THEN base_valid_pred ELSE TRUE END`, so an out-of-scope row stays on the valid side. Set, table, and referential assertions always run after the fact, whatever the mode.

`split` evaluates the predicate once. It writes the source rows plus one label column per assertion to `_quarantine_labels_<token>` in the source's schema (the token is a random UUID per run). It builds `__valid` and `__quarantine` from those labels, then drops the label table. A run killed in between leaves the label table behind. Two runs of one pipeline at the same time can overwrite each other's outputs.

`__valid` keeps exactly the source's columns. Rocky writes it with `SELECT * EXCLUDE (...)` on DuckDB and Snowflake, and `SELECT * EXCEPT (...)` on Databricks and BigQuery. A dialect with no such form, Trino included, refuses `split` with a `quarantine:compile` check.

**Rocky never overwrites a column it cannot prove it wrote.** Before any statement runs, it compares each label name with the source's columns, ignoring case.

| Mode | Rule |
|---|---|
| `split` | Refuses any source column named like a label. It never writes its source, so the column is not Rocky's. |
| `tag` | After each successful run, Rocky records the label columns and the table's full column list in the state store. On the next run it replaces a recorded column only while the table has exactly the recorded columns. A table that was replaced, or gained or lost a column, has its label columns refused. |
| `tag`, state store lost | Nothing proves ownership, so `tag` refuses. This includes every existing `tag` table on its first run after an upgrade. Remove the column, and the next run writes and records it again. |

A refusal names the column and the label. Rename or remove the column, or give the assertion a `name` so its label is `_error_<name>`. Where the warehouse cannot drop or rename a column (a Databricks table without column mapping, say), rebuild the table without it.

Every name quarantine creates (suffixed tables and label columns) must fit in 255 characters. A longer name is refused with a `quarantine:compile` check before any statement runs.

A quarantine that fails or is refused fails the run, whatever `fail_on_error` says. The table counts in `tables_failed` and is listed in `errors`.

| Failure | What it leaves behind |
|---|---|
| Refused (`quarantine:compile`) | Nothing runs. No table is written. Downstream reads the previous run's rows. |
| Failed at the warehouse (`quarantine:execute`) | The check names the statement's role and the warehouse error. `split` runs the label table, `__quarantine`, `__valid`, then the drop. Finished statements keep what they wrote, so `__quarantine` can be new while `__valid` is old. A failed drop leaves the label table. A statement that times out may still have committed. |

### Output

Every assertion writes one `check_results[]` entry in the `rocky apply` JSON, named by the assertion's `name` or else `"{kind}:{column}"` (for example `not_null:order_id`, with `kind`, `column`, `failing_rows`, `passed` and `severity`). Pipeline-level checks share this shape. See [JSON Output](/reference/json-output/).

## Cross-source overlap

Assertions check one table at a time. They miss one duplication: the same business key arrives through two sources that are later `UNION`-ed into one target. Each source is unique, so every `unique` check passes. The target still double-counts every shared key. `cross_source_overlap` catches this.

```toml
[pipeline.bronze.checks.cross_source_overlap]
keys = ["order_id"]          # or: key_expr = "md5(a || '-' || b)"
severity = "warning"
max_overlap_rows = 0          # any overlap fails; raise to tolerate a known set
sample = 20                   # overlapping keys attached to the result
```

Give exactly one of `keys` (a column tuple) or `key_expr` (a SQL expression). `key_expr` goes through the same gate, including the key-position rules under [Per-assertion `filter`](#per-assertion-filter).

The runner groups the pipeline's managed source tables into **sibling groups**. Siblings share a source type and a table name, and landed in more than one target schema. Rocky tags each sibling's rows with its source, `UNION ALL`s them, groups by key, and keeps keys where `COUNT(DISTINCT _src) > 1`. One source's own duplicates never raise a flag.

Rocky skips a sibling table it cannot evaluate (the key column is missing, or the table is keyless). It logs the reason and does not fail the check. The result is named `cross_source_overlap:<source_type>.<table>` and carries `overlap_count`, `contributing_tables` and `sample` (see [JSON Output](/reference/json-output/)). Rocky counts how many siblings in a group carry the key:

```
0 siblings carry the key  -> not evaluated, error severity, fails   (a typo)
1 sibling carries it      -> passes, your severity, does not gate   (nothing to compare)
2 or more carry it        -> measured across those siblings, and it can fail
```

A group whose query failed, or whose key expression was refused, is an unevaluated failure like every other kind.

Rocky catches cross-source duplication at two points:

| Layer | Mechanism | Runs | Config |
|---|---|---|---|
| Preventive | `on_collision` | `rocky discover`, before a stray catalog is created | `[pipeline.NAME.source.discovery] on_collision` -> `collision_candidates` |
| Detective | `cross_source_overlap` | `rocky run`, after the sibling tables are written | `[pipeline.NAME.checks.cross_source_overlap]` |

The preventive layer needs an adapter that resolves external object ids, such as Fivetran. The detective layer works on any warehouse. See [discovery configuration](/reference/configuration/#pipelinenamesourcediscovery).

## AI-generated tests

`rocky ai-test` generates assertions from a model's intent and schema, each a SQL query that returns 0 rows when it holds. Rocky saves them to a `tests/` directory. See [AI and Intent](/concepts/ai-intent/).
