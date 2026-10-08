---
title: The Rocky Compiler
description: The type system, the column lineage graph, and the stages a compile runs through.
sidebar:
  order: 7
---

Rocky ships a real compiler, in the `rocky-compiler` crate. It analyses SQL
models before a later run sends generated SQL to the warehouse. It reports type
mismatches, missing columns, contract violations, and lineage findings when the
needed information is available.

## What a clean compile means

A clean compile means the enabled checks found no error in the information they
could resolve. It does not establish general warehouse SQL validity, source data
values, or business correctness. Run the relevant data checks and inspect a
plan before execution.

## Compile pipeline

The compiler runs a fixed sequence of stages. The first five do the core work:
load, resolve, build the graph, type-check, validate contracts. Three lint passes
run after that. Then Rocky merges every diagnostic into one result.

```
 ┌──────────────────────────────────────┐
 │  .sql files   .toml sidecars         │
 │  .rocky files  contracts/*.toml      │
 └───────────────────┬──────────────────┘
                     │
          ┌──────────▼──────────┐
          │   1. Load models    │   parse SQL + TOML from disk
          └──────────┬──────────┘
                     │
          ┌──────────▼──────────┐
          │ 2. Resolve deps     │   bare name → DAG edge
          │    (build DAG)      │   schema.table → external ref
          └──────────┬──────────┘
                     │
          ┌──────────▼──────────┐
          │ 3. Semantic graph   │   track column lineage across DAG
          │    (column lineage) │   a.id ──Direct──▶ b.id
          └──────────┬──────────┘
                     │
          ┌──────────▼──────────┐
          │  4. Type check      │   propagate types through graph
          │                     │   INT + FLOAT → FLOAT
          │                     │   String + INT → Unknown
          └──────────┬──────────┘
                     │
          ┌──────────▼──────────┐
          │ 5. Validate         │   required columns present?
          │    contracts        │   types match declarations?
          └──────────┬──────────┘
                     │
          ┌──────────▼──────────┐
          │ 6. Lint passes      │   blast-radius (P002),
          │    + merge          │   classification (W004),
          │                     │   freshness (W005)
          └──────────┬──────────┘
                     │
          ┌──────────▼──────────┐
          │  CompileResult      │   models, diagnostics,
          │                     │   semantic_graph, timings
          └─────────────────────┘
```

### 1. Load models

Rocky loads the model files from the models directory. A model is a `.sql` file
holding the transformation, plus a `.toml` sidecar holding its configuration:
name, target, strategy, intent.

A `.rocky` DSL file takes one extra step first. `lower_to_sql()` in the
`rocky-lang` crate lowers the DSL to a SQL string. From there the model follows
exactly the same path as a hand-written `.sql` file. Raw SQL and the DSL are two
front ends onto one pipeline.

### 2. Resolve dependencies

The resolver reads each model's SQL, pulls out the table references, and sorts
them into three kinds:

- A **bare name** becomes a DAG edge to the model whose `[target]` table has that name. For example, `FROM orders` depends on the model that writes `orders`. The model's own name does not matter: a bare name has no schema, so the warehouse finds it by table name, and `rocky test` runs each model at its configured target and finds it the same way. When several models write `orders`, the one named `orders` wins, then the one in `depends_on`. Otherwise the read is refused as `E056`. An `ephemeral` model writes nothing, so it is read by its name.
- A **two-part name** such as `schema.table` is an external source reference.
- A **three-part name** such as `catalog.schema.table` is a fully qualified external reference.

Rocky merges any explicit `depends_on` entries from the model config with the
dependencies it resolved. It then drops self-references and duplicates.

### 3. Build semantic graph

Rocky walks the models in topological order. For each one it pulls column-level
lineage out of the SQL AST. It resolves table aliases to real model or source
names. It expands `SELECT *` against the upstream schemas.

The result is a `SemanticGraph`: per-model schemas, upstream and downstream
relationships, and cross-model lineage edges. The [Semantic graph](#semantic-graph)
section below covers it in detail.

### 4. Type check

The type checker pushes inferred types through the semantic graph. It walks the
SQL AST expressions to find problems.

It infers types from:

- `CAST` expressions
- Aggregation functions (`SUM`, `COUNT`, `AVG`, etc.)
- Arithmetic operators (numeric promotion rules)
- Literals (string, numeric, boolean, date)
- `CASE`/`WHEN` branches (common supertype)
- Comparison operators (infer a Boolean result; see the operand checks below)
- Join keys (can report compatible-type problems when types are known)

Each compiled model schema contains `TypedColumn` entries with a name, a
`RockyType`, and a nullability flag. A type can remain `Unknown`.

Outer joins can introduce nulls even when source columns are non-nullable.
Rocky marks direct references and ordinary casts of those references from the right side of `LEFT JOIN` as nullable.
`RIGHT JOIN` marks the accumulated left side; `FULL JOIN` marks both sides.
Aliases remain distinct in self-joins, and downstream models inherit the resulting nullability.
A `nullable = false` contract on an affected column with a known type raises `E012`.
This analysis is conservative: a later `WHERE` filter does not narrow nullability.

A cast keeps its operand's nullability only when the operand is a column.
A cast of a computed value takes that value's nullability: `CAST(MAX(x) AS BIGINT)` and `CAST(NULLIF(x, 0) AS INT)` are nullable even when `x` is not.
An aggregate other than `COUNT`, `NULLIF`, a `CASE` with no `ELSE`, a division, a modulo and any function Rocky does not model are nullable.
`COUNT(...)`, including `COUNT(*)`, is a non-null `Int64`.
An aggregate over a computed argument takes its type from that argument only when the argument's type comes from the SQL itself (a cast target): `SUM(CAST(y AS DOUBLE))` is `Float64`. Otherwise it stays `Unknown`: `MAX(LENGTH(n))` is `Unknown`, because the width of `LENGTH` differs between warehouses. `SUM` or `AVG` over a cast to `DECIMAL` is also `Unknown`, because each warehouse widens the precision differently.

For `USING` and `NATURAL` joins, Rocky distinguishes merged join keys from qualified references to either input.

#### Aggregate and comparison operands

`rocky compile` also checks two kinds of operands against the target warehouse:

- **Aggregate arguments.** `SUM(customer_name)` over a `VARCHAR` column has no
  overload on DuckDB, BigQuery, Trino, SQL Server, PostgreSQL or Redshift.
  Rocky reports `E042`. Snowflake and Databricks cast the text at run time
  instead, so there it is `W042`.
- **Comparison operands.** This covers `=`, `<>`, `<`, `>`, `<=`, `>=`, `IN`,
  `BETWEEN` and join `ON` predicates. A `BIGINT` column compared with a
  `VARCHAR` column casts the text on every row on DuckDB, Snowflake,
  Databricks, SQL Server and Redshift. The query fails on the first value that does not parse, so Rocky
  reports `W043`. BigQuery, Trino and PostgreSQL refuse the pair outright: `E043`.

The warehouse comes from, in order: `--target-dialect`, the adapter `type` of
the warehouse each model runs on, then `[portability] target_dialect`. A model
runs on the target adapter of each transformation pipeline whose `models` glob
loads it. A model no pipeline loads runs on every pipeline's target. When a
model has several targets, the strictest verdict wins. With none of these,
Rocky reports the mildest verdict across all warehouses. That is always a
warning. ClickHouse has no operand rules yet; the message says so.

These stay clean, so valid SQL is never refused:

- An operand whose type Rocky does not know.
- A string literal that parses as a number, such as `10::BIGINT = '10'::VARCHAR`.
- A string literal compared with a date, such as `order_date >= '2024-01-01'`.
- `DATE` compared with `TIMESTAMP`, and numbers of different widths.
- `MIN`, `MAX`, `COUNT(*)` and `COUNT(DISTINCT x)` over any type.

A same-named join key whose type differs between two upstream models is still
reported once, as `E001` or `W001`.

To fail the compile on the warnings, run
`rocky compile --deny-warnings W042,W043`.

### 5. Validate contracts

If a contracts directory exists, Rocky loads the `.contract.toml` files and
checks resolvable facts against the inferred schemas. The
[Testing and Contracts](/concepts/testing) page has the contract format.

### 6. Lint passes and merge

Three lint passes always run after contract validation, against the typed models:

- The blast-radius lint (`P002`) flags a `SELECT *` model whose downstream consumers read specific columns.
- The classification-tag check (`W004`) flags a `[classification]` tag with no matching `[mask]` strategy.
- The freshness-coverage check (`W005`) flags a model that has temporal columns but no `freshness` declaration in scope.

Rocky merges their diagnostics with the type-checker and contract diagnostics
into the final `CompileResult`.

## The type system

`RockyType` is Rocky's one type representation. Every warehouse type maps to and
from `RockyType` through a `TypeMapper` trait, so the compiler behaves the same
whichever warehouse you target.

### Variants

| Category | Types |
|----------|-------|
| Numeric | `Boolean`, `Int32`, `Int64`, `Float32`, `Float64`, `Decimal { precision, scale }` |
| String | `String` |
| Temporal | `Date`, `Timestamp`, `TimestampNtz` |
| Binary | `Binary` |
| Complex | `Array(T)`, `Map(K, V)`, `Struct(fields)` |
| Semi-structured | `Variant` |
| Unresolved | `Unknown` |

`Unknown` is not an error. It means the compiler could not infer a type from
what it had. A missing reference and unsupported inference can both lead to
`Unknown`. `Unknown` is compatible with every other type during type checking,
so checks that need the type can remain unresolved. A declared type does not
validate an unresolved reference.

Rocky reports `E039` for one bounded missing-reference shape. The consumer must
directly project a name from one complete in-project model. The name must be
absent from that model's output. Other shapes can remain `Unknown`. `E039`
does not validate them.

`E039` covers in-project models only. Incomplete scopes, duplicate output
names, struct field reads, and warehouse metadata columns remain conservative.
The upstream output must use plain column projections or aliased columns and
literals. Functions and other expressions remain conservative.

### Missing columns in external sources (`E041` / `W041`)

An external source is a table such as `raw.orders` that Rocky reads but does
not build. Rocky knows its columns only from a source schema: a seed file
(`rocky compile --with-seed`) or the schema cache. A reference to a column the
source schema lacks is `E041` or `W041`. The code depends on how much Rocky
trusts that schema:

| Where the schema came from | Without strict sources | With strict sources |
|---|---|---|
| Read from the warehouse during this invocation (an embedding caller; no CLI command does this for a compile yet) | `E041` (error) | `E041` |
| Schema cache, younger than `trusted_max_age_seconds` | `E041` (error) | `E041` |
| Schema cache, older (or the key is unset) | `W041` (warning) | `E041` |
| Seed file (`--with-seed`) | `W041` (warning) | `E041` |
| Unknown | nothing (`Unknown`) | nothing |

A seed or an old cache entry can miss a column the warehouse already has. So
by default these schemas only warn, and the compile exits `0`. Turn on strict
sources with `rocky compile --strict-sources` or
`[cache.schemas] strict_sources = true`. Every `W041` then becomes `E041`.
Both codes name the column and the source. They suggest close column names,
or list the source's columns. `W041` also says how to refresh the schema.

Rocky reports the name only when it binds to known sources and nothing else.
Every relation the name could resolve against must be a known source. That
includes enclosing scopes, for correlated and lateral subqueries. These keep
the name `Unknown`:

- A CTE, derived table, in-project model, or table function in scope.
- A `SELECT` alias with that name, including DuckDB lateral aliases.
- A relation binding with that name (a whole-row reference).
- A qualified `a.b` where `a` can also be a column (a struct field read).
- A 3-part reference, a lambda parameter, or a keyword-like function
  argument such as `day` in `DATEADD(day, 1, ts)`.
- A quoted name. BigQuery, and Databricks by default, read `"shipped"` as a
  string, not a column.
- A name that starts with `_`. Warehouses use these for metadata columns,
  such as BigQuery `_FILE_NAME`.

`rocky run` compiles against the schema cache before it executes. An `E041`
model is excluded like any model with an error, before Rocky touches the
warehouse. A `W041` is logged as a warning and the model runs.

During `rocky run`, a selected model with an `Error` diagnostic records a
`compile-error`. Rocky withholds that model's declared DAG descendants. Healthy
branches can still run. Retained target tables are old output, not validated
output. `RunOutput.contained` lists the withheld descendants.

With `rocky run --model <name> --defer`, a successful external rewrite
suppresses local `E039` only for the rewritten reference. A qualified local
reference or an unrewritten input remains local and keeps its normal blocking
rules. The exemption requires a complete plain `SELECT` `FROM` or `JOIN` read
set. CTEs, subqueries, and set operations keep local failure dependencies.
An invalid external schema still fails when the warehouse runs the SQL.

### GROUP BY validity

Rocky reports `E044` when an aggregating query reads a column that is not
grouped. The check covers the `SELECT` list, `HAVING`, and `ORDER BY`. A query
aggregates when it has `GROUP BY`, `HAVING`, or an aggregate in its `SELECT`
list. Without `GROUP BY`, every column read outside an aggregate is reported.

```sql
-- E044: column 'status' in the SELECT list is neither in GROUP BY nor inside an aggregate
SELECT customer_id, status, SUM(amount) AS t
FROM raw.orders
GROUP BY customer_id
```

Fix it by adding the column to `GROUP BY`, or by wrapping it in an aggregate
such as `ANY_VALUE(status)`.

PostgreSQL accepts a column outside `GROUP BY` when it depends on a grouped
primary key. Rocky cannot see primary keys. So when every warehouse a model
runs on is PostgreSQL, the finding is the warning `W044`, not `E044`.
Redshift has no such rule and keeps `E044`.

`E044` fires only when Rocky is certain. The column must belong to a relation
in the same query whose columns Rocky knows: an upstream model, a source
schema from `--with-seed` or the schema cache, a CTE, or a subquery in `FROM`.
These shapes stay silent:

- `GROUP BY ALL`, `ROLLUP`, `CUBE`, and `GROUPING SETS` columns.
- Grouping by ordinal (`GROUP BY 1`) or by a `SELECT` alias.
- Any name that is also a `SELECT` alias, such as a lateral column alias.
- Expressions built only from grouped columns, such as `UPPER(status)`.
- A column inside a grouped expression. `order_date` is accepted when
  `DATE_TRUNC('month', order_date)` is grouped.
- Arguments of aggregates, `FILTER`, and unknown functions. An unknown
  function may be a user-defined aggregate.
- `QUALIFY`, and outer references inside a subquery.
- Names Rocky cannot place: unknown relations, stale schemas, session
  variables.

Each subquery and CTE is checked as its own query.

### Numeric promotion

When two numeric types meet in one expression (arithmetic, `COALESCE`, `CASE`,
`UNION`), the compiler works out a common supertype:

- `Int32` widens to `Int64`
- `Float32` widens to `Float64`
- An integer widens to `Float64` when mixed with a float
- An integer widens to `Decimal` when mixed with a decimal, with the precision adjusted
- Two decimals take the larger precision and the larger scale
- `Timestamp` and `TimestampNtz` resolve to `Timestamp`

Types that cannot mix, such as `String` and `Int64`, produce an error diagnostic.

### Assignability

The `is_assignable` function decides whether a value of one type can be written
into a column of another. It allows a widening conversion, such as `Int32` into
`Int64`. It rejects a narrowing conversion, such as `Int64` into `Int32`.

## Semantic graph

The semantic graph is a cross-model map of represented column lineage. It records
where resolved columns came from and how they were transformed across the graph.

```
raw_orders                  orders_enriched              orders_summary
──────────                  ───────────────              ──────────────
order_id  ──[Direct]──────▶ order_id  ──[Direct]───────▶ order_id
amount    ──[Cast:DECIMAL]─▶ amount   ──[Agg:SUM]────────▶ total
customer_id──[Direct]──────▶ customer_id
                            region    ◀──[Direct]── raw_customers.region
```

Rocky builds the graph in topological order. A downstream model always sees the
full column list of its upstreams, including anything a `SELECT *` expanded to.

Four compiler features sit on top of the graph.

**Column lineage tracing.** Take any output column in any model and trace it
backward to the source columns it came from. The `trace_column` method walks
lineage edges recursively:

```
c.id → b.id → a.id → source.raw.users.id
```

**Transform tracking.** Each lineage edge records how the column changed:

- `Direct` — the column passed through unchanged
- `Cast` — an explicit type cast
- `Expression` — derived from an expression
- `Aggregation` — the result of an aggregate function

**Star expansion.** When a model uses `SELECT *`, the compiler expands it against
the upstream model's inferred schema, or against a known source schema. That is
why downstream models still see the full column list through a star select.

**Intent propagation.** Rocky stores each model's `intent` field, from its TOML
config, in the semantic graph. The AI features (`ai-sync`, `ai-explain`) read it
from there.

## Diagnostics

Every compiler finding is structured. It carries a code, a severity, a source
span, and sometimes a suggested fix.

### Severity levels

- **Error** — compilation cannot continue. The model has a definite problem.
- **Warning** — something looks wrong, but it does not block.
- **Info** — informational, usually about a limit in type inference.

### Diagnostic codes

| Code | Meaning |
|------|---------|
| `E001` | Type-checking error (unresolved reference, type mismatch) |
| `E010` | Required column missing from model output |
| `E011` | Column type mismatch against contract |
| `E012` | Nullability violation against contract |
| `E013` | Protected column removed |
| `E020`--`E026` | `time_interval` validation (`@start_date`/`@end_date` placeholders, `time_column` presence/type/nullability/granularity) |
| `E027` | Budget exceeded -- projected spend over the model's `[budget]` ceiling |
| `E028` | Required run variable (`@var(name)`) referenced but no `--var` supplied and no inline default |
| `E030` | Imported producer dropped a column this project reads (cross-team contract) |
| `E031` | Imported producer narrowed the type of a column this project reads (cross-team contract) |
| `E032` | Imported producer tightened a column this project reads from nullable to NOT NULL (cross-team contract) |
| `E033` | Imported snapshot's recipe hash does not match the configured `pin` |
| `E034` | Imported snapshot declares a format version newer than this build of rocky can read |
| `E035` | Managed-Iceberg `format_options` declares a combination the warehouse rejects (e.g. `partition_by` + `cluster_by`) |
| `E036` | Two or more models write the same target table |
| `E038` | An `ephemeral` model is used in a way inlining cannot serve: it declares `[[tests]]`, another model reads its nominal target by a qualified name, a consumer's SQL cannot be rewritten, or `rocky run --model` selects it directly |
| `E039` | A direct projection names a column absent from a complete in-project upstream model |
| `E040` | A `.rocky` string literal contains a backslash; use a `.sql` model with the target's own escaping |
| `E044` | An aggregating query reads a column that is neither in `GROUP BY` nor inside an aggregate |
| `E042` | Aggregate argument type has no overload on the target warehouse, such as `SUM(VARCHAR)` on DuckDB |
| `E043` | Comparison between types the target warehouse refuses, such as `INT64 = STRING` on BigQuery |
| `E041` | A direct reference names a column absent from an external source whose schema Rocky trusts. See [Missing columns in external sources](#missing-columns-in-external-sources-e041--w041) |
| `E051` | A [user-defined function](/concepts/user-defined-functions/) or a call to one is invalid: bad definition, Python language, wrong argument count, a certainly incompatible argument type, or a warehouse that cannot create functions (Trino, ClickHouse, SQL Server) |
| `E050` | A freshness declaration cannot be evaluated: no threshold, a bad duration, `error_after` shorter than `warn_after`, a bad `loaded_at_field` or `filter`, or a model `time_column` absent from a complete output |
| `E037` | A transformation model declares `type = "incremental"` with no `timestamp_column` (watermark), which would append every row again on each run. Declare the watermark and use `@incremental_filter`, or use `merge`, `delete_insert`, `time_interval` or `full_refresh` |
| `E046` | An `incremental` model's watermark filter has no safe place: no `@incremental_filter` and the watermark is not a provable passthrough column; or the watermark is not an output column or not a plain name; or `@incremental_filter` appears under another strategy |
| `E049` | A `type = "snapshot"` model has an invalid config: no `unique_key` or `strategy`, `timestamp` without `updated_at`, `check` without `check_cols`, a key or change column that is an expression, an `updated_at` or `check_cols` entry the model's explicit SELECT does not output, a key computed with `random()`/`uuid()`/`now()`, or an output column named like a snapshot metadata column |
| `E047` | A model reads a `private` model outside its ownership group, or a producer model that is not `public` (see [Model governance](/concepts/model-governance/)) |
| `E048` | A model-version problem: undeclared latest version, missing version file, or a reference to an undeclared version |
| `E052` | A model's `[redshift]` table options cannot render (an invalid or contradictory `dist_key` / `sort_key`), or sit on a strategy that builds no table. See [Redshift](/reference/adapters/redshift/#table-distribution-and-sort-keys) |
| `E053` | ClickHouse cannot run the model as configured: its `[clickhouse]` table options cannot render or sit on a strategy that builds no table, or it is a `merge` model (or `incremental` with `unique_key`) and a warehouse the model runs on is ClickHouse, which has no `MERGE`. See [ClickHouse](/reference/adapters/clickhouse/#strategies) |
| `E054` | SQL Server cannot run the model's SQL: its CTEs cannot be lifted to the start of the statement, even after Rocky renames colliding nested CTEs. Emitted when a warehouse the model runs on is SQL Server. See [SQL Server](/reference/adapters/sqlserver/) |
| `E055` | `rocky package` refused to vendor a dbt package: a bad spec, `dbt` not on `PATH`, an adapter with no dbt profile mapping, a failed `dbt deps` or `dbt compile`, a compile that came out wrong without `--build-empty` (all-NULL columns or a placeholder `*`), a package model name or `[target]` table the project already uses (ignoring case), a package model that reads a seed or a model that was not vendored, vendored SQL that does not parse, Jinja or a credential-like name in a var, a dbt step past `--dbt-timeout`, or a `remove` that would delete edited files without `--force`. See [Use dbt packages](/guides/dbt-packages/) |
| `E056` | A bare read (`FROM orders`, no schema) is ambiguous: several models write a table called `orders`, none is also named `orders`, and the reader's `depends_on` does not pick one. Rocky binds a bare read by the table a model writes, not by its name, and does not guess between candidates. Qualify the read with its schema or list the intended model in `depends_on` |
| `W001` | Unused model (no downstream consumers) |
| `W002` | Duplicate column in model output |
| `W004` | Classification tag with no matching `[mask]` strategy |
| `W005` | Temporal column present but no `freshness` declaration in scope |
| `W006` | `merge` strategy declares a `unique_key` column the model does not output |
| `W050` | A freshness `loaded_at_field` / `time_column` is not a date or time type, or a source `loaded_at_field` is missing from the known source schema |
| `W010` | Contract defines a column not in model output (not required) |
| `W011` | Contract exists for a model not found in the project |
| `W012` | An `[imports.<name>]` snapshot could not be loaded; `E030`/`E033` checks skipped |
| `W013` | `rocky.toml` is present but could not be read, so every project-level check is silent (`rocky lsp` and `rocky serve` only; one-shot commands refuse instead) |
| `W030` | Imported producer added a column, surfaced only to consumers reading it via `SELECT *` |
| `W031` | Imported producer widened the type of a column this project reads (cross-team contract) |
| `W042` | Aggregate argument is cast implicitly at run time and fails on values that do not convert (escalate with `--deny-warnings W042`) |
| `W043` | Comparison relies on an implicit cast that fails on values that do not convert, such as a `BIGINT` column compared with a `VARCHAR` column on DuckDB (escalate with `--deny-warnings W043`) |
| `W044` | `E044`'s finding on a model that runs only on PostgreSQL, which accepts a column that depends on a grouped primary key (escalate with `--deny-warnings W044`) |
| `W041` | A direct reference names a column absent from an external source schema that may be out of date (seed or old cache entry) |
| `W051` | A user-defined function call could not be fully verified: an unknown argument type, or an argument the warehouse must convert implicitly |
| `W046` | An `incremental` model sets `lookback` without `unique_key`, so the re-read window is appended again on each run |
| `W056` | An `incremental` model sets no `lookback`, so a late row whose timestamp equals the target's `MAX` watermark is never loaded. `unique_key` alone does not fix this: it merges only the rows the filter reads |
| `W049` | A `type = "snapshot"` model is valid but risky: a `unique_key` the SELECT does not output (it may be a `[[surrogate_key]]` column), `check` over more than 20 columns, an `updated_at` that is not a timestamp or date, or a key or change column missing from a `SELECT *` model's compile-time schema (which may be stale) |
| `W048` | A model reads a model version whose `deprecation_date` has passed or is less than 30 days away |
| `W052` | A `[redshift]` `dist_key` or `sort_key` column is not in the model's output |
| `W053` | A `[clickhouse]` `order_by` or `partition_by` column is not in the model's output |
| `W055` | `rocky package` vendored a package with something to review: an edited file the new version changed (written beside it as `.incoming`), a package model it could not vendor, an incremental model that fell back to full refresh, or dbt tests it did not map |
| `I001` | Model dependency inferred from SQL |
| `I002` | Some, but not all, output columns have unknown types — provide source schemas for more type checking |
| `I003` | A contract declares a type for a column whose type Rocky could not infer, so `E011` did not check it |
| `P001` | Construct not portable to the target dialect (opt-in via `--target-dialect`) |
| `P002` | `SELECT *` model has downstream consumers that read specific columns |

### Format

Diagnostics render in a format modelled on `rustc`:

```
error[E011]: column 'id' type mismatch: contract expects Int64, got String
 --> models/orders.sql:3:8
 = help: add CAST(id AS BIGINT) to fix the type
```

Each diagnostic carries:
- **code** — a machine-readable identifier, for filtering and suppression
- **message** — a description you can read
- **span** — the file, line, and column, when Rocky knows them
- **model** — which model the diagnostic belongs to
- **suggestion** — an actionable fix, when the compiler can work one out

## Reference tracking

The type checker builds a `ReferenceMap` as it runs. That map records three
things:

- Where each model is referenced in `FROM` and `JOIN` clauses across the project
- Where each column is referenced
- Where each model is defined

This is what powers Find References and Rename Symbol when Rocky runs as an LSP
server.

## Using the compiler

### CLI

```bash
# Compile all models
rocky compile --models models/

# Compile with contracts
rocky compile --models models/ --contracts contracts/
```

### Programmatic

```rust
use rocky_compiler::compile::{compile, CompilerConfig};

let config = CompilerConfig {
    models_dir: "models/".into(),
    contracts_dir: Some("contracts/".into()),
    ..Default::default()
};

let result = compile(&config)?;

if result.has_errors {
    for d in &result.diagnostics {
        eprintln!("{d}");
    }
}
```

`CompileResult` gives you the resolved project, semantic graph, inferred
schemas, and diagnostics. Types can remain `Unknown`. The test runner, CI
pipeline, and AI sync all build on it.
