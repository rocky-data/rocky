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

#### Dependency cycles (`E058`)

A cycle is a set of models that depend on each other, so no model can run
first. For example, `fct_orders` reads `customer_ltv` in a `WHERE` sub-query,
and `customer_ltv` reads `fct_orders`:

```sql
-- fct_orders.sql. E058: fct_orders depends on customer_ltv, which depends on fct_orders
SELECT order_id, customer_id, amount
FROM raw.orders
WHERE customer_id IN (SELECT customer_id FROM customer_ltv)
```

`rocky compile`, `rocky test` and `rocky ci` report `E058` once for each model
on the cycle. Each diagnostic names the models on the cycle and points at the
line of the read that closes it. A dependency that only `depends_on` declares
has no line to point at. The JSON output is printed as for any other error,
and the command exits `1`. No later compile step runs, so a cycle hides other
diagnostics until you remove it.

`rocky run` refuses a project with a cycle before it writes anything.

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

A cast keeps its operand's nullability only when the operand is a column and the cast cannot fail.
A cast that can fail is nullable even over a `NOT NULL` input, because some warehouses return `NULL` for a value that does not convert (Spark and Databricks with ANSI mode off). These casts can fail: text or an unknown type to a number, boolean, date or timestamp; a number to a narrower number (`BIGINT` to `INT`, anything to `SMALLINT` or `TINYINT`, a number to a `DECIMAL` that may not hold it); a `DOUBLE` to `FLOAT`. These casts cannot fail and keep the operand's nullability: the same type, `INT` to `BIGINT`, a number to `DOUBLE` or `FLOAT`, anything to text, `DATE` to `TIMESTAMP`, and an integer literal that fits the target (`CAST(0 AS DECIMAL(18,2))`). `TRY_CAST` and `SAFE_CAST` are always nullable.
A `nullable = false` contract over `CAST(text_col AS INT)` therefore fails `E012`. Use `COALESCE` or a source that is already numeric.
A cast of a computed value takes that value's nullability: `CAST(MAX(x) AS BIGINT)` and `CAST(NULLIF(x, 0) AS INT)` are nullable even when `x` is not.
An aggregate other than `COUNT`, `NULLIF`, a `CASE` with no `ELSE`, a division, a modulo and any function Rocky does not model are nullable.
`COUNT(...)`, including `COUNT(*)`, is a non-null `Int64`.
A `UNION`, `INTERSECT` or `EXCEPT` is typed from all its branches, paired by position. The column name comes from the first branch. A column is non-null only if it is non-null in every branch, and its type is the common supertype of the branches (`Unknown` if there is none). If Rocky cannot type the query (for example branches with different column counts, or a `VALUES` branch), every column is nullable and computed columns are `Unknown`.
A cast of a column Rocky can type is the cast's target type: `CAST(o.quantity * p.price AS DECIMAL(12, 2))` is `Decimal(12, 2)`. A cast of a column Rocky cannot type takes its target type only for `BOOLEAN`, `DOUBLE` (`DOUBLE PRECISION`, `FLOAT64`), `DATE`, text types (`VARCHAR`, `CHAR`, `TEXT`, `STRING`), binary types (`BINARY`, `VARBINARY`, `BLOB`), and `DECIMAL` or `NUMERIC` with digits (`DECIMAL(p)` or `DECIMAL(p, s)`, `1 <= p <= 38`, `0 <= s <= p`); the column stays nullable. `INT`, `INTEGER`, `SMALLINT`, `TINYINT`, `BIGINT`, `FLOAT`, `REAL`, `TIMESTAMP`, a bare `DECIMAL` or `NUMERIC`, and `DECIMAL` digits out of that range stay `Unknown`, because their width differs by warehouse (Snowflake `BIGINT` and `INTEGER` are `NUMBER(38,0)`, `FLOAT` is 64-bit, `TIMESTAMP` is `TIMESTAMP_NTZ`).
A `CASE` or `COALESCE` takes a type when its branches agree exactly. Each branch must be a column, a cast, `COUNT`, a text or boolean literal, or `NULL`, and every branch must have the same type. `CASE WHEN price < 20 THEN 'budget' ELSE 'premium' END` is `String`, and `COALESCE(order_count, 0)` over a `BIGINT` is `Int64`. An integer literal counts only when it does not change the type, so `COALESCE(qty, 0)` over an `INT` stays `Unknown`. A branch with a fraction (`1.5`) or two branches of different widths also leave the result `Unknown`.
Date arithmetic is `Unknown`, because warehouses disagree on its type: `DATE - DATE` is a `BIGINT` in DuckDB and an interval in Databricks.
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
- **Dates against numbers.** `order_date > 5` compares a `DATE` or `TIMESTAMP`
  with a number. DuckDB, PostgreSQL, BigQuery and Trino have no such
  comparison, so Rocky reports `E043`. On Snowflake, Databricks, SQL Server and
  Redshift it is `W043`, because Rocky has not verified their rule.

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
- Date arithmetic, such as `order_date >= CURRENT_DATE - 5`. Rocky does not
  type it, so it is never compared with a number.
- `DATE` compared with `TIMESTAMP`, and numbers of different widths.
- `MIN`, `MAX`, `COUNT(*)` and `COUNT(DISTINCT x)` over any type.

A same-named join key whose type differs between two upstream models is still
reported once, as `E001` or `W001`.

To fail the compile on the warnings, run
`rocky compile --deny-warnings W042,W043`.

#### Unknown functions

Rocky reports a call to a function that the target warehouse does not have
and that the project does not declare in `functions/`:

```sql
-- E057: function `SUMM` does not exist in DuckDB and is not a project function in `functions/`
SELECT customer_id, SUMM(amount) AS lifetime_value FROM fct_orders GROUP BY customer_id
```

The message suggests close names (`did you mean sum?`). Each warehouse has a
list of its functions: aggregate, window and table functions, and common
aliases. How Rocky treats a miss depends on how the list was made:

| Warehouse | List | Checked against a live engine | Code |
|---|---|---|---|
| DuckDB | `duckdb_functions()` of DuckDB 1.5, plus functions its extensions load on first use | yes | `E057` (error) |
| PostgreSQL | The PostgreSQL 17 built-in function catalog (`pg_proc`), plus the SQL-standard forms it lacks | yes, PostgreSQL 17.11 | `W057`: extensions add functions |
| Snowflake | Vendor function reference | no, built from the docs | `W057` (warning) |
| Databricks | Vendor function reference and the Spark list | no, built from the docs | `W057` |
| Spark | `SHOW FUNCTIONS` of Spark 4.0.1 with Delta Lake 4.0.0 | yes | `W057`: UDFs and session extensions add functions |
| BigQuery | Vendor function reference | no, built from the docs | `W057` |
| Trino | `SHOW FUNCTIONS` of Trino 483, plus the names it hides (`version`, `format`, SQL special forms) | yes | `W057`: connectors add functions |
| Redshift | Vendor function reference and the PostgreSQL list | no, built from the docs | `W057` |
| SQL Server, ClickHouse | none yet | | not checked |

A false refusal of a real function costs more than a missed typo, so only
DuckDB gets the error `E057`. A list built from documentation can lag a
release, so Snowflake, Databricks, BigQuery and Redshift get the warning
`W057`. The PostgreSQL, Trino and Spark lists were checked live, but they hold
the built-in catalog of one version only: a PostgreSQL extension (PostGIS,
pgcrypto), a Trino connector, or a Spark UDF, `CREATE FUNCTION` or session
extension adds functions with plain names, so those three get `W057` too. A warning never fails
a compile alone. To fail on it, run `rocky compile --deny-warnings W057`. The
lists were built and checked on 2026-10-09. Each file under `engine/crates/rocky-compiler/src/data/` names its source.
A model that runs on several warehouses is checked against each. When
`[portability] target_dialect` names a warehouse, a model is checked only
against that warehouse.

These calls are never reported:

- A schema-qualified call, such as `main.my_macro(x)`. Use this form for a
  macro or an extension function that you load outside Rocky.
- A quoted function name.
- A function declared in `functions/`.
- A form the SQL parser or the warehouse rewrites, such as `COALESCE`, `IF` or
  `IFNULL`.

### 5. Validate contracts

Rocky loads the `.contract.toml` files and checks resolvable facts against
the inferred schemas. It reads a `<model>.contract.toml` next to a model file,
then the project `contracts/` directory beside the models directory. A
`--contracts <DIR>` flag replaces the project directory. A file in the
directory wins over a file next to the model.

One `contracts/` directory serves every pipeline. A compile skips a file
for a model it does not include. If that file does not parse, the compile
logs a warning and goes on. A file that does not parse for a model in the
compile is an error.

A `rocky plan` fingerprints the contract file of each model it covers, from
either place. If that file changes or is deleted before `rocky apply`, the
apply refuses with `plan_models_changed`. The
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

Rocky reports `E039` when a model reads a column that an upstream model does
not output. The read can be in any clause: the `SELECT` list, `WHERE`, join
`ON`, `GROUP BY`, `HAVING`, a `CASE`, a function argument, a subquery or a CTE
body. A qualified read such as `p.category` counts too.

```sql
-- E039: column 'stats' does not exist in complete upstream model 'int_order_lines'
SELECT order_id FROM int_order_lines WHERE stats = 'completed'
```

Rocky reports the name only when absence is a fact:

- The model depends on the upstream model and reads it by its bare name.
  The upstream model writes a table of that name, or is ephemeral. A
  qualified read of its target, such as `main.int_order_lines`, is not
  checked.
- The upstream output is complete. Every projection item is a column or has an
  alias, with no `SELECT *` and no set operation. An alias over a function that
  can return several columns, such as `unnest`, `explode` or `COLUMNS`, does
  not count.
- No two upstream output names differ only in case.

The binding rules are the ones for external sources below. A CTE, derived
table, unknown relation, `SELECT` alias, struct field read, quoted name or
warehouse metadata column keeps the name `Unknown`.

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

### Missing tables in external sources (`E045` / `W045`)

A two-part read such as `FROM staging.orderz` names a schema and a table. When
Rocky has source schemas for other tables in `staging`, but none for `orderz`,
the table may not exist. The code depends on how complete Rocky's table list
for that schema is:

| Where the table list came from | Without strict sources | With strict sources |
|---|---|---|
| Every table read from the warehouse during this invocation (an embedding caller; no CLI command does this for a compile yet) | `E045` (error) | `E045` |
| Seed file (`--with-seed`) or schema cache | `W045` (warning) | `E045` |

A seed or the cache lists only the tables it was given, so by default these
only warn, and the compile exits `0`. `--strict-sources` and
`[cache.schemas] strict_sources = true` turn every `W045` into `E045`. The
message suggests a close table name, or lists the schema's known tables.

These reads stay silent:

- A schema Rocky has no source schema in.
- A schema that a model of the project writes to. The project adds tables
  that no source schema lists.
- A one-part name (a model, a CTE, or a table on the search path) and a
  three-part name (its catalog may hold a different schema of that name).
- A name bound by `WITH`.

`rocky run` logs a `W045` as a warning and the model runs.

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

### Ambiguous column names (`E029`)

Rocky reports `E029` when a bare column name could come from two relations in
the same `FROM` clause. Every warehouse Rocky targets refuses such a query.

```sql
-- E029: column 'customer_id' is ambiguous: the joined relations 'c', 'l' all have a column called 'customer_id'
SELECT customer_id, c.name
FROM stg_customers AS c
LEFT JOIN customer_ltv AS l ON c.customer_id = l.customer_id
```

Fix it by qualifying the name (`c.customer_id`), or join with
`USING (customer_id)` when the columns are the same key.

`E029` fires only when Rocky knows that both relations have the column: each
is an upstream model, a source schema from `--with-seed` or the schema cache,
a CTE, or a subquery in `FROM`. These stay silent:

- A relation whose columns Rocky does not know, such as an external table
  with no source schema, or a model of another pipeline compiled separately.
- A name merged by `USING (…)`. A scope with `NATURAL`, semi, anti or
  `ARRAY JOIN`, or `LATERAL VIEW` is not checked.
- A name that is also a `SELECT` alias, or the output name of a qualified
  projection such as `c.customer_id`. `ORDER BY` is not checked when the
  `SELECT` list has a star.
- A relation binding name (a whole-row reference), a quoted name, a date-part
  keyword, or a niladic keyword such as `current_date`.
- A scope that uses `->`. A lambda parses as that operator, so its parameter
  looks like a column.
- A name inside a subquery that the subquery's own `FROM` does not bind. It
  resolves to the outer query (a correlated reference).

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
| `E039` | A model reads a column absent from a complete in-project upstream model. See [The type system](#the-type-system) |
| `E040` | A `.rocky` string literal contains a backslash; use a `.sql` model with the target's own escaping |
| `E044` | An aggregating query reads a column that is neither in `GROUP BY` nor inside an aggregate |
| `E029` | A bare column name is ambiguous: two joined relations both have it. See [Ambiguous column names](#ambiguous-column-names-e029) |
| `E045` | A two-part read names a table absent from a known schema whose table list Rocky holds as complete (or strict sources are on). See [Missing tables in external sources](#missing-tables-in-external-sources-e045--w045) |
| `E057` | A call names a function the target warehouse does not have and `functions/` does not declare (DuckDB; the other warehouses with a list get `W057`). See [Unknown functions](#unknown-functions) |
| `E060` | A downstream-consumer file in `consumers/` is invalid: it does not parse, two consumers share a name, or `depends_on` names something that is not a model. See [Downstream consumers](/concepts/downstream-consumers/) |
| `E058` | The models form a dependency cycle, so they have no execution order. One diagnostic for each model on the cycle. See [Dependency cycles](#dependency-cycles-e058) |
| `E042` | Aggregate argument type has no overload on the target warehouse, such as `SUM(VARCHAR)` on DuckDB |
| `E043` | Comparison between types the target warehouse refuses, such as `INT64 = STRING` on BigQuery or `DATE > 5` on DuckDB |
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
| `W014` | `rocky serve` could not load the transformation pipelines' models (a model name shared across pipelines, a malformed sidecar, a dangling models directory), so `/api/v1/models` shows only `models/` and `/api/v1/dag` answers the load error until it is fixed. If `models/` has no model either, the server reports `engine_not_ready` with the load error instead |
| `W030` | Imported producer added a column, surfaced only to consumers reading it via `SELECT *` |
| `W031` | Imported producer widened the type of a column this project reads (cross-team contract) |
| `W042` | Aggregate argument is cast implicitly at run time and fails on values that do not convert (escalate with `--deny-warnings W042`) |
| `W043` | Comparison relies on an implicit cast that fails on values that do not convert, such as a `BIGINT` column compared with a `VARCHAR` column on DuckDB (escalate with `--deny-warnings W043`) |
| `W044` | `E044`'s finding on a model that runs only on PostgreSQL, which accepts a column that depends on a grouped primary key (escalate with `--deny-warnings W044`) |
| `W041` | A direct reference names a column absent from an external source schema that may be out of date (seed or old cache entry) |
| `W045` | A two-part read names a table absent from a known schema whose table list came from a seed or the schema cache (escalate with `--strict-sources`) |
| `W051` | A user-defined function call could not be fully verified: an unknown argument type, or an argument the warehouse must convert implicitly |
| `W046` | An `incremental` model sets `lookback` without `unique_key`, so the re-read window is appended again on each run |
| `W056` | An `incremental` model sets no `lookback`, so a late row whose timestamp equals the target's `MAX` watermark is never loaded. `unique_key` alone does not fix this: it merges only the rows the filter reads |
| `W057` | A call names a function that is not in Rocky's function list for the target warehouse (every warehouse with a list except DuckDB, which gets `E057`). The list was built from the vendor's reference, or holds only the built-in functions of one engine version, so the call may still be valid: an extension, a connector or a UDF may define it. Escalate with `--deny-warnings W057` |
| `W049` | A `type = "snapshot"` model is valid but risky: a `unique_key` the SELECT does not output (it may be a `[[surrogate_key]]` column), `check` over more than 20 columns, an `updated_at` that is not a timestamp or date, or a key or change column missing from a `SELECT *` model's compile-time schema (which may be stale) |
| `W048` | A model reads a model version whose `deprecation_date` has passed or is less than 30 days away |
| `W052` | A `[redshift]` `dist_key` or `sort_key` column is not in the model's output |
| `W053` | A `[clickhouse]` `order_by` or `partition_by` column is not in the model's output |
| `W055` | `rocky package` vendored a package with something to review: an edited file the new version changed (written beside it as `.incoming`), a package model it could not vendor, an incremental model that fell back to full refresh, or dbt tests it did not map |
| `I001` | Model dependency inferred from SQL |
| `I002` | Some, but not all, output columns have unknown types — provide source schemas for more type checking |
| `E059` | A contract declares a type for a column whose type Rocky could not infer, and strict contracts are on (`--strict-contracts` or `[contracts] strict = true`). The `I003` note, as an error |
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
