---
title: The Rocky Compiler
description: The type system, the column lineage graph, and the stages a compile runs through.
sidebar:
  order: 7
---

Rocky ships a compiler in the `rocky-compiler` crate. It analyses SQL models before any generated SQL reaches the warehouse. It reports type mismatches, missing columns, contract violations and lineage findings when it has the information.

## What a clean compile means

A clean compile means the enabled checks found no error in the information they could resolve. It does not prove warehouse SQL validity, source data values or business correctness. Run the data checks and inspect a plan before execution.

## Compile pipeline

The compiler runs a fixed sequence of stages and returns one merged `CompileResult` (models, diagnostics, `semantic_graph`, timings).

```
 .sql  .rocky  .toml sidecars  contracts/*.toml
   │
   ▼
 1 Load models ──▶ 2 Resolve deps ──▶ 3 Semantic graph ──▶ 4 Type check
   (parse)         (build DAG)         (column lineage)      (propagate types)
                                                                  │
 CompileResult ◀── 6 Lint passes + merge ◀── 5 Validate contracts ◀┘
                   P002, W004, W005          required columns, types
```

### 1. Load models

A model is a `.sql` file plus a `.toml` sidecar (name, target, strategy, intent). A `.rocky` file goes through `lower_to_sql()` in `rocky-lang` first, then follows the same path. Raw SQL and the DSL are two front ends onto one pipeline.

### 2. Resolve dependencies

The resolver pulls the table references out of each model's SQL:

| Reference | Meaning |
|---|---|
| Bare name (`FROM orders`) | DAG edge to the model whose `[target]` table has that name. The model's own name does not matter. |
| `schema.table` | External source reference |
| `catalog.schema.table` | Fully qualified external reference |

When several models write `orders`, the one named `orders` wins, then the one in `depends_on`. Otherwise the read is refused as `E056`. An `ephemeral` model writes nothing, so it is read by its name. Rocky merges explicit `depends_on` entries, then drops self-references and duplicates.

#### Dependency cycles (`E058`)

A cycle is a set of models that depend on each other, so no model can run first:

```sql
-- fct_orders.sql. E058: fct_orders depends on customer_ltv, which depends on fct_orders
SELECT order_id, customer_id, amount
FROM raw.orders
WHERE customer_id IN (SELECT customer_id FROM customer_ltv)
```

`rocky compile`, `rocky test` and `rocky ci` report `E058` once for each model on the cycle, name the models, and point at the line of the read that closes it. A `depends_on`-only edge has no line. The command exits `1`. No later compile step runs, so a cycle hides other diagnostics. `rocky run` refuses a project with a cycle before it writes anything.

### 3. Build semantic graph

Rocky walks the models in topological order. It pulls column lineage out of each SQL AST, resolves aliases to model or source names, and expands `SELECT *` against the upstream schemas. See [Semantic graph](#semantic-graph).

### 4. Type check

The type checker pushes inferred types through the semantic graph and walks the SQL AST for problems. It infers types from `CAST`, aggregates, arithmetic (numeric promotion), literals, `CASE`/`WHEN` branches (common supertype), comparisons (Boolean result) and join keys (it can report incompatible types when known).

Each compiled model schema holds `TypedColumn` entries: a name, a `RockyType` and a nullability flag. A type can stay `Unknown`.

#### Nullability

Outer joins add nulls to non-nullable columns:

| Join | Marked nullable |
|---|---|
| `LEFT JOIN` | Direct references and ordinary casts from the right side |
| `RIGHT JOIN` | The accumulated left side |
| `FULL JOIN` | Both sides |

Self-join aliases stay distinct. Downstream models inherit the result. A `nullable = false` contract on an affected column with a known type raises `E012`. The analysis is conservative: a later `WHERE` does not narrow nullability.

| Expression | Nullability |
|---|---|
| Column, or a cast that cannot fail | The operand's |
| A cast that can fail | Nullable, even over `NOT NULL` input (Spark and Databricks with ANSI mode off return `NULL`) |
| `TRY_CAST`, `SAFE_CAST` | Always nullable |
| Cast of a computed value | That value's: `CAST(MAX(x) AS BIGINT)` and `CAST(NULLIF(x, 0) AS INT)` are nullable even when `x` is not |
| Aggregate other than `COUNT`, `NULLIF`, `CASE` with no `ELSE`, division, modulo, any function Rocky does not model | Nullable |
| `COUNT(...)`, including `COUNT(*)` | Non-null `Int64` |
| `+`, `-`, `*` over columns of a known integer, float or sized `DECIMAL` type, and over integer literals | Non-null over `NOT NULL` inputs (`qty * amount`); the type can still be `Unknown` |
| `SUM(x)` with a plain `GROUP BY` key list | Non-null, because every group has a row |
| `UNION`, `INTERSECT`, `EXCEPT` | Typed from all branches by position. Name from the first branch. Non-null only if non-null in every branch |

These keep `+`, `-`, `*` nullable: a `NULL`, a cast, a function call, a division, or a literal that is not an integer or is above the `BIGINT` range anywhere in the expression; a column whose type Rocky only guesses (a CTE column built from a decimal literal); a result over 38 digits, which Databricks with ANSI mode off writes as `NULL`.

`SUM(x)` stays nullable with no `GROUP BY`, with `GROUP BY ALL`, `ROLLUP`, `CUBE`, `GROUPING SETS` (also in parentheses, as `(ROLLUP(n))`), `GROUP BY ()`, a tuple key, keys that read no column (`GROUP BY 'a'`), `FILTER`, and `OVER`. Each can sum no rows. The type of `x` must be read from the SQL: `SUM(x * 1.5)` stays nullable. A `DECIMAL` sum is non-null only when every warehouse the model runs on is known and none is Databricks, because Spark and Databricks with ANSI mode off write `NULL` on a decimal overflow.

These rules assume SQL Server runs with `ARITHABORT` and `ANSI_WARNINGS` on, the defaults. With both off, it returns `NULL` on an overflow.

If Rocky cannot type a set operation (branches with different column counts, or a `VALUES` branch), every column is nullable and computed columns are `Unknown`. The common supertype of the branches is the type (`Unknown` if none).

**Casts that can fail.** A `nullable = false` contract over `CAST(text_col AS INT)` fails `E012`: use `COALESCE` or a numeric source.
| Cast | Can fail |
|---|---|
| Text or an unknown type to a number, boolean, date or timestamp | Yes |
| Number to a narrower number (`BIGINT` to `INT`, anything to `SMALLINT` or `TINYINT`, a number to a `DECIMAL` that may not hold it); `DOUBLE` to `FLOAT` | Yes |
| Same type; `INT` to `BIGINT`; a number to `DOUBLE` or `FLOAT`; anything to text; `DATE` to `TIMESTAMP`; an integer literal that fits the target (`CAST(0 AS DECIMAL(18,2))`) | No |

#### Inferred types

| Expression | Type |
|---|---|
| Cast of a column Rocky can type | The target type: `CAST(o.quantity * p.price AS DECIMAL(12, 2))` is `Decimal(12, 2)` |
| Cast of a column Rocky cannot type | The target type only for `BOOLEAN`, `DOUBLE` (`DOUBLE PRECISION`, `FLOAT64`), `DATE`, text (`VARCHAR`, `CHAR`, `TEXT`, `STRING`), binary (`BINARY`, `VARBINARY`, `BLOB`), and `DECIMAL` or `NUMERIC` with digits (`DECIMAL(p)` or `DECIMAL(p, s)`, `1 <= p <= 38`, `0 <= s <= p`). The column stays nullable. |
| Bare `DECIMAL` or `NUMERIC` | A decimal with digits Rocky does not claim: a bare `Decimal` contract passes, a `Decimal(p, s)` one is not checked. Digits out of range stay `Unknown`. |
| `CASE` or `COALESCE` | A type when every branch has the same type. A branch is a column, a cast, `COUNT`, a text or boolean literal, or `NULL`. `COALESCE(order_count, 0)` over a `BIGINT` is `Int64`. An integer literal counts only when it does not change the type: `COALESCE(qty, 0)` over an `INT` stays `Unknown`, as do a fraction (`1.5`) and branches of different widths. |
| Date arithmetic | `Unknown`: `DATE - DATE` is a `BIGINT` in DuckDB and an interval in Databricks |
| Aggregate over a computed argument | The argument's type only when it comes from the SQL itself (a cast target): `SUM(CAST(y AS DOUBLE))` is `Float64`. Otherwise `Unknown`: `MAX(LENGTH(n))`, because the width differs by warehouse. The same holds through a CTE, subquery or model column. |
| `SUM` over a `DECIMAL` | `DECIMAL(?,?)`: DuckDB returns `DECIMAL(38, s)`, Databricks `DECIMAL(p + 10, s)`. A bare `Decimal` contract passes. A `Decimal(p, s)` one is reported as `I003`. Cast the sum to `DECIMAL(p, s)` to check the digits. |
| `AVG` over a `DECIMAL` | `Unknown`: DuckDB returns a `DOUBLE` |

`INT`, `INTEGER`, `SMALLINT`, `TINYINT`, `BIGINT`, `FLOAT`, `REAL` and `TIMESTAMP` mean different widths on different warehouses. A cast to one takes the width of the warehouse the model is written to, from the pipelines that load it:

| Warehouse | Integer names | `FLOAT`, `REAL` | `TIMESTAMP` |
|---|---|---|---|
| Snowflake | `Decimal(38,0)` (`NUMBER(38,0)`) | `Float64` | `Unknown` (the session's `TIMESTAMP_TYPE_MAPPING` picks it) |
| BigQuery | `Int64` | | |
| DuckDB, Databricks | | `FLOAT` is `Float32` | |
| PostgreSQL, Redshift, SQL Server | | `FLOAT` is `Float64` | |

The cast stays `Unknown` when the warehouse is not known or not agreed: no transformation pipeline, Spark or ClickHouse, or a model loaded by pipelines on two warehouses that disagree. `--target-dialect` and `[portability] target_dialect` name a dialect to lint against. They do not set the warehouse.

For `USING` and `NATURAL` joins, Rocky distinguishes merged join keys from qualified references to either input.

#### Aggregate and comparison operands

`rocky compile` checks operands against the target warehouse:

| Check | Example | Verdict |
|---|---|---|
| Aggregate argument | `SUM(customer_name)` over a `VARCHAR` | `E042` on DuckDB, BigQuery, Trino, SQL Server, PostgreSQL, Redshift. `W042` on Snowflake and Databricks, which cast at run time. |
| Comparison (`=`, `<>`, `<`, `>`, `<=`, `>=`, `IN`, `BETWEEN`, join `ON`) | `BIGINT` column against `VARCHAR` column | `W043` on DuckDB, Snowflake, Databricks, SQL Server, Redshift (they cast each row and fail on the first bad value). `E043` on BigQuery, Trino, PostgreSQL. |
| Date against number | `order_date > 5` | `E043` on DuckDB, PostgreSQL, BigQuery, Trino. `W043` on Snowflake, Databricks, SQL Server, Redshift (rule not verified). |

The warehouse comes from, in order: `--target-dialect`, the adapter `type` of the warehouse each model runs on, then `[portability] target_dialect`. A model runs on the target adapter of each transformation pipeline whose `models` glob loads it. A model no pipeline loads runs on every pipeline's target. With several targets the strictest verdict wins. With none of these, Rocky reports the mildest verdict across all warehouses, always a warning. ClickHouse has no operand rules yet; the message says so.

These stay clean, so valid SQL is never refused:

- An operand whose type Rocky does not know.
- A string literal that parses as a number (`10::BIGINT = '10'::VARCHAR`), or compared with a date (`order_date >= '2024-01-01'`).
- Date arithmetic (`order_date >= CURRENT_DATE - 5`). Rocky does not type it.
- `DATE` against `TIMESTAMP`, and numbers of different widths.
- `MIN`, `MAX`, `COUNT(*)` and `COUNT(DISTINCT x)` over any type.

A same-named join key whose type differs between two upstream models is still reported once, as `E001` or `W001`. To fail the compile on the warnings, run `rocky compile --deny-warnings W042,W043`.

#### Unknown functions

Rocky reports a call to a function that the target warehouse does not have and `functions/` does not declare:

```sql
-- E057: function `SUMM` does not exist in DuckDB and is not a project function in `functions/`
SELECT customer_id, SUMM(amount) AS lifetime_value FROM fct_orders GROUP BY customer_id
```

The message suggests close names (`did you mean sum?`). Each warehouse has a list of aggregate, window and table functions and common aliases. Each file under `engine/crates/rocky-compiler/src/data/` names its source. The lists were built and checked on 2026-10-09.

| Warehouse | List (live = checked against a live engine) | Code |
|---|---|---|
| DuckDB | `duckdb_functions()` of DuckDB 2.0, plus functions its extensions load on first use (live) | `E057` |
| PostgreSQL | The PostgreSQL 17 built-in catalog (`pg_proc`) plus SQL-standard forms it lacks (live, 17.11) | `W057`: extensions add functions |
| Spark | `SHOW FUNCTIONS` of Spark 4.0.1 with Delta Lake 4.0.0 (live) | `W057`: UDFs and session extensions add functions |
| Trino | `SHOW FUNCTIONS` of Trino 483, plus the names it hides (`version`, `format`, SQL special forms) (live) | `W057`: connectors add functions |
| Snowflake, BigQuery | Vendor function reference (docs) | `W057` |
| Databricks | Vendor function reference and the Spark list (docs) | `W057` |
| Redshift | Vendor function reference and the PostgreSQL list (docs) | `W057` |
| SQL Server, ClickHouse | none yet | not checked |

A false refusal of a real function costs more than a missed typo, so only DuckDB gets the error. A documentation-built list can lag a release. A live-checked list holds one version's built-in catalog only. A warning never fails a compile alone: run `rocky compile --deny-warnings W057`. A model that runs on several warehouses is checked against each. When `[portability] target_dialect` names a warehouse, only that one is checked.

These calls are never reported:

- A schema-qualified call, such as `main.my_macro(x)`. Use this form for a
  macro or an extension function that you load outside Rocky.
- A quoted function name.
- A function declared in `functions/`.
- A form the SQL parser or the warehouse rewrites, such as `COALESCE`, `IF` or
  `IFNULL`.
- On DuckDB, a function or macro the project's `data/seed.sql` creates. Rocky
  takes the names from the DuckDB that ran the seed and from a scan of the
  seed's text, so a seed that does not run still counts, and so do
  `rocky serve` and `rocky lsp`, which never run it. Other warehouses still
  check the call: the seed runs only in DuckDB. A macro the seed brings in
  with `IMPORT DATABASE` or `ATTACH` is not in its text, so only the commands
  that run the seed (`rocky compile`, `rocky ci`, `rocky test`) skip it.

#### A seed file that cannot be read (`E073`)

When `data/seed.sql` is there but cannot be read (it is not valid UTF-8, it
is a directory, or Rocky has no permission), Rocky cannot tell which
functions and macros it creates. The check then fails closed. A DuckDB call
that `E057` or `E068` would refuse may be one of the seed's macros, so Rocky
refuses it with `E073`. The message names the seed file and the read failure:

```
E073: cannot check the call to `my_fn` on DuckDB: the seed file `data/seed.sql` could not be read (stream did not contain valid UTF-8), so Rocky cannot tell whether it creates `my_fn`
```

A call to a function DuckDB has, with a number of arguments it takes, is
not affected. A `W068` call stays a warning. Other warehouses check the call
as before. This holds for `rocky compile`, `rocky emit-sql`, `rocky serve`
and `rocky lsp`. The commands that run the seed (`rocky compile --with-seed`,
`rocky ci`, `rocky test`) refuse the whole run instead when the seed does not
read, with a message that names the seed file.

#### Calls with an argument count DuckDB refuses (`E068`, `W068`)

When a model runs on DuckDB, Rocky reports a call to a DuckDB built-in with
a number of arguments DuckDB refuses before it reads any data:

```sql
-- E068: DuckDB has no `lower` that takes 2 arguments
SELECT lower(customer_name, 'x') AS n FROM customers
```

Rocky does not guess the counts. A test probes the bundled DuckDB 2.0 and
writes them to `engine/crates/rocky-compiler/src/data/duckdb_signatures.txt`.
Each count is probed with every argument `NULL`, and with every argument a
column of one type from each type group. Another test replays every probe
and fails if DuckDB accepts one, or if the bundled version changes.

It is the error `E068` only in the places `E071` treats as checked (see
[the list below](#set-operations-with-different-column-counts-e071-w071)): the
select list, `WHERE`, `HAVING` or a join's `ON` of the model's query, a
set-operation branch, a derived table, or a subquery reached from them,
through comparisons, `AND`, `OR`, `NOT`, `CASE`, casts, `IN`, `EXISTS` or
parentheses. Anywhere else it is the warning `W068`: a CTE body, a named
`WINDOW`, `ORDER BY`, `GROUP BY`, `QUALIFY`, an operand of `+` or `||`, an
argument of another call, or anywhere in an ephemeral model, whose SQL
DuckDB sees only as a CTE in the models that read it. DuckDB skips a CTE nothing reads and a window
nothing uses, and a macro drops an argument its body does not use (the
built-in `pg_table_is_visible` does), so the model may still run. To fail on
the warning, run `rocky compile --deny-warnings W068`.

These are not reported:

- The same calls as for `E057`: schema-qualified, quoted, declared in
  `functions/`, a rewritten form, or a function or macro the seed creates.
- A call with named arguments (`name := value` too), `*`, `DISTINCT`,
  `ORDER BY` inside the parentheses, a lambda, an argument that expands to
  several (`UNPACK(COLUMNS(...))`), `FILTER`, `WITHIN GROUP` or `OVER`.
- A name DuckDB also has as a macro, window or table function, and a
  function such as `struct_pack`, which treats a `NULL` argument differently
  from a column.
- A call in a dot-call chain, as in `upper(status).lower()`: the value
  before the dot is the call's first argument.

A DuckDB macro created outside the project that reuses a built-in name is a
known false refusal: Rocky checks the call against the built-in. Call the
macro schema-qualified (`main.lower(...)`), which Rocky does not check.

#### Set operations with different column counts (`E071`, `W071`)

`E071` reports a `UNION`, `INTERSECT` or `EXCEPT` whose branches return different numbers of columns, on every warehouse:

```sql
-- E071: the branches of this UNION return different numbers of columns: 2 on the left, 1 on the right
SELECT order_id, status FROM orders UNION ALL SELECT order_id FROM returns
```

A branch is counted only when its select list states its width. A `*`, a `t.*`, or a call that returns several columns (`COLUMNS(...)`, `UNNEST`, `EXPLODE`) is not counted. `UNION BY NAME` is not checked.

| Place of the set operation | Code |
|---|---|
| The model's query, a query in parentheses, each branch of a set operation | `E071` |
| A derived table in `FROM` or a join (`LATERAL` and parenthesised joins included) | `E071` |
| A subquery in the select list, `WHERE`, `HAVING` or a join's `ON` (inner, left, right, full), reached only through comparisons, `AND`, `OR`, `NOT`, `CASE`, `BETWEEN`, `IS [NOT] DISTINCT FROM`, casts, `IN`, `EXISTS` or parentheses | `E071` |
| A CTE body, a named `WINDOW`, `ORDER BY`, `GROUP BY`, `QUALIFY`, `LIMIT`, any function call's arguments, a lambda, a wildcard's `REPLACE`, an operand of any other operator (`+`, `\|\|`, `LIKE`) | `W071` |

`E071` marks the places where tests show DuckDB checks the query, even a subquery that never runs. These places nest: a subquery in the `WHERE` of a derived table counts. `W071` marks places a warehouse can skip (a CTE nothing reads, a window nothing uses, an argument a DuckDB macro drops), so the model may still run.

#### Set-operation branch types with no common type (`W072`)

`W072` warns, on every warehouse, when the branches of a `UNION`, `INTERSECT` or `EXCEPT` give a column types with no common type:

```sql
-- W072: the branches give column `id` the types INT, STRING
SELECT order_id AS id FROM orders UNION ALL SELECT name FROM customers
```

DuckDB casts the branches and runs the query, though a value can still fail to cast (`Conversion Error`). Another warehouse may refuse the query. Cast the branches to one type.

These are not reported:

- a branch whose type Rocky does not know;
- a set of numeric types, even one Rocky has no common type for (a float with a decimal): every warehouse has a numeric type for it;
- a set operation inside a CTE or a subquery. Only the model's own query is checked, nested set operations included.

#### A model whose SQL does not parse (`E074`)

`rocky compile` and the language server refuse a model whose SQL does not parse with `E074`, and compile the rest of the project. Every model that reads it is refused with `E074` too, through a bare read or a declared `depends_on`, to any depth. A refused model is not type-checked, so its readers get no other errors. `rocky compile` still exits non-zero.

`rocky run`, `rocky plan` and every other command still refuse the whole project when one model does not parse.

### 5. Validate contracts

Rocky checks `.contract.toml` files against the inferred schemas. It reads `<model>.contract.toml` next to a model, then the project `contracts/` directory beside the models directory. `--contracts <DIR>` replaces the project directory. A file in the directory wins. The format and codes are in [Checks and Tests](/concepts/data-quality-checks/#data-contracts).

One `contracts/` directory serves every pipeline. A compile skips a file for a model it does not include, and logs a warning if that file does not parse. A file that does not parse for a model in the compile is an error. `rocky plan` fingerprints each covered contract file. If it changes or is deleted before `rocky apply`, the apply refuses with `plan_models_changed`.

### 6. Lint passes and merge

Three lint passes always run after contract validation:

| Code | Pass |
|---|---|
| `P002` | Blast radius: a `SELECT *` model whose downstream consumers read specific columns |
| `W004` | A `[classification]` tag with no matching `[mask]` strategy |
| `W005` | A model with temporal columns and no `freshness` declaration in scope |

Rocky merges their diagnostics with the type-checker and contract diagnostics into the final `CompileResult`.

## The type system

`RockyType` is Rocky's one type representation. Every warehouse type maps to and from it through a `TypeMapper` trait, so the compiler behaves the same on every warehouse.

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

`Unknown` is not an error. The compiler could not infer a type from what it had. It is compatible with every other type, so checks that need the type stay unresolved. A declared type does not validate an unresolved reference.

Rocky reports `E039` when a model reads a column that an upstream model does not output, in any clause or a qualified read such as `p.category`:

```sql
-- E039: column 'stats' does not exist in complete upstream model 'int_order_lines'
SELECT order_id FROM int_order_lines WHERE stats = 'completed'
```

Rocky reports the name only when absence is a fact: the model depends on the upstream and reads it by its bare name (a qualified read of its target, `main.int_order_lines`, is not checked); the upstream output is complete (every projection item is a column or has an alias, with no `SELECT *` and no set operation, and no alias over a function that can return several columns such as `unnest`, `explode`, `COLUMNS`); and no two upstream output names differ only in case.

`E039` checks the upstream model's SQL, not its table. A `merge` model's table can still hold a column its SQL used to select, so the suggestion says so for a `merge` upstream, and the read is still refused. See [Merge strategy](/concepts/incremental/#merge-strategy). The binding rules are those for external sources below. A CTE, derived table, unknown relation, `SELECT` alias, struct field read, quoted name or warehouse metadata column keeps the name `Unknown`.

A name in a star's `EXCEPT`, `EXCLUDE` or `REPLACE` that the star's relation does not output is `E039` (`E041` or `W041` on an external source). Rocky checks it only when it knows every column of every relation the star covers. It skips a star over a top-level `USING` or `NATURAL` join, a star with `ILIKE` or `RENAME`, a quoted name, a `_`-prefixed name, a qualified `EXCLUDE` name, and a qualified star `t.*` when `t` may also be a column or `SELECT` alias in the star's own `FROM` scope (it may expand a struct). A CTE, derived table or unknown relation in that scope counts as such a column.

### Missing columns in external sources (`E041` / `W041`)

An external source is a table such as `raw.orders` that Rocky reads but does not build. Rocky knows its columns only from a seed file (`rocky compile --with-seed`) or the schema cache. A reference to a column the schema lacks is `E041` or `W041`, by how far Rocky trusts the schema:

| Where the schema came from (a warehouse read in the same invocation is `E041` too, for embedding callers) | Without strict sources | With strict sources |
|---|---|---|
| Schema cache, younger than `trusted_max_age_seconds` | `E041` | `E041` |
| Schema cache, older (or the key is unset) | `W041` | `E041` |
| Seed file (`--with-seed`) | `W041` | `E041` |
| Unknown | nothing (`Unknown`) | nothing |

A seed or old cache entry can miss a column the warehouse has, so by default these only warn and the compile exits `0`. Turn on strict sources with `rocky compile --strict-sources` or `[cache.schemas] strict_sources = true`: every `W041` becomes `E041`. Both codes name the column and source and suggest close names or list the source's columns. `W041` also says how to refresh the schema.

Rocky reports the name only when it binds to known sources and nothing else. Every relation the name could resolve against, enclosing scopes included, must be a known source. These keep the name `Unknown`:

- A CTE, derived table, in-project model or table function in scope.
- A `SELECT` alias with that name, including DuckDB lateral aliases.
- A relation binding with that name (a whole-row reference).
- A qualified `a.b` where `a` can also be a column (a struct field read).
- A 3-part reference, a lambda parameter, or a keyword-like argument such as `day` in `DATEADD(day, 1, ts)`.
- A quoted name (BigQuery, and Databricks by default, read `"shipped"` as a string).
- A name that starts with `_` (metadata columns such as BigQuery `_FILE_NAME`).

`rocky run` compiles against the schema cache first. An `E041` model is excluded like any model with an error, before Rocky touches the warehouse. A `W041` is logged and the model runs.

### Missing tables in external sources (`E045` / `W045`)

A two-part read such as `FROM staging.orderz` may name a table that does not exist when Rocky has source schemas for other tables in `staging` but none for `orderz`.

| Where the table list came from (a full warehouse read in the same invocation is `E045`, for embedding callers) | Without strict sources | With strict sources |
|---|---|---|
| Seed file (`--with-seed`) or schema cache | `W045` | `E045` |

A seed or the cache lists only the tables it was given, so by default these warn and the compile exits `0`. `--strict-sources` and `[cache.schemas] strict_sources = true` turn every `W045` into `E045`. The message suggests a close table name or lists the schema's known tables. `rocky run` logs a `W045` and the model runs.

These reads stay silent: a schema Rocky has no source schema in; a schema a model of the project writes to; a one-part name; a three-part name (its catalog may hold a different schema of that name); a name bound by `WITH`.

During `rocky run`, a model with an `Error` diagnostic records a `compile-error` and Rocky withholds its declared DAG descendants (`RunOutput.contained`). Healthy branches still run. See [Failure containment](/advanced/failure-modes/#failure-containment-across-the-model-graph).

With `rocky run --model <name> --defer`, a successful external rewrite suppresses local `E039` only for the rewritten reference. The exemption needs a complete plain `SELECT` `FROM` or `JOIN` read set: CTEs, subqueries and set operations keep local failure dependencies, as do qualified local references and unrewritten inputs. An invalid external schema still fails when the warehouse runs the SQL.

### GROUP BY validity

Rocky reports `E044` when an aggregating query reads a column that is not grouped, in the `SELECT` list, `HAVING` or `ORDER BY`. A query aggregates when it has `GROUP BY`, `HAVING`, or an aggregate in its `SELECT` list. Without `GROUP BY`, every column read outside an aggregate is reported.

```sql
-- E044: column 'status' in the SELECT list is neither in GROUP BY nor inside an aggregate
SELECT customer_id, status, SUM(amount) AS t
FROM raw.orders
GROUP BY customer_id
```

Fix it by adding the column to `GROUP BY`, or wrapping it in an aggregate such as `ANY_VALUE(status)`. PostgreSQL accepts a column that depends on a grouped primary key, and Rocky cannot see primary keys. So when every warehouse a model runs on is PostgreSQL, the finding is `W044`. Redshift keeps `E044`.

`E044` fires only when the column belongs to a relation in the same query whose columns Rocky knows: an upstream model, a source schema from `--with-seed` or the cache, a CTE, or a subquery in `FROM`. Each subquery and CTE is checked as its own query. These stay silent:

- `GROUP BY ALL`, `ROLLUP`, `CUBE` and `GROUPING SETS` columns; grouping by ordinal (`GROUP BY 1`) or by a `SELECT` alias; any name that is also a `SELECT` alias (a lateral column alias).
- Expressions built only from grouped columns (`UPPER(status)`), and a column inside a grouped expression (`order_date` when `DATE_TRUNC('month', order_date)` is grouped).
- Arguments of aggregates, `FILTER`, and unknown functions (they may be user-defined aggregates). `QUALIFY`, and outer references inside a subquery.
- Names Rocky cannot place: unknown relations, stale schemas, session variables.

### Aggregates and windows in WHERE, and GROUP BY positions

Every warehouse refuses these shapes before it reads any data:

```sql
-- E065: WHERE filters rows before they are grouped
SELECT customer_id FROM main.orders WHERE SUM(amount) > 1 GROUP BY customer_id
-- E066: window functions run after WHERE
SELECT order_id FROM main.orders WHERE ROW_NUMBER() OVER (ORDER BY order_id) = 1
-- E067: the select list has 2 columns
SELECT customer_id, COUNT(*) AS n FROM main.orders GROUP BY 3
```

Move an aggregate condition to `HAVING`, and a window condition to `QUALIFY`. These fire only when Rocky is certain:

- `E065` counts `COUNT`, `SUM`, `AVG`, `MIN` and `MAX`, and any call with `FILTER` or `WITHIN GROUP`. Other aggregate names may be user functions.
- An aggregate or window inside a subquery belongs to that subquery: `WHERE amount > (SELECT AVG(amount) FROM orders)` is valid.
- The `WHERE` of a scalar, `IN` or `EXISTS` subquery or a `LATERAL` table is not checked. An aggregate over outer columns there belongs to the outer query.
- `E067` checks plain integer positions only, outside `ROLLUP`, `CUBE` and `GROUPING SETS`. A select list with `*`, `t.*` or `COLUMNS(...)` has no known length, so it is not checked.

### Ambiguous column names (`E029`)

Rocky reports `E029` when a bare column name could come from two relations in the same `FROM` clause. Every warehouse Rocky targets refuses such a query.

```sql
-- E029: column 'customer_id' is ambiguous: the joined relations 'c', 'l' all have a column called 'customer_id'
SELECT customer_id, c.name
FROM stg_customers AS c
LEFT JOIN customer_ltv AS l ON c.customer_id = l.customer_id
```

Fix it by qualifying the name (`c.customer_id`), or join with `USING (customer_id)` when the columns are the same key.

`E029` fires only when Rocky knows both relations have the column (as for `E044`). Each subquery and CTE is checked as its own query. These stay silent:

- A relation whose columns Rocky does not know (an external table with no source schema, a model of another pipeline compiled separately).
- A name merged by `USING (…)`. A scope with `NATURAL`, semi, anti or `ARRAY JOIN`, or `LATERAL VIEW`.
- A name that is also a `SELECT` alias, or the output name of a qualified projection such as `c.customer_id`. `ORDER BY` when the `SELECT` list has a star.
- A relation binding name, a quoted name, a date-part keyword, or a niladic keyword such as `current_date`.
- A scope that uses `->` (a lambda parameter looks like a column).
- A name inside a subquery that the subquery's own `FROM` does not bind: it resolves to the outer query (a correlated reference).

### Numeric promotion and assignability

When two numeric types meet (arithmetic, `COALESCE`, `CASE`, `UNION`), the compiler picks a common supertype:

| Mix | Result |
|---|---|
| `Int32` and `Int64` | `Int64` |
| `Float32` and `Float64` | `Float64` |
| Integer and float | `Float64` |
| Integer and decimal | `Decimal` |
| Two decimals | The larger precision and the larger scale |
| `Timestamp` and `TimestampNtz` | `Timestamp` |

Types that cannot mix, such as `String` and `Int64`, produce an error. `is_assignable` allows a widening write (`Int32` into `Int64`) and rejects a narrowing one.

## Semantic graph

The semantic graph is a cross-model map of represented column lineage: where resolved columns came from and how they were transformed. Rocky builds it in topological order, so a downstream model sees the full column list of its upstreams, including anything a `SELECT *` expanded to.

```
raw_orders                  orders_enriched              orders_summary
──────────                  ───────────────              ──────────────
order_id  ──[Direct]──────▶ order_id  ──[Direct]───────▶ order_id
amount    ──[Cast:DECIMAL]─▶ amount   ──[Agg:SUM]────────▶ total
customer_id──[Direct]──────▶ customer_id
                            region    ◀──[Direct]── raw_customers.region
```

- **Column lineage tracing.** `trace_column` walks lineage edges backward from any output column: `c.id → b.id → a.id → source.raw.users.id`.
- **Transform tracking.** Each edge records `Direct` (unchanged), `Cast` (explicit type cast), `Expression` (derived) or `Aggregation` (aggregate function).
- **Star expansion.** `SELECT *` expands against the upstream model's inferred schema or a known source schema. A star can carry `EXCEPT (...)`, `EXCLUDE (...)` or `REPLACE (expr AS col)`. `EXCEPT` and `EXCLUDE` remove the named columns. `REPLACE` keeps the column's name and position, and the column takes its type and nullability from the new expression. A modifier on a qualified star (`t.* EXCEPT (n), u.*`) applies to that relation only. When an unqualified star sits beside another star and either has a modifier, the star's columns are unknown. A star with `ILIKE`, `RENAME`, an alias or a qualified `EXCLUDE` name leaves the columns of the whole query unknown.
- **Intent propagation.** Each model's `intent` field from its TOML config is stored in the graph. The AI features (`ai-sync`, `ai-explain`) read it from there.

## Diagnostics

Every finding carries a code, a severity, a source span and sometimes a suggested fix. Severities: **Error** (the model has a definite problem), **Warning** (looks wrong, does not block), **Info** (usually a limit in type inference).

### Diagnostic codes

| Code | Meaning |
|------|---------|
| `E001` | Type-checking error (unresolved reference, type mismatch) |
| `E010`--`E014` | Contract violations: a missing required column, a type mismatch, a nullability violation, a removed protected column, an undeclared nullable column. See [Checks and Tests](/concepts/data-quality-checks/#data-contracts) |
| `E020`--`E026` | `time_interval` validation (`@start_date`/`@end_date` placeholders, `time_column` presence/type/nullability/granularity) |
| `E027` | Budget exceeded -- projected spend over the model's `[budget]` ceiling |
| `E028` | Required run variable (`@var(name)`) referenced but no `--var` supplied and no inline default |
| `E029` | A bare column name is ambiguous: two joined relations both have it. See [Ambiguous column names](#ambiguous-column-names-e029) |
| `E030`--`E032` | Imported producer dropped a column, narrowed its type, or tightened it to NOT NULL, and this project reads it (cross-team contract). See [Cross-Team Contracts](/concepts/cross-team-contracts/) |
| `E033` | Imported snapshot's recipe hash does not match the configured `pin` |
| `E034` | Imported snapshot declares a format version newer than this build of rocky can read |
| `E035` | Managed-Iceberg `format_options` declares a combination the warehouse rejects (e.g. `partition_by` + `cluster_by`) |
| `E036` | Two or more models write the same target table |
| `E037` | A transformation model declares `type = "incremental"` with no `timestamp_column` (watermark), so it would append every row each run. Declare it and use `@incremental_filter`, or use another strategy |
| `E038` | An `ephemeral` model is used in a way inlining cannot serve (`[[tests]]`, a qualified read of its target, an unrewritable consumer, or `rocky run --model` on it). See [Model format](/reference/model-format/) |
| `E039` | A model reads a column absent from a complete in-project upstream model. See [The type system](#the-type-system) |
| `E040` | A `.rocky` string literal contains a backslash; use a `.sql` model with the target's own escaping |
| `E041` / `W041` | A column is absent from an external source schema. `E041` when Rocky trusts the schema, `W041` when it may be out of date (seed or old cache entry). See [Missing columns](#missing-columns-in-external-sources-e041--w041) |
| `E042` / `W042` | Aggregate argument type has no overload on the target warehouse (`SUM(VARCHAR)` on DuckDB), or is cast implicitly at run time (`W042`, `--deny-warnings W042`) |
| `E043` / `W043` | Comparison between types the target warehouse refuses (`INT64 = STRING` on BigQuery, `DATE > 5` on DuckDB), or relies on an implicit cast that fails on values that do not convert (`W043`, `--deny-warnings W043`) |
| `E044` / `W044` | An aggregating query reads a column that is neither in `GROUP BY` nor inside an aggregate. `W044` on a model that runs only on PostgreSQL (`--deny-warnings W044`) |
| `E045` / `W045` | A two-part read names a table absent from a known schema. `W045` when the table list came from a seed or the cache (`--strict-sources` escalates). See [Missing tables](#missing-tables-in-external-sources-e045--w045) |
| `E046` | An `incremental` model's watermark filter has no safe place: no `@incremental_filter` and no provable passthrough watermark column, a watermark that is not a plain output column, or `@incremental_filter` under another strategy |
| `E047` | A model reads a `private` model outside its ownership group, or a producer model that is not `public`. See [Model governance](/concepts/model-governance/) |
| `E048` | A model-version problem: undeclared latest version, missing version file, or a reference to an undeclared version |
| `E049` / `W049` | A `type = "snapshot"` model has an invalid config (`unique_key`, `strategy`, `updated_at`, `check_cols`, a computed or non-deterministic key, a metadata-named output column). `W049`: a valid one is risky (a `unique_key` the SELECT does not output, `check` over more than 20 columns, a non-date `updated_at`, a key or change column missing from a stale `SELECT *` schema). See [Model format](/reference/model-format/) |
| `E050` / `W050` | A freshness declaration cannot be evaluated: no threshold, a bad duration, `error_after` shorter than `warn_after`, a bad `loaded_at_field` or `filter`, or a `time_column` absent from a complete output. `W050`: a `loaded_at_field` / `time_column` is not a date or time type, or a source `loaded_at_field` is missing from the known source schema |
| `E051` | A [user-defined function](/concepts/user-defined-functions/) or a call to one is invalid: bad definition, Python language, wrong argument count, an incompatible argument type, or a warehouse that cannot create functions (Trino, ClickHouse, SQL Server, Spark) |
| `E052` / `W052` | A model's `[redshift]` table options cannot render (an invalid or contradictory `dist_key` / `sort_key`) or sit on a strategy that builds no table. `W052`: a `dist_key` or `sort_key` column is not in the output. See [Redshift](/reference/adapters/redshift/#table-distribution-and-sort-keys) |
| `E053` | ClickHouse cannot run the model: its `[clickhouse]` options cannot render or sit on a strategy that builds no table, or it is a `merge` model (or `incremental` with `unique_key`) and ClickHouse has no `MERGE`. See [ClickHouse](/reference/adapters/clickhouse/#strategies) |
| `E054` | SQL Server cannot run the model's SQL: its CTEs cannot be lifted to the start of the statement, even after Rocky renames colliding nested CTEs. See [SQL Server](/reference/adapters/sqlserver/) |
| `E055` / `W055` | `rocky package` refused to vendor a dbt package (a bad spec, missing `dbt`, a failed `dbt deps` or `dbt compile`, a name or `[target]` clash, unvendored reads, unparseable SQL, a step past `--dbt-timeout`, or a `remove` that would delete edited files without `--force`). `W055`: vendored with something to review (an edited file the new version changed, written beside it as `.incoming`; an unvendored model; an incremental model that fell back to full refresh; unmapped dbt tests). See [Vendor dbt Packages](/guides/dbt-packages/) |
| `E056` | A bare read (`FROM orders`) is ambiguous: several models write that table, none is named for it, and `depends_on` does not pick one. Qualify the read, or list the model in `depends_on` |
| `E057` / `W057` | A call names a function the target warehouse lacks and `functions/` does not declare. `E057` on DuckDB, `W057` on every other warehouse with a list (`--deny-warnings W057`). See [Unknown functions](#unknown-functions) |
| `E058` | The models form a dependency cycle. One diagnostic for each model on it. See [Dependency cycles](#dependency-cycles-e058) |
| `E059` | A contract declares a type for a column Rocky could not infer, with strict contracts on (`--strict-contracts` or `[contracts] strict = true`): `I003` as an error |
| `E060` | A file in `consumers/` is invalid: it does not parse, two consumers share a name, or `depends_on` names something that is not a model. See [Downstream consumers](/concepts/downstream-consumers/) |
| `E061` | The output repeats a column name (`SELECT a.id, b.id`) and the strategy writes into the existing table (`incremental`, `delete_insert`, `time_interval`, `microbatch`, `merge`). On DuckDB every run after the first fails. Alias it (`b.id AS b_id`). Names match as the target warehouse compares them (DuckDB, Databricks, BigQuery and Trino ignore case even when quoted). Star expansions are not checked |
| `E062` | A model with a contract repeats an output name the contract constrains (`[[columns]]`, `required` or `protected`). Alias the repeat. Names match as for `E061` |
| `E063` | `[rules] no_extra_columns = true` and the model outputs a column the contract does not name, or Rocky cannot prove the column list (a `SELECT *`, an unaliased expression) |
| `E064` | A one-part read (`FROM stg_ordrs`) names no model, `WITH` name or known source table, and is one or two edits from a model name. Qualify a real warehouse table (`main.stg_ordrs`) |
| `E065` | A `WHERE` clause contains an aggregate (`WHERE SUM(amount) > 1`). Move the condition to `HAVING` |
| `E066` | A `WHERE` clause contains a window function, a call with `OVER`. Filter on it with `QUALIFY`, or in a subquery |
| `E067` | A `GROUP BY` position names no output column (`GROUP BY 3` over two columns, or `GROUP BY 0`) |
| `E068` / `W068` | A call to a DuckDB built-in passes a number of arguments it does not take, in a place tests show DuckDB checks. Only when the model runs on DuckDB. `W068` in a place DuckDB may skip, such as a CTE body, a named `WINDOW` or an argument of another call. See [Calls with an argument count DuckDB refuses](#calls-with-an-argument-count-duckdb-refuses-e068-w068) |
| `E071` / `W071` | The branches of a `UNION`, `INTERSECT` or `EXCEPT` return different numbers of columns. `W071` in a place the warehouse may skip. See [Set operations](#set-operations-with-different-column-counts-e071-w071) |
| `E073` | The project's `data/seed.sql` is there but cannot be read, and a DuckDB call would get `E057` or `E068`. The seed may create that name, so the call is refused. The message names the seed and the read failure. See [A seed file that cannot be read](#a-seed-file-that-cannot-be-read-e073) |
| `E074` | `rocky compile` and the language server only: the model's SQL does not parse, or the model reads one whose SQL does not parse. The rest of the project still compiles. See [A model whose SQL does not parse](#a-model-whose-sql-does-not-parse-e074) |
| `W001` | Unused model (no downstream consumers) |
| `W002` | The output repeats a column name and the strategy rebuilds the relation (`full_refresh`, `view`, `materialized_view`, `dynamic_table`, `ephemeral`, `snapshot`, `content_addressed`): the warehouse may rename the repeat or refuse the table. Also emitted when a `SELECT *` reads an upstream with unknown schema |
| `W004` | Classification tag with no matching `[mask]` strategy |
| `W005` | Temporal column present but no `freshness` declaration in scope |
| `W006` | `merge` strategy declares a `unique_key` column the model does not output |
| `W010`, `W011` | A contract column is not in the model output, or a contract exists for a model not in the project |
| `W012` | An `[imports.<name>]` snapshot could not be loaded; `E030`/`E033` checks skipped |
| `W013` | `rocky.toml` is present but could not be read, so every project-level check is silent (`rocky lsp` and `rocky serve` only; one-shot commands refuse instead) |
| `W014` | `rocky serve` could not load the transformation pipelines' models (a shared model name, a malformed sidecar, a dangling models directory). `/api/v1/models` shows only `models/`, `/api/v1/dag` answers the load error, and with no model in `models/` the server reports `engine_not_ready`. Also raised for a pipeline whose models directory is missing, unless it sets `allow_empty_models = true`. Clears at the next recompile or restart |
| `W030` | Imported producer added a column, surfaced only to consumers reading it via `SELECT *` |
| `W031` | Imported producer widened the type of a column this project reads (cross-team contract) |
| `W046` | An `incremental` model sets `lookback` without `unique_key`, so the re-read window is appended again each run |
| `W048` | A model reads a model version whose `deprecation_date` has passed or is less than 30 days away |
| `W051` | A user-defined function call could not be fully verified (an unknown argument type, or an implicit argument conversion) |
| `W053` | A `[clickhouse]` `order_by` or `partition_by` column is not in the model's output |
| `W056` | An `incremental` model sets no `lookback`, so a late row whose timestamp equals the target's `MAX` watermark is never loaded. `unique_key` alone does not fix this |
| `W072` | The branches of a `UNION`, `INTERSECT` or `EXCEPT` give a column types with no common type. Numeric sets and set operations inside a CTE or subquery are not checked. See [Set-operation branch types](#set-operation-branch-types-with-no-common-type-w072) |
| `W058` | `rocky validate` only: a freshness block uses a deprecated spelling (`threshold_seconds`, `max_lag_seconds`, `expected_lag_seconds`, `max_lag`, `severity`). It still works; the message names the `warn_after` / `error_after` replacement |
| `I001` | Model dependency inferred from SQL |
| `I002` | Some, but not all, output columns have unknown types — provide source schemas for more type checking |
| `I003` | A contract declares a type for a column whose type Rocky could not infer, so `E011` did not check it |
| `P001` | Construct not portable to the target dialect (opt-in via `--target-dialect` or `[portability] target_dialect`) |
| `P002` | `SELECT *` model has downstream consumers that read specific columns |

### Format

Diagnostics render like `rustc` and carry a `code`, `message`, `span` (file, line, column, when known), `model` and `suggestion` (when the compiler can work one out):

```
error[E011]: column 'id' type mismatch: contract expects Int64, got String
 --> models/orders.sql:3:8
 = help: add CAST(id AS BIGINT) to fix the type
```

The type checker also builds a `ReferenceMap` of where each model and column is defined and referenced. It powers Find References and Rename Symbol in the LSP server.

## Using the compiler

```bash
rocky compile --models models/
rocky compile --models models/ --contracts contracts/
```

```rust
use rocky_compiler::compile::{compile, CompilerConfig};

let config = CompilerConfig {
    models_dir: "models/".into(),
    contracts_dir: Some("contracts/".into()),
    ..Default::default()
};
let result = compile(&config)?;
if result.has_errors {
    for d in &result.diagnostics { eprintln!("{d}"); }
}
```

`CompileResult` holds the resolved project, semantic graph, inferred schemas and diagnostics. The test runner, CI pipeline and AI sync build on it.
