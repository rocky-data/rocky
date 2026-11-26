---
title: User-Defined Functions
description: Declare SQL functions in functions/, type-check their calls, and let rocky run create them before the models that use them
sidebar:
  order: 7.4
---

A user-defined function (UDF) is a reusable SQL expression with a name, typed arguments, and a return type. Declare it once in `functions/`. Models call it like a built-in. Rocky checks every call at compile time and creates the function in the warehouse before any model that calls it.

## How to declare a function

Each function is a pair of files in `functions/`, next to `models/`. This is
the same pair convention models use: a `.sql` body and a `.toml` sidecar.

```
my-project/
├── rocky.toml
├── models/
│   └── fct_orders.sql
└── functions/
    ├── cents_to_dollars.sql     # the body: one SQL expression
    └── cents_to_dollars.toml    # the signature
```

The `.sql` file holds one scalar expression. Refer to the arguments by name.
Rocky removes a trailing `;`.

```sql
-- functions/cents_to_dollars.sql
cents / 100.0
```

The `.toml` file declares the signature:

```toml
# functions/cents_to_dollars.toml
description = "Integer cents to dollars"
returns = "DOUBLE"
deterministic = true

[[arguments]]
name = "cents"
type = "BIGINT"
```

| Key | Required | Meaning |
|-----|----------|---------|
| `name` | no | The function name. The default is the file stem. Must match `[A-Za-z0-9_]+`. |
| `returns` | yes | The return type, in the warehouse's own spelling. |
| `[[arguments]]` | no | One table per argument, in call order: `name` and `type`. |
| `language` | no | Must be `"sql"`, the default. Rocky refuses Python UDFs with `E051`. |
| `description` | no | Shown in editor hover. Sent as the function comment on Snowflake, Databricks and BigQuery. |
| `deterministic` | no | `true` or `false`. Omit it to keep the warehouse default. |
| `[target]` | no | `schema`, and optionally `catalog`, to create the function in. |

Write types the way the target warehouse spells them: `BIGINT` on DuckDB, `INT64` on BigQuery, `NUMBER(38,0)` on Snowflake. Rocky does not translate types. It checks each type against a character allowlist before it puts the type into DDL.

## How a model calls a function

A model calls the function by name, like any SQL function:

```sql
-- models/fct_orders.sql
SELECT order_id, cents_to_dollars(amount_cents) AS amount_usd
FROM raw.orders
```

| Call | Result |
|---|---|
| Qualified, and every qualifier part matches the function's `[target]` | Matches. With `schema = "util"`, `util.cents_to_dollars(x)` matches and `other.cents_to_dollars(x)` does not. |
| Qualified, and the function has no `[target]` | Never matches. `governance.mask_email(s)` is someone else's function, so Rocky does not check it. |
| Unqualified, and the function has a `[target] schema` | `W051`. Rocky creates the function in that schema, but an unqualified call resolves in the session's current schema. Qualify the call. |
| Name is a built-in SQL function, such as `round` or `coalesce` | Ignored, with `W051`. The built-in wins on the warehouse. Rename the function. |

## What the compiler checks

`rocky compile` loads `functions/` with the models. It checks each definition,
then each call:

```
 functions/*.toml + *.sql
          │
          ▼
   validate definition ──── bad name / type, Python, no body, `;` in body,
          │                 duplicate name, call cycle ──► E051 on the function
          ▼
   check each model call ── wrong argument count,
          │                 call to an invalid function ──► E051 on the model
          │                 argument type certainly wrong ─► E051 on the model
          │                 argument type unknown, or
          │                 relies on implicit conversion ─► W051 on the model
          ▼
   declared return type ──► column type ──► downstream models and contracts
```

`E051` is certain from the SQL text and the declared types: a wrong argument count, or an argument type with no implicit conversion on any supported warehouse (a `DATE` passed to a `BIGINT` parameter). A model with an `E051` is not built.

`W051` is a warning. Rocky raises it when it cannot infer an argument's type (usually no source schema is known), or when the call depends on the warehouse converting a value (a `VARCHAR` passed to a `BIGINT` parameter). The warehouse may accept it, so Rocky does not refuse it.

Rocky checks argument types in the `SELECT` list, including CTEs, and argument counts everywhere in the statement.

### How the return type reaches contracts

A projected column that is exactly a function call takes the declared return
type. In the example above, `amount_usd` is `Float64`. A downstream model
that selects `amount_usd` sees `Float64` too, and a contract on either model
checks it:

```toml
# models/fct_orders.contract.toml
[[columns]]
name = "amount_usd"
type = "Float64"
```

Only a direct call takes the declared type. An expression over a call, such as
`cents_to_dollars(x) + 1`, stays `Unknown`, because arithmetic result types
differ between warehouses.

Some return type names mean different types on different warehouses. `FLOAT` and `REAL` are 32-bit on DuckDB and Databricks but 64-bit on Snowflake. `INT`, `INTEGER` and `BIGINT` are `NUMBER(38,0)` on Snowflake. `TIMESTAMP` has no time zone on DuckDB and Snowflake. Rocky reads these return types as `Unknown`, so a contract on such a column reports `I003` (not checked) instead of a wrong `E011`. Use `DOUBLE`, `FLOAT64`, `INT64`, `DECIMAL(p,s)`, `VARCHAR` or `DATE` when you want the contract checked. Parameter types are read more loosely, because they only decide whether a call is flagged.

### How lineage traces a call

Column lineage traces a function call to its first column argument. In the
example, `fct_orders.amount_usd` derives from `raw.orders.amount_cents`.

## How `rocky run` creates functions

`rocky run` creates every function that a model in the run calls. It does
this before the first model runs. A function that calls another function is
created after its callee. Models that failed to compile, and models outside
the selection, do not cause their functions to be created.

A function that cannot be created fails only the models that call it, and
their declared downstream models. Rocky records an error for each of those
models. Every other model still builds.

Each warehouse gets its own statement. Rocky always uses `CREATE OR REPLACE`,
so a second run replaces the function in place.

| Warehouse | Statement |
|-----------|-----------|
| DuckDB | `CREATE OR REPLACE MACRO name(cents) AS CAST((cents / 100.0) AS DOUBLE)` |
| Snowflake | `CREATE OR REPLACE FUNCTION name(cents NUMBER(38,0)) RETURNS FLOAT LANGUAGE SQL [IMMUTABLE \| VOLATILE] [COMMENT = '…'] AS $$ … $$` |
| Databricks | `CREATE OR REPLACE FUNCTION name(cents BIGINT) RETURNS DOUBLE LANGUAGE SQL [[NOT] DETERMINISTIC] [COMMENT '…'] RETURN …` |
| BigQuery | ``CREATE OR REPLACE FUNCTION `project`.`dataset`.`name`(cents INT64) RETURNS FLOAT64 AS (…) [OPTIONS (description = '…')]`` |
| PostgreSQL | `CREATE OR REPLACE FUNCTION name(cents BIGINT) RETURNS NUMERIC LANGUAGE sql [IMMUTABLE \| VOLATILE] AS $rocky$ SELECT … $rocky$` |
| Redshift | `CREATE OR REPLACE FUNCTION name(BIGINT) RETURNS FLOAT8 { IMMUTABLE \| VOLATILE } AS $$ SELECT … $$ LANGUAGE sql` |

The table shows each statement on one line. In the real statement, the parenthesis that closes the body starts a new line, so a trailing `-- comment` in the body cannot hide it.

| Warehouse | Notes |
|---|---|
| DuckDB | Macros have no return type, so Rocky wraps the body in a `CAST` to the declared type. The warehouse column type then matches the compiler's. |
| BigQuery | Needs `[target] schema` (the dataset). Rocky refuses a function without one. SQL functions take no determinism clause, so `deterministic` is ignored. |
| PostgreSQL | The body is quoted with `$rocky$`, so it may contain `$$`. Rocky refuses a body that contains `$rocky$`. Without `deterministic`, the warehouse default (`VOLATILE`) applies. No description is set: PostgreSQL keeps it in a separate `COMMENT ON FUNCTION`. |
| Redshift | See below. |
| Trino | Cannot create persistent functions. `E051` from `rocky compile` (when a calling model runs on a Trino target), `rocky run` and the plan preview. |
| ClickHouse, SQL Server, Spark | Refused with `E051`: Rocky has no function DDL for them. On SQL Server a scalar function takes `@`-prefixed parameters and must be called with its schema (`dbo.f(x)`), so a bare `f(x)` would not reach it. |

**Redshift.** SQL functions take no argument names: the body uses `$1`, `$2`, in declaration order. Rocky writes the body with names and rewrites each argument reference. It parses the body first, so a string literal, a function name or the date part of `DATEADD`, `DATEDIFF` and `DATE_PART` that spells an argument stays as it is. The rewritten body is printed from the parsed SQL, so comments are dropped. Rocky refuses a Redshift function when:

- the body does not parse as one expression,
- the body uses an argument in a dotted reference such as `arg.field`,
- the rewritten body contains `$$`, or
- the function sets `[target] catalog`. Redshift creates a function in the connected database, so set only `schema`.

Redshift requires a volatility clause. `deterministic = true` gives `IMMUTABLE`. Otherwise Rocky writes `VOLATILE`. Rocky sets no description on Redshift.

`rocky compile` renders each called function for PostgreSQL and Redshift and reports these refusals as `E051` when a warehouse that a calling model runs on refuses the function. A model runs on the target of each transformation pipeline that loads it.

## How to select a function

Pass the function name to `--model`:

```bash
rocky compile --model cents_to_dollars   # the function's own diagnostics
rocky run --model cents_to_dollars       # create it (and its callees) only
```

`rocky run --model cents_to_dollars` creates the function and builds no
model. A governed apply (`rocky apply`) cannot select a function, because the
plan fingerprint does not cover function DDL.

`rocky test` and `rocky ci` create every valid function as a DuckDB macro
before they run models locally. `rocky compile --output json` lists every valid function under
`functions`, with its `signature` and the models that call it (`called_by`).
The plan preview lists each `CREATE` statement with the purpose
`create_function`, before the model statements.

In VS Code, hover over a function name to see its signature and description.

## Limits

- Scalar SQL functions only. Table functions and Python UDFs are not
  supported.
- `rocky run --shadow` and `--branch` create functions under their declared
  names, not under shadow names. A changed function body therefore replaces
  the function that production models also use.
- Models that call a project function are never skipped by
  `--skip-unchanged` and never reused by `[reuse]`, even with
  `[skip] deterministic = true`. A model's logic hash covers its own SQL,
  not the bodies of the functions it calls.
- `rocky run --dag` runs each model as its own sub-run. Each sub-run creates
  the functions its model needs, so a function can be replaced more than
  once in one run.
- A function body is passed to the warehouse unchanged, except on Redshift
  (see above). Otherwise Rocky parses it only to find calls to other project
  functions. When it cannot parse a body, it
  reports `W051` and does not track calls inside it.
- The governed-apply fingerprint covers models, not functions.
