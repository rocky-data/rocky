---
title: User-Defined Functions
description: Declare SQL functions in functions/, type-check their calls, and let rocky run create them before the models that use them
sidebar:
  order: 7.4
---

A user-defined function (UDF) is a reusable SQL expression with a name, typed
arguments, and a return type. You declare it once in a `functions/` directory.
Models call it like a built-in function. Rocky checks every call at compile
time and creates the function in the warehouse before any model that calls it.

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

Write types the way the target warehouse spells them: `BIGINT` on DuckDB,
`INT64` on BigQuery, `NUMBER(38,0)` on Snowflake. Rocky does not translate
types, just as it does not translate model SQL. Rocky checks each type
against a character allowlist before it puts the type into DDL.

## How a model calls a function

A model calls the function by name, like any SQL function:

```sql
-- models/fct_orders.sql
SELECT order_id, cents_to_dollars(amount_cents) AS amount_usd
FROM raw.orders
```

When the function declares `[target] schema`, a qualified call must name the
same schema: `util.cents_to_dollars(x)`. Rocky does not treat
`other.cents_to_dollars(x)` as a call to this project's function.

## What the compiler checks

`rocky compile` loads `functions/` with the models. It checks each definition,
then each call:

```
 functions/*.toml + *.sql
          │
          ▼
   validate definition ──── bad name / type, Python, no body,
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

An `E051` is certain from the SQL text and the declared types. Rocky raises
it for a call with the wrong number of arguments. It also raises it when an
argument type has no implicit conversion on any supported warehouse, for
example a `DATE` passed to a `BIGINT` parameter. A model with an `E051` is
not built.

A `W051` is a warning. Rocky raises it when it cannot infer an argument's
type, usually because no source schema is known. It also raises it when the
call depends on the warehouse converting a value, for example a `VARCHAR`
passed to a `BIGINT` parameter. The warehouse may accept that call, so Rocky
does not refuse it.

Rocky checks argument types in the `SELECT` list, including CTEs. It checks
argument counts everywhere in the statement.

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

Some type names mean different widths on different warehouses. `FLOAT` and
`REAL` are 32-bit on DuckDB and Databricks but 64-bit on Snowflake. `INT` and
`INTEGER` are `NUMBER(38,0)` on Snowflake. Rocky reads these names as
`Unknown`, so a contract on such a column reports `I003` (not checked)
instead of a wrong `E011`. Use `DOUBLE`, `BIGINT`, `FLOAT64`, `INT64` or
`DECIMAL(p,s)` when you want the contract checked.

### How lineage traces a call

Column lineage traces a function call to its first column argument. In the
example, `fct_orders.amount_usd` derives from `raw.orders.amount_cents`.

## How `rocky run` creates functions

`rocky run` creates every function that a model in the run calls. It does
this before the first model runs. A function that calls another function is
created after its callee. Models that failed to compile, and models outside
the selection, do not cause their functions to be created.

Each warehouse gets its own statement. Rocky always uses `CREATE OR REPLACE`,
so a second run replaces the function in place.

| Warehouse | Statement |
|-----------|-----------|
| DuckDB | `CREATE OR REPLACE MACRO name(cents) AS CAST((cents / 100.0) AS DOUBLE)` |
| Snowflake | `CREATE OR REPLACE FUNCTION name(cents NUMBER(38,0)) RETURNS FLOAT LANGUAGE SQL [IMMUTABLE \| VOLATILE] [COMMENT = '…'] AS $$ … $$` |
| Databricks | `CREATE OR REPLACE FUNCTION name(cents BIGINT) RETURNS DOUBLE LANGUAGE SQL [[NOT] DETERMINISTIC] [COMMENT '…'] RETURN …` |
| BigQuery | ``CREATE OR REPLACE FUNCTION `project`.`dataset`.`name`(cents INT64) RETURNS FLOAT64 AS (…) [OPTIONS (description = '…')]`` |
| Trino | Refused with `E051` |

DuckDB macros have no return type, so Rocky wraps the body in a `CAST` to the
declared type. That makes the column type in the warehouse match the type
the compiler reports.

BigQuery needs a dataset for a persistent function. Set `[target] schema`.
Rocky refuses a BigQuery function without one. BigQuery SQL functions take no
determinism clause, so Rocky ignores `deterministic` there.

Rocky's Trino adapter cannot create persistent functions. `rocky compile`
reports `E051` when Trino is the only warehouse adapter in `rocky.toml`.
`rocky run` and the plan preview refuse with `E051` on a Trino target.

## How to select a function

Pass the function name to `--model`:

```bash
rocky compile --model cents_to_dollars   # the function's own diagnostics
rocky run --model cents_to_dollars       # create it (and its callees) only
```

`rocky run --model cents_to_dollars` creates the function and builds no
model. `rocky compile --output json` lists every valid function under
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
- A function body is passed to the warehouse unchanged. Rocky parses it only
  to find calls to other project functions. When it cannot parse a body, it
  reports `W051` and does not track calls inside it.
- The governed-apply fingerprint covers models, not functions.
