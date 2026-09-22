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

- A **bare name** that matches another model in the project becomes a DAG edge. For example, `FROM orders` where `orders` is a model.
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
- Comparison operators (infer a Boolean result; operand checks have limits)
- Join keys (can report compatible-type problems when types are known)

Each compiled model schema contains `TypedColumn` entries with a name, a
`RockyType`, and a nullability flag. A type can remain `Unknown`.

Outer joins can introduce nulls even when source columns are non-nullable.
Rocky marks direct references and ordinary casts of those references from the right side of `LEFT JOIN` as nullable.
`RIGHT JOIN` marks the accumulated left side; `FULL JOIN` marks both sides.
Aliases remain distinct in self-joins, and downstream models inherit the resulting nullability.
A `nullable = false` contract on an affected column with a known type raises `E012`.
This analysis is conservative: a later `WHERE` filter does not narrow nullability.
It does not change the existing nullability inference for casts of computed expressions.

For `USING` and `NATURAL` joins, Rocky distinguishes merged join keys from qualified references to either input.

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

External source schemas do not prove completeness or freshness. Incomplete
scopes, duplicate output names, struct field reads, and warehouse metadata
columns remain conservative. The upstream output must use plain column
projections or aliased columns and literals. Functions and other expressions
remain conservative.

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
| `E037` | A transformation model declares `type = "incremental"`, which would append every row again on each run. Use `merge`, `delete_insert`, `time_interval` or `full_refresh` |
| `E039` | A direct projection names a column absent from a complete in-project upstream model |
| `W001` | Unused model (no downstream consumers) |
| `W002` | Duplicate column in model output |
| `W003` | `time_column` is TIMESTAMP where DATE is preferred for the granularity |
| `W004` | Classification tag with no matching `[mask]` strategy |
| `W005` | Temporal column present but no `freshness` declaration in scope |
| `W006` | `merge` strategy declares a `unique_key` column the model does not output |
| `W010` | Contract defines a column not in model output (not required) |
| `W011` | Contract exists for a model not found in the project |
| `W012` | An `[imports.<name>]` snapshot could not be loaded; `E030`/`E033` checks skipped |
| `W013` | `rocky.toml` is present but could not be read, so every project-level check is silent (`rocky lsp` and `rocky serve` only; one-shot commands refuse instead) |
| `W030` | Imported producer added a column, surfaced only to consumers reading it via `SELECT *` |
| `W031` | Imported producer widened the type of a column this project reads (cross-team contract) |
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
