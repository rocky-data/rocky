---
title: The Rocky Compiler
description: Type system, semantic graph, and compile pipeline
sidebar:
  order: 7
---

Rocky includes a full compiler (`rocky-compiler` crate) that performs static analysis on your SQL models before they reach the warehouse. It catches type mismatches, missing columns, contract violations, and broken lineage at compile time rather than at execution time.

## Compile pipeline

The compiler runs five stages in sequence:

```
Load models → Resolve dependencies → Build semantic graph → Type check → Validate contracts
```

### 1. Load models

Model files (`.sql` + `.toml` sidecar) are loaded from the models directory. Each model has a SQL file containing the transformation logic and a TOML file containing configuration (name, target, strategy, intent).

### 2. Resolve dependencies

The resolver parses each model's SQL to extract table references and classifies them:

- **Bare names** matching another model in the project become DAG edges (e.g., `FROM orders` where `orders` is a model)
- **Two-part names** like `schema.table` are treated as external source references
- **Three-part names** like `catalog.schema.table` are treated as fully qualified external references

Explicit `depends_on` entries in the model config are merged with auto-resolved dependencies. Self-references and duplicates are removed.

### 3. Build semantic graph

The semantic graph is a cross-DAG column lineage map. It tracks which source columns flow through which models to which final outputs.

For each model (processed in topological order), the compiler:

- Extracts column-level lineage from the SQL AST
- Resolves table aliases to real model or source names
- Records lineage edges with transform types (Direct, Cast, Expression, Aggregation)
- Expands `SELECT *` by inheriting columns from upstream models or known source schemas

The result is a `SemanticGraph` containing per-model schemas, upstream/downstream relationships, and all cross-model lineage edges.

### 4. Type check

The type checker propagates inferred types through the semantic graph and walks SQL AST expressions to detect issues.

It infers types from:

- `CAST` expressions
- Aggregation functions (`SUM`, `COUNT`, `AVG`, etc.)
- Arithmetic operators (numeric promotion rules)
- Literals (string, numeric, boolean, date)
- `CASE`/`WHEN` branches (common supertype)
- Comparison operators (both sides must be compatible)
- Join keys (must have compatible types)

Each model receives a typed schema: a list of `TypedColumn` entries with name, `RockyType`, and nullability.

### 5. Validate contracts

If a contracts directory exists, `.contract.toml` files are loaded and validated against the inferred schemas. See the [Testing and Contracts](/concepts/testing) page for details on the contract format.

## The type system

`RockyType` is Rocky's unified type representation. All warehouse-specific types map to and from `RockyType` via a `TypeMapper` trait, so the compiler works identically regardless of the target warehouse.

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

`Unknown` is not an error. It means the type could not be inferred from available information. `Unknown` is compatible with any other type during type checking, so it does not produce false positives.

### Numeric promotion

When two numeric types appear in the same expression (arithmetic, `COALESCE`, `CASE`, `UNION`), the compiler computes a common supertype:

- `Int32` widens to `Int64`
- `Float32` widens to `Float64`
- Integer widens to `Float64` when mixed with floats
- Integer widens to `Decimal` when mixed with decimals (precision adjusted)
- `Decimal` pairs take the maximum precision and scale
- `Timestamp` and `TimestampNtz` resolve to `Timestamp`

Incompatible types (e.g., `String` + `Int64`) produce an error diagnostic.

### Assignability

The `is_assignable` function determines whether a value of one type can be written to a column of another type. It allows widening conversions (e.g., `Int32` into `Int64`) but rejects narrowing conversions (e.g., `Int64` into `Int32`).

## Semantic graph

The semantic graph is the foundation for several compiler features:

**Column lineage tracing.** Given any output column in any model, you can trace it backward through the DAG to its ultimate source columns. The `trace_column` method walks lineage edges recursively:

```
c.id → b.id → a.id → source.raw.users.id
```

**Transform tracking.** Each lineage edge records how the column was transformed:

- `Direct` -- column passed through unchanged
- `Cast` -- explicit type cast
- `Expression` -- derived from an expression
- `Aggregation` -- result of an aggregate function

**Star expansion.** When a model uses `SELECT *`, the compiler expands it using the upstream model's inferred schema or known source schemas. This means downstream models always see the full column list, even through star selects.

**Intent propagation.** Each model's `intent` field (from its TOML config) is stored in the semantic graph, making it available for AI-powered features like sync and explain.

## Diagnostics

The compiler produces structured diagnostics with codes, severity levels, source spans, and optional suggestions.

### Severity levels

- **Error** -- compilation cannot proceed. The model has a definite problem.
- **Warning** -- something looks wrong but is not blocking.
- **Info** -- informational, usually about limitations in type inference.

### Diagnostic codes

| Code | Meaning |
|------|---------|
| `E001`--`E009` | Type checking errors (type mismatch, incompatible join keys) |
| `E010` | Required column missing from model output |
| `E011` | Column type mismatch against contract |
| `E012` | Nullability violation against contract |
| `E013` | Protected column removed |
| `W001`--`W009` | Type checking warnings |
| `W010` | Contract column not found in model output |
| `W011` | Contract exists for unknown model |

### Format

Diagnostics render in a format inspired by `rustc`:

```
error[E011]: column 'id' type mismatch: contract expects Int64, got String
 --> models/orders.sql:3:8
 = help: add CAST(id AS BIGINT) to fix the type
```

Each diagnostic includes:
- **code** -- machine-readable identifier for filtering and suppression
- **message** -- human-readable description of the issue
- **span** -- file, line, and column where the issue was found (when available)
- **model** -- which model the diagnostic relates to
- **suggestion** -- actionable fix (when the compiler can determine one)

## Reference tracking

The type checker builds a `ReferenceMap` as a side effect. This map records:

- Where each model is referenced in `FROM` and `JOIN` clauses across the project
- Where each column is referenced
- Where each model is defined

This data powers IDE features like Find References and Rename Symbol when Rocky runs as an LSP server.

## Using the compiler

### CLI

```bash
# Compile all models
rocky compile --models-dir models/

# Compile with contracts
rocky compile --models-dir models/ --contracts-dir contracts/
```

### Programmatic

```rust
use rocky_compiler::compile::{compile, CompilerConfig};

let config = CompilerConfig {
    models_dir: "models/".into(),
    contracts_dir: Some("contracts/".into()),
    source_schemas: HashMap::new(),
    source_column_info: HashMap::new(),
};

let result = compile(&config)?;

if result.has_errors {
    for d in &result.diagnostics {
        eprintln!("{d}");
    }
}
```

The `CompileResult` gives you access to the resolved project, semantic graph, typed schemas, and all diagnostics. Downstream tools (test runner, CI pipeline, AI sync) build on this result.
