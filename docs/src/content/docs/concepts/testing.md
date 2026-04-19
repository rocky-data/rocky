---
title: Testing and Contracts
description: Data contracts, local testing, and CI pipelines
sidebar:
  order: 11
---

Rocky provides compile-time contract validation, local model testing via DuckDB, and a CI pipeline command that combines both. These features catch problems before models reach the warehouse.

## Data contracts

A data contract is a TOML file that declares expectations about a model's output schema. The compiler validates inferred schemas against contracts at compile time, catching issues like missing columns, type mismatches, and nullability violations.

### Contract format

Contracts are stored as `{model_name}.contract.toml` files in a contracts directory:

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

[[columns]]
name = "order_count"
type = "Int64"
nullable = false

[rules]
required = ["customer_id", "total_revenue"]
protected = ["customer_id"]
no_new_nullable = true
```

### Column constraints

Each `[[columns]]` entry can specify:

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Column name |
| `type` | No | Expected Rocky type (`Int64`, `String`, `Decimal`, `Timestamp`, etc.) |
| `nullable` | No | If `false`, the column must be non-nullable |
| `description` | No | Documentation (not validated, for human readers) |

Type names correspond to `RockyType` variants: `Boolean`, `Int32`, `Int64`, `Float32`, `Float64`, `Decimal`, `String`, `Binary`, `Date`, `Timestamp`, `TimestampNtz`, `Array`, `Map`, `Struct`, `Variant`.

### Schema rules

The `[rules]` section enforces schema-level constraints:

| Rule | Description |
|------|-------------|
| `required` | Columns that must exist in the model's output. Missing required columns produce error `E010`. |
| `protected` | Columns that must never be removed. If a protected column disappears from the output, it produces error `E013`. |
| `no_new_nullable` | If `true`, no new nullable columns may be added to the model's output. |

### Diagnostic codes

| Code | Severity | Meaning |
|------|----------|---------|
| `E010` | Error | Required column missing from model output |
| `E011` | Error | Column type mismatch (contract expects one type, model produces another) |
| `E012` | Error | Nullability violation (contract says non-nullable, model says nullable) |
| `E013` | Error | Protected column has been removed |
| `W010` | Warning | Contract defines a column that is not in the model output (but not required) |
| `W011` | Warning | Contract exists for a model that was not found in the project |

When a column has type `Unknown` (the compiler could not infer its type), type checks against contracts pass without error. This avoids false positives when type information is incomplete.

## rocky test

The `rocky test` command compiles models and executes them locally using DuckDB, without requiring a warehouse connection. This provides fast feedback during development.

### How it works

1. **Compile.** All models are compiled through the full pipeline (load, resolve, semantic graph, type check, contracts).
2. **Execute locally.** Each model's SQL is executed against an in-memory DuckDB instance. Models run in topological order so upstream models exist before downstream models reference them.
3. **Validate.** If contracts are present, the output schemas are checked. Compilation diagnostics are also reported.
4. **Report.** Pass/fail results are printed for each model.

```bash
# Run all tests
rocky test --models models/

# Run with contracts
rocky test --models models/ --contracts contracts/

# JSON output for CI systems
rocky test --models models/ --output json
```

### Test output

```
Testing 12 models...

  All 12 models passed

  Result: 12 passed, 0 failed
```

On failure:

```
Testing 12 models...

  x orders_summary -- column 'revenue' type mismatch: expected Decimal, got String
  x customer_ltv -- required column 'customer_id' missing

  Result: 10 passed, 2 failed
```

### JSON output

```json
{
  "version": "1.6.0",
  "command": "test",
  "total": 12,
  "passed": 10,
  "failed": 2,
  "failures": [
    ["orders_summary", "column 'revenue' type mismatch"],
    ["customer_ltv", "required column 'customer_id' missing"]
  ]
}
```

## rocky ci

The `rocky ci` command runs the full CI pipeline: compile + test. It is designed for CI/CD systems and returns a non-zero exit code on failure.

```bash
rocky ci --models models/ --contracts contracts/
```

### Pipeline

1. **Compile** -- Run the full compiler (type checking, contract validation)
2. **Test** -- Execute all models locally via DuckDB

Both phases must pass for the CI pipeline to succeed.

### Output

```
Rocky CI Pipeline

  Compile: PASS (12 models)
  Test:    PASS (12 passed, 0 failed)

  Exit code: 0
```

### Exit codes

| Code | Meaning |
|------|---------|
| 0 | All checks passed |
| 1 | Compilation failed (type errors, contract violations) |
| 2 | Tests failed (models failed to execute locally) |

### JSON output

```json
{
  "version": "1.6.0",
  "command": "ci",
  "compile_ok": true,
  "tests_ok": true,
  "models_compiled": 12,
  "tests_passed": 12,
  "tests_failed": 0,
  "exit_code": 0,
  "diagnostics": [],
  "failures": []
}
```

## AI-generated tests

Rocky can generate test assertions from a model's intent and schema using `rocky ai-test`. See the [AI and Intent](/concepts/ai-intent) page for the full AI workflow.

Each generated assertion is a SQL query that returns 0 rows when the assertion holds:

```sql
-- test: orders_summary_no_null_customer_id
-- description: customer_id must never be NULL
SELECT *
FROM warehouse.silver.orders_summary
WHERE customer_id IS NULL
```

```sql
-- test: orders_summary_positive_revenue
-- description: total_revenue must be non-negative
SELECT *
FROM warehouse.silver.orders_summary
WHERE total_revenue < 0
```

Generated tests cover:

- Not-null constraints on key columns
- Grain uniqueness (no duplicate rows for the primary key)
- Value range expectations (non-negative amounts, valid dates)
- Referential integrity (foreign keys exist in parent tables)

Tests are saved to a `tests/` directory and can be run alongside contract validation.

## Workflow

A typical development workflow combines contracts, testing, and CI:

1. Write a model (SQL or Rocky DSL)
2. Write a contract defining the expected output schema
3. Run `rocky test` locally to verify everything compiles and executes
4. Commit and push -- CI runs `rocky ci` to catch regressions
5. Optionally, run `rocky ai-test --save` to generate additional assertions from intent

Contracts serve as the stable interface between your model and its downstream consumers. If a model change would break a contract, the compiler catches it before anything reaches the warehouse.
