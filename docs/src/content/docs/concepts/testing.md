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
    { "name": "orders_summary", "error": "column 'revenue' type mismatch" },
    { "name": "customer_ltv", "error": "required column 'customer_id' missing" }
  ]
}
```

## Unit tests (`[[test]]`)

A unit test feeds a model mocked input rows and asserts on the rows it produces. This is the same approach dbt 1.8 ships as unit tests: you exercise the model's logic in isolation, against fixtures you control, without touching the warehouse. Rocky runs unit tests on the default `rocky test` path via DuckDB, alongside the local model-execution check above.

Unit tests live in a model's `.toml` sidecar as singular `[[test]]` blocks. Each block names the test, declares one or more mocked inputs under `[[test.given]]`, and declares the expected output under `[test.expect]`:

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
    { id = 3, amount = 200.0, status = "cancelled" },
]

[test.expect]
rows = [
    { id = 1, amount = 150.0, is_high_value = true },
    { id = 3, amount = 200.0, is_high_value = true },
]
```

The runner seeds DuckDB with each `[[test.given]]` fixture as a table named after its `ref`, executes the model's compiled SQL against those fixtures, and compares the result to `[test.expect]`.

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Test name, unique within the model. |
| `description` | No | Documentation for human readers. |
| `[[test.given]]` | No | A mocked upstream input. `ref` is the model or source name to stand in for (matches the model's `from` / `depends_on` references); `rows` is an inline list of input rows. Repeat the block to mock more than one input. |
| `[test.expect]` | Yes | The expected output. `rows` is an inline list of expected rows; `ordered` is an optional boolean. |

Comparison rules:

- **Multiset by default.** Rows are compared as a multiset (order does not matter, but duplicate counts do), implemented as `EXCEPT ALL` in both directions so a missing row and an unexpected row are both reported.
- **Ordered comparison.** Set `ordered = true` under `[test.expect]` to compare rows positionally, in the model's output order against the declaration order of the expected rows.
- **Only asserted columns are compared.** The comparison uses the columns present in the expected rows. Extra columns in the model's output are ignored, so you assert on the columns you care about.
- **Empty `rows` asserts zero output.** An empty `[test.expect]` `rows` list asserts that the model produces no rows for the given inputs.

```bash
# Unit tests run automatically on the default test path
rocky test --models models/

# Scope to one model
rocky test --models models/ --model orders_summary
```

When a project declares any `[[test]]` blocks, `rocky test` reports a unit-test summary after the model results, and the `--output json` payload gains a `unit_tests` object:

```json
{
  "version": "1.6.0",
  "command": "test",
  "total": 12,
  "passed": 12,
  "failed": 0,
  "failures": [],
  "unit_tests": {
    "total": 3,
    "passed": 2,
    "failed": 1,
    "results": [
      {
        "model": "orders_summary",
        "test": "high_value_orders",
        "passed": false,
        "error": "output mismatch: 1 expected row(s) missing, 0 unexpected row(s)"
      }
    ]
  }
}
```

A failed unit test also carries a `mismatches` array of row-level diagnostics (each entry naming a missing, extra, or value-differing row), omitted above for brevity. A unit-test failure fails the `rocky test` run with a non-zero exit code, the same as a model-execution failure.

## Declarative tests (`[[tests]]`)

Declarative tests are assertions about the data already in your warehouse: not-null columns, uniqueness, accepted values, referential integrity, row-count ranges, and more. They share the assertion vocabulary of pipeline-level data quality checks. See [Data quality checks](/concepts/data-quality-checks/) for the full catalog of assertion kinds, severity, and quarantine behavior.

Declarative tests use the plural `[[tests]]` array in a model's `.toml` sidecar. Each entry declares a `type`, an optional `column`, an optional `severity`, an optional `filter`, and type-specific parameters:

```toml
# models/orders_summary.toml

[[tests]]
type = "not_null"
column = "customer_id"

[[tests]]
type = "unique"
column = "order_id"

[[tests]]
type = "accepted_values"
column = "status"
values = ["pending", "shipped", "delivered"]
severity = "warning"
```

Run them with `--declarative`. Unlike unit tests, declarative tests execute against the configured warehouse adapter rather than DuckDB, so they need a `rocky.toml` and a reachable warehouse:

```bash
# Run declarative assertions against the warehouse
rocky test --declarative

# Pick a pipeline when the config defines more than one
rocky test --declarative --pipeline silver

# Scope to one model
rocky test --declarative --model orders_summary
```

Each assertion compiles to a SQL query in the adapter's dialect, runs against the model's target table, and reports `pass`, `fail`, or `error`. An assertion with `severity = "error"` (the default) that fails causes a non-zero exit; `severity = "warning"` reports without failing the run. The `--output json` payload carries a `declarative` summary with per-assertion results and the SQL that ran.

### Reusable named tests

To apply the same assertion across many models, define it once in `models/test_definitions.toml` and reference it by name with a `[[use_test]]` block. Inline `[[tests]]` and `[[use_test]]` references coexist in a sidecar, and references resolve into ordinary assertions at load. See the [Reusable named tests](/concepts/data-quality-checks/#reusable-named-tests) section of the data quality checks page for the full syntax.

### `[[test]]` vs `[[tests]]`

The singular and plural keys are two different test mechanisms. The names are close, so keep the distinction in mind:

| | `[[test]]` (singular) | `[[tests]]` (plural) |
|---|---|---|
| What it tests | Model logic against mocked inputs | Data already in the warehouse |
| Inputs | `[[test.given]]` fixtures you supply | The model's real target table |
| Executes against | DuckDB, locally | The configured warehouse adapter |
| How to run | `rocky test` (default path) | `rocky test --declarative` |
| Analogous to | dbt 1.8 unit tests | dbt / DQX data tests and assertions |

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
