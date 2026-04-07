# Declarative Tests

Demonstrates `rocky test --declarative` using `[[tests]]` entries in model TOML sidecars. Instead of writing separate SQL test files, you declare data quality assertions inline next to the model definition.

## Test Types

Rocky supports six declarative test types:

| Type | Purpose | Requires `column` |
|------|---------|-------------------|
| `not_null` | Assert no NULL values in a column | Yes |
| `unique` | Assert no duplicate values in a column | Yes |
| `accepted_values` | Assert column values are from a fixed set | Yes |
| `relationships` | Assert foreign key integrity | Yes |
| `expression` | Assert a custom SQL expression holds for every row | No |
| `row_count_range` | Assert total row count falls within bounds | No |

## Project Structure

```
test-declarative/
  rocky.toml                  # DuckDB adapter
  models/
    fct_orders.sql            # Order fact table (plain SQL)
    fct_orders.toml           # Sidecar with 4 [[tests]] entries
```

## Running

```bash
# Run all declarative tests
rocky --config engine/examples/test-declarative/rocky.toml test \
  --declarative --models engine/examples/test-declarative/models/

# JSON output
rocky --config engine/examples/test-declarative/rocky.toml test \
  --declarative --models engine/examples/test-declarative/models/ \
  --output json

# Filter to a single model
rocky --config engine/examples/test-declarative/rocky.toml test \
  --declarative --models engine/examples/test-declarative/models/ \
  --model fct_orders
```

## Expected Output

```
Declarative tests for fct_orders (warehouse.analytics.fct_orders):
  [PASS] not_null(order_id)
  [PASS] unique(order_id)
  [PASS] accepted_values(status)
  [PASS] expression(amount > 0)

  Result: 4 passed, 0 failed, 0 warned, 0 errored
```

## Test Configuration

Tests are declared as `[[tests]]` TOML array entries in the model sidecar:

```toml
# Assert order_id is never NULL
[[tests]]
type = "not_null"
column = "order_id"

# Assert order_id is unique
[[tests]]
type = "unique"
column = "order_id"

# Assert status is one of the known values (soft failure)
[[tests]]
type = "accepted_values"
column = "status"
values = ["pending", "shipped", "completed", "returned"]
severity = "warning"

# Assert a custom SQL expression holds for every row
[[tests]]
type = "expression"
expression = "amount > 0"
```

## Severity Levels

Each test has an optional `severity` field:

- **`error`** (default) -- test failure causes the command to exit with a non-zero code
- **`warning`** -- test failure is reported but does not fail the command

## How It Works

For each `[[tests]]` entry, Rocky generates assertion SQL against the model's target table:

| Test Type | Generated SQL |
|-----------|--------------|
| `not_null` | `SELECT COUNT(*) FROM <table> WHERE <col> IS NULL` |
| `unique` | `SELECT <col>, COUNT(*) FROM <table> GROUP BY <col> HAVING COUNT(*) > 1` |
| `accepted_values` | `SELECT DISTINCT <col> FROM <table> WHERE <col> NOT IN (...)` |
| `expression` | `SELECT COUNT(*) FROM <table> WHERE NOT (<expr>)` |

A test passes when the query returns 0 rows (or a count of 0).

## Key Concepts

- **Co-located with models** -- tests live in the same `.toml` sidecar as model config
- **No separate test files** -- unlike `rocky test` (which runs `.sql` files from a tests/ directory), declarative tests need no extra files
- **Severity control** -- mark non-critical tests as `warning` to avoid blocking pipelines
- **Generated SQL** -- Rocky generates the assertion SQL from the test type, so you only declare the intent
