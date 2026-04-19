# AI Intent

Demonstrates Rocky's AI features: intent-driven development, automatic test generation, and model explanation.

## What is Intent?

The `intent` field in a model's `.toml` sidecar describes what the model should accomplish in plain English. Rocky's AI layer uses this to:

1. **Generate tests** -- `rocky ai-test` reads the intent and produces SQL assertions that verify the model satisfies its business requirements
2. **Explain models** -- `rocky ai-explain` generates a human-readable description of what a model does
3. **Sync models** -- `rocky ai-sync` detects schema changes and proposes intent-guided updates
4. **Generate models** -- `rocky ai "description"` creates a new model from natural language

## Project Structure

```
ai-intent/
  rocky.toml                 # DuckDB adapter + pipeline config
  models/
    stg_orders.rocky           # Staging model with intent
    stg_orders.toml            # intent = "Clean raw orders..."
    fct_daily_revenue.rocky    # Fact model with intent
    fct_daily_revenue.toml     # intent = "Daily revenue metrics..."
  tests/
    test_stg_orders_not_null.sql           # AI-generated test
    test_stg_orders_no_cancelled.sql       # AI-generated test
    test_fct_daily_revenue_positive.sql    # AI-generated test
    test_fct_daily_revenue_unique.sql      # AI-generated test
```

## Workflow

### 1. Write models with intent

Add an `intent` field to your `.toml` sidecar:

```toml
name = "fct_daily_revenue"
intent = "Daily revenue metrics grouped by date. One row per day. Only completed orders."
```

### 2. Generate tests from intent

```bash
# Requires ANTHROPIC_API_KEY environment variable
rocky ai-test --models models/
```

Rocky sends the model source code, column schema, and intent to the LLM. The LLM returns SQL assertions -- queries that return 0 rows when the assertion holds.

### 3. Explain existing models

```bash
rocky ai-explain --models models/ --model stg_orders
```

### 4. Generate a new model from natural language

```bash
rocky ai "Monthly active customers who placed at least one order"
```

### 5. Sync after schema changes

```bash
# Detect upstream schema changes and propose model updates
rocky ai-sync --models models/ --with-intent
```

## Test Format

Each generated test is a SQL query that returns 0 rows when the assertion passes. If any rows are returned, the test fails.

```sql
-- test_stg_orders_not_null.sql
-- Assertion: order_id should never be null
SELECT *
FROM warehouse.staging.stg_orders
WHERE order_id IS NULL
```

## Prerequisites

- Rocky CLI with AI features enabled
- `ANTHROPIC_API_KEY` environment variable set
