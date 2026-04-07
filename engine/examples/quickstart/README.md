# Quickstart

A minimal 3-model pipeline that demonstrates the core Rocky workflow: source, staging, and fact table.

## Models

```
raw_orders (SQL)  -->  stg_orders (Rocky DSL)  -->  fct_revenue (Rocky DSL)
```

1. **raw_orders** -- Reads raw order data from an external source table
2. **stg_orders** -- Filters cancelled orders, renames columns, adds computed fields
3. **fct_revenue** -- Aggregates daily revenue by date

## Project Structure

```
quickstart/
  rocky.toml              # DuckDB backend, local state
  models/
    raw_orders.sql           # Source model (plain SQL)
    raw_orders.toml          # Sidecar config
    stg_orders.rocky         # Staging model (Rocky DSL)
    stg_orders.toml          # Sidecar config
    fct_revenue.rocky        # Fact model (Rocky DSL)
    fct_revenue.toml         # Sidecar config
```

## Running

```bash
# Preview the generated SQL without executing
rocky plan --config rocky.toml

# Execute the pipeline
rocky run --config rocky.toml

# Check state after execution
rocky state --config rocky.toml
```

## Key Concepts

- **`.sql` files** contain plain SQL and coexist with `.rocky` files in the same project
- **`.rocky` files** use Rocky's pipeline DSL where data flows top to bottom
- **`.toml` sidecar files** configure model name, materialization strategy, and target location
- **`!=` in Rocky is NULL-safe** -- `status != "cancelled"` includes rows where status is NULL
