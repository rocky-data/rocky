# Docs Demo

Demonstrates `rocky docs` -- generates a self-contained HTML documentation catalog from your models and their `.toml` sidecar metadata.

## Models

```
raw_customers (SQL) ──> stg_customers (Rocky DSL) ──┐
                                                     ├──> fct_customer_orders (Rocky DSL)
raw_orders (SQL) ───────────────────────────────────┘
```

1. **raw_customers** -- Source customer data with profile information
2. **raw_orders** -- Source order data with status and amounts
3. **stg_customers** -- Active customers with derived full name and tenure
4. **fct_customer_orders** -- Per-customer order aggregation joining orders with customer profiles

## Project Structure

```
docs-demo/
  rocky.toml                        # DuckDB backend, local state
  models/
    raw_customers.sql               # Source model (plain SQL)
    raw_customers.toml              # Sidecar: intent, tests
    raw_orders.sql                  # Source model (plain SQL)
    raw_orders.toml                 # Sidecar: intent, tests
    stg_customers.rocky             # Staging model (Rocky DSL)
    stg_customers.toml              # Sidecar: depends_on, intent, tests
    fct_customer_orders.rocky       # Fact model (Rocky DSL)
    fct_customer_orders.toml        # Sidecar: depends_on, intent, tests
```

## Running

```bash
# Generate the HTML documentation catalog
rocky --config engine/examples/docs-demo/rocky.toml docs \
  --models engine/examples/docs-demo/models/

# Open the generated file in your browser
open docs/index.html
```

## What Gets Generated

The generated HTML site includes:

- **Model catalog** -- all models listed with name, intent (description), and strategy
- **Dependency graph** -- visual DAG showing how models connect
- **Model detail pages** -- generated SQL, sidecar config, column list, and test definitions
- **Search** -- full-text search across model names and descriptions
- **Self-contained** -- single HTML file with embedded CSS and JS, no server needed

## Key Sidecar Fields

The `.toml` sidecars drive the documentation content:

| Field | Purpose |
|-------|---------|
| `name` | Model display name |
| `intent` | Description shown in the catalog (maps to the model's documentation) |
| `depends_on` | Upstream model references, used to build the DAG |
| `[strategy]` | Materialization strategy (full_refresh, incremental, etc.) |
| `[target]` | Where the model materializes (catalog, schema, table) |
| `[[tests]]` | Data quality assertions listed on the model detail page |

Richer sidecar metadata produces a more useful documentation catalog.
