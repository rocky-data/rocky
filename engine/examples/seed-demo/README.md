# Seed Demo

Demonstrates `rocky seed` loading CSV files into DuckDB. Seeds are static reference data (lookup tables, dimension data, test fixtures) that Rocky discovers, infers column types for, and loads into the target warehouse.

## How It Works

1. Place `.csv` files in a `seeds/` directory
2. Rocky discovers all CSV files and infers column types from the data
3. For each file, Rocky generates `DROP TABLE IF EXISTS` + `CREATE TABLE` + `INSERT INTO` SQL
4. Tables are created in the `main.seeds` schema by default (configurable via sidecar `.toml`)

## Project Structure

```
seed-demo/
  rocky.toml                # DuckDB adapter + replication pipeline
  seeds/
    customers.csv           # 8 rows: id, name, email, country, created_at
    products.csv            # 7 rows: id, name, price, category
```

## Running

```bash
# Load all seed files into DuckDB
rocky --config engine/examples/seed-demo/rocky.toml seed \
  --seeds-dir engine/examples/seed-demo/seeds/

# Load a specific seed by name
rocky --config engine/examples/seed-demo/rocky.toml seed \
  --seeds-dir engine/examples/seed-demo/seeds/ \
  --filter customers

# JSON output for programmatic consumption
rocky --config engine/examples/seed-demo/rocky.toml seed \
  --seeds-dir engine/examples/seed-demo/seeds/ \
  --output json
```

## Expected Output

```
Seed complete: 2 loaded, 0 failed (XXX ms)
  [OK] customers -> main.seeds.customers (8 rows, 5 cols, XX ms)
  [OK] products -> main.seeds.products (7 rows, 4 cols, XX ms)
```

## Type Inference

Rocky infers column types from CSV data:

| CSV Value | Inferred Type |
|-----------|---------------|
| `1`, `42` | `BIGINT` |
| `29.99` | `DOUBLE` |
| `2024-01-15 09:30:00` | `TIMESTAMP` |
| `alice@example.com` | `VARCHAR` |

You can override inferred types with a `.toml` sidecar next to the CSV:

```toml
# seeds/customers.toml (optional)
[column_types]
id = "INTEGER"
```

## Key Concepts

- **Automatic discovery** -- any `.csv` in the seeds directory is picked up
- **Type inference** -- column types are inferred from data, with sensible defaults
- **Sidecar overrides** -- optional `.toml` files can override target table name, schema, and column types
- **Idempotent** -- re-running seed drops and recreates the table
