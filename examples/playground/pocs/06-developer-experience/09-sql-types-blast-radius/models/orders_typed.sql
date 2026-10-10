-- Typed raw SQL model. With no source schemas, Rocky can't know what columns
-- raw__orders.orders has, so every column resolves to `Unknown`. When the
-- project has data/seed.sql, `rocky compile` loads it into an in-memory
-- DuckDB (no flag needed since engine 1.79.0; `--with-seed` makes the seed
-- required), introspects information_schema, and populates source_schemas.
-- order_id → INTEGER, amount → DECIMAL(10,2), etc.

SELECT
    order_id,
    customer_id,
    amount,
    status
FROM raw__orders.orders
