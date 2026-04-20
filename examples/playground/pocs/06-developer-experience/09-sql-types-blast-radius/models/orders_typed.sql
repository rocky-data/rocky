-- Typed raw SQL model. Without --with-seed, Rocky can't know what columns
-- raw__orders.orders has, so every column resolves to `Unknown`. With
-- --with-seed, Rocky loads data/seed.sql into an in-memory DuckDB,
-- introspects information_schema, and populates source_schemas.
-- order_id → INTEGER, amount → DECIMAL(10,2), etc.

SELECT
    order_id,
    customer_id,
    amount,
    status
FROM raw__orders.orders
