-- Seed run against an ephemeral in-memory DuckDB by `rocky compile --with-seed`.
-- Rocky introspects information_schema and feeds typed columns into source_schemas
-- so that raw .sql leaf models resolve from Unknown to concrete types.

CREATE SCHEMA IF NOT EXISTS raw__orders;

CREATE OR REPLACE TABLE raw__orders.orders (
    order_id    INTEGER,
    customer_id INTEGER,
    amount      DECIMAL(10, 2),
    status      VARCHAR
);

INSERT INTO raw__orders.orders VALUES
    (1, 10, 99.95,  'completed'),
    (2, 11, 120.00, 'completed'),
    (3, 12, 45.50,  'cancelled');
