-- Seed data for `rocky test` (in-memory DuckDB).
-- The persistent `rocky run` flow uses `rocky seed --seeds seeds/` instead.
CREATE SCHEMA IF NOT EXISTS raw__orders;

CREATE OR REPLACE TABLE raw__orders.orders (
    order_id    INTEGER,
    customer_id INTEGER,
    product     VARCHAR,
    amount      DECIMAL(10,2),
    status      VARCHAR,
    ordered_at  DATE
);

INSERT INTO raw__orders.orders VALUES
    (101, 1, 'Widget A', 29.99, 'shipped',   DATE '2025-08-01'),
    (102, 2, 'Widget B', 49.50, 'delivered', DATE '2025-08-02'),
    (103, 1, 'Gadget X', 15.00, 'pending',   DATE '2025-08-03'),
    (104, 3, 'Widget A', 29.99, 'shipped',   DATE '2025-08-04'),
    (105, 5, 'Gadget Y', 89.95, 'delivered', DATE '2025-08-05'),
    (106, 4, 'Widget B', 49.50, 'pending',   DATE '2025-08-06'),
    (107, 6, 'Gadget X', 15.00, 'shipped',   DATE '2025-08-07'),
    (108, 2, 'Widget A', 29.99, 'delivered', DATE '2025-08-08'),
    (109, 7, 'Gadget Y', 89.95, 'pending',   DATE '2025-08-09'),
    (110, 3, 'Widget B', 49.50, 'shipped',   DATE '2025-08-10');
