-- Source schema: raw__orders.orders (8 rows; 2 cancelled, 6 kept by stg_orders)
CREATE SCHEMA IF NOT EXISTS raw__orders;

CREATE TABLE raw__orders.orders (
    order_id    INTEGER,
    customer_id INTEGER,
    amount      DOUBLE,
    status      VARCHAR,
    ordered_at  TIMESTAMP
);

INSERT INTO raw__orders.orders VALUES
    (1, 101,  29.99, 'shipped',   TIMESTAMP '2026-05-01 09:00:00'),
    (2, 102,  49.50, 'shipped',   TIMESTAMP '2026-05-01 10:30:00'),
    (3, 101,  14.25, 'shipped',   TIMESTAMP '2026-05-02 11:45:00'),
    (4, 103, 199.00, 'shipped',   TIMESTAMP '2026-05-02 14:00:00'),
    (5, 102,  79.95, 'cancelled', TIMESTAMP '2026-05-03 08:20:00'),
    (6, 104,   9.99, 'shipped',   TIMESTAMP '2026-05-03 16:10:00'),
    (7, 101,  64.00, 'cancelled', TIMESTAMP '2026-05-04 12:00:00'),
    (8, 103,  39.50, 'shipped',   TIMESTAMP '2026-05-04 17:30:00');
