-- Seed a tiny `orders` source the replication pipeline picks up.
CREATE SCHEMA IF NOT EXISTS raw__orders;

CREATE OR REPLACE TABLE raw__orders.orders AS
SELECT * FROM (VALUES
    (1, 101, 'completed', 49.50,  TIMESTAMP '2026-05-01 10:15:00'),
    (2, 102, 'completed', 119.00, TIMESTAMP '2026-05-01 10:20:00'),
    (3, 103, 'cancelled', 0.00,   TIMESTAMP '2026-05-01 10:30:00'),
    (4, 101, 'completed', 19.99,  TIMESTAMP '2026-05-02 09:05:00'),
    (5, 104, 'pending',   0.00,   TIMESTAMP '2026-05-02 09:42:00')
) AS t(order_id, customer_id, status, amount, ordered_at);
