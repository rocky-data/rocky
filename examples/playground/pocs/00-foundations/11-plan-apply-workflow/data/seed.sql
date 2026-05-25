CREATE SCHEMA IF NOT EXISTS raw__orders;

CREATE OR REPLACE TABLE raw__orders.orders AS
SELECT
    i                       AS order_id,
    (i % 25)                AS customer_id,
    ROUND(i * 1.5, 2)       AS amount,
    CASE WHEN i % 7 = 0 THEN 'cancelled' ELSE 'completed' END AS status
FROM generate_series(1, 200) AS t(i);
