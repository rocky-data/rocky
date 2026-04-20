CREATE SCHEMA IF NOT EXISTS raw__orders;

CREATE OR REPLACE TABLE raw__orders.orders AS
SELECT
    i AS order_id,
    1 + (i % 20) AS customer_id,
    ROUND(CAST(10.0 + random() * 90.0 AS DECIMAL(10,2)), 2) AS amount,
    CASE WHEN i % 11 = 0 THEN 'cancelled' ELSE 'completed' END AS status
FROM generate_series(1, 200) AS t(i);
