CREATE SCHEMA IF NOT EXISTS raw__orders;

CREATE OR REPLACE TABLE raw__orders.orders AS
SELECT
    i AS order_id,
    1 + (i % 50) AS customer_id,
    ROUND(CAST(10.0 + random() * 990.0 AS DECIMAL(10,2)), 2) AS amount
FROM generate_series(1, 500) AS t(i);
