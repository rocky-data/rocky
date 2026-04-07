CREATE SCHEMA IF NOT EXISTS raw__orders;

CREATE OR REPLACE TABLE raw__orders.orders AS
SELECT i AS order_id, ROUND(CAST(10.0 + i AS DECIMAL(10,2)), 2) AS amount
FROM generate_series(1, 100) AS t(i);
