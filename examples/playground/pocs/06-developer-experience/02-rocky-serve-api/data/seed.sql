CREATE SCHEMA IF NOT EXISTS seeds;

CREATE OR REPLACE TABLE seeds.orders AS
SELECT i AS order_id, 1 + (i % 20) AS customer_id, CAST(10.0 + i AS DECIMAL(10,2)) AS amount
FROM generate_series(1, 50) AS t(i);
