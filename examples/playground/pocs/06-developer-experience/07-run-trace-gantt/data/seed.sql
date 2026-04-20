CREATE SCHEMA IF NOT EXISTS raw__orders;
CREATE SCHEMA IF NOT EXISTS raw__customers;

CREATE OR REPLACE TABLE raw__orders.orders AS
SELECT
    i AS order_id,
    1 + (i % 50) AS customer_id,
    ROUND(CAST(10.0 + random() * 990.0 AS DECIMAL(10,2)), 2) AS amount
FROM generate_series(1, 300) AS t(i);

CREATE OR REPLACE TABLE raw__customers.customers AS
SELECT
    i AS customer_id,
    'customer_' || i AS name,
    CASE WHEN i % 3 = 0 THEN 'enterprise' ELSE 'self_serve' END AS segment
FROM generate_series(1, 50) AS t(i);
