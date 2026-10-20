-- Byte-stable raw data: no random(), no now().
CREATE SCHEMA IF NOT EXISTS raw__shop;

CREATE OR REPLACE TABLE raw__shop.customers AS
SELECT i AS customer_id,
       'customer_' || i AS name,
       CASE WHEN i % 2 = 0 THEN 'eu' ELSE 'us' END AS region
FROM generate_series(1, 50) AS t(i);

CREATE OR REPLACE TABLE raw__shop.orders AS
SELECT i AS order_id,
       1 + (i % 50) AS customer_id,
       10 + (i % 7) * 5 AS amount,
       DATE '2026-01-01' + CAST(i % 30 AS INTEGER) AS order_date
FROM generate_series(1, 500) AS t(i);
