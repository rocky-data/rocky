CREATE SCHEMA IF NOT EXISTS raw__orders;
CREATE SCHEMA IF NOT EXISTS raw__customers;

CREATE OR REPLACE TABLE raw__orders.orders AS
SELECT
    i AS order_id,
    1 + (i % 50) AS customer_id,
    ROUND(10 + RANDOM() * 490, 2) AS amount,
    CASE (i % 4) WHEN 0 THEN 'pending' WHEN 1 THEN 'shipped' WHEN 2 THEN 'delivered' ELSE 'cancelled' END AS status,
    TIMESTAMP '2026-04-01' + INTERVAL (i * 300) SECOND AS created_at
FROM generate_series(1, 200) AS t(i);

CREATE OR REPLACE TABLE raw__customers.customers AS
SELECT
    i AS customer_id,
    'Customer ' || i AS name,
    'customer' || i || '@example.com' AS email,
    CASE WHEN i % 10 = 0 THEN NULL ELSE 'Region ' || (1 + i % 5) END AS region
FROM generate_series(1, 50) AS t(i);
