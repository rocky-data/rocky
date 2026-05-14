CREATE SCHEMA IF NOT EXISTS raw__orders;
CREATE SCHEMA IF NOT EXISTS raw__customers;
CREATE SCHEMA IF NOT EXISTS raw__events;

CREATE OR REPLACE TABLE raw__orders.orders AS
SELECT
    i AS order_id,
    1 + (i % 200) AS customer_id,
    ROUND(CAST(10.0 + random() * 990.0 AS DECIMAL(10,2)), 2) AS amount,
    DATE '2026-01-01' + INTERVAL (i % 120) DAY AS ordered_at
FROM generate_series(1, 10000) AS t(i);

CREATE OR REPLACE TABLE raw__customers.customers AS
SELECT
    i AS customer_id,
    'customer_' || i AS name,
    CASE WHEN i % 3 = 0 THEN 'enterprise' ELSE 'self_serve' END AS segment
FROM generate_series(1, 200) AS t(i);

CREATE OR REPLACE TABLE raw__events.events AS
SELECT
    i AS event_id,
    1 + (i % 200) AS customer_id,
    CASE (i % 4)
        WHEN 0 THEN 'page_view'
        WHEN 1 THEN 'click'
        WHEN 2 THEN 'purchase'
        ELSE 'signup'
    END AS event_type,
    DATE '2026-01-01' + INTERVAL (i % 120) DAY AS occurred_at
FROM generate_series(1, 5000) AS t(i);
