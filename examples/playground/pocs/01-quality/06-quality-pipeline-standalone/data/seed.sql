-- Seed the staging schemas directly; the POC exercises the quality
-- pipeline against pre-populated tables (no data movement).
CREATE SCHEMA IF NOT EXISTS staging__orders;
CREATE SCHEMA IF NOT EXISTS staging__customers;

CREATE OR REPLACE TABLE staging__orders.orders AS
SELECT
    i AS order_id,
    -- Inject 2 NULL customer_ids to exercise the not_null assertion.
    CASE WHEN i IN (7, 42) THEN NULL ELSE 1 + (i % 50) END AS customer_id,
    -- One negative amount to exercise the `amount >= 0` expression assertion.
    CASE WHEN i = 99 THEN -5.0 ELSE ROUND(10 + RANDOM() * 490, 2) END AS amount,
    -- One out-of-list status to exercise the accepted_values assertion.
    CASE
        WHEN i = 13 THEN 'unknown'
        WHEN (i % 4) = 0 THEN 'pending'
        WHEN (i % 4) = 1 THEN 'shipped'
        WHEN (i % 4) = 2 THEN 'delivered'
        ELSE 'cancelled'
    END AS status,
    TIMESTAMP '2026-04-01' + INTERVAL (i * 300) SECOND AS created_at
FROM generate_series(1, 200) AS t(i);

CREATE OR REPLACE TABLE staging__customers.customers AS
SELECT
    i AS customer_id,
    'Customer ' || i AS name,
    'customer' || i || '@example.com' AS email,
    CASE WHEN i % 10 = 0 THEN NULL ELSE 'Region ' || (1 + i % 5) END AS region
FROM generate_series(1, 50) AS t(i);
