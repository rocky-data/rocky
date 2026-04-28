-- Synthetic seed data for the preview-workflow POC.
-- Two raw tables (~200 rows each) feed a 5-model transformation DAG.

CREATE SCHEMA IF NOT EXISTS poc;
CREATE SCHEMA IF NOT EXISTS poc.demo;

CREATE OR REPLACE TABLE poc.demo.seed_orders AS
SELECT
    i AS order_id,
    1 + (i % 25) AS customer_id,
    ROUND(CAST(10.0 + ((i * 37) % 9000) / 100.0 AS DECIMAL(10, 2)), 2) AS amount,
    CASE WHEN i % 11 = 0 THEN 'cancelled'
         WHEN i % 17 = 0 THEN 'refunded'
         ELSE 'completed' END AS status,
    DATE '2026-01-01' + ((i % 90) * INTERVAL 1 DAY) AS order_date
FROM generate_series(1, 200) AS t(i);

CREATE OR REPLACE TABLE poc.demo.seed_customers AS
SELECT
    i AS customer_id,
    'customer_' || LPAD(CAST(i AS VARCHAR), 3, '0') AS customer_name,
    CASE WHEN i % 3 = 0 THEN 'enterprise'
         WHEN i % 3 = 1 THEN 'mid_market'
         ELSE 'smb' END AS segment,
    CASE WHEN i % 5 = 0 THEN 'EU' ELSE 'US' END AS region
FROM generate_series(1, 25) AS t(i);
