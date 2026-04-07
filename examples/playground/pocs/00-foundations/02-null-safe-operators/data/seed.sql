CREATE SCHEMA IF NOT EXISTS seeds;

-- Includes a few rows with status = NULL to expose the != difference.
CREATE OR REPLACE TABLE seeds.orders AS
SELECT
    i AS order_id,
    CASE WHEN i % 10 = 0 THEN NULL
         WHEN i % 7  = 0 THEN 'cancelled'
         ELSE 'completed' END AS status,
    ROUND(CAST(10.0 + random() * 100.0 AS DECIMAL(10,2)), 2) AS amount
FROM generate_series(1, 100) AS t(i);
