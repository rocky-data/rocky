CREATE SCHEMA IF NOT EXISTS seeds;

CREATE OR REPLACE TABLE seeds.orders AS
SELECT
    CAST(i AS BIGINT) AS order_id,
    CASE WHEN i % 2 = 0 THEN 'emea' ELSE 'us_west' END AS region,
    ROUND(CAST(10.0 + (i * 7) % 300 AS DECIMAL(10, 2)), 2) AS amount
FROM generate_series(1, 50) AS t(i);
