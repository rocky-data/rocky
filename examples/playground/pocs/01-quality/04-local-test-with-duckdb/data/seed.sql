CREATE SCHEMA IF NOT EXISTS seeds;

CREATE OR REPLACE TABLE seeds.orders AS
SELECT
    CAST(i AS BIGINT) AS order_id,
    CAST(1 + (i % 30) AS BIGINT) AS customer_id,
    ROUND(CAST(10.0 + random() * 90.0 AS DECIMAL(10,2)), 2) AS amount
FROM generate_series(1, 200) AS t(i);
