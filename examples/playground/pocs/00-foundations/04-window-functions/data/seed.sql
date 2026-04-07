CREATE SCHEMA IF NOT EXISTS seeds;

CREATE OR REPLACE TABLE seeds.orders AS
SELECT
    i AS order_id,
    1 + (i % 20) AS customer_id,
    ROUND(CAST(10.0 + random() * 200.0 AS DECIMAL(10,2)), 2) AS amount,
    CAST(TIMESTAMP '2025-01-01' + INTERVAL (i * 86400) SECOND AS DATE) AS order_date
FROM generate_series(1, 100) AS t(i);
