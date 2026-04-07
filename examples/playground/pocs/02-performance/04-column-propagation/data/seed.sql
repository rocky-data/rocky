CREATE SCHEMA IF NOT EXISTS seeds;

CREATE OR REPLACE TABLE seeds.orders AS
SELECT
    i AS order_id,
    1 + (i % 30) AS customer_id,
    ROUND(CAST(10.0 + random() * 90.0 AS DECIMAL(10,2)), 2) AS amount,
    CASE WHEN i % 13 = 0 THEN 'cancelled' ELSE 'completed' END AS status,
    CAST(TIMESTAMP '2026-01-01' + INTERVAL (i * 3600) SECOND AS DATE) AS order_date
FROM generate_series(1, 100) AS t(i);
