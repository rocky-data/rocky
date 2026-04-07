CREATE SCHEMA IF NOT EXISTS seeds;

CREATE OR REPLACE TABLE seeds.orders AS
SELECT
    i AS order_id,
    ROUND(CAST(20.0 + random() * 700.0 AS DECIMAL(10,2)), 2) AS amount,
    CAST(TIMESTAMP '2025-04-01' + INTERVAL (i * 86400 / 2) SECOND AS DATE) AS order_date
FROM generate_series(1, 200) AS t(i);
